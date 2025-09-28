from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats,
    ChatMemberUpdated
)
import asyncio
import os
import datetime
import time
import logging
import sys
import threading
import requests
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.operations import ReplaceOne
from pymongo.errors import ConnectionError as PyMongoConnectionError
import json
from datetime import timedelta
import pytz

# Constants for limits and config
MAX_ACTIVE_USERS = 1000
MAX_CONCURRENT_MATCHES = 100
BATCH_UPDATE_INTERVAL = 60  # seconds
MONGODB_RETRY_ATTEMPTS = 3
COOLDOWN_HOURS = 1  # Reduced for testing
PERIODIC_SAVE_INTERVAL = 300  # 5 minutes
PERIODIC_MATCH_INTERVAL = 30  # seconds
CLEANUP_COOLDOWN_INTERVAL = 300  # 5 minutes
NOTIFY_WAITING_INTERVAL = 120  # 2 minutes
CLEANUP_WAITING_INTERVAL = 600  # 10 minutes

# Locks for synchronization
user_data_lock = asyncio.Lock()
active_matches_lock = asyncio.Lock()
waiting_users_lock = asyncio.Lock()
cooldown_tracker_lock = asyncio.Lock()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log")
    ]
)
logger = logging.getLogger(__name__)

# Bot token, channel ID, group ID, and group invite link setup
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')
GROUP_ID = os.getenv('GROUP_ID')
GROUP_INVITE_LINK = os.getenv('GROUP_INVITE_LINK')
MONGODB_URI = os.getenv('MONGODB_URI')
WEBHOOK_PATH = '/webhook'
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', '')
KOYEB_PUBLIC_DOMAIN = os.getenv('KOYEB_PUBLIC_DOMAIN')

# Validation
if not all([BOT_TOKEN, CHANNEL_ID, GROUP_ID, GROUP_INVITE_LINK, MONGODB_URI, KOYEB_PUBLIC_DOMAIN]):
    raise ValueError("Missing required environment variables.")

WEBHOOK_URL = f"https://{KOYEB_PUBLIC_DOMAIN}{WEBHOOK_PATH}"
WEBAPP_HOST = '0.0.0.0'
WEBAPP_PORT = int(os.getenv('PORT', 8080))

bot = Bot(token=BOT_TOKEN)
router = Router()
dp = Dispatcher()
dp.include_router(router)

# MongoDB setup
client = AsyncIOMotorClient(MONGODB_URI)
db = client['bot_database']
users_collection = db['users']

# Note: For scaling, create indexes: db.users.createIndex({"age":1, "gender":1, "religion":1, "partner.min_age":1, "partner.max_age":1, "partner.gender":1, "partner.religion":1})

# Initialize data structures
user_data = {}
active_matches = {}
cooldown_tracker = {}
waiting_users = set()
waiting_start_times = {}  # Unix timestamps
message_id_map = {}
pending_updates = {}
last_batch_time = 0

# Button texts
BEGIN_TEXT = "🚀 Begin"
STOP_SEARCHING_TEXT = "⏹️ Stop Searching"
END_CHAT_TEXT = "🔚 End Chat"

# Data sanitization
def sanitize_data(data):
    """Convert non-serializable fields to MongoDB-compatible formats."""
    if isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            if isinstance(value, datetime.datetime):
                sanitized[key] = value.isoformat()
            elif isinstance(value, dict):
                sanitized[key] = sanitize_data(value)
            elif isinstance(value, list):
                sanitized[key] = [sanitize_data(item) if isinstance(item, (dict, list)) else item for item in value]
            elif value is None:
                sanitized[key] = None
            else:
                sanitized[key] = value
        return sanitized
    elif isinstance(data, list):
        return [sanitize_data(item) for item in data]
    return data

# MongoDB connection check
async def ensure_mongo_connection():
    global client, db, users_collection
    for attempt in range(MONGODB_RETRY_ATTEMPTS):
        try:
            await client.admin.command('ping')
            logger.info("MongoDB connection verified")
            return True
        except PyMongoConnectionError as e:
            logger.error(f"MongoDB connection failed (attempt {attempt + 1}): {e}")
            if attempt < MONGODB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(2 ** attempt)
                client = AsyncIOMotorClient(MONGODB_URI)
                db = client['bot_database']
                users_collection = db['users']
    logger.critical("Failed to connect to MongoDB after retries")
    return False

# Function to get gender emoji
def get_gender_emoji(gender):
    if gender.lower() == "male":
        return "👨"
    elif gender.lower() == "female":
        return "👩"
    else:
        return "❓"

# Function to save all user data to MongoDB with retries
async def save_user_data():
    if not await ensure_mongo_connection():
        logger.error("Skipping save due to MongoDB connection failure")
        return
    async with user_data_lock:
        users_to_save = {uid: sanitize_data(data.copy()) for uid, data in user_data.items()}
    async with active_matches_lock, waiting_users_lock, cooldown_tracker_lock:
        for user_id in list(users_to_save):
            if user_id not in user_data:
                continue
            users_to_save[user_id]["match_partner"] = active_matches.get(user_id)
            users_to_save[user_id]["waiting_since"] = waiting_start_times.get(user_id)
            # Save cooldowns per user
            cooldowns = cooldown_tracker.get(user_id, {})
            if cooldowns:
                users_to_save[user_id]["cooldowns"] = {str(pid): ts.isoformat() if isinstance(ts, datetime.datetime) else str(ts) for pid, ts in cooldowns.items()}
            else:
                users_to_save[user_id]["cooldowns"] = {}
    logger.info(f"Preparing to save {len(users_to_save)} users to MongoDB")
    saved_count = 0
    failed_users = []
    for user_id, data in users_to_save.items():
        for attempt in range(MONGODB_RETRY_ATTEMPTS):
            try:
                await users_collection.replace_one(
                    {'_id': user_id},
                    {'_id': user_id, **data},
                    upsert=True
                )
                saved_count += 1
                logger.debug(f"Saved user {user_id} on attempt {attempt + 1}")
                break
            except Exception as e:
                logger.error(f"Error saving user {user_id} (attempt {attempt + 1}): {e}")
                if attempt == MONGODB_RETRY_ATTEMPTS - 1:
                    failed_users.append(user_id)
                    logger.critical(f"Failed to save user {user_id} after {MONGODB_RETRY_ATTEMPTS} attempts: {e}")
    logger.info(f"Saved {saved_count}/{len(users_to_save)} users successfully")
    if failed_users:
        logger.warning(f"Failed to save {len(failed_users)} users: {failed_users}")

# Function to load user data from MongoDB and rebuild states
async def load_user_data():
    global user_data, active_matches, waiting_users, waiting_start_times, cooldown_tracker
    if not await ensure_mongo_connection():
        logger.error("Skipping load due to MongoDB connection failure")
        return
    user_data = {}
    active_matches = {}
    waiting_users = set()
    waiting_start_times = {}
    cooldown_tracker = {}
    try:
        async for document in users_collection.find():
            user_id = document['_id']
            data = {k: v for k, v in document.items() if k != '_id'}
            # Convert back datetimes
            if "waiting_since" in data and data["waiting_since"]:
                try:
                    if isinstance(data["waiting_since"], str):
                        data["waiting_since"] = float(data["waiting_since"])
                    else:
                        data["waiting_since"] = time.time()  # Fallback
                except ValueError:
                    data["waiting_since"] = time.time()  # Fallback
            if "cooldowns" in data:
                cd = {}
                for pid_str, ts_str in data["cooldowns"].items():
                    pid = int(pid_str)
                    try:
                        if isinstance(ts_str, str):
                            cd[pid] = float(ts_str)
                        else:
                            cd[pid] = time.time() + float(ts_str) if isinstance(ts_str, str) else ts_str
                    except ValueError:
                        cd[pid] = time.time()
                cooldown_tracker[user_id] = cd
                del data["cooldowns"]
            user_data[user_id] = data
            match_partner = data.get("match_partner")
            if match_partner and match_partner in user_data:
                if user_data[match_partner].get("match_partner") == user_id:
                    active_matches[user_id] = match_partner
                    active_matches[match_partner] = user_id
                else:
                    logger.warning(f"Inconsistent match for {user_id}: partner {match_partner} does not match back")
                    data["match_partner"] = None
            waiting_since = data.get("waiting_since")
            if waiting_since:
                waiting_users.add(user_id)
                waiting_start_times[user_id] = waiting_since if isinstance(waiting_since, (int, float)) else time.time()
        # Clean up orphaned matches
        async with active_matches_lock:
            for user_id in list(active_matches):
                partner = active_matches[user_id]
                if partner not in active_matches or active_matches[partner] != user_id:
                    del active_matches[user_id]
                    if partner in active_matches:
                        del active_matches[partner]
                    user_data[user_id]["match_partner"] = None
                    logger.info(f"Cleaned up orphaned match for {user_id}")
        logger.info(f"Loaded {len(user_data)} users from MongoDB")
        logger.info(f"Recovered {len(active_matches) // 2} active matches and {len(waiting_users)} waiting users")
    except Exception as e:
        logger.error(f"Error loading user data: {e}")

# Batch updates
async def batch_update_users():
    global pending_updates, last_batch_time
    now = time.time()
    if not pending_updates or (now - last_batch_time <= BATCH_UPDATE_INTERVAL):
        return
    user_ids = list(pending_updates.keys())
    bulk_ops = []
    async with user_data_lock, active_matches_lock, waiting_users_lock, cooldown_tracker_lock:
        for user_id in user_ids:
            if user_id in user_data:
                user_info = sanitize_data(user_data[user_id].copy())
                user_info["match_partner"] = active_matches.get(user_id)
                user_info["waiting_since"] = waiting_start_times.get(user_id)
                cooldowns = cooldown_tracker.get(user_id, {})
                user_info["cooldowns"] = {str(pid): str(ts) for pid, ts in cooldowns.items()}
                bulk_ops.append(
                    ReplaceOne({'_id': user_id}, {'_id': user_id, **user_info}, upsert=True)
                )
    if bulk_ops:
        try:
            result = await users_collection.bulk_write(bulk_ops)
            logger.info(f"Batched {len(bulk_ops)} updates: modified {result.modified_count}, upserted {result.upserted_count}")
        except Exception as e:
            logger.error(f"Batch update failed: {e}")
            # Fallback individual saves
            for op in bulk_ops:
                user_id = next(iter(op.filter))  # Extract _id from filter
                for attempt in range(MONGODB_RETRY_ATTEMPTS):
                    try:
                        await users_collection.replace_one(
                            op.filter,
                            op.replacement,
                            upsert=True
                        )
                        logger.debug(f"Fallback save for {user_id}")
                        break
                    except Exception as e2:
                        logger.error(f"Fallback failed for {user_id}: {e2}")
        finally:
            pending_updates.clear()
            last_batch_time = now

def queue_user_update(user_id):
    pending_updates[user_id] = True
    asyncio.create_task(batch_update_users())

# Helper function to check if a user is a group member
async def is_group_member(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception as e:
        logger.error(f"Error checking group membership for {user_id}: {e}")
        return False

# Function to send join group message
async def send_join_group_message(message: Message):
    join_button = InlineKeyboardButton(text="Join Group", url=GROUP_INVITE_LINK)
    join_keyboard = InlineKeyboardMarkup(inline_keyboard=[[join_button]])
    await message.answer(
        text="Please join the group to use the bot.",
        reply_markup=join_keyboard
    )

# Helper function to check if setup is complete
def is_setup_complete(user_id):
    if user_id not in user_data:
        return False, ["Age", "Gender", "Religion", "Partner Minimum Age", "Partner Maximum Age", "Partner Gender", "Partner Religion"]
    user_prefs = user_data[user_id]
    missing_fields = []
    if "age" not in user_prefs or user_prefs["age"] == "Not set":
        missing_fields.append("Age")
    if "gender" not in user_prefs or user_prefs["gender"] == "Not set":
        missing_fields.append("Gender")
    if "religion" not in user_prefs or user_prefs["religion"] == "Not set":
        missing_fields.append("Religion")
    if "partner" not in user_prefs:
        missing_fields.extend(["Partner Minimum Age", "Partner Maximum Age", "Partner Gender", "Partner Religion"])
    else:
        partner = user_prefs["partner"]
        if "min_age" not in partner or partner["min_age"] == "Not set":
            missing_fields.append("Partner Minimum Age")
        if "max_age" not in partner or partner["max_age"] == "Not set":
            missing_fields.append("Partner Maximum Age")
        if "gender" not in partner or partner["gender"] == "Not set":
            missing_fields.append("Partner Gender")
        if "religion" not in partner or partner["religion"] == "Not set":
            missing_fields.append("Partner Religion")
    return len(missing_fields) == 0, missing_fields

# Helper function to get user state
def get_user_state(user_id):
    in_waiting = user_id in waiting_users
    in_active = user_id in active_matches
    if in_waiting and in_active:
        logger.warning(f"User {user_id} in both waiting and active. Correcting.")
        waiting_users.discard(user_id)
        return "chatting"
    elif in_active:
        return "chatting"
    elif in_waiting:
        return "searching"
    return "idle"

# Rate limiting
user_action_timestamps = {}
async def rate_limit_check(user_id: int, action: str, limit: int = 5, window: int = 60) -> bool:
    now = time.time()
    user_action_timestamps.setdefault(user_id, {}).setdefault(action, [])
    timestamps = user_action_timestamps[user_id][action]
    timestamps = [t for t in timestamps if now - t < window]
    if len(timestamps) >= limit:
        return False
    timestamps.append(now)
    user_action_timestamps[user_id][action] = timestamps
    return True

# Define keyboards
def get_main_keyboard(state="idle", chat_type="private"):
    if chat_type in ["group", "supergroup"]:
        return None
    if state == "idle":
        action_text = BEGIN_TEXT
    elif state == "searching":
        action_text = STOP_SEARCHING_TEXT
    elif state == "chatting":
        action_text = END_CHAT_TEXT
    else:
        action_text = BEGIN_TEXT
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=action_text), KeyboardButton(text="⚙️ Setup")],
            [KeyboardButton(text="❓ Help")],
        ],
        resize_keyboard=True
    )

def get_setup_inline_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Your Profile", callback_data="your_setup")],
            [InlineKeyboardButton(text="Partner Profile", callback_data="partner_setup")],
            [InlineKeyboardButton(text="Show Profile", callback_data="show_setup")],
        ]
    )

def create_age_keyboard(callback_prefix="selected_age_", back_callback="your_setup", min_age=18, max_age=99, step=5):
    buttons = [
        [InlineKeyboardButton(text=str(age), callback_data=f"{callback_prefix}{age}") for age in range(start, min(start + step, max_age + 1))]
        for start in range(min_age, max_age + 1, step)
    ]
    buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# Set bot commands
async def set_bot_commands():
    commands = [
        BotCommand(command="start", description="Start the bot"),
        BotCommand(command="begin", description="Begin your journey"),
        BotCommand(command="setup", description="Set up your preferences"),
        BotCommand(command="help", description="Get help or assistance"),
        BotCommand(command="end", description="End your session"),
    ]
    await bot.set_my_commands(commands, scope=BotCommandScopeAllPrivateChats())
    await bot.set_my_commands([], scope=BotCommandScopeAllGroupChats())
    logger.info("Bot commands set for private chats only")

# Check if matching allowed
async def can_attempt_match():
    async with waiting_users_lock, active_matches_lock:
        active_users = len(waiting_users) + len(active_matches)
        active_match_count = len(active_matches) // 2
        logger.debug(f"Match limits: users={active_users}/{MAX_ACTIVE_USERS}, matches={active_match_count}/{MAX_CONCURRENT_MATCHES}")
        return active_users < MAX_ACTIVE_USERS and active_match_count < MAX_CONCURRENT_MATCHES

# Start searching
async def start_searching(message: Message, user_id: int):
    is_complete, missing_fields = is_setup_complete(user_id)
    if not is_complete:
        missing_fields_str = "\n- ".join(missing_fields)
        await message.answer(
            f"⚠️ Please complete your setup before starting a match. Missing fields:\n- {missing_fields_str}\nRedirecting to setup menu...",
            reply_markup=get_main_keyboard(state="idle")
        )
        await show_setup_menu(message)
        return False
    if not await rate_limit_check(user_id, "begin"):
        await message.answer("⏳ Please wait a minute before trying again.")
        return False
    async with waiting_users_lock, active_matches_lock:
        active_users = len(waiting_users) + len(active_matches)
        if active_users >= MAX_ACTIVE_USERS:
            await message.answer(
                f"⚠️ The bot has reached the maximum number of active users ({MAX_ACTIVE_USERS}). Please try again later.",
                reply_markup=get_main_keyboard(state="idle")
            )
            return False
        waiting_start_times[user_id] = time.time()
        waiting_users.add(user_id)
    queue_user_update(user_id)
    async with waiting_users_lock:
        queue_position = sum(1 for u in waiting_users if waiting_start_times.get(u, 0) < waiting_start_times[user_id])
    await message.answer(
        f"🔍 Waiting for a partner. You are position {queue_position + 1} in the queue.",
        reply_markup=get_main_keyboard(state="searching")
    )
    if await can_attempt_match():
        await attempt_match(user_id)
    else:
        await message.answer(
            f"⏳ Maximum concurrent matches ({MAX_CONCURRENT_MATCHES}) reached. You will be matched when a slot opens."
        )
    return True

# Enhanced find_match using MongoDB aggregation for scalability
async def find_match(user_id, candidates, user_prefs, user_cooldowns, now):
    wait_time = now - waiting_start_times.get(user_id, now)
    relax_religion = wait_time > 300  # Relax after 5 min
    logger.debug(f"Matching {user_id} with up to {len(candidates)} candidates via MongoDB, wait {wait_time}s, relax_religion={relax_religion}")

    # Filter candidates excluding self and active cooldowns
    valid_candidates = [c for c in candidates if c != user_id and (c not in user_cooldowns or user_cooldowns[c] <= now)]
    if not valid_candidates:
        logger.info(f"No valid candidates for {user_id}")
        return None

    # Build aggregation pipeline for user's preferences on candidates
    user_age = int(user_prefs.get("age", 0))
    user_gender = user_prefs.get("gender", "").lower()
    user_religion = user_prefs.get("religion", "").lower()
    user_partner = user_prefs.get("partner", {})
    user_min_age = int(user_partner.get("min_age", 0))
    user_max_age = int(user_partner.get("max_age", 100))
    user_partner_gender = user_partner.get("gender", "any").lower()
    user_partner_religion = user_partner.get("religion", "any").lower()

    match_filter = {
        "_id": {"$in": valid_candidates},
        "age": {"$gte": user_min_age, "$lte": user_max_age},
        "partner.min_age": {"$lte": user_age},
        "partner.max_age": {"$gte": user_age},
    }

    if user_partner_gender != "any":
        match_filter["gender"] = user_partner_gender

    if user_partner_religion != "any" and not relax_religion:
        match_filter["religion"] = user_partner_religion

    # Add candidate's gender preference filter (candidate accepts user's gender)
    candidate_partner_gender_filter = {"$in": [user_gender, "any"]} if user_partner.get("gender", "any") != "any" else {"$exists": True}
    match_filter["partner.gender"] = candidate_partner_gender_filter

    # Add candidate's religion preference if not relaxed
    if not relax_religion:
        candidate_partner_religion_filter = {"$in": [user_religion, "any"]} if user_partner.get("religion", "any") != "any" else {"$exists": True}
        match_filter["partner.religion"] = candidate_partner_religion_filter

    pipeline = [
        {"$match": match_filter},
        {"$limit": 50},  # Reasonable limit for in-memory final filter
        {"$project": {"_id": 1, "age": 1, "gender": 1, "religion": 1, "partner": 1}}
    ]

    try:
        async for doc in users_collection.aggregate(pipeline):
            candidate_id = doc["_id"]
            candidate_data = doc
            candidate_age = int(candidate_data.get("age", 0))
            candidate_gender = candidate_data.get("gender", "").lower()
            candidate_religion = candidate_data.get("religion", "").lower()
            candidate_partner = candidate_data.get("partner", {})
            candidate_min_age = int(candidate_partner.get("min_age", 0))
            candidate_max_age = int(candidate_partner.get("max_age", 100))
            candidate_partner_gender = candidate_partner.get("gender", "any").lower()
            candidate_partner_religion = candidate_partner.get("religion", "any").lower()

            # Final bidirectional check for religion if relaxed (or always if strict)
            if relax_religion or user_partner_religion == "any":
                religion_match = (
                    (user_partner_religion == "any" or user_partner_religion == candidate_religion) and
                    (candidate_partner_religion == "any" or candidate_partner_religion == user_religion)
                )
            else:
                religion_match = True  # Already filtered

            age_match = candidate_min_age <= user_age <= candidate_max_age
            gender_match = (
                (user_partner_gender == "any" or user_partner_gender == candidate_gender) and
                (candidate_partner_gender == "any" or candidate_partner_gender == user_gender)
            )

            if age_match and gender_match and religion_match:
                logger.debug(f"Match found via aggregation: {user_id} <-> {candidate_id}")
                return candidate_id

        logger.info(f"No match for {user_id}: {len(valid_candidates)} candidates filtered by DB, none passed final check")
        return None
    except Exception as e:
        logger.error(f"Aggregation error for {user_id}: {e}")
        return None

# Attempt match (updated to use async find_match)
async def attempt_match(user_id):
    now = time.time()
    async with waiting_users_lock:
        if user_id not in waiting_users:
            return False
        candidates = list(waiting_users - {user_id})
    async with user_data_lock:
        user_prefs = user_data.get(user_id, {})
    async with cooldown_tracker_lock:
        user_cooldowns = cooldown_tracker.get(user_id, {})
    match_id = await find_match(user_id, candidates, user_prefs, user_cooldowns, now)
    if match_id:
        async with active_matches_lock, waiting_users_lock:
            if user_id in waiting_users and match_id in waiting_users:
                active_match_count = len(active_matches) // 2
                if active_match_count >= MAX_CONCURRENT_MATCHES:
                    logger.warning(f"Cannot match {user_id} with {match_id}: max matches reached")
                    return False
                active_matches[user_id] = match_id
                active_matches[match_id] = user_id
                waiting_users.discard(user_id)
                waiting_users.discard(match_id)
                waiting_start_times.pop(user_id, None)
                waiting_start_times.pop(match_id, None)
        async with user_data_lock:
            user_data_1 = user_data[user_id]
            user_data_2 = user_data[match_id]
        user_1_info = await bot.get_chat(user_id)
        user_2_info = await bot.get_chat(match_id)
        user_1_name = user_1_info.first_name or user_1_info.username or f"User {user_id}"
        user_2_name = user_2_info.first_name or user_2_info.username or f"User {match_id}"
        await bot.send_message(
            chat_id=user_id,
            text=f"🎉 Match found!\n\n👤 Partner's setup:\n📅 Age: {user_data_2.get('age', 'Not set')}\n🚻 Gender: {user_data_2.get('gender', 'Not set')}\n🙏 Religion: {user_data_2.get('religion', 'Not set')}\nYou can start sending messages.",
            reply_markup=get_main_keyboard(state="chatting"),
        )
        await bot.send_message(
            chat_id=match_id,
            text=f"🎉 Match found!\n\n👤 Partner's setup:\n📅 Age: {user_data_1.get('age', 'Not set')}\n🚻 Gender: {user_data_1.get('gender', 'Not set')}\n🙏 Religion: {user_data_1.get('religion', 'Not set')}\nYou can start sending messages.",
            reply_markup=get_main_keyboard(state="chatting"),
        )
        match_time = datetime.datetime.now(pytz.timezone('Africa/Nairobi')).strftime("%Y-%m-%d %H:%M:%S")
        channel_message = (
            f"🤝 **New Match** at {match_time}\n\n"
            f"👤 User 1: {user_1_name} (ID: {user_id})\n"
            f" - Age: {user_data_1.get('age', 'Not set')}\n"
            f" - Gender: {user_data_1.get('gender', 'Not set')}\n"
            f" - Religion: {user_data_1.get('religion', 'Not set')}\n"
            f" - Partner Preferences:\n"
            f"   - Age Range: {user_data_1.get('partner', {}).get('min_age', 'Not set')} to {user_data_1.get('partner', {}).get('max_age', 'Not set')}\n"
            f"   - Gender: {user_data_1.get('partner', {}).get('gender', 'Not set')}\n"
            f"   - Religion: {user_data_1.get('partner', {}).get('religion', 'Not set')}\n\n"
            f"👤 User 2: {user_2_name} (ID: {match_id})\n"
            f" - Age: {user_data_2.get('age', 'Not set')}\n"
            f" - Gender: {user_data_2.get('gender', 'Not set')}\n"
            f" - Religion: {user_data_2.get('religion', 'Not set')}\n"
            f" - Partner Preferences:\n"
            f"   - Age Range: {user_data_2.get('partner', {}).get('min_age', 'Not set')} to {user_data_2.get('partner', {}).get('max_age', 'Not set')}\n"
            f"   - Gender: {user_data_2.get('partner', {}).get('gender', 'Not set')}\n"
            f"   - Religion: {user_data_2.get('partner', {}).get('religion', 'Not set')}"
        )
        try:
            await bot.send_message(chat_id=CHANNEL_ID, text=channel_message, parse_mode='Markdown')
            logger.info(f"Match logged for {user_id} and {match_id}")
        except Exception as e:
            logger.error(f"Error logging match: {e}")
        queue_user_update(user_id)
        queue_user_update(match_id)
        return True
    return False

# Setup menu
async def show_setup_menu(message_or_callback):
    text = "⚙️ Please select your setup options:"
    markup = get_setup_inline_keyboard()
    if isinstance(message_or_callback, Message):
        await message_or_callback.answer(text, reply_markup=markup)
    elif isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(text, reply_markup=markup)
        await message_or_callback.answer()

# Handlers
@router.message(F.chat.type == "private", F.text == "/start")
async def start_command(message: Message):
    user_id = message.from_user.id
    if not await is_group_member(user_id):
        await send_join_group_message(message)
        return
    current_state = get_user_state(user_id)
    welcome_text = "👋 Welcome to our matching bot! Find your perfect match based on your preferences.\n"
    if current_state == "idle":
        welcome_text += "Press 'Setup' to configure your preferences."
    elif current_state == "searching":
        welcome_text += "You are already searching for a partner. Press 'Stop Searching' to cancel."
    elif current_state == "chatting":
        welcome_text += "You are currently in a chat session. Press 'End Chat' to terminate the session."
    await message.answer(
        text=welcome_text,
        reply_markup=get_main_keyboard(state=current_state)
    )
    if current_state == "idle":
        await show_setup_menu(message)

@router.message(F.chat.type == "private", F.text.in_({"⚙️ Setup", "/setup"}))
async def handle_setup(message: Message):
    await show_setup_menu(message)

@router.callback_query(F.data == "your_setup")
async def handle_your_setup(callback: CallbackQuery):
    inline_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Age", callback_data="age")],
            [InlineKeyboardButton(text="Gender", callback_data="gender")],
            [InlineKeyboardButton(text="Religion", callback_data="religion")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="setup")],
        ]
    )
    await callback.message.edit_text(
        text="🔧 You selected 'Your Setup'. Please choose an option below to configure:",
        reply_markup=inline_keyboard
    )
    await callback.answer()

@router.callback_query(F.data == "partner_setup")
async def handle_partner_setup(callback: CallbackQuery):
    partner_setup_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Age", callback_data="partner_age")],
            [InlineKeyboardButton(text="Gender", callback_data="partner_gender")],
            [InlineKeyboardButton(text="Religion", callback_data="partner_religion")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="setup")],
        ]
    )
    await callback.message.edit_text(
        text="🤝 You selected 'Partner Setup'. Configure partner preferences below:",
        reply_markup=partner_setup_keyboard
    )
    await callback.answer()

@router.callback_query(F.data == "setup")
async def handle_back_to_setup(callback: CallbackQuery):
    await show_setup_menu(callback)

@router.callback_query(F.data == "age")
async def handle_age(callback: CallbackQuery):
    age_keyboard = create_age_keyboard()
    await callback.message.edit_text(text="📅 Choose your age:", reply_markup=age_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("selected_age_"))
async def handle_age_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_age = callback.data.split("_")[-1]
    async with user_data_lock:
        user_data.setdefault(user_id, {})["age"] = selected_age
        queue_user_update(user_id)
    await callback.answer(text=f"You are {selected_age} years old.", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_gender(callback)

@router.callback_query(F.data == "gender")
async def handle_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="selected_gender_male")],
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="selected_gender_female")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(text="🚻 Please specify your gender:", reply_markup=gender_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("selected_gender_"))
async def handle_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1].lower()
    async with user_data_lock:
        user_data.setdefault(user_id, {})["gender"] = selected_gender
        queue_user_update(user_id)
    await callback.answer(text=f"You selected {selected_gender.capitalize()}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_religion(callback)

@router.callback_query(F.data == "religion")
async def handle_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox ☦️", callback_data="selected_religion_orthodox")],
            [InlineKeyboardButton(text="Muslim ☪️", callback_data="selected_religion_muslim")],
            [InlineKeyboardButton(text="Protestant ✝️", callback_data="selected_religion_protestant")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(text="🙏 Please select your religion:", reply_markup=religion_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("selected_religion_"))
async def handle_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_religion = callback.data.split("_")[-1].replace("_", " ").title()
    async with user_data_lock:
        user_data.setdefault(user_id, {})["religion"] = selected_religion.lower()  # Normalize
        queue_user_update(user_id)
    selected_age = user_data[user_id].get("age", "Not set")
    selected_gender = user_data[user_id].get("gender", "Not set").title()
    selected_religion = user_data[user_id].get("religion", "Not set").title()
    await callback.message.edit_text(
        f"🎉 Your selections are confirmed:\n- 📅 Age: {selected_age}\n- 🚻 Gender: {selected_gender}\n- 🙏 Religion: {selected_religion}\n\nReturning to setup menu..."
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await asyncio.sleep(3)
    await handle_back_to_setup(callback)

@router.callback_query(F.data == "partner_age")
async def handle_partner_minimum_age(callback: CallbackQuery):
    age_keyboard = create_age_keyboard("partner_min_age_", "partner_setup")
    await callback.message.edit_text(text="📅 Choose the **minimum age** for your partner:", reply_markup=age_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_min_age_"))
async def handle_partner_min_age(callback: CallbackQuery):
    user_id = callback.from_user.id
    min_age = int(callback.data.split("_")[-1])
    async with user_data_lock:
        user_data.setdefault(user_id, {})["partner"] = user_data[user_id].get("partner", {})
        user_data[user_id]["partner"]["min_age"] = min_age
        queue_user_update(user_id)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    max_age_keyboard = create_age_keyboard(f"partner_max_age_", "partner_age", min_age=min_age)
    await callback.message.edit_text(f"📅 Minimum age: **{min_age}**\nChoose the **maximum age**:", reply_markup=max_age_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_max_age_"))
async def handle_partner_max_age(callback: CallbackQuery):
    user_id = callback.from_user.id
    max_age = int(callback.data.split("_")[-1])
    async with user_data_lock:
        if "partner" not in user_data[user_id] or "min_age" not in user_data[user_id]["partner"]:
            await callback.message.edit_text(
                "❌ Minimum age not set. Please select minimum age first.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data="partner_age")]])
            )
            return
        min_age = user_data[user_id]["partner"]["min_age"]
        if max_age < min_age:
            max_age_keyboard = create_age_keyboard(f"partner_max_age_", "partner_age", min_age=min_age)
            await callback.message.edit_text(
                f"❌ Maximum age cannot be less than {min_age}. Choose higher:",
                reply_markup=max_age_keyboard
            )
            return
        user_data[user_id]["partner"]["max_age"] = max_age
        queue_user_update(user_id)
    await callback.answer(text=f"🎉 Partner age range: {min_age} to {max_age}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_gender(callback)

@router.callback_query(F.data == "partner_gender")
async def handle_partner_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="partner_gender_male")],
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="partner_gender_female")],
            [InlineKeyboardButton(text="Any", callback_data="partner_gender_any")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(text="🚻 Please select your partner gender:", reply_markup=gender_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_gender_"))
async def handle_partner_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1].lower()
    async with user_data_lock:
        user_data.setdefault(user_id, {})["partner"] = user_data[user_id].get("partner", {})
        user_data[user_id]["partner"]["gender"] = selected_gender
        queue_user_update(user_id)
    await callback.answer(text=f"🎉 Partner gender: {selected_gender.capitalize()}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_religion(callback)

@router.callback_query(F.data == "partner_religion")
async def handle_partner_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox ☦️", callback_data="partner_religion_orthodox")],
            [InlineKeyboardButton(text="Muslim ☪️", callback_data="partner_religion_muslim")],
            [InlineKeyboardButton(text="Protestant ✝️", callback_data="partner_religion_protestant")],
            [InlineKeyboardButton(text="Any", callback_data="partner_religion_any")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(text="🙏 Please select your partner religion:", reply_markup=religion_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_religion_"))
async def handle_partner_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_religion = callback.data.split("_")[-1].replace("_", " ").title().lower()  # Normalize
    async with user_data_lock:
        user_data.setdefault(user_id, {})["partner"] = user_data[user_id].get("partner", {})
        user_data[user_id]["partner"]["religion"] = selected_religion
        queue_user_update(user_id)
    partner = user_data[user_id]["partner"]
    await callback.message.edit_text(
        f"🎉 Partner preferences confirmed:\n- 📅 Age: {partner.get('min_age', 'Not set')} to {partner.get('max_age', 'Not set')}\n"
        f"- 🚻 Gender: {partner.get('gender', 'Not set').title()}\n- 🙏 Religion: {partner.get('religion', 'Not set').title()}\n\nReturning to setup..."
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await asyncio.sleep(3)
    await handle_back_to_setup(callback)

@router.callback_query(F.data == "show_setup")
async def handle_show_setup(callback: CallbackQuery):
    user_id = callback.from_user.id
    your_age = user_data.get(user_id, {}).get("age", "Not set")
    your_gender = user_data.get(user_id, {}).get("gender", "Not set").title()
    your_religion = user_data.get(user_id, {}).get("religion", "Not set").title()
    partner = user_data.get(user_id, {}).get("partner", {})
    partner_min_age = partner.get("min_age", "Not set")
    partner_max_age = partner.get("max_age", "Not set")
    partner_gender = partner.get("gender", "Not set").title()
    partner_religion = partner.get("religion", "Not set").title()
    result_text = (
        f"👤 Here is your profile:\n"
        f"- 📅 Your Age: {your_age}\n"
        f"- 🚻 Your Gender: {your_gender}\n"
        f"- 🙏 Your Religion: {your_religion}\n\n"
        f"🤝 Partner Preferences:\n"
        f"- 📅 Age Range: {partner_min_age} to {partner_max_age}\n"
        f"- 🚻 Partner Gender: {partner_gender}\n"
        f"- 🙏 Partner Religion: {partner_religion}"
    )
    await callback.message.edit_text(
        text=result_text,
        reply_markup=get_setup_inline_keyboard()
    )
    await callback.answer()

# Matching buttons
@router.message(F.chat.type == "private", F.text.in_({BEGIN_TEXT, "/begin"}))
async def handle_begin(message: Message):
    user_id = message.from_user.id
    current_state = get_user_state(user_id)
    if current_state == "searching":
        await message.answer(
            "🔍 You are already searching. Please wait.",
            reply_markup=get_main_keyboard(state="searching")
        )
    elif current_state != "idle":
        await message.answer(
            "⚠️ Invalid state for begin.",
            reply_markup=get_main_keyboard(state=current_state)
        )
    else:
        if not await is_group_member(user_id):
            await send_join_group_message(message)
            return
        await start_searching(message, user_id)

@router.message(F.chat.type == "private", F.text == STOP_SEARCHING_TEXT)
async def handle_stop_searching(message: Message):
    user_id = message.from_user.id
    current_state = get_user_state(user_id)
    if current_state != "searching":
        await message.answer(
            "⚠️ Not searching.",
            reply_markup=get_main_keyboard(state=current_state)
        )
        return
    async with waiting_users_lock:
        waiting_users.discard(user_id)
        waiting_start_times.pop(user_id, None)
    queue_user_update(user_id)
    await message.answer(
        "🛑 Stopped searching.",
        reply_markup=get_main_keyboard(state="idle")
    )

@router.message(F.chat.type == "private", F.text.in_({END_CHAT_TEXT, "/end"}))
async def handle_end_chat(message: Message):
    user_id = message.from_user.id
    current_state = get_user_state(user_id)
    if current_state != "chatting":
        await message.answer(
            "⚠️ Not chatting.",
            reply_markup=get_main_keyboard(state=current_state)
        )
        return
    now = time.time()
    async with active_matches_lock, cooldown_tracker_lock:
        if user_id in active_matches:
            match_id = active_matches.pop(user_id)
            active_matches.pop(match_id, None)
            cooldown_end = now + (COOLDOWN_HOURS * 3600)
            cooldown_tracker.setdefault(user_id, {})[match_id] = cooldown_end
            cooldown_tracker.setdefault(match_id, {})[user_id] = cooldown_end
            message_id_map.pop(user_id, None)
            message_id_map.pop(match_id, None)
    queue_user_update(user_id)
    queue_user_update(match_id)
    await message.answer(
        f"❌ Session ended. Cooldown: {COOLDOWN_HOURS} hour with this partner.\nPress 'Begin' for new match.",
        reply_markup=get_main_keyboard(state="idle")
    )
    try:
        await bot.send_message(
            chat_id=match_id,
            text="❌ Partner ended session. Press 'Begin' for new match.",
            reply_markup=get_main_keyboard(state="idle")
        )
    except Exception as e:
        logger.error(f"Failed to notify {match_id}: {e}")
    asyncio.create_task(try_match_queued_users())

# Help
@router.message(F.chat.type == "private", F.text.in_({"❓ Help", "/help"}))
async def handle_help(message: Message):
    await message.answer(
        "💡 Help:\n"
        "- 🚀 Begin: Start matching (after setup).\n"
        "- ⏹️ Stop Searching: Cancel search.\n"
        "- 🔚 End Chat: End current chat.\n"
        "- ⚙️ Setup: Configure preferences.\n"
        "- ❓ Help: This menu.\n"
        "- 📩 Feedback: @Ask_and_feedback_bot"
    )

# Forward messages
@router.message(F.chat.type == "private", F.text | F.document | F.photo | F.video | F.audio | F.voice | F.video_note | F.sticker)
async def forward_messages(message: Message):
    user_id = message.from_user.id
    current_state = get_user_state(user_id)
    if current_state != "chatting":
        await message.answer(
            "⚠️ Not chatting. Press 'Begin' to match.",
            reply_markup=get_main_keyboard(state="idle")
        )
        return
    async with active_matches_lock:
        partner_id = active_matches.get(user_id)
    if not partner_id:
        await message.answer(
            "⚠️ No partner. Press 'Begin'.",
            reply_markup=get_main_keyboard(state="idle")
        )
        return
    if not await rate_limit_check(user_id, "message", limit=10, window=60):
        await message.answer("⏳ Slow down messages.")
        return
    message_id_map.setdefault(user_id, {})
    message_id_map.setdefault(partner_id, {})
    sender_gender = user_data.get(user_id, {}).get("gender", "Not set").lower()
    gender_emoji = get_gender_emoji(sender_gender)
    label = f"Partner {gender_emoji}: "
    reply_to_message_id = None
    reply_info = ""
    if message.reply_to_message:
        original_reply_id = message.reply_to_message.message_id
        reply_to_message_id = message_id_map[user_id].get(original_reply_id)
        if reply_to_message_id:
            reply_info = f" (reply to {reply_to_message_id})"
    user_info = await bot.get_chat(user_id)
    sender_name = user_info.first_name or user_info.username or f"User {user_id}"
    message_time = datetime.datetime.now(pytz.timezone('Africa/Nairobi')).strftime("%Y-%m-%d %H:%M:%S")
    channel_message = f"💬 **Message** at {message_time}\n👤 From: {sender_name} (ID: {user_id}) to {partner_id}{reply_info}\n"
    forwarded_message = None
    try:
        if message.text:
            modified_text = label + message.text
            forwarded_message = await bot.send_message(
                chat_id=partner_id,
                text=modified_text,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += f"📜 {message.text}\n"
        elif message.photo:
            caption = message.caption or ""
            modified_caption = label + caption if caption else label
            forwarded_message = await bot.send_photo(
                chat_id=partner_id,
                photo=message.photo[-1].file_id,
                caption=modified_caption,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += f"🖼️ Photo\n"
            if caption:
                channel_message += f"📝 {caption}\n"
        elif message.document:
            caption = message.caption or ""
            modified_caption = label + caption if caption else label
            forwarded_message = await bot.send_document(
                chat_id=partner_id,
                document=message.document.file_id,
                caption=modified_caption,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += f"📎 {message.document.file_name or 'Document'}\n"
        elif message.video:
            caption = message.caption or ""
            modified_caption = label + caption if caption else label
            forwarded_message = await bot.send_video(
                chat_id=partner_id,
                video=message.video.file_id,
                caption=modified_caption,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += "🎥 Video\n"
            if caption:
                channel_message += f"📝 {caption}\n"
        elif message.audio:
            caption = message.caption or ""
            modified_caption = label + caption if caption else label
            forwarded_message = await bot.send_audio(
                chat_id=partner_id,
                audio=message.audio.file_id,
                caption=modified_caption,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += "🎵 Audio\n"
            if caption:
                channel_message += f"📝 {caption}\n"
        elif message.voice:
            caption = message.caption or ""
            modified_caption = label + caption if caption else label
            forwarded_message = await bot.send_voice(
                chat_id=partner_id,
                voice=message.voice.file_id,
                caption=modified_caption,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += "🎙️ Voice\n"
            if caption:
                channel_message += f"📝 {caption}\n"
        elif message.video_note:
            label_msg = await bot.send_message(
                chat_id=partner_id,
                text=label,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            forwarded_message = await bot.send_video_note(
                chat_id=partner_id,
                video_note=message.video_note.file_id,
                reply_to_message_id=label_msg.message_id,
                protect_content=True
            )
            channel_message += "🎥 Video Note\n"
            message_id_map[user_id][message.message_id] = label_msg.message_id
            message_id_map[partner_id][label_msg.message_id] = message.message_id
        elif message.sticker:
            label_msg = await bot.send_message(
                chat_id=partner_id,
                text=label,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            forwarded_message = await bot.send_sticker(
                chat_id=partner_id,
                sticker=message.sticker.file_id,
                reply_to_message_id=label_msg.message_id,
                protect_content=True
            )
            channel_message += "🏷️ Sticker\n"
            message_id_map[user_id][message.message_id] = label_msg.message_id
            message_id_map[partner_id][label_msg.message_id] = message.message_id
        else:
            if forwarded_message and hasattr(forwarded_message, 'message_id') and message.content_type not in ('video_note', 'sticker'):
                message_id_map[user_id][message.message_id] = forwarded_message.message_id
                message_id_map[partner_id][forwarded_message.message_id] = message.message_id
    except Exception as e:
        logger.error(f"Error forwarding from {user_id} to {partner_id}: {e}")
        await message.answer("⚠️ Failed to send. Try again.")
        return
    try:
        await bot.send_message(chat_id=CHANNEL_ID, text=channel_message, parse_mode='Markdown')
        # Forward media to channel
        if message.photo:
            await bot.send_photo(chat_id=CHANNEL_ID, photo=message.photo[-1].file_id, caption=message.caption or "")
        elif message.document:
            await bot.send_document(chat_id=CHANNEL_ID, document=message.document.file_id, caption=message.caption or "")
        elif message.video:
            await bot.send_video(chat_id=CHANNEL_ID, video=message.video.file_id, caption=message.caption or "")
        elif message.audio:
            await bot.send_audio(chat_id=CHANNEL_ID, audio=message.audio.file_id, caption=message.caption or "")
        elif message.voice:
            await bot.send_voice(chat_id=CHANNEL_ID, voice=message.voice.file_id, caption=message.caption or "")
        elif message.video_note:
            await bot.send_video_note(chat_id=CHANNEL_ID, video_note=message.video_note.file_id)
        elif message.sticker:
            await bot.send_sticker(chat_id=CHANNEL_ID, sticker=message.sticker.file_id)
        logger.info(f"Message forwarded {user_id} -> {partner_id}")
    except Exception as e:
        logger.error(f"Error logging message: {e}")

# Group member updates
@router.chat_member(F.chat.id == int(GROUP_ID))
async def handle_chat_member_update(update: ChatMemberUpdated):
    old_status = update.old_chat_member.status
    new_status = update.new_chat_member.status
    user = update.new_chat_member.user
    user_id = user.id
    first_name = user.first_name or f"User {user_id}"
    username = f"@{user.username}" if user.username else ""
    logger.info(f"Member update: {user_id}, {old_status} -> {new_status}")
    if old_status in ['member', 'administrator', 'creator'] and new_status == 'kicked' and not user.is_bot:
        message_text = f"{first_name} {username} is eliminated due to unsupported behaviour.\n{first_name} {username} ተገቢ ባልሆነ ባህሪ ምክንያት ተወግዷል።"
        entities = []  # Add entities if needed
        try:
            await bot.send_message(chat_id=GROUP_ID, text=message_text, entities=entities)
        except Exception as e:
            logger.error(f"Failed to send kick message: {e}")
            await bot.send_message(chat_id=GROUP_ID, text=message_text.replace(username, first_name))
        sticker_id = "CAACAgEAAxkBAAE5E-xok7FWOS3t3jQUWxT3_Yw8QGgkNQACSQQAAmGwwEehsx6rufaXijYE"
        try:
            await bot.send_sticker(chat_id=GROUP_ID, sticker=sticker_id)
        except:
            fallback_sticker = "CAADAgADBAADfyesDlKEqOOd72VKAg"
            await bot.send_sticker(chat_id=GROUP_ID, sticker=fallback_sticker)
        removal_time = datetime.datetime.now(pytz.timezone('Africa/Nairobi')).strftime("%Y-%m-%d %H:%M:%S")
        channel_message = (
            f"🚫 **User Removed** at {removal_time}\n"
            f"👤 User: {first_name} {username} (ID: {user_id})\n"
            f"📝 Reason: Eliminated due to unsupported behaviour\n"
            f"🚫 **ተጠቃሚ ተወግዷል** በ {removal_time}\n"
            f"👤 ተጠቃሚ: {first_name} {username} (መለያ: {user_id})\n"
            f"📝 ምክንያት: በአግባብ ባልሆነ ባህሪ ምክንያት ተወግዷል"
        )
        try:
            await bot.send_message(chat_id=CHANNEL_ID, text=channel_message, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Failed channel log: {e}")
        # Cleanup
        async with active_matches_lock, waiting_users_lock, user_data_lock, cooldown_tracker_lock:
            if user_id in active_matches:
                partner_id = active_matches.pop(user_id)
                active_matches.pop(partner_id, None)
                message_id_map.pop(user_id, None)
                message_id_map.pop(partner_id, None)
                if partner_id:
                    await bot.send_message(
                        chat_id=partner_id,
                        text="❌ Partner removed from group. Press 'Begin' for new match.\n❌ አጋርህ ተወግዷል። 'ጀምር' ይጫኑ።",
                        reply_markup=get_main_keyboard(state="idle")
                    )
                    queue_user_update(partner_id)
            waiting_users.discard(user_id)
            waiting_start_times.pop(user_id, None)
            user_data.pop(user_id, None)
            cooldown_tracker.pop(user_id, None)
            queue_user_update(user_id)
    elif old_status in ['member', 'administrator', 'creator'] and new_status == 'left' and not user.is_bot:
        # Voluntary leave cleanup
        async with active_matches_lock, waiting_users_lock, user_data_lock, cooldown_tracker_lock:
            if user_id in active_matches:
                partner_id = active_matches.pop(user_id)
                active_matches.pop(partner_id, None)
                message_id_map.pop(user_id, None)
                message_id_map.pop(partner_id, None)
                if partner_id:
                    await bot.send_message(
                        chat_id=partner_id,
                        text="❌ Partner left group. Press 'Begin' for new match.\n❌ አጋርህ ቡድን ለቆ ወጥቷል። 'ጀምር' ይጫኑ።",
                        reply_markup=get_main_keyboard(state="idle")
                    )
                    queue_user_update(partner_id)
            waiting_users.discard(user_id)
            waiting_start_times.pop(user_id, None)
            user_data.pop(user_id, None)
            cooldown_tracker.pop(user_id, None)
            queue_user_update(user_id)
        logger.info(f"User {user_id} left voluntarily, data cleaned")

# Periodic tasks
async def try_match_queued_users():
    if not await can_attempt_match():
        logger.warning("Cannot match: limits reached")
        return
    sorted_waiting = sorted(waiting_users, key=lambda x: waiting_start_times.get(x, 0))
    matched_pairs = 0
    for user_id in sorted_waiting:
        if user_id in waiting_users and is_setup_complete(user_id)[0]:
            if await attempt_match(user_id):
                matched_pairs += 1
    if matched_pairs:
        logger.info(f"Matched {matched_pairs} pairs")
    else:
        logger.debug("No new matches this cycle")

async def cleanup_cooldown_tracker():
    while True:
        await asyncio.sleep(CLEANUP_COOLDOWN_INTERVAL)
        now = time.time()
        async with cooldown_tracker_lock:
            for user_id in list(cooldown_tracker):
                for pid in list(cooldown_tracker[user_id]):
                    if cooldown_tracker[user_id][pid] < now:
                        del cooldown_tracker[user_id][pid]
                if not cooldown_tracker[user_id]:
                    del cooldown_tracker[user_id]
        logger.info("Cleaned expired cooldowns")

async def periodic_save():
    while True:
        await asyncio.sleep(PERIODIC_SAVE_INTERVAL)
        await save_user_data()
        logger.info("Periodic save completed")

async def periodic_match_check():
    while True:
        await asyncio.sleep(PERIODIC_MATCH_INTERVAL)
        if waiting_users:
            logger.debug(f"Matching check for {len(waiting_users)} users")
            await try_match_queued_users()

async def cleanup_waiting_users():
    while True:
        await asyncio.sleep(CLEANUP_WAITING_INTERVAL)
        async with waiting_users_lock, user_data_lock:
            for user_id in list(waiting_users):
                is_complete, missing = is_setup_complete(user_id)
                if not is_complete:
                    waiting_users.discard(user_id)
                    waiting_start_times.pop(user_id, None)
                    logger.info(f"Cleaned incomplete user {user_id}: {missing}")
                    try:
                        await bot.send_message(
                            chat_id=user_id,
                            text=f"⚠️ Search canceled: incomplete setup ({', '.join(missing)}). Use /setup.",
                            reply_markup=get_main_keyboard(state="idle")
                        )
                        queue_user_update(user_id)
                    except Exception as e:
                        logger.error(f"Failed notify {user_id}: {e}")

async def notify_waiting_users():
    while True:
        await asyncio.sleep(NOTIFY_WAITING_INTERVAL)
        now = time.time()
        async with waiting_users_lock:
            for user_id in waiting_users:
                wait_time = now - waiting_start_times.get(user_id, now)
                if wait_time > NOTIFY_WAITING_INTERVAL:
                    try:
                        await bot.send_message(
                            chat_id=user_id,
                            text="🔍 Still searching... Press 'Stop Searching' to cancel.",
                            reply_markup=get_main_keyboard(state="searching")
                        )
                        logger.debug(f"Notified waiting user {user_id} (wait: {wait_time}s)")
                    except Exception as e:
                        logger.error(f"Failed notify {user_id}: {e}")

# Async keep-alive
async def keep_alive():
    from aiohttp import ClientSession
    url = f"https://{KOYEB_PUBLIC_DOMAIN}"
    async with ClientSession() as session:
        while True:
            try:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        logger.debug("Keep-alive successful")
            except Exception as e:
                logger.error(f"Keep-alive failed: {e}")
            await asyncio.sleep(300)

# Admin debug commands (replace ADMIN_ID with your Telegram ID)
ADMIN_ID = 123456789  # Set your ID
@router.message(F.text == "/forcesave", F.from_user.id == ADMIN_ID)
async def force_save(message: Message):
    user_id = message.from_user.id
    async with user_data_lock, active_matches_lock, waiting_users_lock, cooldown_tracker_lock:
        data = sanitize_data(user_data.get(user_id, {}).copy())
        data["match_partner"] = active_matches.get(user_id)
        data["waiting_since"] = waiting_start_times.get(user_id)
        cd = cooldown_tracker.get(user_id, {})
        data["cooldowns"] = {str(pid): str(ts) for pid, ts in cd.items()}
    try:
        await users_collection.replace_one({'_id': user_id}, {'_id': user_id, **data}, upsert=True)
        await message.answer("Saved successfully.")
    except Exception as e:
        await message.answer(f"Failed: {e}")

@router.message(F.text == "/resetcooldowns", F.from_user.id == ADMIN_ID)
async def reset_cooldowns(message: Message):
    async with cooldown_tracker_lock:
        cooldown_tracker.clear()
    await save_user_data()
    await message.answer("Cooldowns reset.")

@router.message(F.text == "/status", F.from_user.id == ADMIN_ID)
async def status(message: Message):
    async with waiting_users_lock, active_matches_lock:
        stats = f"Users: {len(user_data)}\nWaiting: {len(waiting_users)}\nMatches: {len(active_matches)//2}"
    await message.answer(stats)

# Main app setup
if __name__ == "__main__":
    app = web.Application()
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=WEBHOOK_SECRET if WEBHOOK_SECRET else None
    )
    webhook_requests_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    async def health(request):
        return web.Response(text="OK")

    app.router.add_get("/", health)

    async def on_startup(_):
        if not await ensure_mongo_connection():
            raise RuntimeError("MongoDB failed")
        await load_user_data()
        await set_bot_commands()
        await bot.set_webhook(url=WEBHOOK_URL, secret_token=WEBHOOK_SECRET if WEBHOOK_SECRET else None, drop_pending_updates=True)
        logger.info(f"Bot started, webhook: {WEBHOOK_URL}")
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_save())
        loop.create_task(periodic_match_check())
        loop.create_task(cleanup_cooldown_tracker())
        loop.create_task(cleanup_waiting_users())
        loop.create_task(notify_waiting_users())
        loop.create_task(keep_alive())

    async def on_shutdown(_):
        await bot.delete_webhook()
        await save_user_data()
        logger.info("Shutdown: webhook deleted, data saved")

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    web.run_app(app, host=WEBAPP_HOST, port=WEBAPP_PORT)
