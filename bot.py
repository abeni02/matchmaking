import asyncio
import os
import datetime
import logging
import re
from typing import Optional
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
    BotCommandScopeAllGroupChats
)
from aiogram.exceptions import TelegramAPIError
from aiogram.utils import markdown
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError, DuplicateKeyError
from pymongo import MongoClient, ReturnDocument
import json
from collections import defaultdict, deque
from uuid import uuid4

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants from environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')
GROUP_ID = os.getenv('GROUP_ID')
GROUP_INVITE_LINK = os.getenv('GROUP_INVITE_LINK')
MONGODB_URI = os.getenv('MONGODB_URI')
MAX_ACTIVE_USERS = int(os.getenv('MAX_ACTIVE_USERS', 600))
MAX_CONCURRENT_MATCHES = int(os.getenv('MAX_CONCURRENT_MATCHES', 300))
COOLDOWN_HOURS = int(os.getenv('COOLDOWN_HOURS', 4))
RATE_LIMIT_SECONDS = int(os.getenv('RATE_LIMIT_SECONDS', 1))
RATE_LIMIT_MAX_REQUESTS = int(os.getenv('RATE_LIMIT_MAX_REQUESTS', 10))
WAITING_TIMEOUT_MINUTES = int(os.getenv('WAITING_TIMEOUT_MINUTES', 10))
NOTIFICATION_DELAY_SECONDS = float(os.getenv('NOTIFICATION_DELAY_SECONDS', 0))

# Global lock for state consistency
global_state_lock = asyncio.Lock()

# Rate limiting storage
rate_limits = defaultdict(lambda: {
    "commands": deque(maxlen=10),
    "messages": deque(maxlen=20)
})

# Circuit breaker for MongoDB
class CircuitBreaker:
    def __init__(self, max_failures=3, reset_timeout=60):
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure = None
        self.is_open = False

    async def execute(self, operation, *args, **kwargs):
        if self.is_open:
            if (datetime.datetime.now() - self.last_failure).total_seconds() > self.reset_timeout:
                self.is_open = False
                self.failures = 0
                logger.info("Circuit breaker reset, attempting operation")
            else:
                logger.error("Circuit breaker open, operation blocked")
                raise Exception("Database unavailable, please try again later")
        try:
            result = await operation(*args, **kwargs)
            self.failures = 0
            return result
        except PyMongoError as e:
            self.failures += 1
            self.last_failure = datetime.datetime.now()
            logger.error(f"MongoDB operation failed: {e}")
            if self.failures >= self.max_failures:
                self.is_open = True
                logger.error(f"Circuit breaker tripped after {self.failures} failures")
            raise

mongo_circuit_breaker = CircuitBreaker()

# Fallback storage for MongoDB downtime
fallback_storage = {}
FALLBACK_FILE = 'fallback_storage.json'

def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def deserialize_datetime(data):
    for user_id, user_info in data.items():
        if user_info.get("waiting_since"):
            data[user_id]["waiting_since"] = datetime.datetime.fromisoformat(user_info["waiting_since"])
    return data

def save_fallback_storage():
    try:
        with open(FALLBACK_FILE, 'w') as f:
            json.dump(fallback_storage, f, default=serialize_datetime)
        logger.info("Fallback storage saved to file")
    except Exception as e:
        logger.error(f"Failed to save fallback storage: {e}")

def load_fallback_storage():
    global fallback_storage
    try:
        if os.path.exists(FALLBACK_FILE):
            with open(FALLBACK_FILE, 'r') as f:
                fallback_storage = deserialize_datetime(json.load(f))
            logger.info("Fallback storage loaded from file")
    except Exception as e:
        logger.error(f"Failed to load fallback storage: {e}")

# Validate environment variables
def test_mongodb_connection(uri: str, max_attempts=3, delay=5):
    for attempt in range(max_attempts):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            client.close()
            logger.info("MongoDB connection test passed")
            return True
        except Exception as e:
            logger.error(f"MongoDB connection test attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                logger.info(f"Retrying in {delay} seconds...")
                asyncio.sleep(delay)
    logger.error("Failed to connect to MongoDB after retries")
    return False

def validate_env_vars():
    if not BOT_TOKEN or not re.match(r'^\d+:[A-Za-z0-9_-]+$', BOT_TOKEN):
        raise ValueError("Invalid or missing BOT_TOKEN")
    if not CHANNEL_ID or not CHANNEL_ID.startswith('-'):
        raise ValueError("Invalid or missing CHANNEL_ID")
    if not GROUP_ID or not GROUP_ID.startswith('-'):
        raise ValueError("Invalid or missing GROUP_ID")
    if not GROUP_INVITE_LINK or not GROUP_INVITE_LINK.startswith('https://t.me/'):
        raise ValueError("Invalid or missing GROUP_INVITE_LINK")
    if not MONGODB_URI or not MONGODB_URI.startswith('mongodb'):
        raise ValueError("Invalid or missing MONGODB_URI")
    if not test_mongodb_connection(MONGODB_URI):
        raise ValueError("Cannot connect to MongoDB with provided URI")
    logger.info("Environment variables validated successfully")

# Set bot commands for private chats only
async def set_bot_commands(bot: Bot):
    commands = [
        BotCommand(command="start", description="Start the bot"),
        BotCommand(command="begin", description="Begin your journey"),
        BotCommand(command="setup", description="Set up your preferences"),
        BotCommand(command="help", description="Get help or assistance"),
        BotCommand(command="end", description="End your session"),
    ]
    await bot.set_my_commands(commands, scope=BotCommandScopeAllPrivateChats())
    await bot.set_my_commands([], scope=BotCommandScopeAllGroupChats())
    logger.info("Bot commands set for private chats and removed from group chats")

# Bot and dispatcher setup
validate_env_vars()
bot = Bot(token=BOT_TOKEN)
router = Router()
dp = Dispatcher()

# MongoDB setup
client = AsyncIOMotorClient(MONGODB_URI)
db = client['bot_database']
users_collection = db['users']
cooldowns_collection = db['cooldowns']
options_collection = db['options']

async def setup_mongodb_indexes():
    await cooldowns_collection.create_index([("expires_at", 1)], expireAfterSeconds=0)
    await users_collection.create_index([("age", 1), ("gender", 1), ("religion", 1), ("waiting_since", 1)])
    logger.info("MongoDB indexes set up")

# Initialize data structures
user_data = {}
active_matches = {}
waiting_users = set()
waiting_start_times = {}
message_id_map = {}

# Configurable options
async def load_options():
    options = await options_collection.find_one({"_id": "config"})
    if not options:
        options = {
            "_id": "config",
            "genders": ["male", "female"],
            "religions": ["orthodox", "muslim", "protestant"]
        }
        await options_collection.insert_one(options)
    return options

options = None

# Button texts
BEGIN_TEXT = "🚀 Begin"
STOP_SEARCHING_TEXT = "⏹️ Stop Searching"
END_CHAT_TEXT = "🔚 End Chat"

# Function to get gender emoji
def get_gender_emoji(gender: str) -> str:
    gender = gender.lower()
    if gender == "male":
        return "👨"
    elif gender == "female":
        return "👩"
    return "❓"

# Save user data to MongoDB or fallback
async def save_user_data():
    async with global_state_lock:
        users_to_save = {uid: data.copy() for uid, data in user_data.items()}
        for user_id in users_to_save:
            users_to_save[user_id]["match_partner"] = active_matches.get(user_id)
            users_to_save[user_id]["waiting_since"] = waiting_start_times.get(user_id) if user_id in waiting_users else None
        try:
            for user_id, data in users_to_save.items():
                await mongo_circuit_breaker.execute(
                    users_collection.replace_one,
                    {'_id': user_id},
                    {'_id': user_id, **data},
                    upsert=True
                )
            logger.info("All user data saved to MongoDB")
        except Exception as e:
            logger.error(f"Failed to save user data to MongoDB: {e}")
            fallback_storage.update(users_to_save)
            save_fallback_storage()

# Load user data from MongoDB or fallback
async def load_user_data():
    global user_data, active_matches, waiting_users, waiting_start_times
    load_fallback_storage()
    try:
        async for document in users_collection.find():
            user_id = document['_id']
            user_data[user_id] = {k: v for k, v in document.items() if k != '_id'}
            match_partner = document.get("match_partner")
            if match_partner and match_partner in user_data:
                if user_data[match_partner].get("match_partner") == user_id:
                    active_matches[user_id] = match_partner
                    active_matches[match_partner] = user_id
                else:
                    user_data[user_id]["match_partner"] = None
            waiting_since = document.get("waiting_since")
            if waiting_since:
                waiting_users.add(user_id)
                waiting_start_times[user_id] = waiting_since
        for user_id in list(active_matches.keys()):
            if active_matches.get(active_matches[user_id]) != user_id:
                del active_matches[user_id]
                del active_matches[active_matches[user_id]]
                user_data[user_id]["match_partner"] = None
        if fallback_storage:
            for user_id, data in fallback_storage.items():
                await users_collection.replace_one(
                    {'_id': user_id},
                    {'_id': user_id, **data},
                    upsert=True
                )
            fallback_storage.clear()
            os.remove(FALLBACK_FILE) if os.path.exists(FALLBACK_FILE) else None
            logger.info("Fallback storage synced to MongoDB")
        logger.info(f"Loaded data for {len(user_data)} users, {len(active_matches) // 2} matches, {len(waiting_users)} waiting users")
    except Exception as e:
        logger.error(f"Error loading user data from MongoDB: {e}")

# Update single user data
async def update_user_data(user_id: int):
    async with global_state_lock:
        if user_id not in user_data:
            logger.warning(f"Attempted to update non-existent user {user_id}")
            return
        user_data[user_id]["last_active"] = datetime.datetime.now()
        user_info = user_data[user_id].copy()
        user_info["match_partner"] = active_matches.get(user_id)
        user_info["waiting_since"] = waiting_start_times.get(user_id) if user_id in waiting_users else None
        state = await get_user_state(user_id)
        try:
            await mongo_circuit_breaker.execute(
                users_collection.replace_one,
                {'_id': user_id},
                {'_id': user_id, **user_info},
                upsert=True
            )
            logger.info(f"Updated user {user_id} in MongoDB, state: {state}, waiting: {user_id in waiting_users}, matched: {user_id in active_matches}")
        except Exception as e:
            logger.error(f"Failed to update user {user_id}, state: {state}: {e}")
            fallback_storage[user_id] = user_info
            save_fallback_storage()

def update_user_data_now(user_id: int):
    asyncio.create_task(update_user_data(user_id))

# Check group membership
async def is_group_member(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception as e:
        logger.error(f"Error checking group membership for user {user_id}: {e}")
        return False

# Send join group message
async def send_join_group_message(message: Message):
    join_button = InlineKeyboardButton(text="Join Group", url=GROUP_INVITE_LINK)
    join_keyboard = InlineKeyboardMarkup(inline_keyboard=[[join_button]])
    await message.answer(
        text="Please join the group to use the bot.",
        reply_markup=join_keyboard
    )

# Validate user setup
def is_setup_complete(user_id: int) -> tuple[bool, list[str]]:
    if user_id not in user_data:
        return False, ["Age", "Gender", "Religion", "Partner Minimum Age", "Partner Maximum Age", "Partner Gender", "Partner Religion"]
    user_prefs = user_data[user_id]
    missing_fields = []
    try:
        if "age" not in user_prefs or not isinstance(int(user_prefs["age"]), int) or int(user_prefs["age"]) < 18 or int(user_prefs["age"]) > 100:
            missing_fields.append("Age")
        if "gender" not in user_prefs or user_prefs["gender"].lower() not in options["genders"]:
            missing_fields.append("Gender")
        if "religion" not in user_prefs or user_prefs["religion"].lower() not in options["religions"]:
            missing_fields.append("Religion")
        if "partner" not in user_prefs:
            missing_fields.extend(["Partner Minimum Age", "Partner Maximum Age", "Partner Gender", "Partner Religion"])
        else:
            if "min_age" not in user_prefs["partner"] or not isinstance(int(user_prefs["partner"]["min_age"]), int) or int(user_prefs["partner"]["min_age"]) < 18:
                missing_fields.append("Partner Minimum Age")
            if "max_age" not in user_prefs["partner"] or not isinstance(int(user_prefs["partner"]["max_age"]), int) or int(user_prefs["partner"]["max_age"]) > 100:
                missing_fields.append("Partner Maximum Age")
            if "min_age" in user_prefs["partner"] and "max_age" in user_prefs["partner"] and int(user_prefs["partner"]["min_age"]) > int(user_prefs["partner"]["max_age"]):
                missing_fields.append("Partner Age Range (min > max)")
            if "gender" not in user_prefs["partner"] or user_prefs["partner"]["gender"].lower() not in options["genders"] + ["any"]:
                missing_fields.append("Partner Gender")
            if "religion" not in user_prefs["partner"] or user_prefs["partner"]["religion"].lower() not in options["religions"] + ["any"]:
                missing_fields.append("Partner Religion")
    except ValueError:
        missing_fields.append("Invalid numeric field")
    return len(missing_fields) == 0, missing_fields

# Get user state
async def get_user_state(user_id: int) -> str:
    async with global_state_lock:
        in_waiting = user_id in waiting_users
        in_active = user_id in active_matches
        if in_waiting and in_active:
            logger.warning(f"User {user_id} in both waiting and active states, correcting")
            waiting_users.discard(user_id)
            waiting_start_times.pop(user_id, None)
            return "chatting"
        elif in_active:
            return "chatting"
        elif in_waiting:
            return "searching"
        return "idle"

# Main keyboard
def get_main_keyboard(state: str = "idle", chat_type: str = "private") -> Optional[ReplyKeyboardMarkup]:
    if chat_type in ["group", "supergroup"]:
        return None
    action_text = {
        "idle": BEGIN_TEXT,
        "searching": STOP_SEARCHING_TEXT,
        "chatting": END_CHAT_TEXT
    }.get(state, BEGIN_TEXT)
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=action_text), KeyboardButton(text="⚙️ Setup")],
            [KeyboardButton(text="❓ Help")],
        ],
        resize_keyboard=True
    )

# Setup inline keyboard
def get_setup_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Your Profile", callback_data="your_setup")],
            [InlineKeyboardButton(text="Partner Profile", callback_data="partner_setup")],
            [InlineKeyboardButton(text="Show Profile", callback_data="show_setup")],
        ]
    )

# Rate limiting middleware
async def rate_limit_middleware(handler, event, data):
    user_id = event.from_user.id if hasattr(event, 'from_user') else None
    if not user_id:
        return await handler(event, data)
    now = datetime.datetime.now()
    user_limit = rate_limits[user_id]
    is_command = isinstance(event, Message) and event.text.startswith('/')
    is_callback = isinstance(event, CallbackQuery)
    limit_key = "commands" if is_command or is_callback else "messages"
    max_requests = 10 if limit_key == "commands" else 20
    window_seconds = 5 if limit_key == "commands" else 10
    queue = user_limit[limit_key]
    while queue and (now - queue[0]).total_seconds() > window_seconds:
        queue.popleft()
    queue.append(now)
    if len(queue) > max_requests:
        await event.answer(f"⏳ Too many {limit_key}. Please wait a few seconds.", show_alert=True)
        return
    return await handler(event, data)

router.message.middleware(rate_limit_middleware)
router.callback_query.middleware(rate_limit_middleware)

# Start command
@router.message(F.chat.type == "private", F.text == "/start")
async def start_command(message: Message):
    user_id = message.from_user.id
    if not await is_group_member(user_id):
        await send_join_group_message(message)
        return
    current_state = await get_user_state(user_id)
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

# Setup handler
@router.message(F.chat.type == "private", F.text.in_({"⚙️ Setup", "/setup"}))
async def handle_setup(message: Message):
    await show_setup_menu(message)

async def show_setup_menu(message_or_callback: Message | CallbackQuery):
    text = "⚙️ Please select your setup options:"
    reply_markup = get_setup_inline_keyboard()
    if isinstance(message_or_callback, Message):
        await message_or_callback.answer(text=text, reply_markup=reply_markup)
    else:
        await message_or_callback.message.edit_text(text=text, reply_markup=reply_markup)
        await message_or_callback.answer()

# Your setup handler
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

# Partner setup handler
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

# Back to setup
@router.callback_query(F.data == "setup")
async def handle_back_to_setup(callback: CallbackQuery):
    await show_setup_menu(callback)

# Check matching limits
async def can_attempt_match() -> bool:
    async with global_state_lock:
        active_users = len(waiting_users) + len(active_matches)
        active_match_count = len(active_matches) // 2
        return active_users < MAX_ACTIVE_USERS and active_match_count < MAX_CONCURRENT_MATCHES

# Start searching
async def start_searching(message: Message, user_id: int) -> bool:
    async with global_state_lock:
        is_complete, missing_fields = is_setup_complete(user_id)
        if not is_complete:
            missing_fields_str = "\n- ".join(missing_fields)
            await message.answer(
                text=f"⚠️ Please complete your setup before starting a match. Missing fields:\n- {missing_fields_str}\nRedirecting to setup menu...",
                reply_markup=get_main_keyboard(state="idle")
            )
            await show_setup_menu(message)
            return False
        active_users = len(waiting_users) + len(active_matches)
        if active_users >= MAX_ACTIVE_USERS:
            await message.answer(
                text="⚠️ The bot has reached the maximum number of active users. Please try again later.",
                reply_markup=get_main_keyboard(state="idle")
            )
            return False
        waiting_start_times[user_id] = datetime.datetime.now()
        waiting_users.add(user_id)
        update_user_data_now(user_id)
    await message.answer(
        text="🔍 Waiting for a partner. You will be matched when a suitable partner is found.",
        reply_markup=get_main_keyboard(state="searching")
    )
    if await can_attempt_match():
        await attempt_match(user_id)
    else:
        await message.answer(
            text="⏳ The current number of active matches has reached the maximum. You will be matched when a slot becomes available."
        )
    return True

# Attempt match using MongoDB query
async def attempt_match(user_id: int) -> bool:
    try:
        async with global_state_lock:
            if user_id not in waiting_users or not user_data.get(user_id):
                return False
            user_prefs = user_data[user_id]
            now = datetime.datetime.now()
            user_age = int(user_prefs["age"])
            user_gender = user_prefs["gender"].lower()
            user_religion = user_prefs["religion"].lower()
            user_partner_prefs = user_prefs.get("partner", {})
            user_min_age = int(user_partner_prefs.get("min_age", 18))
            user_max_age = int(user_partner_prefs.get("max_age", 100))
            user_partner_gender = user_partner_prefs.get("gender", "any").lower()
            user_partner_religion = user_partner_prefs.get("religion", "any").lower()

            pipeline = [
                {"$match": {
                    "_id": {"$ne": user_id},
                    "waiting_since": {"$ne": None},
                    "age": {"$gte": user_min_age, "$lte": user_max_age},
                    "gender": user_partner_gender if user_partner_gender != "any" else {"$in": options["genders"]},
                    "religion": user_partner_religion if user_partner_religion != "any" else {"$in": options["religions"]},
                    "partner.min_age": {"$lte": user_age},
                    "partner.max_age": {"$gte": user_age},
                    "partner.gender": {"$in": [user_gender, "any"]},
                    "partner.religion": {"$in": [user_religion, "any"]}
                }},
                {"$lookup": {
                    "from": "cooldowns",
                    "let": {"candidate_id": "$_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$user_id", user_id]},
                                    {"$eq": ["$partner_id", "$$candidate_id"]},
                                    {"$gt": ["$expires_at", now]}
                                ]
                            }
                        }}
                    ],
                    "as": "cooldown"
                }},
                {"$match": {"cooldown": []}},
                {"$sort": {"waiting_since": 1}},
                {"$limit": 1}
            ]
            candidate = await users_collection.aggregate(pipeline).to_list(1)
            if not candidate:
                return False
            candidate = candidate[0]
            candidate_id = candidate["_id"]
            if len(active_matches) // 2 >= MAX_CONCURRENT_MATCHES:
                logger.warning(f"Cannot match {user_id} with {candidate_id}: max matches reached")
                return False
            active_matches[user_id] = candidate_id
            active_matches[candidate_id] = user_id
            waiting_users.discard(user_id)
            waiting_users.discard(candidate_id)
            waiting_start_times.pop(user_id, None)
            waiting_start_times.pop(candidate_id, None)
            await cooldowns_collection.insert_one({
                "user_id": user_id,
                "partner_id": candidate_id,
                "expires_at": now + datetime.timedelta(hours=COOLDOWN_HOURS)
            })
            await cooldowns_collection.insert_one({
                "user_id": candidate_id,
                "partner_id": user_id,
                "expires_at": now + datetime.timedelta(hours=COOLDOWN_HOURS)
            })
            user_1_info = await bot.get_chat(user_id)
            user_2_info = await bot.get_chat(candidate_id)
            user_1_name = user_1_info.first_name or user_1_info.username or f"User {user_id}"
            user_2_name = user_2_info.first_name or user_2_info.username or f"User {candidate_id}"
            await bot.send_message(
                chat_id=user_id,
                text=(
                    f"🎉 Match found!\n\n"
                    f"👤 Partner's setup:\n"
                    f"📅 Age: {candidate['age']}\n"
                    f"🚻 Gender: {candidate['gender']}\n"
                    f"🙏 Religion: {candidate['religion']}\n"
                    "You can start sending messages."
                ),
                reply_markup=get_main_keyboard(state="chatting")
            )
            await bot.send_message(
                chat_id=candidate_id,
                text=(
                    f"🎉 Match found!\n\n"
                    f"👤 Partner's setup:\n"
                    f"📅 Age: {user_prefs['age']}\n"
                    f"🚻 Gender: {user_prefs['gender']}\n"
                    f"🙏 Religion: {user_prefs['religion']}\n"
                    "You can start sending messages."
                ),
                reply_markup=get_main_keyboard(state="chatting")
            )
            match_time = now.strftime("%Y-%m-%d %H:%M:%S")
            channel_message = (
                f"🤝 **New Match** at {match_time}\n\n"
                f"👤 User 1: {user_1_name} (ID: {user_id})\n"
                f"  - Age: {user_prefs['age']}\n"
                f"  - Gender: {user_prefs['gender']}\n"
                f"  - Religion: {user_prefs['religion']}\n"
                f"  - Partner Preferences:\n"
                f"    - Age Range: {user_min_age} to {user_max_age}\n"
                f"    - Gender: {user_partner_gender}\n"
                f"    - Religion: {user_partner_religion}\n\n"
                f"👤 User 2: {user_2_name} (ID: {candidate_id})\n"
                f"  - Age: {candidate['age']}\n"
                f"  - Gender: {candidate['gender']}\n"
                f"  - Religion: {candidate['religion']}\n"
                f"  - Partner Preferences:\n"
                f"    - Age Range: {candidate.get('partner', {}).get('min_age', 18)} to {candidate.get('partner', {}).get('max_age', 100)}\n"
                f"    - Gender: {candidate.get('partner', {}).get('gender', 'any')}\n"
                f"    - Religion: {candidate.get('partner', {}).get('religion', 'any')}"
            )
            try:
                await bot.send_message(chat_id=CHANNEL_ID, text=channel_message)
                logger.info(f"Match logged to channel {CHANNEL_ID} for users {user_id} and {candidate_id}")
            except Exception as e:
                logger.error(f"Error logging match to channel {CHANNEL_ID}: {e}")
            update_user_data_now(user_id)
            update_user_data_now(candidate_id)
            return True
    except Exception as e:
        logger.error(f"Error attempting match for user {user_id}: {e}")
    return False

# Matching buttons handler
@router.message(F.chat.type == "private", F.text.in_({BEGIN_TEXT, STOP_SEARCHING_TEXT, END_CHAT_TEXT, "/begin", "/end"}))
async def handle_matching_button(message: Message):
    user_id = message.from_user.id
    text = message.text
    current_state = await get_user_state(user_id)
    async with global_state_lock:
        if text in [BEGIN_TEXT, "/begin"]:
            if current_state == "searching":
                await message.answer(
                    text="🔍 You are already searching for a partner. Please wait.",
                    reply_markup=get_main_keyboard(state="searching")
                )
            elif current_state != "idle":
                await message.answer(
                    text="⚠️ Invalid operation for current state.",
                    reply_markup=get_main_keyboard(state=current_state)
                )
            else:
                if not await is_group_member(user_id):
                    await send_join_group_message(message)
                    return
                await start_searching(message, user_id)
        elif text == STOP_SEARCHING_TEXT:
            if current_state != "searching":
                await message.answer(
                    text="⚠️ Invalid operation for current state.",
                    reply_markup=get_main_keyboard(state=current_state)
                )
                return
            waiting_users.discard(user_id)
            waiting_start_times.pop(user_id, None)
            update_user_data_now(user_id)
            await message.answer(
                text="🛑 You have stopped searching.",
                reply_markup=get_main_keyboard(state="idle")
            )
        elif text == END_CHAT_TEXT or text == "/end":
            if current_state != "chatting":
                await message.answer(
                    text="⚠️ Invalid operation for current state.",
                    reply_markup=get_main_keyboard(state=current_state)
                )
                return
            match_id = active_matches.pop(user_id, None)
            if match_id:
                active_matches.pop(match_id, None)
                message_id_map.pop(user_id, None)
                message_id_map.pop(match_id, None)
                now = datetime.datetime.now()
                await cooldowns_collection.insert_one({
                    "user_id": user_id,
                    "partner_id": match_id,
                    "expires_at": now + datetime.timedelta(hours=COOLDOWN_HOURS)
                })
                await cooldowns_collection.insert_one({
                    "user_id": match_id,
                    "partner_id": user_id,
                    "expires_at": now + datetime.timedelta(hours=COOLDOWN_HOURS)
                })
                update_user_data_now(user_id)
                update_user_data_now(match_id)
                await message.answer(
                    text="❌ You have ended the session. You can press 'Begin' again to find a new partner.",
                    reply_markup=get_main_keyboard(state="idle")
                )
                try:
                    await bot.send_message(
                        chat_id=match_id,
                        text="❌ Your partner has ended the session. You can press 'Begin' again to find a new partner.",
                        reply_markup=get_main_keyboard(state="idle")
                    )
                except TelegramAPIError as e:
                    logger.error(f"Failed to notify user {match_id}: {e}")
                asyncio.create_task(try_match_queued_users())

# Help handler
@router.message(F.chat.type == "private", F.text.in_({"❓ Help", "/help"}))
async def handle_help(message: Message):
    await message.answer(
        text=(
            "💡 Need help? Here's what you can do:\n"
            " - 🚀 Begin: Start your journey (after completing setup).\n"
            " - ⏹️ Stop Searching: Stop looking for a partner.\n"
            " - 🔚 End Chat: Stop chatting with your partner.\n"
            " - ⚙️ Setup: Configure your preferences.\n"
            " - ❓ Help: Get guidance and information.\n"
            " - 📩 Questions or feedback: @Ask_and_feedback_bot ."
        )
    )

# Forward messages with retry and partner check
async def forward_messages(message: Message):
    user_id = message.from_user.id
    current_state = await get_user_state(user_id)
    logger.info(f"Received message from {user_id}, type: {message.content_type}, state: {current_state}")
    if current_state != "chatting":
        await message.answer(
            text="⚠️ You are not currently chatting with anyone. Press 'Begin' to find a partner.",
            reply_markup=get_main_keyboard(state="idle" if current_state != "searching" else "searching")
        )
        return
    async with global_state_lock:
        partner_id = active_matches.get(user_id)
        if not partner_id:
            await message.answer(
                text="⚠️ You are not currently chatting with anyone. Press 'Begin' to find a partner.",
                reply_markup=get_main_keyboard(state="idle")
            )
            return
    try:
        await bot.get_chat(partner_id)
    except TelegramAPIError as e:
        logger.error(f"Partner {partner_id} unavailable: {e}")
        async with global_state_lock:
            active_matches.pop(user_id, None)
            active_matches.pop(partner_id, None)
            message_id_map.pop(user_id, None)
            message_id_map.pop(partner_id, None)
        update_user_data_now(user_id)
        update_user_data_now(partner_id)
        await message.answer(
            text="❌ Your partner is no longer available. Press 'Begin' to find a new partner.",
            reply_markup=get_main_keyboard(state="idle")
        )
        return
    message_id_map.setdefault(user_id, {})
    message_id_map.setdefault(partner_id, {})
    sender_gender = user_data.get(user_id, {}).get("gender", "Not set")
    gender_emoji = get_gender_emoji(sender_gender)
    label = f"Partner {gender_emoji}: "
    reply_to_message_id = message_id_map.get(user_id, {}).get(message.reply_to_message.message_id) if message.reply_to_message else None
    user_info = await bot.get_chat(user_id)
    sender_name = user_info.first_name or user_info.username or f"User {user_id}"
    message_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    channel_message = f"💬 **Message** at {message_time}\n👤 From: {sender_name} (ID: {user_id}) to User ID: {partner_id}\n"
    max_retries = 3
    for attempt in range(max_retries):
        try:
            forwarded_message = None
            if message.text:
                forwarded_message = await bot.send_message(
                    chat_id=partner_id,
                    text=label + message.text,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"📜 Text: {message.text}\n"
            elif message.photo:
                caption = message.caption or ""
                forwarded_message = await bot.send_photo(
                    chat_id=partner_id,
                    photo=message.photo[-1].file_id,
                    caption=label + caption,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"🖼️ Photo sent\n"
                if message.caption:
                    channel_message += f"📝 Caption: {message.caption}\n"
            elif message.document:
                caption = message.caption or ""
                forwarded_message = await bot.send_document(
                    chat_id=partner_id,
                    document=message.document.file_id,
                    caption=label + caption,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"📎 Document: {message.document.file_name or 'Unnamed document'}\n"
            elif message.video:
                caption = message.caption or ""
                forwarded_message = await bot.send_video(
                    chat_id=partner_id,
                    video=message.video.file_id,
                    caption=label + caption,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"🎥 Video sent\n"
                if message.caption:
                    channel_message += f"📝 Caption: {message.caption}\n"
            elif message.audio:
                caption = message.caption or ""
                forwarded_message = await bot.send_audio(
                    chat_id=partner_id,
                    audio=message.audio.file_id,
                    caption=label + caption,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"🎵 Audio sent\n"
                if message.caption:
                    channel_message += f"📝 Caption: {message.caption}\n"
            elif message.voice:
                caption = message.caption or ""
                forwarded_message = await bot.send_voice(
                    chat_id=partner_id,
                    voice=message.voice.file_id,
                    caption=label + caption,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"🎙️ Voice message sent\n"
                if message.caption:
                    channel_message += f"📝 Caption: {message.caption}\n"
            elif message.video_note:
                await bot.send_message(
                    chat_id=partner_id,
                    text=label,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                forwarded_message = await bot.send_video_note(
                    chat_id=partner_id,
                    video_note=message.video_note.file_id,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"🎥 Video note sent\n"
            elif message.sticker:
                await bot.send_message(
                    chat_id=partner_id,
                    text=label,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                forwarded_message = await bot.send_sticker(
                    chat_id=partner_id,
                    sticker=message.sticker.file_id,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"🏷️ Sticker sent\n"
            if forwarded_message:
                async with global_state_lock:
                    message_id_map[user_id][message.message_id] = forwarded_message.message_id
                    message_id_map[partner_id][forwarded_message.message_id] = message.message_id
            break
        except TelegramAPIError as e:
            logger.error(f"Failed to forward message from {user_id} to {partner_id}, attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                await message.answer(
                    "⚠️ Unable to send your message due to a server issue. Please try again later or contact @Ask_and_feedback_bot."
                )
                return
            await asyncio.sleep(2 ** attempt)
    try:
        await bot.send_message(chat_id=CHANNEL_ID, text=channel_message)
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
        logger.info(f"Message from {user_id} to {partner_id} logged to channel {CHANNEL_ID}")
    except TelegramAPIError as e:
        logger.error(f"Error logging message to channel {CHANNEL_ID}: {e}")

# Ignore group messages
@router.message(F.chat.type.in_({"group", "supergroup"}))
async def ignore_group_messages(_message: Message):
    pass

# Age handler
@router.callback_query(F.data == "age")
async def handle_age(callback: CallbackQuery):
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"selected_age_{age}") for age in range(row_start, row_start + 5)]
            for row_start in range(18, 100, 5)
        ] + [[InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")]]
    )
    await callback.message.edit_text(text="📅 Choose your age:", reply_markup=age_keyboard)
    await callback.answer()

# Gender handler
@router.callback_query(F.data == "gender")
async def handle_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=gender.capitalize(), callback_data=f"selected_gender_{gender}")]
            for gender in options["genders"]
        ] + [[InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")]]
    )
    await callback.message.edit_text(text="🚻 Please specify your gender:", reply_markup=gender_keyboard)
    await callback.answer()

# Religion handler
@router.callback_query(F.data == "religion")
async def handle_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=religion.capitalize(), callback_data=f"selected_religion_{religion}")]
            for religion in options["religions"]
        ] + [[InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")]]
    )
    await callback.message.edit_text(text="🙏 Please select your religion:", reply_markup=religion_keyboard)
    await callback.answer()

# Age selection
@router.callback_query(F.data.startswith("selected_age_"))
async def handle_age_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_age = callback.data.split("_")[-1]
    try:
        age = int(selected_age)
        if age < 18 or age > 100:
            raise ValueError
    except ValueError:
        await callback.answer(text="Invalid age selected", show_alert=True)
        return
    async with global_state_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]["age"] = selected_age
        update_user_data_now(user_id)
    await callback.answer(text=f"Your age is {selected_age}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_gender(callback)

# Gender selection
@router.callback_query(F.data.startswith("selected_gender_"))
async def handle_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1]
    if selected_gender not in options["genders"]:
        await callback.answer(text="Invalid gender selected", show_alert=True)
        return
    async with global_state_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]["gender"] = selected_gender
        update_user_data_now(user_id)
    await callback.answer(text=f"You selected {selected_gender.capitalize()}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_religion(callback)

# Religion selection
@router.callback_query(F.data.startswith("selected_religion_"))
async def handle_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_religion = callback.data.split("_")[-1]
    if selected_religion not in options["religions"]:
        await callback.answer(text="Invalid religion selected", show_alert=True)
        return
    async with global_state_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]["religion"] = selected_religion
        update_user_data_now(user_id)
    selected_age = user_data[user_id].get("age", "Not set")
    selected_gender = user_data[user_id].get("gender", "Not set")
    selected_religion = user_data[user_id].get("religion", "Not set")
    await callback.message.edit_text(
        text=(
            f"🎉 Your selections are confirmed:\n"
            f"- 📅 Age: {selected_age}\n"
            f"- 🚻 Gender: {selected_gender}\n"
            f"- 🙏 Religion: {selected_religion}\n\n"
            "Returning to setup menu..."
        )
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    if NOTIFICATION_DELAY_SECONDS > 0:
        await asyncio.sleep(NOTIFICATION_DELAY_SECONDS)
    await handle_back_to_setup(callback)

# Partner minimum age
@router.callback_query(F.data == "partner_age")
async def handle_partner_minimum_age(callback: CallbackQuery):
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"partner_min_age_{age}") for age in range(row_start, row_start + 5)]
            for row_start in range(18, 100, 5)
        ] + [[InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")]]
    )
    await callback.message.edit_text(text="📅 Choose the **minimum age** for your partner:", reply_markup=age_keyboard)
    await callback.answer()

# Partner maximum age
@router.callback_query(F.data.startswith("partner_min_age_"))
async def handle_partner_maximum_age(callback: CallbackQuery):
    user_id = callback.from_user.id
    min_age = int(callback.data.split("_")[-1])
    async with global_state_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        if "partner" not in user_data[user_id]:
            user_data[user_id]["partner"] = {}
        user_data[user_id]["partner"]["min_age"] = min_age
        update_user_data_now(user_id)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    max_age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"partner_max_age_{age}") for age in range(row_start, row_start + 5) if age >= min_age]
            for row_start in range(18, 100, 5)
        ] + [[InlineKeyboardButton(text="⬅️ Back", callback_data="partner_age")]]
    )
    await callback.message.edit_text(
        text=f"📅 Minimum age selected: **{min_age}**\nNow, choose the **maximum age** for your partner:",
        reply_markup=max_age_keyboard
    )
    await callback.answer()

# Partner age range
@router.callback_query(F.data.startswith("partner_max_age_"))
async def handle_partner_age_range(callback: CallbackQuery):
    user_id = callback.from_user.id
    max_age = int(callback.data.split("_")[-1])
    async with global_state_lock:
        if user_id not in user_data or "partner" not in user_data[user_id] or "min_age" not in user_data[user_id]["partner"]:
            await callback.message.edit_text(
                text="❌ Minimum age not set. Please start from selecting minimum age.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data="partner_age")]])
            )
            return
        min_age = user_data[user_id]["partner"]["min_age"]
        if max_age < min_age:
            await callback.message.edit_text(
                text=f"❌ Maximum age cannot be less than minimum age ({min_age}). Please choose a higher age.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text=str(age), callback_data=f"partner_max_age_{age}") for age in range(row_start, row_start + 5) if age >= min_age]
                        for row_start in range(18, 100, 5)
                    ] + [[InlineKeyboardButton(text="⬅️ Back", callback_data="partner_age")]]
                )
            )
            return
        user_data[user_id]["partner"]["max_age"] = max_age
        update_user_data_now(user_id)
    await callback.answer(text=f"🎉 Partner age range set: from {min_age} to {max_age}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_gender(callback)

# Partner gender
@router.callback_query(F.data == "partner_gender")
async def handle_partner_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=gender.capitalize(), callback_data=f"partner_gender_{gender}")]
            for gender in options["genders"]
        ] + [[InlineKeyboardButton(text="Any", callback_data="partner_gender_any")],
             [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")]]
    )
    await callback.message.edit_text(text="🚻 Please select your partner gender:", reply_markup=gender_keyboard)
    await callback.answer()

# Partner gender selection
@router.callback_query(F.data.startswith("partner_gender_"))
async def handle_partner_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1]
    if selected_gender != "any" and selected_gender not in options["genders"]:
        await callback.answer(text="Invalid partner gender selected", show_alert=True)
        return
    async with global_state_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        if "partner" not in user_data[user_id]:
            user_data[user_id]["partner"] = {}
        user_data[user_id]["partner"]["gender"] = selected_gender
        update_user_data_now(user_id)
    await callback.answer(text=f"🎉 Partner gender set to: {selected_gender.capitalize()}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_religion(callback)

# Partner religion
@router.callback_query(F.data == "partner_religion")
async def handle_partner_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=religion.capitalize(), callback_data=f"partner_religion_{religion}")]
            for religion in options["religions"]
        ] + [[InlineKeyboardButton(text="Any", callback_data="partner_religion_any")],
             [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")]]
    )
    await callback.message.edit_text(text="🙏 Please select your partner religion:", reply_markup=religion_keyboard)
    await callback.answer()

# Partner religion selection
@router.callback_query(F.data.startswith("partner_religion_"))
async def handle_partner_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_partner_religion = callback.data.split("_")[-1]
    if selected_partner_religion != "any" and selected_partner_religion not in options["religions"]:
        await callback.answer(text="Invalid partner religion selected", show_alert=True)
        return
    async with global_state_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        if "partner" not in user_data[user_id]:
            user_data[user_id]["partner"] = {}
        user_data[user_id]["partner"]["religion"] = selected_partner_religion
        update_user_data_now(user_id)
    partner_min_age = user_data[user_id]["partner"].get("min_age", "Not set")
    partner_max_age = user_data[user_id]["partner"].get("max_age", "Not set")
    partner_gender = user_data[user_id]["partner"].get("gender", "Not set")
    partner_religion = user_data[user_id]["partner"].get("religion", "Not set")
    await callback.message.edit_text(
        text=(
            f"🎉 Your partner preferences are confirmed:\n"
            f"- 📅 Age Range: {partner_min_age} to {partner_max_age}\n"
            f"- 🚻 Gender: {partner_gender.capitalize()}\n"
            f"- 🙏 Religion: {partner_religion}\n\n"
            "Returning to setup menu..."
        )
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    if NOTIFICATION_DELAY_SECONDS > 0:
        await asyncio.sleep(NOTIFICATION_DELAY_SECONDS)
    await handle_back_to_setup(callback)

# Show setup
@router.callback_query(F.data == "show_setup")
async def handle_show_setup(callback: CallbackQuery):
    if callback.message.text.startswith("👤 Here is your profile:"):
        await callback.answer(text="⚠️ You are already in the show setup menu!", show_alert=True)
        return
    user_id = callback.from_user.id
    your_age = user_data.get(user_id, {}).get("age", "Not set")
    your_gender = user_data.get(user_id, {}).get("gender", "Not set")
    your_religion = user_data.get(user_id, {}).get("religion", "Not set")
    partner_min_age = user_data.get(user_id, {}).get("partner", {}).get("min_age", "Not set")
    partner_max_age = user_data.get(user_id, {}).get("partner", {}).get("max_age", "Not set")
    partner_gender = user_data.get(user_id, {}).get("partner", {}).get("gender", "Not set")
    partner_religion = user_data.get(user_id, {}).get("partner", {}).get("religion", "Not set")
    result_text = (
        f" 👤 Here is your profile:\n"
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

# Check for long wait times
async def check_waiting_timeouts():
    while True:
        await asyncio.sleep(60)
        now = datetime.datetime.now()
        async with global_state_lock:
            for user_id in list(waiting_users):
                wait_time = (now - waiting_start_times.get(user_id, now)).total_seconds() / 60
                if wait_time > WAITING_TIMEOUT_MINUTES:
                    waiting_users.discard(user_id)
                    waiting_start_times.pop(user_id, None)
                    update_user_data_now(user_id)
                    try:
                        await bot.send_message(
                            chat_id=user_id,
                            text="⏳ You've been waiting too long without a match. Try broadening your preferences or press 'Begin' to search again.",
                            reply_markup=get_main_keyboard(state="idle")
                        )
                        logger.info(f"Notified user {user_id} of long wait time")
                    except TelegramAPIError as e:
                        logger.error(f"Failed to notify user {user_id} of wait timeout: {e}")
        await try_match_queued_users()

# Try match queued users
async def try_match_queued_users():
    if not await can_attempt_match():
        logger.warning(f"Cannot match queued users: limits reached (users: {len(waiting_users) + len(active_matches)}, matches: {len(active_matches) // 2})")
        return
    async with global_state_lock:
        sorted_waiting_users = sorted(
            waiting_users,
            key=lambda x: waiting_start_times.get(x, datetime.datetime.now())
        )
    for user_id in sorted_waiting_users:
        if user_id in waiting_users and is_setup_complete(user_id)[0]:
            await attempt_match(user_id)

# Periodic save
async def periodic_save():
    while True:
        await asyncio.sleep(60)
        await save_user_data()
        logger.info("Periodic backup of user data performed")

# Periodic match check
async def periodic_match_check():
    while True:
        await asyncio.sleep(30)
        if waiting_users:
            logger.info(f"Checking for matches among {len(waiting_users)} waiting users")
            await try_match_queued_users()

# Cleanup inactive users
async def cleanup_inactive_users():
    while True:
        await asyncio.sleep(3600)  # Run hourly
        async with global_state_lock:
            now = datetime.datetime.now()
            inactive_threshold = now - datetime.timedelta(days=30)
            users_to_remove = [
                user_id for user_id, data in user_data.items()
                if data.get("last_active", now) < inactive_threshold
                and user_id not in active_matches
                and user_id not in waiting_users
            ]
            for user_id in users_to_remove:
                user_data.pop(user_id, None)
                try:
                    await users_collection.delete_one({"_id": user_id})
                    logger.info(f"Removed inactive user {user_id} from MongoDB")
                except Exception as e:
                    logger.error(f"Failed to remove user {user_id}: {e}")
            logger.info(f"Cleaned up {len(users_to_remove)} inactive users")

# Instance locking functions to prevent multiple instances
async def acquire_instance_lock():
    lock_id = str(uuid4())
    now = datetime.datetime.now()
    staleness_threshold = now - datetime.timedelta(minutes=15)
    try:
        # Attempt to acquire or update lock atomically
        result = await db['instance_locks'].find_one_and_update(
            {
                "_id": "bot_instance",
                "$or": [
                    {"created_at": {"$lt": staleness_threshold}},
                    {"lock_id": {"$exists": False}}
                ]
            },
            {
                "$set": {
                    "lock_id": lock_id,
                    "created_at": now
                }
            },
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        if result and result.get("lock_id") == lock_id:
            logger.info(f"Acquired instance lock with ID {lock_id}")
            return lock_id
        else:
            # Check if another instance holds a valid lock
            existing_lock = await db['instance_locks'].find_one({"_id": "bot_instance"})
            if existing_lock and existing_lock.get("created_at") >= staleness_threshold:
                logger.error(f"Another bot instance is already running. Existing lock: {existing_lock}")
                raise Exception("Another bot instance is running")
            # Retry if lock was stale but update failed
            logger.warning("Failed to acquire lock, retrying...")
            await asyncio.sleep(1)
            return await acquire_instance_lock()
    except Exception as e:
        logger.error(f"Failed to acquire instance lock: {e}")
        raise
async def keep_lock_alive(lock_id):
    while True:
        await asyncio.sleep(300)  # 5 minutes
        try:
            now = datetime.datetime.now()
            result = await db['instance_locks'].update_one(
                {"_id": "bot_instance", "lock_id": lock_id},
                {"$set": {"created_at": now}}
            )
            if result.matched_count == 0:
                logger.warning("Lock no longer held, stopping keep-alive")
                break
        except Exception as e:
            logger.error(f"Failed to update lock: {e}")

async def release_instance_lock(lock_id):
    try:
        await db['instance_locks'].delete_one({"_id": "bot_instance", "lock_id": lock_id})
        logger.info(f"Released instance lock with ID {lock_id}")
    except Exception as e:
        logger.error(f"Failed to release instance lock: {e}")

# Main function with webhook setup
async def main():
    global options
    options = await load_options()  # Load options asynchronously

    lock_id = None
    keep_lock_task = None
    try:
        lock_id = await acquire_instance_lock()
        keep_lock_task = asyncio.create_task(keep_lock_alive(lock_id))
    except Exception as e:
        logger.error(f"Failed to acquire instance lock: {e}")
        raise  # Propagate the exception to FastAPI

    dp.include_router(router)  # Ensure handlers are registered

    # Initialize tasks as None to avoid UnboundLocalError
    periodic_save_task = None
    periodic_match_task = None
    waiting_timeout_task = None
    cleanup_task = None

    try:
        await setup_mongodb_indexes()
        await load_user_data()
        logger.info("Bot is running...")
        await set_bot_commands(bot)

        # Set webhook
        webhook_url = os.getenv("WEBHOOK_URL")
        if not webhook_url:
            logger.error("WEBHOOK_URL not set")
            raise ValueError("WEBHOOK_URL not set")
        await bot.set_webhook(url=webhook_url, drop_pending_updates=True)
        logger.info(f"Webhook set to {webhook_url}")

        # Start background tasks
        periodic_save_task = asyncio.create_task(periodic_save())
        periodic_match_task = asyncio.create_task(periodic_match_check())
        waiting_timeout_task = asyncio.create_task(check_waiting_timeouts())
        cleanup_task = asyncio.create_task(cleanup_inactive_users())

        # Keep the bot running for background tasks
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, saving data...")
        await save_user_data()
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        await save_user_data()
        raise
    finally:
        # Cancel background tasks if they were created
        tasks = [task for task in [periodic_save_task, periodic_match_task, waiting_timeout_task, cleanup_task, keep_lock_task] if task is not None]
        for task in tasks:
            task.cancel()
        # Wait for tasks to complete cancellation
        await asyncio.gather(*tasks, return_exceptions=True)

        # Release instance lock
        if lock_id:
            await release_instance_lock(lock_id)

        # Clean up webhook
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook deleted")
        except Exception as e:
            logger.error(f"Failed to delete webhook: {e}")

        # Close MongoDB client
        client.close()
        logger.info("MongoDB client closed")
        logger.info("Bot has been gracefully shut down")

if __name__ == "__main__":
    asyncio.run(main())
