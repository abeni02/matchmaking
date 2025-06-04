import asyncio
import os
import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
    BotCommandScopeAllPrivateChats
)
import random
import logging
import time
from datetime import datetime, time
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

# Configure logging to use UTC time (from scheduling script, assumed compatible with main script)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
formatter.converter = time.gmtime  # Use UTC time
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Environment variables (extended to include scheduling chat ID)
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')
GROUP_ID = os.getenv('GROUP_ID')
GROUP_INVITE_LINK = os.getenv('GROUP_INVITE_LINK')
MONGODB_URI = os.getenv('MONGODB_URI')
SCHEDULE_CHAT_ID = os.getenv('SCHEDULE_CHAT_ID')  # Added for scheduling

# Validate environment variables
if not BOT_TOKEN:
    raise ValueError("No BOT_TOKEN found in environment variables. Please set it securely.")
if not CHANNEL_ID:
    raise ValueError("No CHANNEL_ID found in environment variables. Please set it securely.")
if not GROUP_ID:
    raise ValueError("No GROUP_ID found in environment variables. Please set it securely.")
if not GROUP_INVITE_LINK:
    raise ValueError("No GROUP_INVITE_LINK found in environment variables. Please set it securely.")
if not MONGODB_URI:
    raise ValueError("No MONGODB_URI found in environment variables. Please set it securely.")
if not SCHEDULE_CHAT_ID:
    raise ValueError("No SCHEDULE_CHAT_ID found in environment variables. Please set it securely.")

# Bot and dispatcher setup
bot = Bot(token=BOT_TOKEN)
router = Router()
dp = Dispatcher()
dp.include_router(router)

# MongoDB setup
client = AsyncIOMotorClient(MONGODB_URI)
db = client['bot_database']
users_collection = db['users']

# Initialize data structures (from main script)
user_data = {}
active_matches = {}
cooldown_tracker = {}
waiting_users = set()
waiting_start_times = {}
message_id_map = {}

# Button texts (from main script)
BEGIN_TEXT = "🚀 Begin"
STOP_SEARCHING_TEXT = "🛑 Stop Searching"
END_CHAT_TEXT = "🛑 End Chat"

# Global variables for scheduling (from scheduling script)
MESSAGES = [
    "Hey team, time for a quick sync! What's the latest?",
    "It's time to check in! Any updates to share?",
    "Hello everyone, let's touch base! What's on your mind?",
    "Time for our daily catch-up! What's new with you all?"
]

target_times = [
    time(9, 21),   # 12:21 PM EAT = 9:21 AM UTC
    time(8, 17),   # 11:17 AM EAT = 8:17 AM UTC
    time(8, 18),   # 11:18 AM EAT = 8:18 AM UTC
    time(8, 19),   # 11:19 AM EAT = 8:19 AM UTC
    time(11, 30)   # 2:30 PM EAT = 11:30 AM UTC (for testing)
]

last_sent_dates = {target: None for target in target_times}

# Placeholder sticker ID (update with a valid file_id obtained via the sticker handler)
STICKER_ID = 'CAACAgIAAxkBAAIBB2cF5x1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ'

# Function definitions (all unchanged)

## From main script
def get_gender_emoji(gender):
    if gender.lower() == "male":
        return "👨"
    elif gender.lower() == "female":
        return "👩"
    else:
        return "❓"

async def save_user_data():
    for user_id, data in user_data.items():
        try:
            await users_collection.replace_one(
                {'_id': user_id},
                {'_id': user_id, **data},
                upsert=True
            )
        except Exception as e:
            print(f"❌ Error saving user {user_id} to MongoDB: {e}")
    print(f"✅ All user data saved to MongoDB")

async def update_user_data(user_id):
    if user_id in user_data:
        user_info = user_data[user_id]
        try:
            await users_collection.replace_one(
                {'_id': user_id},
                {'_id': user_id, **user_info},
                upsert=True
            )
            print(f"✅ Updated user {user_id} in MongoDB")
        except Exception as e:
            print(f"❌ Error updating user {user_id} in MongoDB: {e}")
    else:
        print(f"⚠️ User {user_id} not found in user_data")

def update_user_data_now(user_id):
    asyncio.create_task(update_user_data(user_id))

async def load_user_data():
    global user_data
    user_data = {}
    try:
        async for document in users_collection.find():
            user_id = document['_id']
            user_data[user_id] = {k: v for k, v in document.items() if k != '_id'}
        print(f"✅ Loaded data for {len(user_data)} users from MongoDB")
    except Exception as e:
        print(f"❌ Error loading user data from MongoDB: {e}")

async def is_group_member(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception as e:
        print(f"Error checking group membership for user {user_id}: {e}")
        return False

async def send_join_group_message(message: Message):
    join_button = InlineKeyboardButton(text="Join Group", url=GROUP_INVITE_LINK)
    join_keyboard = InlineKeyboardMarkup(inline_keyboard=[[join_button]])
    await message.answer(
        text="This bot is exclusive to members of our private group. Please join the group to use the bot.",
        reply_markup=join_keyboard
    )

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
        if "min_age" not in user_prefs["partner"] or user_prefs["partner"]["min_age"] == "Not set":
            missing_fields.append("Partner Minimum Age")
        if "max_age" not in user_prefs["partner"] or user_prefs["partner"]["max_age"] == "Not set":
            missing_fields.append("Partner Maximum Age")
        if "gender" not in user_prefs["partner"] or user_prefs["partner"]["gender"] == "Not set":
            missing_fields.append("Partner Gender")
        if "religion" not in user_prefs["partner"] or user_prefs["partner"]["religion"] == "Not set":
            missing_fields.append("Partner Religion")
    return len(missing_fields) == 0, missing_fields

def get_user_state(user_id):
    if user_id in active_matches:
        return "chatting"
    elif user_id in waiting_users:
        return "searching"
    else:
        return "idle"

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
            [InlineKeyboardButton(text="Your profile", callback_data="your_setup")],
            [InlineKeyboardButton(text="Partner Profile", callback_data="partner_setup")],
            [InlineKeyboardButton(text="Show Profile", callback_data="show_setup")],
        ]
    )

async def show_setup_menu(message_or_callback):
    if isinstance(message_or_callback, Message):
        await message_or_callback.answer(
            text="⚙️ Please choose your setup option:",
            reply_markup=get_setup_inline_keyboard()
        )
    elif isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(
            text="⚙️ Please choose your setup option:",
            reply_markup=get_setup_inline_keyboard()
        )
        await message_or_callback.answer()

async def start_searching(message: Message, user_id: int):
    is_complete, missing_fields = is_setup_complete(user_id)
    if not is_complete:
        await message.answer(
            text=f"⚠️ Please complete your setup before starting a match. Missing fields:\n- {', '.join(missing_fields)}\nRedirecting to setup menu...",
            reply_markup=get_main_keyboard(state="idle")
        )
        await show_setup_menu(message)
        return False
    waiting_start_times[user_id] = datetime.datetime.now()
    waiting_users.add(user_id)
    await message.answer(
        "🔍 Waiting for a compatible partner. You'll be matched automatically when one is found.",
        reply_markup=get_main_keyboard(state="searching")
    )
    await attempt_match(user_id)
    return True

def find_match(user_id):
    if user_id not in user_data:
        return None
    now = datetime.datetime.now()
    user_prefs = user_data[user_id]
    sorted_waiting_users = sorted(
        waiting_users,
        key=lambda x: waiting_start_times.get(x, now)
    )
    for candidate_id in sorted_waiting_users:
        if candidate_id == user_id or candidate_id in active_matches:
            continue
        candidate_prefs = user_data.get(candidate_id, {})
        if not candidate_prefs:
            continue
        if user_id in cooldown_tracker and candidate_id in cooldown_tracker[user_id]:
            cooldown_end = cooldown_tracker[user_id][candidate_id]
            if now < cooldown_end:
                continue
        partner_criteria = candidate_prefs.get("partner", {})
        user_partner_prefs = user_prefs.get("partner", {})
        user_religion = user_prefs.get("religion", "Not set")
        candidate_religion = candidate_prefs.get("religion", "Not set")
        partner_religion_pref = partner_criteria.get("religion", "any")
        user_partner_religion_pref = user_prefs.get("partner", {}).get("religion", "any")
        candidate_religion_ok = (
            partner_religion_pref.lower() == "any" or
            partner_religion_pref == user_religion
        )
        user_religion_ok = (
            user_partner_religion_pref.lower() == "any" or
            user_partner_religion_pref == candidate_religion
        )
        if (
            (partner_criteria.get("min_age", 0) <= int(user_prefs.get("age", 0)) <= partner_criteria.get("max_age", 100))
            and (user_partner_prefs.get("min_age", 0) <= int(candidate_prefs.get("age", 0)) <= user_partner_prefs.get("max_age", 100))
            and (partner_criteria.get("gender", "any") in ("any", user_prefs.get("gender", "any")))
            and (user_partner_prefs.get("gender", "any") in ("any", candidate_prefs.get("gender", "any")))
            and candidate_religion_ok
            and user_religion_ok
        ):
            return candidate_id
    return None

async def attempt_match(user_id):
    match_id = find_match(user_id)
    if match_id:
        active_matches[user_id] = match_id
        active_matches[match_id] = user_id
        waiting_users.discard(user_id)
        waiting_users.discard(match_id)
        waiting_start_times.pop(user_id, None)
        waiting_start_times.pop(match_id, None)
        user_data_1 = user_data[user_id]
        user_data_2 = user_data[match_id]
        user_1_info = await bot.get_chat(user_id)
        user_2_info = await bot.get_chat(match_id)
        user_1_name = user_1_info.first_name or user_1_info.username or f"User {user_id}"
        user_2_name = user_2_info.first_name or user_2_info.username or f"User {match_id}"
        await bot.send_message(
            chat_id=user_id,
            text=(
                f"🎉 Match found!\n\n"
                f"👤 Partner’s setup:\n"
                f"📅 Age: {user_data_2.get('age', 'Not set')}\n"
                f"🚻 Gender: {user_data_2.get('gender', 'Not set')}\n"
                f"🙏 Religion: {user_data_2.get('religion', 'Not set')}\n"
                "Start messaging them now! Swipe left on a message to reply."
            ),
            reply_markup=get_main_keyboard(state="chatting"),
        )
        await bot.send_message(
            chat_id=match_id,
            text=(
                f"🎉 Match found!\n\n"
                f"👤 Partner’s setup:\n"
                f"📅 Age: {user_data_1.get('age', 'Not set')}\n"
                f"🚻 Gender: {user_data_1.get('gender', 'Not set')}\n"
                f"🙏 Religion: {user_data_1.get('religion', 'Not set')}\n"
                "Start messaging them now! Swipe left on a message to reply."
            ),
            reply_markup=get_main_keyboard(state="chatting"),
        )
        match_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        channel_message = (
            f"🤝 **New Match** at {match_time}\n\n"
            f"👤 User 1: {user_1_name} (ID: {user_id})\n"
            f"  - Age: {user_data_1.get('age', 'Not set')}\n"
            f"  - Gender: {user_data_1.get('gender', 'Not set')}\n"
            f"  - Religion: {user_data_1.get('religion', 'Not set')}\n"
            f"  - Partner Prefs:\n"
            f"    - Age Range: {user_data_1.get('partner', {}).get('min_age', 'Not set')} to {user_data_1.get('partner', {}).get('max_age', 'Not set')}\n"
            f"    - Gender: {user_data_1.get('partner', {}).get('gender', 'Not set')}\n"
            f"    - Religion: {user_data_1.get('partner', {}).get('religion', 'Not set')}\n\n"
            f"👤 User 2: {user_2_name} (ID: {match_id})\n"
            f"  - Age: {user_data_2.get('age', 'Not set')}\n"
            f"  - Gender: {user_data_2.get('gender', 'Not set')}\n"
            f"  - Religion: {user_data_2.get('religion', 'Not set')}\n"
            f"  - Partner Prefs:\n"
            f"    - Age Range: {user_data_2.get('partner', {}).get('min_age', 'Not set')} to {user_data_2.get('partner', {}).get('max_age', 'Not set')}\n"
            f"    - Gender: {user_data_2.get('partner', {}).get('gender', 'Not set')}\n"
            f"    - Religion: {user_data_2.get('partner', {}).get('religion', 'Not set')}"
        )
        try:
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=channel_message,
                parse_mode="Markdown"
            )
            print(f"📢 Match logged to channel {CHANNEL_ID} for users {user_id} and {match_id}")
        except Exception as e:
            print(f"❌ Error logging match to channel {CHANNEL_ID}: {e}")
        return True
    return False

## From scheduling script
def seconds_since_midnight(t):
    return t.hour * 3600 + t.minute * 60 + t.second

async def send_scheduled_message():
    while True:
        now = datetime.utcnow()
        current_date = now.date()
        current_time = now.time()
        current_seconds = seconds_since_midnight(current_time)

        for target in target_times:
            target_seconds = seconds_since_midnight(target)
            time_diff = abs(current_seconds - target_seconds)
            if time_diff < 60 and (last_sent_dates[target] is None or last_sent_dates[target] < current_date):
                current_time_str = now.strftime("%H:%M:%S")
                message = random.choice(MESSAGES)
                full_message = f"[{current_time_str} UTC] {message}"
                try:
                    await bot.send_sticker(chat_id=SCHEDULE_CHAT_ID, sticker=STICKER_ID)
                    await bot.send_message(chat_id=SCHEDULE_CHAT_ID, text=full_message)
                    logging.info(f"Sticker and message sent at {current_time_str} UTC: {full_message}")
                    last_sent_dates[target] = current_date
                except TelegramBadRequest as e:
                    logging.error(f"Bad request error: {e}")
                    if "wrong remote file identifier" in str(e).lower():
                        logging.error("Invalid STICKER_ID. Send a sticker to the bot to get a valid file_id.")
                except TelegramForbiddenError as e:
                    logging.error(f"Forbidden error: {e}. Check bot permissions.")
                except Exception as e:
                    logging.error(f"Unexpected error: {e}")
                await asyncio.sleep(60)  # Wait to avoid multiple sends

        await asyncio.sleep(30)  # Check every 30 seconds

# Handler definitions (all unchanged)

## From main script
@router.message(F.chat.type == "private", F.text == "/start")
async def start_command(message: Message):
    user_id = message.from_user.id
    if not await is_group_member(user_id):
        await send_join_group_message(message)
        return
    current_state = get_user_state(user_id)
    welcome_text = "👋 Welcome to the bot!\n\n"
    if current_state == "idle":
        welcome_text += "Press 'Setup' to configure your preferences."
    elif current_state == "searching":
        welcome_text += "You are currently searching for a partner. Press 'Stop Searching' to cancel."
    elif current_state == "chatting":
        welcome_text += "You are currently in a chat session. Press 'End Chat' to end the session."
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
        text="🔧 You selected 'Your Setup'. Choose an option below to configure:",
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
    await callback.message.edit_text(
        text="⚙️ Please choose your setup option:",
        reply_markup=get_setup_inline_keyboard()
    )
    await callback.answer()

@router.message(F.chat.type == "private", F.text.in_({BEGIN_TEXT, STOP_SEARCHING_TEXT, END_CHAT_TEXT, "/begin", "/end"}))
async def handle_matching_button(message: Message):
    user_id = message.from_user.id
    text = message.text
    current_state = get_user_state(user_id)
    if text in [BEGIN_TEXT, "/begin"]:
        if current_state != "idle":
            await message.answer(
                "⚠️ Invalid action for current state.",
                reply_markup=get_main_keyboard(state=current_state)
            )
            return
        if not await is_group_member(user_id):
            await send_join_group_message(message)
            return
        await start_searching(message, user_id)
    elif text == STOP_SEARCHING_TEXT:
        if current_state != "searching":
            await message.answer(
                "⚠️ Invalid action for current state.",
                reply_markup=get_main_keyboard(state=current_state)
            )
            return
        waiting_users.remove(user_id)
        waiting_start_times.pop(user_id, None)
        await message.answer(
            "🛑 You have stopped searching.",
            reply_markup=get_main_keyboard(state="idle")
        )
    elif text == END_CHAT_TEXT:
        if current_state != "chatting":
            await message.answer(
                "⚠️ Invalid action for current state.",
                reply_markup=get_main_keyboard(state=current_state)
            )
            return
        match_id = active_matches.pop(user_id)
        active_matches.pop(match_id, None)
        cooldown_period = datetime.timedelta(hours=4)
        now = datetime.datetime.now()
        cooldown_tracker.setdefault(user_id, {})[match_id] = now + cooldown_period
        cooldown_tracker.setdefault(match_id, {})[user_id] = now + cooldown_period
        message_id_map.pop(user_id, None)
        message_id_map.pop(match_id, None)
        await message.answer(
            "❌ You have ended the session. You can 'Begin' again to find a new partner.",
            reply_markup=get_main_keyboard(state="idle")
        )
        await bot.send_message(
            chat_id=match_id,
            text="❌ Your partner has ended the session. You can 'Begin' again to find a new partner.",
            reply_markup=get_main_keyboard(state="idle")
        )
    elif text == "/end":
        if current_state == "chatting":
            match_id = active_matches.pop(user_id)
            active_matches.pop(match_id, None)
            cooldown_period = datetime.timedelta(hours=4)
            now = datetime.datetime.now()
            cooldown_tracker.setdefault(user_id, {})[match_id] = now + cooldown_period
            cooldown_tracker.setdefault(match_id, {})[user_id] = now + cooldown_period
            message_id_map.pop(user_id, None)
            message_id_map.pop(match_id, None)
            await message.answer(
                "❌ You have ended the session. You can 'Begin' again to find a new partner.",
                reply_markup=get_main_keyboard(state="idle")
            )
            await bot.send_message(
                chat_id=match_id,
                text="❌ Your partner has ended the session. You can 'Begin' again to find a new partner.",
                reply_markup=get_main_keyboard(state="idle")
            )
        elif current_state == "searching":
            waiting_users.remove(user_id)
            waiting_start_times.pop(user_id, None)
            await message.answer(
                "🛑 You have stopped searching.",
                reply_markup=get_main_keyboard(state="idle")
            )
        else:
            await message.answer(
                "⚠️ You are not in an active session or searching.",
                reply_markup=get_main_keyboard(state="idle")
            )

@router.message(F.chat.type == "private", F.text.in_({"❓ Help", "/help"}))
async def handle_help(message: Message):
    await message.answer(
        text=(
            "💡 Need assistance? Here's what you can do:\n"
            " - 🚀 Begin: Start your journey (after completing setup).\n"
            " - 🛑 Stop Searching: Stop looking for a partner.\n"
            " - 🛑 End Chat: Stop chatting with your partner.\n"
            " - ⚙️ Setup: Configure your preferences.\n"
            " - ❓ Help: Get guidance and information.\n"
            " - 📩 Reply: Swipe left on a message to reply to it."
        )
    )

@router.message(F.chat.type == "private", F.text | F.document | F.photo | F.video | F.audio | F.voice | F.video_note | F.sticker)
async def forward_messages(message: Message):
    user_id = message.from_user.id
    print(f"📩 Received message from {user_id}, type: {message.content_type}")
    print(f"📋 Active matches: {active_matches}")
    print(f"🗂️ Current message_id_map: {message_id_map}")
    if user_id not in active_matches:
        print(f"⚠️ User {user_id} not in active_matches")
        await message.answer(
            "⚠️ You are not currently chatting with anyone. Press 'Begin' to find a partner.",
            reply_markup=get_main_keyboard(state="idle")
        )
        return
    partner_id = active_matches[user_id]
    message_id_map.setdefault(user_id, {})
    message_id_map.setdefault(partner_id, {})
    sender_gender = user_data.get(user_id, {}).get("gender", "Not set")
    gender_emoji = get_gender_emoji(sender_gender)
    label = f"Partner {gender_emoji}: "
    reply_to_message_id = None
    reply_info = ""
    if message.reply_to_message:
        original_reply_id = message.reply_to_message.message_id
        print(f"↩️ Detected reply from {user_id} to message {original_reply_id}")
        reply_to_message_id = message_id_map.get(user_id, {}).get(original_reply_id)
        if not reply_to_message_id:
            print(f"⚠️ No mapped message ID found for reply from {user_id} to message {original_reply_id}")
            reply_info = f" (Reply to message ID {original_reply_id}, mapping not found)"
        else:
            print(f"✅ Found mapped reply_to_message_id: {reply_to_message_id} for user {user_id}")
            reply_info = f" (Reply to message ID {reply_to_message_id})"
    user_info = await bot.get_chat(user_id)
    sender_name = user_info.first_name or user_info.username or f"User {user_id}"
    message_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    channel_message = f"💬 **Message** at {message_time}\n👤 From: {sender_name} (ID: {user_id}) to User ID: {partner_id}{reply_info}\n"
    try:
        forwarded_message = None
        if message.text:
            print(f"📝 Forwarding text message from {user_id} to {partner_id}")
            modified_text = label + message.text
            forwarded_message = await bot.send_message(
                chat_id=partner_id,
                text=modified_text,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += f"📜 Text: {message.text}\n"
        elif message.photo:
            print(f"📸 Forwarding photo from {user_id} to {partner_id}")
            caption = message.caption or ""
            modified_caption = label + caption
            forwarded_message = await bot.send_photo(
                chat_id=partner_id,
                photo=message.photo[-1].file_id,
                caption=modified_caption,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += f"🖼️ Photo sent\n"
            if message.caption:
                channel_message += f"📝 Caption: {message.caption}\n"
        elif message.document:
            print(f"📄 Forwarding document from {user_id} to {partner_id}")
            caption = message.caption or ""
            modified_caption = label + caption
            forwarded_message = await bot.send_document(
                chat_id=partner_id,
                document=message.document.file_id,
                caption=modified_caption,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += f"📎 Document: {message.document.file_name or 'Unnamed document'}\n"
        elif message.video:
            print(f"🎥 Forwarding video from {user_id} to {partner_id}")
            caption = message.caption or ""
            modified_caption = label + caption
            forwarded_message = await bot.send_video(
                chat_id=partner_id,
                video=message.video.file_id,
                caption=modified_caption,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += f"🎥 Video sent\n"
            if message.caption:
                channel_message += f"📝 Caption: {message.caption}\n"
        elif message.audio:
            print(f"🎵 Forwarding audio from {user_id} to {partner_id}")
            caption = message.caption or ""
            modified_caption = label + caption
            forwarded_message = await bot.send_audio(
                chat_id=partner_id,
                audio=message.audio.file_id,
                caption=modified_caption,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += f"🎵 Audio sent\n"
            if message.caption:
                channel_message += f"📝 Caption: {message.caption}\n"
        elif message.voice:
            print(f"🎙️ Forwarding voice message from {user_id} to {partner_id}")
            caption = message.caption or ""
            modified_caption = label + caption
            forwarded_message = await bot.send_voice(
                chat_id=partner_id,
                voice=message.voice.file_id,
                caption=modified_caption,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += f"🎙️ Voice message sent\n"
            if message.caption:
                channel_message += f"📝 Caption: {message.caption}\n"
        elif message.video_note:
            print(f"🎥 Forwarding video note from {user_id} to {partner_id}")
            label_text = f"Partner {gender_emoji}:"
            await bot.send_message(
                chat_id=partner_id,
                text=label_text,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            forwarded_message = await bot.send_video_note(
                chat_id=partner_id,
                video_note=message.video_note.file_id,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            message_id_map[user_id][message.message_id] = forwarded_message.message_id
            message_id_map[partner_id][forwarded_message.message_id] = message.message_id
            print(f"📌 Mapped message ID {message.message_id} (user {user_id}) to {forwarded_message.message_id} (user {partner_id}) for video note")
            channel_message += f"📜 Label: {label_text}\n🎥 Video note sent\n"
        elif message.sticker:
            print(f"🏷️ Forwarding sticker from {user_id} to {partner_id}")
            label_text = f"Partner {gender_emoji}:"
            await bot.send_message(
                chat_id=partner_id,
                text=label_text,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            forwarded_message = await bot.send_sticker(
                chat_id=partner_id,
                sticker=message.sticker.file_id,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            message_id_map[user_id][message.message_id] = forwarded_message.message_id
            message_id_map[partner_id][forwarded_message.message_id] = message.message_id
            print(f"📌 Mapped message ID {message.message_id} (user {user_id}) to {forwarded_message.message_id} (user {partner_id}) for sticker")
            channel_message += f"📜 Label: {label_text}\n🏷️ Sticker sent\n"
        if forwarded_message and hasattr(forwarded_message, 'message_id') and message.content_type not in ('video_note', 'sticker'):
            message_id_map[user_id][message.message_id] = forwarded_message.message_id
            message_id_map[partner_id][forwarded_message.message_id] = message.message_id
            print(f"📌 Mapped message ID {message.message_id} (user {user_id}) to {forwarded_message.message_id} (user {partner_id})")
        else:
            print(f"⚠️ Failed to map message ID for {user_id}: No valid forwarded_message")
    except Exception as e:
        print(f"❌ Error forwarding message from {user_id} to {partner_id}: {e}")
        await message.answer("⚠️ Failed to send message. Please try again.")

    try:
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=channel_message,
            parse_mode="Markdown"
        )
        if message.photo:
            await bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=message.photo[-1].file_id,
                caption=message.caption or ""
            )
        elif message.document:
            await bot.send_document(
                chat_id=CHANNEL_ID,
                document=message.document.file_id,
                caption=message.caption or ""
            )
        elif message.video:
            await bot.send_video(
                chat_id=CHANNEL_ID,
                video=message.video.file_id,
                caption=message.caption or ""
            )
        elif message.audio:
            await bot.send_audio(
                chat_id=CHANNEL_ID,
                audio=message.audio.file_id,
                caption=message.caption or ""
            )
        elif message.voice:
            await bot.send_voice(
                chat_id=CHANNEL_ID,
                voice=message.voice.file_id,
                caption=message.caption or ""
            )
        elif message.video_note:
            await bot.send_video_note(
                chat_id=CHANNEL_ID,
                video_note=message.video_note.file_id
            )
        elif message.sticker:
            await bot.send_sticker(
                chat_id=CHANNEL_ID,
                sticker=message.sticker.file_id
            )
        print(f"📢 Message logged to channel {CHANNEL_ID} from user {user_id} to {partner_id}")
    except Exception as e:
        print(f"❌ Error logging message to channel {CHANNEL_ID}: {e}")

@router.message(F.chat.type.in_({"group", "supergroup"}))
async def ignore_group_messages(message: Message):
    pass

async def set_bot_commands():
    commands = [
        BotCommand(command="start", description="Start the bot"),
        BotCommand(command="begin", description="Begin your journey"),
        BotCommand(command="setup", description="Set up your preferences"),
        BotCommand(command="help", description="Get help or assistance"),
        BotCommand(command="end", description="End your session"),
        BotCommand(command="group", description="View group information and options")
    ]
    await bot.set_my_commands(commands, scope=BotCommandScopeAllPrivateChats())
    print("✅ Bot commands set for private chats only")

@router.callback_query(F.data == "age")
async def handle_age(callback: CallbackQuery):
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"selected_age_{age}") for age in range(row_start, row_start + 5)]
            for row_start in range(18, 100, 5)
        ]
    )
    age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")])
    await callback.message.edit_text(text="📅 Select your age:", reply_markup=age_keyboard)
    await callback.answer()

@router.callback_query(F.data == "gender")
async def handle_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="selected_gender_male")],
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="selected_gender_female")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(text="🚻 Please indicate your Gender:", reply_markup=gender_keyboard)
    await callback.answer()

@router.callback_query(F.data == "religion")
async def handle_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox", callback_data="selected_religion_orthodox")],
            [InlineKeyboardButton(text="Muslim", callback_data="selected_religion_muslim")],
            [InlineKeyboardButton(text="Protestant", callback_data="selected_religion_protestant")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(text="🙏 Please select your religion:", reply_markup=religion_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("selected_age_"))
async def handle_age_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_age = callback.data.split("_")[-1]
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["age"] = selected_age
    update_user_data_now(user_id)
    await callback.answer(text=f"Your age is {selected_age}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_gender(callback)

@router.callback_query(F.data.startswith("selected_gender_"))
async def handle_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1]
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["gender"] = selected_gender
    update_user_data_now(user_id)
    await callback.answer(text=f"You selected {selected_gender}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_religion(callback)

@router.callback_query(F.data.startswith("selected_religion_"))
async def handle_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_religion = callback.data.split("_")[-1].replace("_", " ").capitalize()
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
            "Returning to the Setup menu..."
        )
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await asyncio.sleep(5)
    await handle_back_to_setup(callback)

@router.callback_query(F.data == "partner_age")
async def handle_partner_minimum_age(callback: CallbackQuery):
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"partner_min_age_{age}") for age in range(row_start, row_start + 5)]
            for row_start in range(18, 100, 5)
        ]
    )
    age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")])
    await callback.message.edit_text(text="📅 Select the **minimum age** for the partner:", reply_markup=age_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_min_age_"))
async def handle_partner_maximum_age(callback: CallbackQuery):
    user_id = callback.from_user.id
    min_age = int(callback.data.split("_")[-1])
    user_data.setdefault(user_id, {}).setdefault("partner", {})["min_age"] = min_age
    update_user_data_now(user_id)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    max_age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"partner_max_age_{age}") for age in range(row_start, row_start + 5) if age >= min_age]
            for row_start in range(18, 100, 5)
        ]
    )
    max_age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="partner_age")])
    await callback.message.edit_text(text=f"📅 Selected minimum age: **{min_age}**\nNow, select the **maximum age** for the partner:", reply_markup=max_age_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_max_age_"))
async def handle_partner_age_range(callback: CallbackQuery):
    user_id = callback.from_user.id
    max_age = int(callback.data.split("_")[-1])
    min_age = user_data[user_id]["partner"].get("min_age", None)
    if min_age is None:
        await callback.message.answer("❌ Minimum age not set. Please start from minimum age selection.")
        return
    user_data[user_id]["partner"]["max_age"] = max_age
    update_user_data_now(user_id)
    await callback.answer(text=f"🎉 Partner age range set: From {min_age} to {max_age}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_gender(callback)

@router.callback_query(F.data == "partner_gender")
async def handle_partner_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="partner_gender_male")],
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="partner_gender_female")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(text="🚻 Please select your partner's gender:", reply_markup=gender_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_gender_"))
async def handle_partner_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1]
    if user_id not in user_data:
        user_data[user_id] = {}
    if "partner" not in user_data[user_id]:
        user_data[user_id]["partner"] = {}
    user_data[user_id]["partner"]["gender"] = selected_gender
    update_user_data_now(user_id)
    await callback.answer(text=f"🎉 Partner's Gender set to: {selected_gender.capitalize()}", show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_religion(callback)

@router.callback_query(F.data == "partner_religion")
async def handle_partner_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox", callback_data="partner_religion_orthodox")],
            [InlineKeyboardButton(text="Muslim", callback_data="partner_religion_muslim")],
            [InlineKeyboardButton(text="Protestant", callback_data="partner_religion_protestant")],
            [InlineKeyboardButton(text="Any", callback_data="partner_religion_Any")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(text="🙏 Please select your partner's religion:", reply_markup=religion_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_religion_"))
async def handle_partner_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_partner_religion = callback.data.split("_")[-1].replace("_", " ").capitalize()
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
            "Returning to the Setup menu..."
        )
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await asyncio.sleep(5)
    await handle_back_to_setup(callback)

@router.callback_query(F.data == "show_setup")
async def handle_show_setup(callback: CallbackQuery):
    if callback.message.text.startswith("🛠️ Here is your setup:"):
        await callback.answer(text="⚠️ You are already in the Show Setup menu!", show_alert=True)
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
        f"🛠️ Here is your setup:\n"
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

async def periodic_save():
    while True:
        await asyncio.sleep(60)
        await save_user_data()
        print("🔄 Performed periodic backup of user data")

## From scheduling script
@dp.message(Command('start'))
async def start_command(message: types.Message):
    await message.reply("Bot is running and will send stickers and messages at scheduled times.")

@dp.message(F.sticker)
async def sticker_handler(message: types.Message):
    sticker_id = message.sticker.file_id
    await message.reply(f"Sticker ID: {sticker_id}")
    logging.info(f"Received sticker with file_id: {sticker_id}")

# Main function (modified to include scheduling task)
async def main():
    await load_user_data()
    print("🤖 Bot is running...")
    print("💾 Individual data points will be saved immediately upon change")
    print("💾 Automatic backups will occur every minute")
    await set_bot_commands()
    periodic_save_task = asyncio.create_task(periodic_save())
    scheduling_task = asyncio.create_task(send_scheduled_message())  # Added scheduling task
    try:
        async with bot:
            await dp.start_polling(bot)
    except KeyboardInterrupt:
        await save_user_data()
        print("💾 Final save completed before shutdown")
    finally:
        periodic_save_task.cancel()
        scheduling_task.cancel()
        try:
            await periodic_save_task
            await scheduling_task
        except asyncio.CancelledError:
            pass
        print("👋 Bot has shut down gracefully")

if __name__ == "__main__":
    asyncio.run(main())
