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
from motor.motor_asyncio import AsyncIOMotorClient

# Constants for limits
MAX_ACTIVE_USERS = 2000
MAX_CONCURRENT_MATCHES = 100

# Locks for synchronization
user_data_lock = asyncio.Lock()
active_matches_lock = asyncio.Lock()
waiting_users_lock = asyncio.Lock()
cooldown_tracker_lock = asyncio.Lock()

# Set bot commands for private chats only
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
    print("✅ Bot commands set for private chats only and removed from group chats")

# Bot token, channel ID, group ID, and group invite link setup
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')
GROUP_ID = os.getenv('GROUP_ID')
GROUP_INVITE_LINK = os.getenv('GROUP_INVITE_LINK')
MONGODB_URI = os.getenv('MONGODB_URI')

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

bot = Bot(token=BOT_TOKEN)
router = Router()
dp = Dispatcher()
dp.include_router(router)

# MongoDB setup
client = AsyncIOMotorClient(MONGODB_URI)
db = client['bot_database']
users_collection = db['users']

# Initialize data structures
user_data = {}
active_matches = {}
cooldown_tracker = {}
waiting_users = set()
waiting_start_times = {}
message_id_map = {}

# Button texts
BEGIN_TEXT = "🚀 Begin"
STOP_SEARCHING_TEXT = "⏹️ Stop Searching"
END_CHAT_TEXT = "🔚 End Chat"

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
    async with user_data_lock:
        users_to_save = {uid: data.copy() for uid, data in user_data.items()}
    async with active_matches_lock, waiting_users_lock:
        for user_id in users_to_save:
            if user_id in active_matches:
                users_to_save[user_id]["match_partner"] = active_matches[user_id]
            else:
                users_to_save[user_id]["match_partner"] = None
            if user_id in waiting_users:
                users_to_save[user_id]["waiting_since"] = waiting_start_times.get(user_id)
            else:
                users_to_save[user_id]["waiting_since"] = None
    for user_id, data in users_to_save.items():
        for attempt in range(3):
            try:
                await users_collection.replace_one(
                    {'_id': user_id},
                    {'_id': user_id, **data},
                    upsert=True
                )
                break
            except Exception as e:
                print(f"❌ Error saving user {user_id} to MongoDB (attempt {attempt+1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    print(f"❌ Failed to save user {user_id} after 3 attempts")
    print(f"✅ All user data saved to MongoDB")

# Function to load user data from MongoDB and rebuild states
async def load_user_data():
    global user_data, active_matches, waiting_users, waiting_start_times
    user_data = {}
    active_matches = {}
    waiting_users = set()
    waiting_start_times = {}
    try:
        async for document in users_collection.find():
            try:
                user_id = document['_id']
                user_data[user_id] = {k: v for k, v in document.items() if k != '_id'}
                match_partner = document.get("match_partner")
                if match_partner and match_partner in user_data:
                    if user_data[match_partner].get("match_partner") == user_id:
                        active_matches[user_id] = match_partner
                        active_matches[match_partner] = user_id
                    else:
                        print(f"⚠️ Inconsistent match for {user_id}: partner {match_partner} does not match back")
                        user_data[user_id]["match_partner"] = None
                waiting_since = document.get("waiting_since")
                if waiting_since:
                    waiting_users.add(user_id)
                    waiting_start_times[user_id] = waiting_since
            except Exception as e:
                print(f"❌ Error loading user {document.get('_id', 'unknown')}: {e}")
        async with active_matches_lock:
            for user_id in list(active_matches.keys()):
                if active_matches.get(active_matches[user_id]) != user_id:
                    del active_matches[user_id]
                    del active_matches[active_matches[user_id]]
                    user_data[user_id]["match_partner"] = None
                    print(f"🧹 Cleaned up orphaned match for {user_id}")
        print(f"✅ Loaded data for {len(user_data)} users from MongoDB")
        print(f"🔄 Recovered {len(active_matches) // 2} active matches and {len(waiting_users)} waiting users")
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")

# Function to update a single user's data in MongoDB, including state
async def update_user_data(user_id):
    if user_id in user_data:
        user_info = user_data[user_id].copy()
        async with active_matches_lock:
            if user_id in active_matches:
                user_info["match_partner"] = active_matches[user_id]
            else:
                user_info["match_partner"] = None
        async with waiting_users_lock:
            if user_id in waiting_users:
                user_info["waiting_since"] = waiting_start_times.get(user_id)
            else:
                user_info["waiting_since"] = None
        for attempt in range(3):
            try:
                await users_collection.replace_one(
                    {'_id': user_id},
                    {'_id': user_id, **user_info},
                    upsert=True
                )
                print(f"✅ Updated user {user_id} in MongoDB")
                return
            except Exception as e:
                print(f"❌ Error updating user {user_id} in MongoDB (attempt {attempt+1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    print(f"❌ Failed to update user {user_id} after 3 attempts")
    else:
        print(f"⚠️ User {user_id} not found in user_data")

# Function for immediate (non-awaited) saving of a single user's data
def update_user_data_now(user_id):
    asyncio.create_task(update_user_data(user_id))

# Helper function to check if a user is a group member
async def is_group_member(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception as e:
        print(f"Error checking group membership for user {user_id}: {e}")
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
        if "min_age" not in user_prefs["partner"] or user_prefs["partner"]["min_age"] == "Not set":
            missing_fields.append("Partner Minimum Age")
        if "max_age" not in user_prefs["partner"] or user_prefs["partner"]["max_age"] == "Not set":
            missing_fields.append("Partner Maximum Age")
        if "gender" not in user_prefs["partner"] or user_prefs["partner"]["gender"] == "Not set":
            missing_fields.append("Partner Gender")
        if "religion" not in user_prefs["partner"] or user_prefs["partner"]["religion"] == "Not set":
            missing_fields.append("Partner Religion")
    return len(missing_fields) == 0, missing_fields

# Helper function to get user state with consistency checks
def get_user_state(user_id):
    in_waiting = user_id in waiting_users
    in_active = user_id in active_matches
    if in_waiting and in_active:
        print(f"⚠️ User {user_id} is both in waiting_users and active_matches. Correcting state.")
        waiting_users.discard(user_id)
        return "chatting"
    elif in_active:
        return "chatting"
    elif in_waiting:
        return "searching"
    else:
        return "idle"

# Define the Reply Keyboard with dynamic state-based buttons
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

# Define the Inline Keyboard for Setup options
def get_setup_inline_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Your Profile", callback_data="your_setup")],
            [InlineKeyboardButton(text="Partner Profile", callback_data="partner_setup")],
            [InlineKeyboardButton(text="Show Profile", callback_data="show_setup")],
        ]
    )

# Define the /start command with membership check
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

# Handle "Setup" button or command
@router.message(F.chat.type == "private", F.text.in_({"⚙️ Setup", "/setup"}))
async def handle_setup(message: Message):
    await show_setup_menu(message)

async def show_setup_menu(message_or_callback):
    if isinstance(message_or_callback, Message):
        await message_or_callback.answer(
            text="⚙️ Please select your setup options:",
            reply_markup=get_setup_inline_keyboard()
        )
    elif isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(
            text="⚙️ Please select your setup options:",
            reply_markup=get_setup_inline_keyboard()
        )
        await message_or_callback.answer()

# Handle "Your Setup" inline button
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

# Handle "Partner Setup" inline button
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

# Handle "Back to Setup" inline button
@router.callback_query(F.data == "setup")
async def handle_back_to_setup(callback: CallbackQuery):
    await callback.message.edit_text(
        text="⚙️ Please select your setup options:",
        reply_markup=get_setup_inline_keyboard()
    )
    await callback.answer()

# Function to check if matching is allowed based on limits
async def can_attempt_match():
    async with waiting_users_lock, active_matches_lock:
        active_users = len(waiting_users) + len(active_matches)
        active_match_count = len(active_matches) // 2
        return active_users < MAX_ACTIVE_USERS and active_match_count < MAX_CONCURRENT_MATCHES

# Modified start_searching with immediate state update
async def start_searching(message: Message, user_id: int):
    async with user_data_lock:
        is_complete, missing_fields = is_setup_complete(user_id)
    if not is_complete:
        missing_fields_str = "\n- ".join(missing_fields)
        await message.answer(
            text=f"⚠️ Please complete your setup before starting a match. Missing fields:\n- {missing_fields_str}\nRedirecting to setup menu...",
            reply_markup=get_main_keyboard(state="idle")
        )
        await show_setup_menu(message)
        return False

    async with waiting_users_lock, active_matches_lock:
        active_users = len(waiting_users) + len(active_matches)
        if active_users >= MAX_ACTIVE_USERS:
            await message.answer(
                "⚠️ The bot has reached the maximum number of active users (600). Please try again later.",
                reply_markup=get_main_keyboard(state="idle")
            )
            return False
        waiting_start_times[user_id] = datetime.datetime.now()
        waiting_users.add(user_id)
    update_user_data_now(user_id)  # Save waiting state immediately

    await message.answer(
        "🔍 Waiting for a partner. You will be matched when a suitable partner is found.",
        reply_markup=get_main_keyboard(state="searching")
    )

    if await can_attempt_match():
        await attempt_match(user_id)
    else:
        await message.answer(
            "⏳ The current number of active matches has reached the maximum (300). You will be matched when a slot becomes available."
        )
    return True

# Modified attempt_match with immediate state update
async def attempt_match(user_id):
    now = datetime.datetime.now()
    async with waiting_users_lock:
        if user_id not in waiting_users:
            return False
        candidates = list(waiting_users - {user_id})
    async with user_data_lock:
        user_prefs = user_data.get(user_id, {})
        candidate_prefs = {cid: user_data.get(cid, {}) for cid in candidates}
    async with cooldown_tracker_lock:
        user_cooldowns = cooldown_tracker.get(user_id, {})

    match_id = find_match(user_id, candidates, user_prefs, candidate_prefs, user_cooldowns, now)
    if match_id:
        async with active_matches_lock, waiting_users_lock:
            if user_id in waiting_users and match_id in waiting_users:
                active_match_count = len(active_matches) // 2
                if active_match_count >= MAX_CONCURRENT_MATCHES:
                    print(f"⚠️ Cannot match {user_id} with {match_id}: maximum concurrent matches reached")
                    return False
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
                        f"👤 Partner's setup:\n"
                        f"📅 Age: {user_data_2.get('age', 'Not set')}\n"
                        f"🚻 Gender: {user_data_2.get('gender', 'Not set')}\n"
                        f"🙏 Religion: {user_data_2.get('religion', 'Not set')}\n"
                        "You can start sending messages."
                    ),
                    reply_markup=get_main_keyboard(state="chatting"),
                )
                await bot.send_message(
                    chat_id=match_id,
                    text=(
                        f"🎉 Match found!\n\n"
                        f"👤 Partner's setup:\n"
                        f"📅 Age: {user_data_1.get('age', 'Not set')}\n"
                        f"🚻 Gender: {user_data_1.get('gender', 'Not set')}\n"
                        f"🙏 Religion: {user_data_1.get('religion', 'Not set')}\n"
                        "You can start sending messages."
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
                    f"  - Partner Preferences:\n"
                    f"    - Age Range: {user_data_1.get('partner', {}).get('min_age', 'Not set')} to {user_data_1.get('partner', {}).get('max_age', 'Not set')}\n"
                    f"    - Gender: {user_data_1.get('partner', {}).get('gender', 'Not set')}\n"
                    f"    - Religion: {user_data_1.get('partner', {}).get('religion', 'Not set')}\n\n"
                    f"👤 User 2: {user_2_name} (ID: {match_id})\n"
                    f"  - Age: {user_data_2.get('age', 'Not set')}\n"
                    f"  - Gender: {user_data_2.get('gender', 'Not set')}\n"
                    f"  - Religion: {user_data_2.get('religion', 'Not set')}\n"
                    f"  - Partner Preferences:\n"
                    f"    - Age Range: {user_data_2.get('partner', {}).get('min_age', 'Not set')} to {user_data_2.get('partner', {}).get('max_age', 'Not set')}\n"
                    f"    - Gender: {user_data_2.get('partner', {}).get('gender', 'Not set')}\n"
                    f"    - Religion: {user_data_2.get('partner', {}).get('religion', 'Not set')}"
                )
                try:
                    await bot.send_message(
                        chat_id=CHANNEL_ID,
                        text=channel_message
                    )
                    print(f"📢 Match logged to channel {CHANNEL_ID} for users {user_id} and {match_id}")
                except Exception as e:
                    print(f"❌ Error logging match to channel {CHANNEL_ID}: {e}")
                
                # Save match state immediately
                update_user_data_now(user_id)
                update_user_data_now(match_id)
                return True
    return False

# Atomic find_match function with data snapshots
def find_match(user_id, candidates, user_prefs, candidate_prefs, user_cooldowns, now):
    for candidate_id in candidates:
        if candidate_id == user_id:
            continue
        candidate_data = candidate_prefs.get(candidate_id, {})
        if not candidate_data or "age" not in candidate_data or "gender" not in candidate_data or "religion" not in candidate_data:
            continue
        if candidate_id in user_cooldowns and now < user_cooldowns[candidate_id]:
            continue
        try:
            user_age = int(user_prefs["age"])
            candidate_age = int(candidate_data["age"])
        except (KeyError, ValueError):
            continue
        user_gender = user_prefs.get("gender", "Not set").lower()
        candidate_gender = candidate_data.get("gender", "Not set").lower()
        user_religion = user_prefs.get("religion", "Not set").lower()
        candidate_religion = candidate_data.get("religion", "Not set").lower()
        user_partner_prefs = user_prefs.get("partner", {})
        candidate_partner_prefs = candidate_data.get("partner", {})
        try:
            user_min_age = int(user_partner_prefs.get("min_age", 0))
            user_max_age = int(user_partner_prefs.get("max_age", 100))
            candidate_min_age = int(candidate_partner_prefs.get("min_age", 0))
            candidate_max_age = int(candidate_partner_prefs.get("max_age", 100))
        except (KeyError, ValueError):
            continue
        age_match = (
            user_min_age <= candidate_age <= user_max_age and
            candidate_min_age <= user_age <= candidate_max_age
        )
        if not age_match:
            continue
        user_partner_gender = user_partner_prefs.get("gender", "any").lower()
        candidate_partner_gender = candidate_partner_prefs.get("gender", "any").lower()
        gender_match = (
            (user_partner_gender == "any" or user_partner_gender == candidate_gender) and
            (candidate_partner_gender == "any" or candidate_partner_gender == user_gender)
        )
        if not gender_match:
            continue
        user_partner_religion = user_partner_prefs.get("religion", "any").lower()
        candidate_partner_religion = candidate_partner_prefs.get("religion", "any").lower()
        religion_match = (
            (user_partner_religion == "any" or user_partner_religion == candidate_religion) and
            (candidate_partner_religion == "any" or candidate_partner_religion == user_religion)
        )
        if not religion_match:
            continue
        return candidate_id
    return None

# Handle matching buttons and commands with immediate state updates
@router.message(F.chat.type == "private", F.text.in_({BEGIN_TEXT, STOP_SEARCHING_TEXT, END_CHAT_TEXT, "/begin", "/end"}))
async def handle_matching_button(message: Message):
    user_id = message.from_user.id
    text = message.text
    current_state = get_user_state(user_id)
    if text in [BEGIN_TEXT, "/begin"]:
        if current_state == "searching":
            await message.answer(
                "🔍 You are already searching for a partner. Please wait.",
                reply_markup=get_main_keyboard(state="searching")
            )
        elif current_state != "idle":
            await message.answer(
                "⚠️ Invalid operation for current state.",
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
                "⚠️ Invalid operation for current state.",
                reply_markup=get_main_keyboard(state=current_state)
            )
            return
        async with waiting_users_lock:
            if user_id in waiting_users:
                waiting_users.remove(user_id)
                waiting_start_times.pop(user_id, None)
        update_user_data_now(user_id)  # Save state immediately
        await message.answer(
            "🛑 You have stopped searching.",
            reply_markup=get_main_keyboard(state="idle")
        )
    elif text == END_CHAT_TEXT or text == "/end":
        if current_state != "chatting":
            await message.answer(
                "⚠️ Invalid operation for current state.",
                reply_markup=get_main_keyboard(state=current_state)
            )
            return
        async with active_matches_lock, cooldown_tracker_lock:
            if user_id in active_matches:
                match_id = active_matches.pop(user_id)
                active_matches.pop(match_id, None)
                cooldown_period = datetime.timedelta(hours=4)
                now = datetime.datetime.now()
                cooldown_tracker.setdefault(user_id, {})[match_id] = now + cooldown_period
                cooldown_tracker.setdefault(match_id, {})[user_id] = now + cooldown_period
                message_id_map.pop(user_id, None)
                message_id_map.pop(match_id, None)
        update_user_data_now(user_id)  # Save state immediately
        update_user_data_now(match_id)  # Save state immediately
        await message.answer(
            "❌ You have ended the session. You can press 'Begin' again to find a new partner.",
            reply_markup=get_main_keyboard(state="idle")
        )
        await bot.send_message(
            chat_id=match_id,
            text="❌ Your partner has ended the session. You can press 'Begin' again to find a new partner.",
            reply_markup=get_main_keyboard(state="idle")
        )
        asyncio.create_task(try_match_queued_users())

# Handle "Help" button or command
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

# Forward messages with improved error handling
@router.message(F.chat.type == "private", F.text | F.document | F.photo | F.video | F.audio | F.voice | F.video_note | F.sticker)
async def forward_messages(message: Message):
    user_id = message.from_user.id
    current_state = get_user_state(user_id)
    print(f"📩 Received message from {user_id}, type: {message.content_type}, state: {current_state}")
    print(f"📋 Active matches: {active_matches}")
    print(f"🗂️ Current message_id_map: {message_id_map}")

    if current_state == "chatting":
        async with active_matches_lock:
            partner_id = active_matches.get(user_id)
        if partner_id is None:
            await message.answer(
                "⚠️ You are not currently chatting with anyone. Press 'Begin' to find a partner.",
                reply_markup=get_main_keyboard(state="idle")
            )
            return

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
                print(f"⚠️ No mapped message ID found for reply from {user_id} to {original_reply_id}")
                reply_info = f" (reply to message ID {original_reply_id}, mapping not found)"
            else:
                print(f"✅ Found mapped reply_to_message_id for user {user_id}: {reply_to_message_id}")
                reply_info = f" (reply to message ID {reply_to_message_id})"

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
                print(f"⚠️ Could not map message ID for {user_id}: no valid forwarded_message")
        except Exception as e:
            print(f"❌ Error forwarding message from {user_id} to {partner_id}: {e}")
            await message.answer("⚠️ Failed to send message. Please try again later.")

        try:
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=channel_message
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
            print(f"📢 Message from user {user_id} to {partner_id} logged to channel {CHANNEL_ID}")
        except Exception as e:
            print(f"❌ Error logging message to channel {CHANNEL_ID}: {e}")
    elif current_state == "searching":
        await message.answer(
            "🔍 You are already searching for a partner. Please wait.",
            reply_markup=get_main_keyboard(state="searching")
        )
    else:
        await message.answer(
            "⚠️ You are not currently chatting with anyone. Press 'Begin' to find a partner.",
            reply_markup=get_main_keyboard(state="idle")
        )
  #       
@router.chat_member(F.chat.id == int(GROUP_ID))
async def handle_chat_member_update(update: ChatMemberUpdated):
    old_status = update.old_chat_member.status
    new_status = update.new_chat_member.status
    user = update.new_chat_member.user  # Get the user whose status changed
    user_id = user.id
    first_name = user.first_name or f"User {user_id}"  # Use first name, fallback to User {user_id}
    username = f"@{user.username}" if user.username else ""  # Include @username if available
    
    # Debug logging to confirm update details
    print(f"Received chat member update: user_id={user_id}, first_name={first_name}, username={username}, old_status={old_status}, new_status={new_status}")

    # Check if user was kicked (banned) by an admin, exclude bot or admin self-actions
    if old_status in ['member', 'administrator', 'creator'] and new_status == 'kicked' and not user.is_bot:
        try:
            # Format the message: first_name (@username) if username exists, else just first_name
            message_text = (
                f"{first_name} {username} is eliminated due to unsupported behaviour.\n"
                f"{first_name} {username} ተገቢ ባልሆነ ባህሪ ምክንያት ተወግዷል።"
            ).strip()
            # Prepare message entities for clickable name/username
            entities = []
            name_length = len(first_name)
            if username:
                # If username exists, make it a clickable mention (applies to both English and Amharic lines)
                entities.append({
                    "type": "mention",
                    "offset": name_length + 1,  # After first_name and space (English line)
                    "length": len(username)
                })
                entities.append({
                    "type": "mention",
                    "offset": name_length + len(f" {username} is eliminated due to unsupported behaviour.\n") + 1,  # After first_name and space (Amharic line)
                    "length": len(username)
                })
            else:
                # If no username, make first_name a clickable text_mention (applies to both English and Amharic lines)
                entities.append({
                    "type": "text_mention",
                    "offset": 0,
                    "length": name_length,
                    "user": user
                })
                entities.append({
                    "type": "text_mention",
                    "offset": len(f"{first_name} {username} is eliminated due to unsupported behaviour.\n"),
                    "length": name_length,
                    "user": user
                })
            # Send sticker to group (using a default Telegram sticker)
            await bot.send_sticker(
                chat_id=GROUP_ID,
                sticker="CAACAgEAAxkBAAE5E-xok7FWOS3t3jQUWxT3_Yw8QGgkNQACSQQAAmGwwEehsx6rufaXijYE"
            )
            # Send message to group with entities
            await bot.send_message(
                chat_id=GROUP_ID,
                text=message_text,
                entities=entities
            )
           
            # Log to channel (plain text, no entities needed)
            removal_time = datetime.datetime.now(pytz.timezone('Africa/Nairobi')).strftime("%Y-%m-%d %H:%M:%S")
            channel_message = (
                f"🚫 **User Removed** at {removal_time}\n"
                f"👤 User: {first_name} {username} (ID: {user_id})\n"
                f"📝 Reason: Eliminated due to unsupported behaviour\n"
                f"🚫 **ተጠቃሚ ተወግዷል** በ {removal_time}\n"
                f"👤 ተጠቃሚ: {first_name} {username} (መለያ: {user_id})\n"
                f"📝 ምክንያት: በአግባብ ባልሆነ ባህሪ ምክንያት ተወግዷል"
            )
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=channel_message
            )
            # Clean up user data
            async with active_matches_lock, waiting_users_lock, user_data_lock, cooldown_tracker_lock:
                if user_id in active_matches:
                    partner_id = active_matches.pop(user_id, None)
                    active_matches.pop(partner_id, None)
                    message_id_map.pop(user_id, None)
                    message_id_map.pop(partner_id, None)
                    if partner_id:
                        await bot.send_message(
                            chat_id=partner_id,
                            text=(
                                "❌ Your partner has been removed from the group. You can press 'Begin' to find a new partner.\n"
                                "❌ አጋርህ ከቡድኑ ተወግዷል። አዲስ አጋር ለመፈለግ 'ጀምር' ን መጫን ትችላለህ።"
                            ),
                            reply_markup=get_main_keyboard(state="idle")
                        )
                        update_user_data_now(partner_id)
                if user_id in waiting_users:
                    waiting_users.discard(user_id)
                    waiting_start_times.pop(user_id, None)
                if user_id in user_data:
                    del user_data[user_id]
                if user_id in cooldown_tracker:
                    del cooldown_tracker[user_id]
                update_user_data_now(user_id)  # Ensure user data is removed from MongoDB
            print(f"🚫 User {first_name} {username} (ID: {user_id}) removed from group and data cleaned up")
        except Exception as e:
            print(f"❌ Error handling user {user_id} removal: {e}")
    elif old_status in ['member', 'administrator', 'creator'] and new_status == 'left' and not user.is_bot:
        # Handle voluntary leave (clean up data without sending elimination message)
        try:
            async with active_matches_lock, waiting_users_lock, user_data_lock, cooldown_tracker_lock:
                if user_id in active_matches:
                    partner_id = active_matches.pop(user_id, None)
                    active_matches.pop(partner_id, None)
                    message_id_map.pop(user_id, None)
                    message_id_map.pop(partner_id, None)
                    if partner_id:
                        await bot.send_message(
                            chat_id=partner_id,
                            text=(
                                "❌ Your partner has left the group. You can press 'Begin' to find a new partner.\n"
                                "❌ አጋርህ ቡድኑን ለቆ ወጥቷል። አዲስ አጋር ለመፈለግ 'ጀምር' ን መጫን ትችላለህ።"
                            ),
                            reply_markup=get_main_keyboard(state="idle")
                        )
                        update_user_data_now(partner_id)
                if user_id in waiting_users:
                    waiting_users.discard(user_id)
                    waiting_start_times.pop(user_id, None)
                if user_id in user_data:
                    del user_data[user_id]
                if user_id in cooldown_tracker:
                    del cooldown_tracker[user_id]
                update_user_data_now(user_id)  # Ensure user data is removed from MongoDB
            print(f"👋 User {first_name} {username} (ID: {user_id}) left the group voluntarily and data cleaned up")
        except Exception as e:
            print(f"❌ Error handling user {user_id} voluntary leave: {e}")
# Callback query handlers
@router.callback_query(F.data == "age")
async def handle_age(callback: CallbackQuery):
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"selected_age_{age}") for age in range(row_start, row_start + 5)]
            for row_start in range(18, 100, 5)
        ]
    )
    age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")])
    await callback.message.edit_text(text="📅 Choose your age:", reply_markup=age_keyboard)
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
    await callback.message.edit_text(text="🚻 Please specify your gender:", reply_markup=gender_keyboard)
    await callback.answer()

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

@router.callback_query(F.data.startswith("selected_age_"))
async def handle_age_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_age = callback.data.split("_")[-1]
    async with user_data_lock:
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
    async with user_data_lock:
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
    async with user_data_lock:
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
    await callback.message.edit_text(text="📅 Choose the **minimum age** for your partner:", reply_markup=age_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_min_age_"))
async def handle_partner_maximum_age(callback: CallbackQuery):
    user_id = callback.from_user.id
    min_age = int(callback.data.split("_")[-1])
    async with user_data_lock:
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
        ]
    )
    max_age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="partner_age")])
    await callback.message.edit_text(text=f"📅 Minimum age selected: **{min_age}**\nNow, choose the **maximum age** for your partner:", reply_markup=max_age_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_max_age_"))
async def handle_partner_age_range(callback: CallbackQuery):
    user_id = callback.from_user.id
    max_age = int(callback.data.split("_")[-1])
    async with user_data_lock:
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

@router.callback_query(F.data == "partner_gender")
async def handle_partner_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="partner_gender_male")],
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="partner_gender_female")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(text="🚻 Please select your partner gender:", reply_markup=gender_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_gender_"))
async def handle_partner_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1]
    async with user_data_lock:
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

@router.callback_query(F.data == "partner_religion")
async def handle_partner_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox ☦️", callback_data="partner_religion_orthodox")],
            [InlineKeyboardButton(text="Muslim ☪️", callback_data="partner_religion_muslim")],
            [InlineKeyboardButton(text="Protestant ✝️", callback_data="partner_religion_protestant")],
            [InlineKeyboardButton(text="Any", callback_data="partner_religion_Any")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(text="🙏 Please select your partner religion:", reply_markup=religion_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_religion_"))
async def handle_partner_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_partner_religion = callback.data.split("_")[-1].replace("_", " ").capitalize()
    async with user_data_lock:
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
    await asyncio.sleep(5)
    await handle_back_to_setup(callback)

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

# Periodic task to attempt matching for queued users
async def try_match_queued_users():
    if not await can_attempt_match():
        print(f"⚠️ Cannot match queued users: limits reached (active users: {len(waiting_users) + len(active_matches)}, matches: {len(active_matches) // 2})")
        return
    sorted_waiting_users = sorted(
        waiting_users,
        key=lambda x: waiting_start_times.get(x, datetime.datetime.now())
    )
    for user_id in sorted_waiting_users:
        if user_id in waiting_users and is_setup_complete(user_id)[0]:
            await attempt_match(user_id)

# Periodic task to clean up expired cooldown entries
async def cleanup_cooldown_tracker():
    while True:
        await asyncio.sleep(300)  # every 5 minutes
        now = datetime.datetime.now()
        async with cooldown_tracker_lock:
            for user_id in list(cooldown_tracker.keys()):
                for partner_id in list(cooldown_tracker[user_id].keys()):
                    if cooldown_tracker[user_id][partner_id] < now:
                        del cooldown_tracker[user_id][partner_id]
                if not cooldown_tracker[user_id]:
                    del cooldown_tracker[user_id]
        print("🧹 Cleaned up expired cooldown entries")

# Periodic save task
async def periodic_save():
    while True:
        await asyncio.sleep(60)
        await save_user_data()
        print("🔄 Periodic backup of user data performed")

# Periodic match check task
async def periodic_match_check():
    while True:
        await asyncio.sleep(30)
        if waiting_users:
            print(f"🔄 Checking for matches among {len(waiting_users)} waiting users")
            await try_match_queued_users()

# Main function
async def main():
    await load_user_data()
    print("🤖 Bot is running...")
    print("💾 Individual data points will be saved immediately upon changes")
    print("💾 Automatic backups will be performed every minute")
    print("🔄 Matching checks for queued users will be performed every 30 seconds")
    await set_bot_commands()
    periodic_save_task = asyncio.create_task(periodic_save())
    periodic_match_task = asyncio.create_task(periodic_match_check())
    cleanup_task = asyncio.create_task(cleanup_cooldown_tracker())
    try:
        async with bot:
            await dp.start_polling(bot, allowed_updates=['message', 'callback_query', 'chat_member'])
    except KeyboardInterrupt:
        await save_user_data()
        print("💾 Final save completed before shutdown")
    finally:
        periodic_save_task.cancel()
        periodic_match_task.cancel()
        cleanup_task.cancel()
        try:
            await periodic_save_task
            await periodic_match_task
            await cleanup_task
        except asyncio.CancelledError:
            pass
        print("👋 Bot has been gracefully shut down")

if __name__ == "__main__":
    asyncio.run(main())
