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
import asyncio
import os
import datetime
from motor.motor_asyncio import AsyncIOMotorClient

# Constants for limits
MAX_ACTIVE_USERS = 400
MAX_CONCURRENT_MATCHES = 200

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

# Function to save all user data to MongoDB
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

# Function to update a single user's data in MongoDB
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

# Function for immediate (non-awaited) saving of a single user's data
def update_user_data_now(user_id):
    asyncio.create_task(update_user_data(user_id))

# Function to load user data from MongoDB
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

# Helper function to check if a user is a group member
async def is_group_member(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']  # Translated "退出" to "left" and "被封禁" to "kicked"
    except Exception as e:
        print(f"Error checking group membership for user {user_id}: {e}")  # Translated error message
        return False

# Function to send join group message
async def send_join_group_message(message: Message):
    join_button = InlineKeyboardButton(text="Join Group", url=GROUP_INVITE_LINK)  # Translated "加入群组" to "Join Group"
    join_keyboard = InlineKeyboardMarkup(inline_keyboard=[[join_button]])
    await message.answer(
        text="Please join the group to use the bot.",  # Translated "请加入群组以使用机器人。"
        reply_markup=join_keyboard
    )

# Helper function to check if setup is complete
def is_setup_complete(user_id):
    if user_id not in user_data:
        return False, ["Age", "Gender", "Religion", "Partner Minimum Age", "Partner Maximum Age", "Partner Gender", "Partner Religion"]  # Translated "年龄" to "Age", "性别" to "Gender", "宗教" to "Religion"
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

# Helper function to get user state
def get_user_state(user_id):
    if user_id in active_matches:  # Fixed syntax error: removed extra "in"
        return "chatting"
    elif user_id in waiting_users:
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
            [KeyboardButton(text=action_text), KeyboardButton(text="⚙️ Setup")],  # Translated "设置" to "Setup"
            [KeyboardButton(text="❓ Help")],  # Translated "帮助" to "Help"
        ],
        resize_keyboard=True
    )

# Define the Inline Keyboard for Setup options
def get_setup_inline_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Your Profile", callback_data="your_setup")],  # Translated "你的个人资料" to "Your Profile"
            [InlineKeyboardButton(text="Partner Profile", callback_data="partner_setup")],
            [InlineKeyboardButton(text="Show Profile", callback_data="show_setup")],  # Translated "显示个人资料" to "Show Profile"
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
    welcome_text = "👋 Welcome to our matching bot! Find your perfect match based on your preferences.\n"  # Translated welcome message
    if current_state == "idle":
        welcome_text += "Press 'Setup' to configure your preferences."  # Translated
    elif current_state == "searching":
        welcome_text += "You are already searching for a partner. Press 'Stop Searching' to cancel."  # Translated
    elif current_state == "chatting":
        welcome_text += "You are currently in a chat session. Press 'End Chat' to terminate the session."  # Translated
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
            text="⚙️ Please select your setup options:",  # Translated "请选择你的设置选项："
            reply_markup=get_setup_inline_keyboard()
        )
    elif isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(
            text="⚙️ Please select your setup options:",  # Translated "请选择你的设置选项："
            reply_markup=get_setup_inline_keyboard()
        )
        await message_or_callback.answer()

# Handle "Your Setup" inline button
@router.callback_query(F.data == "your_setup")
async def handle_your_setup(callback: CallbackQuery):
    inline_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Age", callback_data="age")],  # Translated "年龄" to "Age"
            [InlineKeyboardButton(text="Gender", callback_data="gender")],  # Translated "性别" to "Gender"
            [InlineKeyboardButton(text="Religion", callback_data="religion")],  # Translated "宗教" to "Religion"
            [InlineKeyboardButton(text="⬅️ Back", callback_data="setup")],  # Translated "返回" to "Back"
        ]
    )
    await callback.message.edit_text(
        text="🔧 You selected 'Your Setup'. Please choose an option below to configure:",  # Translated message
        reply_markup=inline_keyboard
    )
    await callback.answer()

# Handle "Partner Setup" inline button
@router.callback_query(F.data == "partner_setup")
async def handle_partner_setup(callback: CallbackQuery):
    partner_setup_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Age", callback_data="partner_age")],  # Translated "年龄" to "Age"
            [InlineKeyboardButton(text="Gender", callback_data="partner_gender")],  # Translated "性别" to "Gender"
            [InlineKeyboardButton(text="Religion", callback_data="partner_religion")],  # Translated "宗教" to "Religion"
            [InlineKeyboardButton(text="⬅️ Back", callback_data="setup")],  # Translated "返回" to "Back"
        ]
    )
    await callback.message.edit_text(
        text="🤝 You selected 'Partner Setup'. Configure partner preferences below:",  # Translated message
        reply_markup=partner_setup_keyboard
    )
    await callback.answer()

# Handle "Back to Setup" inline button
@router.callback_query(F.data == "setup")
async def handle_back_to_setup(callback: CallbackQuery):
    await callback.message.edit_text(
        text="⚙️ Please select your setup options:",  # Translated "请选择你的设置选项："
        reply_markup=get_setup_inline_keyboard()
    )
    await callback.answer()

# Function to check if matching is allowed based on limits
def can_attempt_match():
    active_users = len(waiting_users) + len(active_matches)
    active_match_count = len(active_matches) // 2
    return active_users < MAX_ACTIVE_USERS and active_match_count < MAX_CONCURRENT_MATCHES

# Modified start_searching with limit checks
async def start_searching(message: Message, user_id: int):
    is_complete, missing_fields = is_setup_complete(user_id)
    if not is_complete:
        await message.answer(
            text=f"⚠️ Please complete your setup before starting a match. Missing fields:\n- {', '.join(missing_fields)}\nRedirecting to setup menu...",  # Translated message
            reply_markup=get_main_keyboard(state="idle")
        )
        await show_setup_menu(message)
        return False
    
    active_users = len(waiting_users) + len(active_matches)
    if active_users >= MAX_ACTIVE_USERS:
        await message.answer(
            "⚠️ The bot has reached the maximum number of active users (400). Please try again later.",  # Translated message
            reply_markup=get_main_keyboard(state="idle")
        )
        return False

    waiting_start_times[user_id] = datetime.datetime.now()
    waiting_users.add(user_id)
    await message.answer(
        "🔍 Waiting for a partner. You will be matched when a suitable partner is found.",  # Translated message
        reply_markup=get_main_keyboard(state="searching")
    )
    
    if can_attempt_match():
        await attempt_match(user_id)
    else:
        await message.answer(
            "⏳ The current number of active matches has reached the maximum (200). You will be matched when a slot becomes available."  # Translated message
        )
    return True

# Modified attempt_match with limit checks
async def attempt_match(user_id):
    if not can_attempt_match():
        print(f"⚠️ Cannot attempt match for user {user_id}: limits reached (active users: {len(waiting_users) + len(active_matches)}, matches: {len(active_matches) // 2})")  # Translated message
        return False
    
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
                f"👤 Partner's setup:\n"
                f"📅 Age: {user_data_2.get('age', 'Not set')}\n"
                f"🚻 Gender: {user_data_2.get('gender', 'Not set')}\n"
                f"🙏 Religion: {user_data_2.get('religion', 'Not set')}\n"
                "You can start sending messages."
            ),  # Translated message
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
            ),  # Translated message
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
        )  # Translated message
        try:
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=channel_message
            )
            print(f"📢 Match logged to channel {CHANNEL_ID} for users {user_id} and {match_id}")  # Translated message
        except Exception as e:
            print(f"❌ Error logging match to channel {CHANNEL_ID}: {e}")  # Translated message
        return True
    return False

# Atomic find_match function
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

        user_age = int(user_prefs.get("age", 0))
        user_gender = user_prefs.get("gender", "Not set").lower()
        user_religion = user_prefs.get("religion", "Not set").lower()  # Made case-insensitive
        candidate_age = int(candidate_prefs.get("age", 0))
        candidate_gender = candidate_prefs.get("gender", "Not set").lower()
        candidate_religion = candidate_prefs.get("religion", "Not set").lower()  # Made case-insensitive
        
        user_partner_prefs = user_prefs.get("partner", {})
        candidate_partner_prefs = candidate_prefs.get("partner", {})

        user_min_age = int(user_partner_prefs.get("min_age", 0))
        user_max_age = int(user_partner_prefs.get("max_age", 100))
        candidate_min_age = int(candidate_partner_prefs.get("min_age", 0))
        candidate_max_age = int(candidate_partner_prefs.get("max_age", 100))

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

# Handle matching buttons and commands with membership check for Begin
@router.message(F.chat.type == "private", F.text.in_({BEGIN_TEXT, STOP_SEARCHING_TEXT, END_CHAT_TEXT, "/begin", "/end"}))
async def handle_matching_button(message: Message):
    user_id = message.from_user.id
    text = message.text
    current_state = get_user_state(user_id)
    if text in [BEGIN_TEXT, "/begin"]:
        if current_state == "searching":
            await message.answer(
                "🔍 You are already searching for a partner. Please wait.",  # Translated message
                reply_markup=get_main_keyboard(state="searching")
            )
        elif current_state != "idle":
            await message.answer(
                "⚠️ Invalid operation for current state.",  # Translated message
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
                "⚠️ Invalid operation for current state.",  # Translated message
                reply_markup=get_main_keyboard(state=current_state)
            )
            return
        waiting_users.remove(user_id)
        waiting_start_times.pop(user_id, None)
        await message.answer(
            "🛑 You have stopped searching.",  # Translated message
            reply_markup=get_main_keyboard(state="idle")
        )
    elif text == END_CHAT_TEXT or text == "/end":
        if current_state != "chatting":
            await message.answer(
                "⚠️ Invalid operation for current state.",  # Translated message
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
            "❌ You have ended the session. You can press 'Begin' again to find a new partner.",  # Translated message
            reply_markup=get_main_keyboard(state="idle")
        )
        await bot.send_message(
            chat_id=match_id,
            text="❌ Your partner has ended the session. You can press 'Begin' again to find a new partner.",  # Translated message
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
        )  # Translated message
    )

# Forward messages
@router.message(F.chat.type == "private", F.text | F.document | F.photo | F.video | F.audio | F.voice | F.video_note | F.sticker)
async def forward_messages(message: Message):
    user_id = message.from_user.id
    current_state = get_user_state(user_id)
    print(f"📩 Received message from {user_id}, type: {message.content_type}, state: {current_state}")  # Translated message
    print(f"📋 Active matches: {active_matches}")
    print(f"🗂️ Current message_id_map: {message_id_map}")

    if current_state == "chatting":
        partner_id = active_matches[user_id]
        message_id_map.setdefault(user_id, {})
        message_id_map.setdefault(partner_id, {})
        sender_gender = user_data.get(user_id, {}).get("gender", "Not set")
        gender_emoji = get_gender_emoji(sender_gender)
        label = f"Partner {gender_emoji}: "  # Translated "伙伴" to "Partner"
        reply_to_message_id = None
        reply_info = ""
        if message.reply_to_message:
            original_reply_id = message.reply_to_message.message_id
            print(f"↩️ Detected reply from {user_id} to message {original_reply_id}")  # Translated message
            reply_to_message_id = message_id_map.get(user_id, {}).get(original_reply_id)
            if not reply_to_message_id:
                print(f"⚠️ No mapped message ID found for reply from {user_id} to {original_reply_id}")  # Translated message
                reply_info = f" (reply to message ID {original_reply_id}, mapping not found)"  # Translated message
            else:
                print(f"✅ Found mapped reply_to_message_id for user {user_id}: {reply_to_message_id}")  # Translated message
                reply_info = f" (reply to message ID {reply_to_message_id})"  # Translated message
        user_info = await bot.get_chat(user_id)
        sender_name = user_info.first_name or user_info.username or f"User {user_id}"
        message_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        channel_message = f"💬 **Message** at {message_time}\n👤 From: {sender_name} (ID: {user_id}) to User ID: {partner_id}{reply_info}\n"  # Translated message
        try:
            forwarded_message = None
            if message.text:
                print(f"📝 Forwarding text message from {user_id} to {partner_id}")  # Translated message
                modified_text = label + message.text
                forwarded_message = await bot.send_message(
                    chat_id=partner_id,
                    text=modified_text,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"📜 Text: {message.text}\n"  # Translated "文本" to "Text"
            elif message.photo:
                print(f"📸 Forwarding photo from {user_id} to {partner_id}")  # Translated message
                caption = message.caption or ""
                modified_caption = label + caption
                forwarded_message = await bot.send_photo(
                    chat_id=partner_id,
                    photo=message.photo[-1].file_id,
                    caption=modified_caption,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"🖼️ Photo sent\n"  # Translated "已发送照片" to "Photo sent"
                if message.caption:
                    channel_message += f"📝 Caption: {message.caption}\n"  # Translated "标题" to "Caption"
            elif message.document:
                print(f"📄 Forwarding document from {user_id} to {partner_id}")  # Translated message
                caption = message.caption or ""
                modified_caption = label + caption
                forwarded_message = await bot.send_document(
                    chat_id=partner_id,
                    document=message.document.file_id,
                    caption=modified_caption,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"📎 Document: {message.document.file_name or 'Unnamed document'}\n"  # Translated "文档" to "Document" and "未命名文档" to "Unnamed document"
            elif message.video:
                print(f"🎥 Forwarding video from {user_id} to {partner_id}")  # Translated message
                caption = message.caption or ""
                modified_caption = label + caption
                forwarded_message = await bot.send_video(
                    chat_id=partner_id,
                    video=message.video.file_id,
                    caption=modified_caption,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"🎥 Video sent\n"  # Translated "已发送视频" to "Video sent"
                if message.caption:
                    channel_message += f"📝 Caption: {message.caption}\n"  # Translated "标题" to "Caption"
            elif message.audio:
                print(f"🎵 Forwarding audio from {user_id} to {partner_id}")  # Translated message
                caption = message.caption or ""
                modified_caption = label + caption
                forwarded_message = await bot.send_audio(
                    chat_id=partner_id,
                    audio=message.audio.file_id,
                    caption=modified_caption,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"🎵 Audio sent\n"  # Translated "已发送音频" to "Audio sent"
                if message.caption:
                    channel_message += f"📝 Caption: {message.caption}\n"  # Translated "标题" to "Caption"
            elif message.voice:
                print(f"🎙️ Forwarding voice message from {user_id} to {partner_id}")  # Translated message
                caption = message.caption or ""
                modified_caption = label + caption
                forwarded_message = await bot.send_voice(
                    chat_id=partner_id,
                    voice=message.voice.file_id,
                    caption=modified_caption,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=True
                )
                channel_message += f"🎙️ Voice message sent\n"  # Translated "已发送语音消息" to "Voice message sent"
                if message.caption:
                    channel_message += f"📝 Caption: {message.caption}\n"  # Translated "标题" to "Caption"
            elif message.video_note:
                print(f"🎥 Forwarding video note from {user_id} to {partner_id}")  # Translated message
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
                print(f"📌 Mapped message ID {message.message_id} (user {user_id}) to {forwarded_message.message_id} (user {partner_id}) for video note")  # Translated message
                channel_message += f"📜 Label: {label_text}\n🎥 Video note sent\n"  # Translated "标签" to "Label" and "已发送视频笔记" to "Video note sent"
            elif message.sticker:
                print(f"🏷️ Forwarding sticker from {user_id} to {partner_id}")  # Translated message
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
                print(f"📌 Mapped message ID {message.message_id} (user {user_id}) to {forwarded_message.message_id} (user {partner_id}) for sticker")  # Translated message
                channel_message += f"📜 Label: {label_text}\n🏷️ Sticker sent\n"  # Translated "标签" to "Label" and "已发送贴纸" to "Sticker sent"
            if forwarded_message and hasattr(forwarded_message, 'message_id') and message.content_type not in ('video_note', 'sticker'):
                message_id_map[user_id][message.message_id] = forwarded_message.message_id
                message_id_map[partner_id][forwarded_message.message_id] = message.message_id
                print(f"📌 Mapped message ID {message.message_id} (user {user_id}) to {forwarded_message.message_id} (user {partner_id})")  # Translated message
            else:
                print(f"⚠️ Could not map message ID for {user_id}: no valid forwarded_message")  # Translated message
        except Exception as e:
            print(f"❌ Error forwarding message from {user_id} to {partner_id}: {e}")  # Translated message
            await message.answer("⚠️ Failed to send message. Please try again.")  # Translated message

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
            print(f"📢 Message from user {user_id} to {partner_id} logged to channel {CHANNEL_ID}")  # Translated message
        except Exception as e:
            print(f"❌ Error logging message to channel {CHANNEL_ID}: {e}")  # Translated message
    elif current_state == "searching":
        await message.answer(
            "🔍 You are already searching for a partner. Please wait.",  # Translated message
            reply_markup=get_main_keyboard(state="searching")
        )
    else:
        await message.answer(
            "⚠️ You are not currently chatting with anyone. Press 'Begin' to find a partner.",  # Translated message
            reply_markup=get_main_keyboard(state="idle")
        )

# Ignore group messages
@router.message(F.chat.type.in_({"group", "supergroup"}))
async def ignore_group_messages(_message: Message):
    pass

# Callback query handlers
@router.callback_query(F.data == "age")
async def handle_age(callback: CallbackQuery):
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"selected_age_{age}") for age in range(row_start, row_start + 5)]
            for row_start in range(18, 100, 5)
        ]
    )
    age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")])  # Translated "返回" to "Back"
    await callback.message.edit_text(text="📅 Choose your age:", reply_markup=age_keyboard)  # Translated "选择你的年龄："
    await callback.answer()

@router.callback_query(F.data == "gender")
async def handle_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="selected_gender_male")],  # Translated "男" to "Male"
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="selected_gender_female")],  # Translated "女" to "Female"
            [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")],  # Translated "返回" to "Back"
        ]
    )
    await callback.message.edit_text(text="🚻 Please specify your gender:", reply_markup=gender_keyboard)  # Translated "请指明你的性别："
    await callback.answer()

@router.callback_query(F.data == "religion")
async def handle_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox", callback_data="selected_religion_orthodox")],  # Translated "东正教" to "Orthodox"
            [InlineKeyboardButton(text="Muslim", callback_data="selected_religion_muslim")],  # Translated "穆斯林" to "Muslim"
            [InlineKeyboardButton(text="Protestant", callback_data="selected_religion_protestant")],  # Translated "新教" to "Protestant"
            [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")],  # Translated "返回" to "Back"
        ]
    )
    await callback.message.edit_text(text="🙏 Please select your religion:", reply_markup=religion_keyboard)  # Translated "请选择你的宗教："
    await callback.answer()

@router.callback_query(F.data.startswith("selected_age_"))
async def handle_age_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_age = callback.data.split("_")[-1]
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["age"] = selected_age
    update_user_data_now(user_id)
    await callback.answer(text=f"Your age is {selected_age}", show_alert=True)  # Translated message
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
    await callback.answer(text=f"You selected {selected_gender}", show_alert=True)  # Translated message
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
            "Returning to setup menu..."
        )  # Translated message
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
    age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")])  # Translated "返回" to "Back"
    await callback.message.edit_text(text="📅 Choose the **minimum age** for your partner:", reply_markup=age_keyboard)  # Translated "选择伙伴的**最小年龄**："
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
    max_age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="partner_age")])  # Translated "返回" to "Back"
    await callback.message.edit_text(text=f"📅 Minimum age selected: **{min_age}**\nNow, choose the **maximum age** for your partner:", reply_markup=max_age_keyboard)  # Translated message
    await callback.answer()

@router.callback_query(F.data.startswith("partner_max_age_"))
async def handle_partner_age_range(callback: CallbackQuery):
    user_id = callback.from_user.id
    max_age = int(callback.data.split("_")[-1])
    min_age = user_data[user_id]["partner"].get("min_age", None)
    if min_age is None:
        await callback.message.answer("❌ Minimum age not set. Please start from selecting minimum age.")  # Translated message
        return
    user_data[user_id]["partner"]["max_age"] = max_age
    update_user_data_now(user_id)
    await callback.answer(text=f"🎉 Partner age range set: from {min_age} to {max_age}", show_alert=True)  # Translated message
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_gender(callback)

@router.callback_query(F.data == "partner_gender")
async def handle_partner_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="partner_gender_male")],  # Translated "男" to "Male"
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="partner_gender_female")],  # Translated "女" to "Female"
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],  # Translated "返回" to "Back"
        ]
    )
    await callback.message.edit_text(text="🚻 Please select your partner gender:", reply_markup=gender_keyboard)  # Translated "请选择你的伙伴性别："
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
    await callback.answer(text=f"🎉 Partner gender set to: {selected_gender.capitalize()}", show_alert=True)  # Translated message
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_religion(callback)

@router.callback_query(F.data == "partner_religion")
async def handle_partner_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox", callback_data="partner_religion_orthodox")],  # Translated "东正教" to "Orthodox"
            [InlineKeyboardButton(text="Muslim", callback_data="partner_religion_muslim")],  # Translated "穆斯林" to "Muslim"
            [InlineKeyboardButton(text="Protestant", callback_data="partner_religion_protestant")],  # Translated "新教" to "Protestant"
            [InlineKeyboardButton(text="Any", callback_data="partner_religion_Any")],  # Translated "任意" to "Any"
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],  # Translated "返回" to "Back"
        ]
    )
    await callback.message.edit_text(text="🙏 Please select your partner religion:", reply_markup=religion_keyboard)  # Translated "请选择你的伙伴宗教："
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
            "Returning to setup menu..."
        )  # Translated message
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await asyncio.sleep(5)
    await handle_back_to_setup(callback)

@router.callback_query(F.data == "show_setup")
async def handle_show_setup(callback: CallbackQuery):
    if callback.message.text.startswith("👤 Here is your profile:"):  # Translated "这是你的个人资料："
        await callback.answer(text="⚠️ You are already in the show setup menu!", show_alert=True)  # Translated message
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
    )  # Translated message
    await callback.message.edit_text(
        text=result_text,
        reply_markup=get_setup_inline_keyboard()
    )
    await callback.answer()

# Periodic task to attempt matching for queued users
async def try_match_queued_users():
    if not can_attempt_match():
        print(f"⚠️ Cannot match queued users: limits reached (active users: {len(waiting_users) + len(active_matches)}, matches: {len(active_matches) // 2})")  # Translated message
        return
    sorted_waiting_users = sorted(
        waiting_users,
        key=lambda x: waiting_start_times.get(x, datetime.datetime.now())
    )
    for user_id in sorted_waiting_users:
        if user_id in waiting_users and is_setup_complete(user_id)[0]:
            await attempt_match(user_id)

async def periodic_save():
    while True:
        await asyncio.sleep(60)
        await save_user_data()
        print("🔄 Periodic backup of user data performed")  # Translated message

async def periodic_match_check():
    while True:
        await asyncio.sleep(30)
        if waiting_users:
            print(f"🔄 Checking for matches among {len(waiting_users)} waiting users")  # Translated message
            await try_match_queued_users()

async def main():
    await load_user_data()
    print("🤖 Bot is running...")  # Translated message
    print("💾 Individual data points will be saved immediately upon changes")  # Translated message
    print("💾 Automatic backups will be performed every minute")  # Translated message
    print("🔄 Matching checks for queued users will be performed every 30 seconds")  # Translated message
    await set_bot_commands()
    periodic_save_task = asyncio.create_task(periodic_save())
    periodic_match_task = asyncio.create_task(periodic_match_check())
    try:
        async with bot:
            await dp.start_polling(bot)
    except KeyboardInterrupt:
        await save_user_data()
        print("💾 Final save completed before shutdown")  # Translated message
    finally:
        periodic_save_task.cancel()
        periodic_match_task.cancel()
        try:
            await periodic_save_task
            await periodic_match_task
        except asyncio.CancelledError:
            pass
        print("👋 Bot has been gracefully shut down")  # Translated message

if __name__ == "__main__":
    asyncio.run(main())
