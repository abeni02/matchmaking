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

# Translation dictionary for English and Amharic
texts = {
    "English": {
        "welcome": "👋 Welcome to our matchmaking bot! Discover your perfect match based on your preferences. \n",
        "setup_prompt": "Press 'Setup' to configure your preferences.",
        "searching_prompt": "You are currently searching for a partner. Press 'Stop Searching' to cancel.",
        "chatting_prompt": "You are currently in a chat session. Press 'End Chat' to end the session.",
        "setup_option": "⚙️ Please choose your setup option:",
        "your_setup": "Your profile",
        "partner_setup": "Partner Profile",
        "show_setup": "Show Profile",
        "age": "Age",
        "gender": "Gender",
        "religion": "Religion",
        "language": "Language",
        "back": "⬅️ Back",
        "select_age": "📅 Select your age:",
        "select_gender": "🚻 Please indicate your Gender:",
        "select_religion": "🙏 Please select your religion:",
        "select_language": "🌐 Please select your language:",
        "age_selected": "Your age is {age}",
        "gender_selected": "You selected {gender}",
        "religion_selected": "Your religion is {religion}",
        "language_selected": "Your language is {language}",
        "partner_min_age": "📅 Select the minimum age for the partner:",
        "partner_max_age": "📅 Selected minimum age: {min_age}\nNow, select the maximum age for the partner:",
        "partner_age_range_set": "🎉 Partner age range set: From {min_age} to {max_age}",
        "select_partner_gender": "🚻 Please select your partner's gender:",
        "partner_gender_set": "🎉 Partner's Gender set to: {gender}",
        "select_partner_religion": "🙏 Please select your partner's religion:",
        "partner_religion_set": "🎉 Partner's Religion set to: {religion}",
        "select_partner_language": "🌐 Please select your partner's language preference:",
        "partner_language_set": "🎉 Partner's language preference set to: {language}",
        "your_profile": "👤 Here is your Profile:",
        "partner_preferences": "🤝 Partner Preferences:",
        "age_range": "Age Range",
        "match_found": "🎉 Match found!",
        "partner_setup_info": "👤 Partner’s setup:",
        "start_messaging": "You can Start messaging.",
        "setup_incomplete": "⚠️ Please complete your setup before starting a match. Missing fields:\n- {fields}\nRedirecting to setup menu...",
        "waiting_for_partner": "🔍 Waiting for a partner. ",
        "already_searching": "🔍 You are already searching for a partner. Please wait.",
        "invalid_action": "⚠️ Invalid action for current state.",
        "stopped_searching": "🛑 You have stopped searching.",
        "session_ended": "❌ You have ended the session. You can 'Begin' again to find a new partner.",
        "partner_ended_session": "❌ Your partner has ended the session. You can 'Begin' again to find a new partner.",
        "not_in_session": "⚠️ You are not in an active session or searching.",
        "not_chatting": "⚠️ You are not currently chatting with anyone. Press 'Begin' to find a partner.",
        "help_text": (
            "💡 Need assistance? Here's what you can do:\n"
            " - 🚀 Begin: Start your journey (after completing setup).\n"
            " - ⏹️ Stop Searching: Stop looking for a partner.\n"
            " - 🔚 End Chat: Stop chatting with your partner.\n"
            " - ⚙️ Setup: Configure your preferences.\n"
            " - ❓ Help: Get guidance and information.\n"
            " - 📩 ask or feedback: @Ask_and_feedback_bot ."
        )
    },
    "Amharic": {
        "welcome": "እንኳን ወደ ተጋባችሁ ቦት በደህና መጡ! በእርስዎ ምርጫዎች ላይ ተመስርተው ፍጹም ግጥሚያዎን ያግኙ። \n",
        "setup_prompt": "ቅንብሮችዎን ለማዋቀር 'Setup' ን ይጫኑ።",
        "searching_prompt": "አሁን ባልደረባ እየፈለጉ ነው። ለመሰረዝ 'Stop Searching' ን ይጫኑ።",
        "chatting_prompt": "አሁን በውይይት ክፍለ ጊዜ ውስጥ ነዎት። ለመጨረስ 'End Chat' ን ይጫኑ።",
        "setup_option": "⚙️ እባክዎ የማዋቀሪያ አማራጭዎን ይምረጡ:",
        "your_setup": "የእርስዎ መገለጫ",
        "partner_setup": "የባልደረባ መገለጫ",
        "show_setup": "መገለጫውን አሳይ",
        "age": "ዕድሜ",
        "gender": "ጾታ",
        "religion": "ሃይማኖት",
        "language": "ቋንቋ",
        "back": "⬅️ ተመለስ",
        "select_age": "📅 ዕድሜዎን ይምረጡ:",
        "select_gender": "🚻 እባክዎ ጾታዎን ያመልክቱ:",
        "select_religion": "🙏 እባክዎ ሃይማኖትዎን ይምረጡ:",
        "select_language": "🌐 እባክዎ ቋንቋዎን ይምረጡ:",
        "age_selected": "ዕድሜዎ {age} ነው",
        "gender_selected": "እርስዎ {gender} መረጡ",
        "religion_selected": "ሃይማኖትዎ {religion} ነው",
        "language_selected": "ቋንቋዎ {language} ነው",
        "partner_min_age": "📅 ለባልደረባ ዝቅተኛውን ዕድሜ ይምረጡ:",
        "partner_max_age": "📅 የተመረጠው ዝቅተኛ ዕድሜ: {min_age}\nአሁን፣ ለባልደረባ ከፍተኛውን ዕድሜ ይምረጡ:",
        "partner_age_range_set": "🎉 የባልደረባ ዕድሜ ክልል ተቀናብሯል: ከ{min_age} እስከ {max_age}",
        "select_partner_gender": "🚻 እባክዎ የባልደረባዎን ጾታ ይምረጡ:",
        "partner_gender_set": "🎉 የባልደረባ ጾታ ወደ {gender} ተቀናብሯል",
        "select_partner_religion": "🙏 እባክዎ የባልደረባዎን ሃይማኖት ይምረጡ:",
        "partner_religion_set": "🎉 የባልደረባ ሃይማኖት ወደ {religion} ተቀናብሯል",
        "select_partner_language": "🌐 እባክዎ የባልደረባዎን ቋንቋ ምርጫ ይምረጡ:",
        "partner_language_set": "🎉 የባልደረባ ቋንቋ ምርጫ ወደ {language} ተቀናብሯል",
        "your_profile": "👤 የእርስዎ መገለጫ እነሆ:",
        "partner_preferences": "🤝 የባልደረባ ምርጫዎች:",
        "age_range": "ዕድሜ ክልል",
        "match_found": "🎉 ተጋባችሁ!",
        "partner_setup_info": "👤 የባልደረባ ቅንብር:",
        "start_messaging": "መልእክት መላላክ ይችላሉ。",
        "setup_incomplete": "⚠️ እባክዎ ግጥሚያ ከመጀመርዎ በፊት ቅንብሮችዎን ያጠናቁ። የጎደሉ መስኮች:\n- {fields}\nወደ ቅንብር ምናሌ እየተመራ ነው...",
        "waiting_for_partner": "🔍 ባልደረባን በመጠባበቅ ላይ። ",
        "already_searching": "🔍 አስቀድሞ ባልደረባ እየፈለጉ ነው። እባክዎ ይጠብቁ።",
        "invalid_action": "⚠️ ለአሁኑ ሁኔታ ልክ ያልሆነ እርምጃ።",
        "stopped_searching": "🛑 መፈለግዎን አቁመዋል።",
        "session_ended": "❌ ክፍለ ጊዜውን አጠናቀዋል። እንደገና 'Begin' መጫን ይችላሉ አዲስ ባልደረባ ለመፈለግ።",
        "partner_ended_session": "❌ ባልደረባዎ ክፍለ ጊዜውን አጠናቋል። እንደገና 'Begin' መጫን ይችላሉ አዲስ ባልደረባ ለመፈለግ።",
        "not_in_session": "⚠️ እርስዎ በንቃት ክፍለ ጊዜ ውስጥ ወይም በመፈለግ ላይ አይደሉም።",
        "not_chatting": "⚠️ አሁን ከማንም ጋር እየተወያዩ አይደሉም። ባልደረባ ለመፈለግ 'Begin' ይጫኑ።",
        "help_text": (
            "💡 እርዳታ ይፈልጋሉ? እነዚህን መፈጸም ይችላሉ:\n"
            " - 🚀 Begin: ጉዞዎን ይጀምሩ (ቅንብሮችን ካጠናቀቁ በኋላ)።\n"
            " - ⏹️ Stop Searching: ባልደረባ መፈለግ ያቁሙ።\n"
            " - 🔚 End Chat: ከባልደረባዎ ጋር መወያየት ያቁሙ።\n"
            " - ⚙️ Setup: ምርጫዎችዎን ያዋቅሩ።\n"
            " - ❓ Help: መመሪያ እና መረጃ ያግኙ።\n"
            " - 📩 ጥያቄ ወይም ግብረ መልስ: @Ask_and_feedback_bot ."
        )
    }
}

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

# Function to save all user data to MongoDB (for periodic save)
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
        return member.status not in ['left', 'kicked']
    except Exception as e:
        print(f"Error checking group membership for user {user_id}: {e}")
        return False

# Function to send join group message
async def send_join_group_message(message: Message):
    join_button = InlineKeyboardButton(text="Join Group", url=GROUP_INVITE_LINK)
    join_keyboard = InlineKeyboardMarkup(inline_keyboard=[[join_button]])
    await message.answer(
        text=" Please join the group to use the bot.",
        reply_markup=join_keyboard
    )

# Helper function to check if setup is complete
def is_setup_complete(user_id):
    if user_id not in user_data:
        return False, ["Age", "Gender", "Religion", "Language", "Partner Minimum Age", "Partner Maximum Age", "Partner Gender", "Partner Religion", "Partner Language"]
    user_prefs = user_data[user_id]
    missing_fields = []
    if "age" not in user_prefs or user_prefs["age"] == "Not set":
        missing_fields.append("Age")
    if "gender" not in user_prefs or user_prefs["gender"] == "Not set":
        missing_fields.append("Gender")
    if "religion" not in user_prefs or user_prefs["religion"] == "Not set":
        missing_fields.append("Religion")
    if "language" not in user_prefs or user_prefs["language"] == "Not set":
        missing_fields.append("Language")
    if "partner" not in user_prefs:
        missing_fields.extend(["Partner Minimum Age", "Partner Maximum Age", "Partner Gender", "Partner Religion", "Partner Language"])
    else:
        if "min_age" not in user_prefs["partner"] or user_prefs["partner"]["min_age"] == "Not set":
            missing_fields.append("Partner Minimum Age")
        if "max_age" not in user_prefs["partner"] or user_prefs["partner"]["max_age"] == "Not set":
            missing_fields.append("Partner Maximum Age")
        if "gender" not in user_prefs["partner"] or user_prefs["partner"]["gender"] == "Not set":
            missing_fields.append("Partner Gender")
        if "religion" not in user_prefs["partner"] or user_prefs["partner"]["religion"] == "Not set":
            missing_fields.append("Partner Religion")
        if "language" not in user_prefs["partner"] or user_prefs["partner"]["language"] == "Not set":
            missing_fields.append("Partner Language")
    return len(missing_fields) == 0, missing_fields

# Helper function to get user state
def get_user_state(user_id):
    if user_id in active_matches:
        return "chatting"
    elif user_id in waiting_users:
        return "searching"
    else:
        return "idle"

# Define the Reply Keyboard with dynamic state-based buttons
def get_main_keyboard(state="idle", chat_type="private", user_language="English"):
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
def get_setup_inline_keyboard(user_language="English"):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=texts[user_language]["your_setup"], callback_data="your_setup")],
            [InlineKeyboardButton(text=texts[user_language]["partner_setup"], callback_data="partner_setup")],
            [InlineKeyboardButton(text=texts[user_language]["show_setup"], callback_data="show_setup")],
        ]
    )

# Define the /start command with membership check
@router.message(F.chat.type == "private", F.text == "/start")
async def start_command(message: Message):
    user_id = message.from_user.id
    if not await is_group_member(user_id):
        await send_join_group_message(message)
        return
    user_language = user_data.get(user_id, {}).get("language", "English")
    current_state = get_user_state(user_id)
    welcome_text = texts[user_language]["welcome"]
    if current_state == "idle":
        welcome_text += texts[user_language]["setup_prompt"]
    elif current_state == "searching":
        welcome_text += texts[user_language]["searching_prompt"]
    elif current_state == "chatting":
        welcome_text += texts[user_language]["chatting_prompt"]
    await message.answer(
        text=welcome_text,
        reply_markup=get_main_keyboard(state=current_state, user_language=user_language)
    )
    if current_state == "idle":
        await show_setup_menu(message, user_language)

# Handle "Setup" button or command
@router.message(F.chat.type == "private", F.text.in_({"⚙️ Setup", "/setup"}))
async def handle_setup(message: Message):
    user_id = message.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    await show_setup_menu(message, user_language)

async def show_setup_menu(message_or_callback, user_language="English"):
    if isinstance(message_or_callback, Message):
        await message_or_callback.answer(
            text=texts[user_language]["setup_option"],
            reply_markup=get_setup_inline_keyboard(user_language)
        )
    elif isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(
            text=texts[user_language]["setup_option"],
            reply_markup=get_setup_inline_keyboard(user_language)
        )
        await message_or_callback.answer()

# Handle "Your Setup" inline button
@router.callback_query(F.data == "your_setup")
async def handle_your_setup(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    inline_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=texts[user_language]["age"], callback_data="age")],
            [InlineKeyboardButton(text=texts[user_language]["gender"], callback_data="gender")],
            [InlineKeyboardButton(text=texts[user_language]["religion"], callback_data="religion")],
            [InlineKeyboardButton(text=texts[user_language]["language"], callback_data="language")],
            [InlineKeyboardButton(text=texts[user_language]["back"], callback_data="setup")],
        ]
    )
    await callback.message.edit_text(
        text=texts[user_language]["setup_option"],
        reply_markup=inline_keyboard
    )
    await callback.answer()

# Handle "Partner Setup" inline button
@router.callback_query(F.data == "partner_setup")
async def handle_partner_setup(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    partner_setup_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=texts[user_language]["age"], callback_data="partner_age")],
            [InlineKeyboardButton(text=texts[user_language]["gender"], callback_data="partner_gender")],
            [InlineKeyboardButton(text=texts[user_language]["religion"], callback_data="partner_religion")],
            [InlineKeyboardButton(text=texts[user_language]["language"], callback_data="partner_language")],
            [InlineKeyboardButton(text=texts[user_language]["back"], callback_data="setup")],
        ]
    )
    await callback.message.edit_text(
        text=texts[user_language]["setup_option"],
        reply_markup=partner_setup_keyboard
    )
    await callback.answer()

# Handle "Back to Setup" inline button
@router.callback_query(F.data == "setup")
async def handle_back_to_setup(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    await callback.message.edit_text(
        text=texts[user_language]["setup_option"],
        reply_markup=get_setup_inline_keyboard(user_language)
    )
    await callback.answer()

# Language selection handlers
@router.callback_query(F.data == "language")
async def handle_language(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    language_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Amharic", callback_data="selected_language_amharic")],
            [InlineKeyboardButton(text="English", callback_data="selected_language_english")],
            [InlineKeyboardButton(text=texts[user_language]["back"], callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(
        text=texts[user_language]["select_language"],
        reply_markup=language_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("selected_language_"))
async def handle_language_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_language = callback.data.split("_")[-1].capitalize()
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["language"] = selected_language
    update_user_data_now(user_id)
    user_language = selected_language
    await callback.answer(
        text=texts[user_language]["language_selected"].format(language=selected_language),
        show_alert=True
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_your_setup(callback)

@router.callback_query(F.data == "partner_language")
async def handle_partner_language(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    language_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Amharic", callback_data="partner_language_amharic")],
            [InlineKeyboardButton(text="English", callback_data="partner_language_english")],
            [InlineKeyboardButton(text="Any", callback_data="partner_language_any")],
            [InlineKeyboardButton(text=texts[user_language]["back"], callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(
        text=texts[user_language]["select_partner_language"],
        reply_markup=language_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("partner_language_"))
async def handle_partner_language_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_partner_language = callback.data.split("_")[-1].capitalize()
    if user_id not in user_data:
        user_data[user_id] = {}
    if "partner" not in user_data[user_id]:
        user_data[user_id]["partner"] = {}
    user_data[user_id]["partner"]["language"] = selected_partner_language
    update_user_data_now(user_id)
    user_language = user_data[user_id].get("language", "English")
    await callback.answer(
        text=texts[user_language]["partner_language_set"].format(language=selected_partner_language),
        show_alert=True
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_setup(callback)

# Function to start searching with setup check
async def start_searching(message: Message, user_id: int):
    user_language = user_data.get(user_id, {}).get("language", "English")
    is_complete, missing_fields = is_setup_complete(user_id)
    if not is_complete:
        await message.answer(
            text=texts[user_language]["setup_incomplete"].format(fields=', '.join(missing_fields)),
            reply_markup=get_main_keyboard(state="idle", user_language=user_language)
        )
        await show_setup_menu(message, user_language)
        return False
    waiting_start_times[user_id] = datetime.datetime.now()
    waiting_users.add(user_id)
    await message.answer(
        texts[user_language]["waiting_for_partner"],
        reply_markup=get_main_keyboard(state="searching", user_language=user_language)
    )
    await attempt_match(user_id)
    return True

# Modified find_match to include language compatibility
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
        user_language = user_prefs.get("language", "Not set").lower()
        candidate_language = candidate_prefs.get("language", "Not set").lower()
        partner_language_pref = partner_criteria.get("language", "any").lower()
        user_partner_language_pref = user_partner_prefs.get("language", "any").lower()
        candidate_language_ok = (
            partner_language_pref == "any" or
            partner_language_pref == user_language
        )
        user_language_ok = (
            user_partner_language_pref == "any" or
            user_partner_language_pref == candidate_language
        )
        if (
            (partner_criteria.get("min_age", 0) <= int(user_prefs.get("age", 0)) <= partner_criteria.get("max_age", 100))
            and (user_partner_prefs.get("min_age", 0) <= int(candidate_prefs.get("age", 0)) <= user_partner_prefs.get("max_age", 100))
            and (partner_criteria.get("gender", "any") in ("any", user_prefs.get("gender", "any")))
            and (user_partner_prefs.get("gender", "any") in ("any", candidate_prefs.get("gender", "any")))
            and candidate_religion_ok
            and user_religion_ok
            and candidate_language_ok
            and user_language_ok
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
        user_language_1 = user_data_1.get("language", "English")
        user_language_2 = user_data_2.get("language", "English")
        await bot.send_message(
            chat_id=user_id,
            text=(
                f"{texts[user_language_1]['match_found']}\n\n"
                f"{texts[user_language_1]['partner_setup_info']}\n"
                f"{texts[user_language_1]['age']}: {user_data_2.get('age', 'Not set')}\n"
                f"{texts[user_language_1]['gender']}: {user_data_2.get('gender', 'Not set')}\n"
                f"{texts[user_language_1]['religion']}: {user_data_2.get('religion', 'Not set')}\n"
                f"{texts[user_language_1]['language']}: {user_data_2.get('language', 'Not set')}\n"
                f"{texts[user_language_1]['start_messaging']}"
            ),
            reply_markup=get_main_keyboard(state="chatting", user_language=user_language_1),
        )
        await bot.send_message(
            chat_id=match_id,
            text=(
                f"{texts[user_language_2]['match_found']}\n\n"
                f"{texts[user_language_2]['partner_setup_info']}\n"
                f"{texts[user_language_2]['age']}: {user_data_1.get('age', 'Not set')}\n"
                f"{texts[user_language_2]['gender']}: {user_data_1.get('gender', 'Not set')}\n"
                f"{texts[user_language_2]['religion']}: {user_data_1.get('religion', 'Not set')}\n"
                f"{texts[user_language_2]['language']}: {user_data_1.get('language', 'Not set')}\n"
                f"{texts[user_language_2]['start_messaging']}"
            ),
            reply_markup=get_main_keyboard(state="chatting", user_language=user_language_2),
        )
        match_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        channel_message = (
            f"🤝 **New Match** at {match_time}\n\n"
            f"👤 User 1: {user_1_name} (ID: {user_id})\n"
            f"  - Age: {user_data_1.get('age', 'Not set')}\n"
            f"  - Gender: {user_data_1.get('gender', 'Not set')}\n"
            f"  - Religion: {user_data_1.get('religion', 'Not set')}\n"
            f"  - Language: {user_data_1.get('language', 'Not set')}\n"
            f"  - Partner Prefs:\n"
            f"    - Age Range: {user_data_1.get('partner', {}).get('min_age', 'Not set')} to {user_data_1.get('partner', {}).get('max_age', 'Not set')}\n"
            f"    - Gender: {user_data_1.get('partner', {}).get('gender', 'Not set')}\n"
            f"    - Religion: {user_data_1.get('partner', {}).get('religion', 'Not set')}\n"
            f"    - Language: {user_data_1.get('partner', {}).get('language', 'Not set')}\n\n"
            f"👤 User 2: {user_2_name} (ID: {match_id})\n"
            f"  - Age: {user_data_2.get('age', 'Not set')}\n"
            f"  - Gender: {user_data_2.get('gender', 'Not set')}\n"
            f"  - Religion: {user_data_2.get('religion', 'Not set')}\n"
            f"  - Language: {user_data_2.get('language', 'Not set')}\n"
            f"  - Partner Prefs:\n"
            f"    - Age Range: {user_data_2.get('partner', {}).get('min_age', 'Not set')} to {user_data_2.get('partner', {}).get('max_age', 'Not set')}\n"
            f"    - Gender: {user_data_2.get('partner', {}).get('gender', 'Not set')}\n"
            f"    - Religion: {user_data_2.get('partner', {}).get('religion', 'Not set')}\n"
            f"    - Language: {user_data_2.get('partner', {}).get('language', 'Not set')}"
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

# Handle matching buttons and commands with membership check for Begin
@router.message(F.chat.type == "private", F.text.in_({BEGIN_TEXT, STOP_SEARCHING_TEXT, END_CHAT_TEXT, "/begin", "/end"}))
async def handle_matching_button(message: Message):
    user_id = message.from_user.id
    text = message.text
    current_state = get_user_state(user_id)
    user_language = user_data.get(user_id, {}).get("language", "English")
    if text in [BEGIN_TEXT, "/begin"]:
        if current_state == "searching":
            await message.answer(
                texts[user_language]["already_searching"],
                reply_markup=get_main_keyboard(state="searching", user_language=user_language)
            )
        elif current_state != "idle":
            await message.answer(
                texts[user_language]["invalid_action"],
                reply_markup=get_main_keyboard(state=current_state, user_language=user_language)
            )
        else:
            if not await is_group_member(user_id):
                await send_join_group_message(message)
                return
            await start_searching(message, user_id)
    elif text == STOP_SEARCHING_TEXT:
        if current_state != "searching":
            await message.answer(
                texts[user_language]["invalid_action"],
                reply_markup=get_main_keyboard(state=current_state, user_language=user_language)
            )
            return
        waiting_users.remove(user_id)
        waiting_start_times.pop(user_id, None)
        await message.answer(
            texts[user_language]["stopped_searching"],
            reply_markup=get_main_keyboard(state="idle", user_language=user_language)
        )
    elif text == END_CHAT_TEXT:
        if current_state != "chatting":
            await message.answer(
                texts[user_language]["invalid_action"],
                reply_markup=get_main_keyboard(state=current_state, user_language=user_language)
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
            texts[user_language]["session_ended"],
            reply_markup=get_main_keyboard(state="idle", user_language=user_language)
        )
        match_language = user_data.get(match_id, {}).get("language", "English")
        await bot.send_message(
            chat_id=match_id,
            text=texts[match_language]["partner_ended_session"],
            reply_markup=get_main_keyboard(state="idle", user_language=match_language)
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
                texts[user_language]["session_ended"],
                reply_markup=get_main_keyboard(state="idle", user_language=user_language)
            )
            match_language = user_data.get(match_id, {}).get("language", "English")
            await bot.send_message(
                chat_id=match_id,
                text=texts[match_language]["partner_ended_session"],
                reply_markup=get_main_keyboard(state="idle", user_language=match_language)
            )
        elif current_state == "searching":
            waiting_users.remove(user_id)
            waiting_start_times.pop(user_id, None)
            await message.answer(
                texts[user_language]["stopped_searching"],
                reply_markup=get_main_keyboard(state="idle", user_language=user_language)
            )
        else:
            await message.answer(
                texts[user_language]["not_in_session"],
                reply_markup=get_main_keyboard(state="idle", user_language=user_language)
            )

# Handle "Help" button or command
@router.message(F.chat.type == "private", F.text.in_({"❓ Help", "/help"}))
async def handle_help(message: Message):
    user_id = message.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    await message.answer(
        text=texts[user_language]["help_text"]
    )

@router.message(F.chat.type == "private", F.text | F.document | F.photo | F.video | F.audio | F.voice | F.video_note | F.sticker)
async def forward_messages(message: Message):
    user_id = message.from_user.id
    current_state = get_user_state(user_id)
    user_language = user_data.get(user_id, {}).get("language", "English")
    print(f"📩 Received message from {user_id}, type: {message.content_type}, state: {current_state}")
    print(f"📋 Active matches: {active_matches}")
    print(f"🗂️ Current message_id_map: {message_id_map}")
    if current_state == "chatting":
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
    elif current_state == "searching":
        await message.answer(
            texts[user_language]["already_searching"],
            reply_markup=get_main_keyboard(state="searching", user_language=user_language)
        )
    else:
        await message.answer(
            texts[user_language]["not_chatting"],
            reply_markup=get_main_keyboard(state="idle", user_language=user_language)
        )
        return

# Optional: Explicitly ignore messages in group chats
@router.message(F.chat.type.in_({"group", "supergroup"}))
async def ignore_group_messages(_message: Message):
    pass

# Callback query handlers
@router.callback_query(F.data == "age")
async def handle_age(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"selected_age_{age}") for age in range(row_start, row_start + 5)]
            for row_start in range(18, 100, 5)
        ]
    )
    age_keyboard.inline_keyboard.append([InlineKeyboardButton(text=texts[user_language]["back"], callback_data="your_setup")])
    await callback.message.edit_text(text=texts[user_language]["select_age"], reply_markup=age_keyboard)
    await callback.answer()

@router.callback_query(F.data == "gender")
async def handle_gender(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="selected_gender_male")],
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="selected_gender_female")],
            [InlineKeyboardButton(text=texts[user_language]["back"], callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(text=texts[user_language]["select_gender"], reply_markup=gender_keyboard)
    await callback.answer()

@router.callback_query(F.data == "religion")
async def handle_religion(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox", callback_data="selected_religion_orthodox")],
            [InlineKeyboardButton(text="Muslim", callback_data="selected_religion_muslim")],
            [InlineKeyboardButton(text="Protestant", callback_data="selected_religion_protestant")],
            [InlineKeyboardButton(text=texts[user_language]["back"], callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(text=texts[user_language]["select_religion"], reply_markup=religion_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("selected_age_"))
async def handle_age_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    selected_age = callback.data.split("_")[-1]
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["age"] = selected_age
    update_user_data_now(user_id)
    await callback.answer(text=texts[user_language]["age_selected"].format(age=selected_age), show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_gender(callback)

@router.callback_query(F.data.startswith("selected_gender_"))
async def handle_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    selected_gender = callback.data.split("_")[-1]
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["gender"] = selected_gender
    update_user_data_now(user_id)
    await callback.answer(text=texts[user_language]["gender_selected"].format(gender=selected_gender), show_alert=True)
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_religion(callback)

@router.callback_query(F.data.startswith("selected_religion_"))
async def handle_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
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
            f"- {texts[user_language]['age']}: {selected_age}\n"
            f"- {texts[user_language]['gender']}: {selected_gender}\n"
            f"- {texts[user_language]['religion']}: {selected_religion}\n\n"
            "Returning to the Setup menu..."
        )
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await asyncio.sleep(5)
    await handle_back_to_setup(callback)

@router.callback_query(F.data == "partner_age")
async def handle_partner_minimum_age(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"partner_min_age_{age}") for age in range(row_start, row_start + 5)]
            for row_start in range(18, 100, 5)
        ]
    )
    age_keyboard.inline_keyboard.append([InlineKeyboardButton(text=texts[user_language]["back"], callback_data="partner_setup")])
    await callback.message.edit_text(text=texts[user_language]["partner_min_age"], reply_markup=age_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_min_age_"))
async def handle_partner_maximum_age(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
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
    max_age_keyboard.inline_keyboard.append([InlineKeyboardButton(text=texts[user_language]["back"], callback_data="partner_age")])
    await callback.message.edit_text(
        text=texts[user_language]["partner_max_age"].format(min_age=min_age),
        reply_markup=max_age_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("partner_max_age_"))
async def handle_partner_age_range(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    max_age = int(callback.data.split("_")[-1])
    min_age = user_data[user_id]["partner"].get("min_age", None)
    if min_age is None:
        await callback.message.answer("❌ Minimum age not set. Please start from minimum age selection.")
        return
    user_data[user_id]["partner"]["max_age"] = max_age
    update_user_data_now(user_id)
    await callback.answer(
        text=texts[user_language]["partner_age_range_set"].format(min_age=min_age, max_age=max_age),
        show_alert=True
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_gender(callback)

@router.callback_query(F.data == "partner_gender")
async def handle_partner_gender(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="partner_gender_male")],
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="partner_gender_female")],
            [InlineKeyboardButton(text=texts[user_language]["back"], callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(
        text=texts[user_language]["select_partner_gender"],
        reply_markup=gender_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("partner_gender_"))
async def handle_partner_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    selected_gender = callback.data.split("_")[-1]
    if user_id not in user_data:
        user_data[user_id] = {}
    if "partner" not in user_data[user_id]:
        user_data[user_id]["partner"] = {}
    user_data[user_id]["partner"]["gender"] = selected_gender
    update_user_data_now(user_id)
    await callback.answer(
        text=texts[user_language]["partner_gender_set"].format(gender=selected_gender.capitalize()),
        show_alert=True
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await handle_partner_religion(callback)

@router.callback_query(F.data == "partner_religion")
async def handle_partner_religion(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox", callback_data="partner_religion_orthodox")],
            [InlineKeyboardButton(text="Muslim", callback_data="partner_religion_muslim")],
            [InlineKeyboardButton(text="Protestant", callback_data="partner_religion_protestant")],
            [InlineKeyboardButton(text="Any", callback_data="partner_religion_Any")],
            [InlineKeyboardButton(text=texts[user_language]["back"], callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(
        text=texts[user_language]["select_partner_religion"],
        reply_markup=religion_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("partner_religion_"))
async def handle_partner_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
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
            f"- {texts[user_language]['age_range']}: {partner_min_age} to {partner_max_age}\n"
            f"- {texts[user_language]['gender']}: {partner_gender.capitalize()}\n"
            f"- {texts[user_language]['religion']}: {partner_religion}\n\n"
            "Returning to the Setup menu..."
        )
    )
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)
    await asyncio.sleep(5)
    await handle_back_to_setup(callback)

@router.callback_query(F.data == "show_setup")
async def handle_show_setup(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = user_data.get(user_id, {}).get("language", "English")
    if callback.message.text.startswith(texts[user_language]["your_profile"]):
        await callback.answer(text="⚠️ You are already in the Show Setup menu!", show_alert=True)
        return
    your_age = user_data.get(user_id, {}).get("age", "Not set")
    your_gender = user_data.get(user_id, {}).get("gender", "Not set")
    your_religion = user_data.get(user_id, {}).get("religion", "Not set")
    your_language = user_data.get(user_id, {}).get("language", "Not set")
    partner_min_age = user_data.get(user_id, {}).get("partner", {}).get("min_age", "Not set")
    partner_max_age = user_data.get(user_id, {}).get("partner", {}).get("max_age", "Not set")
    partner_gender = user_data.get(user_id, {}).get("partner", {}).get("gender", "Not set")
    partner_religion = user_data.get(user_id, {}).get("partner", {}).get("religion", "Not set")
    partner_language = user_data.get(user_id, {}).get("partner", {}).get("language", "Not set")
    result_text = (
        f"{texts[user_language]['your_profile']}\n"
        f"- {texts[user_language]['age']}: {your_age}\n"
        f"- {texts[user_language]['gender']}: {your_gender}\n"
        f"- {texts[user_language]['religion']}: {your_religion}\n"
        f"- {texts[user_language]['language']}: {your_language}\n\n"
        f"{texts[user_language]['partner_preferences']}\n"
        f"- {texts[user_language]['age_range']}: {partner_min_age} to {partner_max_age}\n"
        f"- {texts[user_language]['gender']}: {partner_gender}\n"
        f"- {texts[user_language]['religion']}: {partner_religion}\n"
        f"- {texts[user_language]['language']}: {partner_language}"
    )
    await callback.message.edit_text(
        text=result_text,
        reply_markup=get_setup_inline_keyboard(user_language)
    )
    await callback.answer()

async def periodic_save():
    while True:
        await asyncio.sleep(60)
        await save_user_data()
        print("🔄 Performed periodic backup of user data")

async def main():
    await load_user_data()
    print("🤖 Bot is running...")
    print("💾 Individual data points will be saved immediately upon change")
    print("💾 Automatic backups will occur every minute")
    await set_bot_commands()
    periodic_save_task = asyncio.create_task(periodic_save())
    try:
        async with bot:
            await dp.start_polling(bot)
    except KeyboardInterrupt:
        await save_user_data()
        print("💾 Final save completed before shutdown")
    finally:
        periodic_save_task.cancel()
        try:
            await periodic_save_task
        except asyncio.CancelledError:
            pass
        print("👋 Bot has shut down gracefully")

if __name__ == "__main__":
    asyncio.run(main())
