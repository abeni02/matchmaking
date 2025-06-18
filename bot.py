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

# Initialize a single lock for all shared data structures
data_lock = asyncio.Lock()

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

# Bot token and environment variables setup
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')
GROUP_ID = os.getenv('GROUP_ID')
GROUP_INVITE_LINK = os.getenv('GROUP_INVITE_LINK')
MONGODB_URI = os.getenv('MONGODB_URI')

if not BOT_TOKEN:
    raise ValueError("No BOT_TOKEN found in environment variables.")
if not CHANNEL_ID:
    raise ValueError("No CHANNEL_ID found in environment variables.")
if not GROUP_ID:
    raise ValueError("No GROUP_ID found in environment variables.")
if not GROUP_INVITE_LINK:
    raise ValueError("No GROUP_INVITE_LINK found in environment variables.")
if not MONGODB_URI:
    raise ValueError("No MONGODB_URI found in environment variables.")

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

BEGIN_TEXT = "🚀 Begin"
STOP_SEARCHING_TEXT = "⏹️ Stop Searching"
END_CHAT_TEXT = "🔚 End Chat"

def get_gender_emoji(gender):
    if gender.lower() == "male":
        return "👨"
    elif gender.lower() == "female":
        return "👩"
    else:
        return "❓"

async def save_user_data():
    async with data_lock:
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
    async with data_lock:
        if user_id in user_data:
            user_info = user_data[user_id].copy()
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
    async with data_lock:
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
        text="Please join the group to use the bot.",
        reply_markup=join_keyboard
    )

async def is_setup_complete(user_id):
    async with data_lock:
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

async def get_user_state(user_id):
    async with data_lock:
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

@router.message(F.chat.type == "private", F.text == "/start")
async def start_command(message: Message):
    user_id = message.from_user.id
    if not await is_group_member(user_id):
        await send_join_group_message(message)
        return
    current_state = await get_user_state(user_id)
    welcome_text = "👋 Welcome to our matchmaking bot! Discover your perfect match based on your preferences.\n"
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

@router.callback_query(F.data == "age")
async def handle_age(callback: CallbackQuery):
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"selected_age_{age}") for age in range(start, min(start + 5, 101))]
            for start in range(18, 101, 5)
        ]
    )
    age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")])
    await callback.message.edit_text(text="📅 Select your age:", reply_markup=age_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("selected_age_"))
async def handle_age_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_age = callback.data.split("_")[-1]
    async with data_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]["age"] = selected_age
    update_user_data_now(user_id)
    await callback.answer(text=f"Your age is {selected_age}", show_alert=True)
    await handle_your_setup(callback)

@router.callback_query(F.data == "gender")
async def handle_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 👨", callback_data="gender_male")],
            [InlineKeyboardButton(text="Female 👩", callback_data="gender_female")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(text="🚻 Select your gender:", reply_markup=gender_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("gender_"))
async def handle_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1].capitalize()
    async with data_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]["gender"] = selected_gender
    update_user_data_now(user_id)
    await callback.answer(text=f"Your gender is set to {selected_gender}", show_alert=True)
    await handle_your_setup(callback)

@router.callback_query(F.data == "religion")
async def handle_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Christianity ✝️", callback_data="religion_Christianity")],
            [InlineKeyboardButton(text="Islam ☪️", callback_data="religion_Islam")],
            [InlineKeyboardButton(text="Hinduism 🕉️", callback_data="religion_Hinduism")],
            [InlineKeyboardButton(text="Buddhism ☸️", callback_data="religion_Buddhism")],
            [InlineKeyboardButton(text="Other ❓", callback_data="religion_Other")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(text="🙏 Select your religion:", reply_markup=religion_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("religion_"))
async def handle_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_religion = callback.data.split("_")[-1]
    async with data_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]["religion"] = selected_religion
    update_user_data_now(user_id)
    await callback.answer(text=f"Your religion is set to {selected_religion}", show_alert=True)
    await handle_your_setup(callback)

@router.callback_query(F.data == "partner_age")
async def handle_partner_age(callback: CallbackQuery):
    partner_age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"partner_min_age_{age}") for age in range(start, min(start + 5, 101))]
            for start in range(18, 101, 5)
        ]
    )
    partner_age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")])
    await callback.message.edit_text(
        text="📅 Set your partner’s minimum age:",
        reply_markup=partner_age_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("partner_min_age_"))
async def handle_partner_min_age_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_min_age = int(callback.data.split("_")[-1])
    async with data_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        if "partner" not in user_data[user_id]:
            user_data[user_id]["partner"] = {}
        user_data[user_id]["partner"]["min_age"] = selected_min_age
    update_user_data_now(user_id)
    # Now ask for max age
    max_age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=str(age), callback_data=f"partner_max_age_{age}") for age in range(selected_min_age, min(selected_min_age + 5, 101))]
            for selected_min_age in range(selected_min_age, 101, 5)
        ]
    )
    max_age_keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="partner_age")])
    await callback.message.edit_text(
        text=f"📅 Selected minimum age: **{selected_min_age}**\nNow, select the **maximum age** for the partner:",
        reply_markup=max_age_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("partner_max_age_"))
async def handle_partner_max_age_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_max_age = int(callback.data.split("_")[-1])
    async with data_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        if "partner" not in user_data[user_id]:
            user_data[user_id]["partner"] = {}
        user_data[user_id]["partner"]["max_age"] = selected_max_age
    update_user_data_now(user_id)
    await callback.answer(text=f"Partner maximum age set to {selected_max_age}", show_alert=True)
    await handle_partner_setup(callback)

@router.callback_query(F.data == "partner_gender")
async def handle_partner_gender(callback: CallbackQuery):
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 👨", callback_data="partner_gender_male")],
            [InlineKeyboardButton(text="Female 👩", callback_data="partner_gender_female")],
            [InlineKeyboardButton(text="Any ❓", callback_data="partner_gender_any")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(text="🚻 Select partner’s gender preference:", reply_markup=gender_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_gender_"))
async def handle_partner_gender_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1].capitalize() if "any" not in callback.data else "any"
    async with data_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        if "partner" not in user_data[user_id]:
            user_data[user_id]["partner"] = {}
        user_data[user_id]["partner"]["gender"] = selected_gender
    update_user_data_now(user_id)
    await callback.answer(text=f"Partner gender preference set to {selected_gender}", show_alert=True)
    await handle_partner_setup(callback)

@router.callback_query(F.data == "partner_religion")
async def handle_partner_religion(callback: CallbackQuery):
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Christianity ✝️", callback_data="partner_religion_Christianity")],
            [InlineKeyboardButton(text="Islam ☪️", callback_data="partner_religion_Islam")],
            [InlineKeyboardButton(text="Hinduism 🕉️", callback_data="partner_religion_Hinduism")],
            [InlineKeyboardButton(text="Buddhism ☸️", callback_data="partner_religion_Buddhism")],
            [InlineKeyboardButton(text="Other ❓", callback_data="partner_religion_Other")],
            [InlineKeyboardButton(text="Any 🌍", callback_data="partner_religion_any")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(text="🙏 Select partner’s religion preference:", reply_markup=religion_keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("partner_religion_"))
async def handle_partner_religion_selection(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_religion = callback.data.split("_")[-1] if "any" not in callback.data else "any"
    async with data_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        if "partner" not in user_data[user_id]:
            user_data[user_id]["partner"] = {}
        user_data[user_id]["partner"]["religion"] = selected_religion
    update_user_data_now(user_id)
    await callback.answer(text=f"Partner religion preference set to {selected_religion}", show_alert=True)
    await handle_partner_setup(callback)

@router.callback_query(F.data == "show_setup")
async def handle_show_setup(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with data_lock:
        if user_id not in user_data:
            user_data[user_id] = {}
        user_prefs = user_data[user_id]
        partner_prefs = user_prefs.get("partner", {})
    profile_text = (
        f"👤 **Your Profile**:\n"
        f"📅 Age: {user_prefs.get('age', 'Not set')}\n"
        f"🚻 Gender: {user_prefs.get('gender', 'Not set')}\n"
        f"🙏 Religion: {user_prefs.get('religion', 'Not set')}\n\n"
        f"🤝 **Partner Preferences**:\n"
        f"📅 Age Range: {partner_prefs.get('min_age', 'Not set')} - {partner_prefs.get('max_age', 'Not set')}\n"
        f"🚻 Gender: {partner_prefs.get('gender', 'Not set')}\n"
        f"🙏 Religion: {partner_prefs.get('religion', 'Not set')}"
    )
    await callback.message.edit_text(
        text=profile_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data="setup")]]),
        parse_mode="Markdown"
    )
    await callback.answer()

async def start_searching(message: Message, user_id: int):
    is_complete, missing_fields = await is_setup_complete(user_id)
    if not is_complete:
        await message.answer(
            text=f"⚠️ Please complete your setup before starting a match. Missing fields:\n- {', '.join(missing_fields)}\nRedirecting to setup menu...",
            reply_markup=get_main_keyboard(state="idle")
        )
        await show_setup_menu(message)
        return False
    async with data_lock:
        waiting_start_times[user_id] = datetime.datetime.now()
        waiting_users.add(user_id)
    await message.answer(
        "🔍 Waiting for a partner.",
        reply_markup=get_main_keyboard(state="searching")
    )
    await attempt_match(user_id)
    return True

async def find_match(user_id):
    async with data_lock:
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
    match_id = await find_match(user_id)
    if match_id:
        async with data_lock:
            active_matches[user_id] = match_id
            active_matches[match_id] = user_id
            waiting_users.discard(user_id)
            waiting_users.discard(match_id)
            waiting_start_times.pop(user_id, None)
            waiting_start_times.pop(match_id, None)
            user_data_1 = user_data[user_id].copy()
            user_data_2 = user_data[match_id].copy()
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
                "You can start messaging."
            ),
            reply_markup=get_main_keyboard(state="chatting")
        )
        await bot.send_message(
            chat_id=match_id,
            text=(
                f"🎉 Match found!\n\n"
                f"👤 Partner’s setup:\n"
                f"📅 Age: {user_data_1.get('age', 'Not set')}\n"
                f"🚻 Gender: {user_data_1.get('gender', 'Not set')}\n"
                f"🙏 Religion: {user_data_1.get('religion', 'Not set')}\n"
                "You can start messaging."
            ),
            reply_markup=get_main_keyboard(state="chatting")
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

@router.message(F.chat.type == "private", F.text.in_({BEGIN_TEXT, STOP_SEARCHING_TEXT, END_CHAT_TEXT, "/begin", "/end"}))
async def handle_matching_button(message: Message):
    user_id = message.from_user.id
    text = message.text
    current_state = await get_user_state(user_id)
    if text in [BEGIN_TEXT, "/begin"]:
        if current_state == "searching":
            await message.answer(
                "🔍 You are already searching for a partner. Please wait.",
                reply_markup=get_main_keyboard(state="searching")
            )
        elif current_state != "idle":
            await message.answer(
                "⚠️ Invalid action for current state.",
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
                "⚠️ Invalid action for current state.",
                reply_markup=get_main_keyboard(state=current_state)
            )
            return
        async with data_lock:
            waiting_users.discard(user_id)
            waiting_start_times.pop(user_id, None)
        await message.answer(
            "🛑 You have stopped searching.",
            reply_markup=get_main_keyboard(state="idle")
        )
    elif text == END_CHAT_TEXT or text == "/end":
        if current_state != "chatting":
            if text == "/end" and current_state == "searching":
                async with data_lock:
                    waiting_users.discard(user_id)
                    waiting_start_times.pop(user_id, None)
                await message.answer(
                    "🛑 You have stopped searching.",
                    reply_markup=get_main_keyboard(state="idle")
                )
            else:
                await message.answer(
                    "⚠️ Invalid action for current state.",
                    reply_markup=get_main_keyboard(state=current_state)
                )
            return
        async with data_lock:
            match_id = active_matches.pop(user_id, None)
            if match_id is not None:
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
        if match_id is not None:
            await bot.send_message(
                chat_id=match_id,
                text="❌ Your partner has ended the session. You can 'Begin' again to find a new partner.",
                reply_markup=get_main_keyboard(state="idle")
            )

@router.message(F.chat.type == "private", F.text.in_({"❓ Help", "/help"}))
async def handle_help(message: Message):
    await message.answer(
        text=(
            "💡 Need assistance? Here's what you can do:\n"
            " - 🚀 Begin: Start your journey (after completing setup).\n"
            " - ⏹️ Stop Searching: Stop looking for a partner.\n"
            " - 🔚 End Chat: Stop chatting with your partner.\n"
            " - ⚙️ Setup: Configure your preferences.\n"
            " - ❓ Help: Get guidance and information.\n"
            " - 📩 Ask or feedback: @Ask_and_feedback_bot."
        )
    )

@router.message(F.chat.type == "private", F.text | F.document | F.photo | F.video | F.audio | F.voice | F.video_note | F.sticker)
async def forward_messages(message: Message):
    user_id = message.from_user.id
    current_state = await get_user_state(user_id)
    if current_state != "chatting":
        if current_state == "searching":
            await message.answer(
                "🔍 You are already searching for a partner. Please wait.",
                reply_markup=get_main_keyboard(state="searching")
            )
        else:
            await message.answer(
                "⚠️ You are not currently chatting with anyone. Press 'Begin' to find a partner.",
                reply_markup=get_main_keyboard(state="idle")
            )
        return
    async with data_lock:
        partner_id = active_matches[user_id]
        message_id_map.setdefault(user_id, {})
        message_id_map.setdefault(partner_id, {})
        sender_gender = user_data.get(user_id, {}).get("gender", "Not set")
        gender_emoji = get_gender_emoji(sender_gender)
        label = f"Partner {gender_emoji}: "
    reply_to_message_id = None
    if message.reply_to_message:
        async with data_lock:
            original_reply_id = message.reply_to_message.message_id
            reply_to_message_id = message_id_map.get(user_id, {}).get(original_reply_id)
    user_info = await bot.get_chat(user_id)
    sender_name = user_info.first_name or user_info.username or f"User {user_id}"
    message_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    channel_message = f"💬 **Message** at {message_time}\n👤 From: {sender_name} (ID: {user_id}) to User ID: {partner_id}\n"
    try:
        forwarded_message = None
        if message.text:
            modified_text = label + message.text
            forwarded_message = await bot.send_message(
                chat_id=partner_id,
                text=modified_text,
                reply_to_message_id=reply_to_message_id,
                protect_content=True
            )
            channel_message += f"📜 Text: {message.text}\n"
        elif message.photo:
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
            channel_message += f"📜 Label: {label_text}\n🎥 Video note sent\n"
        elif message.sticker:
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
            channel_message += f"📜 Label: {label_text}\n🏷️ Sticker sent\n"
        else:
            await message.answer("⚠️ Unsupported message type. Please send text, photo, document, video, audio, voice, video note, or sticker.")
            print(f"⚠️ Unsupported message type from {user_id}: {message.content_type}")
            channel_message += f"⚠️ Unsupported message type: {message.content_type}\n"
            return

        if forwarded_message and hasattr(forwarded_message, 'message_id'):
            async with data_lock:
                message_id_map[user_id][message.message_id] = forwarded_message.message_id
                message_id_map[partner_id][forwarded_message.message_id] = message.message_id
    except Exception as e:
        print(f"❌ Error forwarding message from {user_id} to {partner_id}: {e}")
        await message.answer("⚠️ Failed to send message. Please try again.")

    try:
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=channel_message,
            parse_mode="Markdown"
        )
        # Log media if applicable
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
