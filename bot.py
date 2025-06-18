
import asyncio
import datetime
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, BotCommandScopeAllPrivateChats
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure
import os

# Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN")
GROUP_ID = os.getenv("GROUP_ID", "-1001234567890")
GROUP_INVITE_LINK = os.getenv("GROUP_INVITE_LINK", "https://t.me/yourgroup")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = "bot_database"

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# MongoDB setup
mongo_client = AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
db = mongo_client[DB_NAME]
users_collection = db['users']

# In-memory storage with locks
user_data = {}
waiting_users = set()
waiting_start_times = {}
cooldown_tracker = {}
active_matches = {}
user_data_lock = asyncio.Lock()
waiting_users_lock = asyncio.Lock()
waiting_start_times_lock = asyncio.Lock()
cooldown_tracker_lock = asyncio.Lock()
active_matches_lock = asyncio.Lock()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Utility functions
def get_gender_emoji(gender):
    return "👨" if gender.lower() == "male" else "👩" if gender.lower() == "female" else "❓"

async def set_bot_commands():
    commands = [
        BotCommand(command="start", description="Start the bot"),
        BotCommand(command="help", description="Get help"),
    ]
    await bot.set_my_commands(commands, scope=BotCommandScopeAllPrivateChats())
    logger.info("Bot commands set for private chats")

def get_main_keyboard(state):
    if state == "idle":
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Begin", callback_data="begin_searching")],
            [InlineKeyboardButton(text="⚙️ Setup", callback_data="setup_profile")],
            [InlineKeyboardButton(text="❓ Help", callback_data="show_help")]
        ])
    elif state == "searching":
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏹️ Stop Searching", callback_data="stop_searching")]
        ])
    elif state == "chatting":
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔚 End Chat", callback_data="end_chat")]
        ])
    return InlineKeyboardMarkup(inline_keyboard=[])

def is_setup_complete(user_id):
    if user_id not in user_data:
        return False, ["Age", "Gender", "Religion", "Partner Minimum Age", "Partner Maximum Age", "Partner Gender", "Partner Religion"]
    user_prefs = user_data[user_id]
    missing_fields = []
    if "age" not in user_prefs or not str(user_prefs["age"]).isdigit() or not (18 <= int(user_prefs["age"]) <= 100):
        missing_fields.append("Age (must be 18-100)")
    if "gender" not in user_prefs or user_prefs["gender"].lower() not in ["male", "female"]:
        missing_fields.append("Gender (male/female)")
    if "religion" not in user_prefs or user_prefs["religion"].lower() not in ["orthodox", "muslim", "protestant"]:
        missing_fields.append("Religion (Orthodox/Muslim/Protestant)")
    if "partner" not in user_prefs:
        missing_fields.extend(["Partner Minimum Age", "Partner Maximum Age", "Partner Gender", "Partner Religion"])
    else:
        partner = user_prefs["partner"]
        if "min_age" not in partner or not str(partner["min_age"]).isdigit() or int(partner["min_age"]) < 18:
            missing_fields.append("Partner Minimum Age (min 18)")
        if "max_age" not in partner or not str(partner["max_age"]).isdigit() or int(partner["max_age"]) < int(partner.get("min_age", 18)):
            missing_fields.append("Partner Maximum Age (must be >= min age)")
        if "gender" not in partner or partner["gender"].lower() not in ["male", "female"]:
            missing_fields.append("Partner Gender (male/female)")
        if "religion" not in partner or partner["religion"].lower() not in ["orthodox", "muslim", "protestant", "any"]:
            missing_fields.append("Partner Religion (Orthodox/Muslim/Protestant/Any)")
    return len(missing_fields) == 0, missing_fields

async def is_group_member(user_id):
    try:
        member = await bot.get_chat_member(GROUP_ID, user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logger.error(f"Error checking group membership for user {user_id}: {e}")
        return False

# Database operations
async def connect_to_mongodb():
    for attempt in range(3):
        try:
            await mongo_client.admin.command('ping')
            return True
        except ConnectionFailure as e:
            logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
            if attempt < 2:
                await asyncio.sleep(5)
    raise ConnectionFailure("Failed to connect to MongoDB after retries")

async def load_user_data():
    async with user_data_lock, cooldown_tracker_lock:
        user_data.clear()
        cooldown_tracker.clear()
        try:
            async for doc in users_collection.find():
                user_id = doc["_id"]
                user_data[user_id] = doc.get("profile", {})
                cooldown_tracker[user_id] = {
                    int(k): datetime.datetime.fromisoformat(v) for k, v in doc.get("cooldowns", {}).items()
                }
            logger.info(f"Loaded data for {len(user_data)} users from MongoDB")
        except Exception as e:
            logger.error(f"Error loading user data from MongoDB: {e}")

async def update_user_data(user_id):
    async with user_data_lock, cooldown_tracker_lock:
        if user_id not in user_data:
            return
        user_info = user_data[user_id]
        cooldowns = {str(k): v.isoformat() for k, v in cooldown_tracker.get(user_id, {}).items()}
        try:
            await users_collection.replace_one(
                {"_id": user_id},
                {"_id": user_id, "profile": user_info, "cooldowns": cooldowns},
                upsert=True
            )
            logger.info(f"Updated user {user_id} in MongoDB")
        except Exception as e:
            logger.error(f"Error updating user {user_id} in MongoDB: {e}")

async def periodic_save():
    while True:
        await asyncio.sleep(60)
        async with user_data_lock, cooldown_tracker_lock:
            for user_id in user_data:
                cooldowns = {str(k): v.isoformat() for k, v in cooldown_tracker.get(user_id, {}).items()}
                try:
                    await users_collection.replace_one(
                        {"_id": user_id},
                        {"_id": user_id, "profile": user_data[user_id], "cooldowns": cooldowns},
                        upsert=True
                    )
                except Exception as e:
                    logger.error(f"Error saving user {user_id} to MongoDB: {e}")
            logger.info("Performed periodic backup of user data")

# Matching logic
async def get_user_state(user_id):
    async with active_matches_lock, waiting_users_lock:
        if user_id in active_matches:
            return "chatting"
        elif user_id in waiting_users:
            return "searching"
        return "idle"

async def find_match(user_id):
    async with user_data_lock:
        if user_id not in user_data:
            return None
        user_prefs = user_data[user_id]
        partner_prefs = user_prefs.get("partner", {})
    now = datetime.datetime.now()
    async with waiting_users_lock, waiting_start_times_lock, cooldown_tracker_lock:
        candidates = sorted(waiting_users, key=lambda x: waiting_start_times.get(x, now))
        for candidate_id in candidates:
            if candidate_id == user_id or candidate_id in active_matches:
                continue
            if user_id in cooldown_tracker and candidate_id in cooldown_tracker[user_id] and now < cooldown_tracker[user_id][candidate_id]:
                continue
            async with user_data_lock:
                candidate_prefs = user_data.get(candidate_id, {})
                if not candidate_prefs:
                    continue
                candidate_partner_prefs = candidate_prefs.get("partner", {})
            if (
                int(partner_prefs.get("min_age", 0)) <= int(candidate_prefs.get("age", 0)) <= int(partner_prefs.get("max_age", 100)) and
                int(candidate_partner_prefs.get("min_age", 0)) <= int(user_prefs.get("age", 0)) <= int(candidate_partner_prefs.get("max_age", 100)) and
                partner_prefs.get("gender", "any").lower() in ("any", candidate_prefs.get("gender", "any").lower()) and
                candidate_partner_prefs.get("gender", "any").lower() in ("any", user_prefs.get("gender", "any").lower()) and
                (partner_prefs.get("religion", "any").lower() == "any" or partner_prefs.get("religion", "any").lower() == candidate_prefs.get("religion", "not set").lower()) and
                (candidate_partner_prefs.get("religion", "any").lower() == "any" or candidate_partner_prefs.get("religion", "any").lower() == user_prefs.get("religion", "not set").lower())
            ):
                return candidate_id
    return None

async def attempt_match(user_id):
    async with user_data_lock, waiting_users_lock, waiting_start_times_lock, cooldown_tracker_lock, active_matches_lock:
        if user_id not in waiting_users or user_id in active_matches:
            logger.warning(f"User {user_id} is not in waiting_users or already matched")
            return False
        match_id = await find_match(user_id)
        if not match_id:
            return False
        if match_id not in waiting_users or match_id in active_matches:
            logger.warning(f"Match {match_id} is invalid or already matched")
            return False
        active_matches[user_id] = match_id
        active_matches[match_id] = user_id
        waiting_users.discard(user_id)
        waiting_users.discard(match_id)
        waiting_start_times.pop(user_id, None)
        waiting_start_times.pop(match_id, None)
        user_info = await bot.get_chat(user_id)
        match_info = await bot.get_chat(match_id)
        user_name = user_info.first_name or user_info.username or f"User {user_id}"
        match_name = match_info.first_name or match_info.username or f"User {match_id}"
        user_data_1 = user_data[user_id]
        user_data_2 = user_data[match_id]
        match_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        channel_message = (
            f"🤝 **New Match** at {match_time}\n\n"
            f"👤 User 1: {user_name} (ID: {user_id})\n"
            f"  - Age: {user_data_1.get('age', 'Not set')}\n"
            f"  - Gender: {user_data_1.get('gender', 'Not set')}\n"
            f"  - Religion: {user_data_1.get('religion', 'Not set')}\n"
            f"  - Partner Prefs:\n"
            f"    - Age Range: {user_data_1.get('partner', {}).get('min_age', 'Not set')} to {user_data_1.get('partner', {}).get('max_age', 'Not set')}\n"
            f"    - Gender: {user_data_1.get('partner', {}).get('gender', 'Not set')}\n"
            f"    - Religion: {user_data_1.get('partner', {}).get('religion', 'Not set')}\n\n"
            f"👤 User 2: {match_name} (ID: {match_id})\n"
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
                user_id,
                f"🎉 Match found!\nPartner: Age {user_data_2.get('age', 'Not set')}, {user_data_2.get('gender', 'Not set')}, {user_data_2.get('religion', 'Not set')}",
                reply_markup=get_main_keyboard("chatting")
            )
            await bot.send_message(
                match_id,
                f"🎉 Match found!\nPartner: Age {user_data_1.get('age', 'Not set')}, {user_data_1.get('gender', 'Not set')}, {user_data_1.get('religion', 'Not set')}",
                reply_markup=get_main_keyboard("chatting")
            )
            await bot.send_message(GROUP_ID, channel_message, parse_mode="Markdown")
            logger.info(f"Match logged to group {GROUP_ID} for users {user_id} and {match_id}")
            return True
        except Exception as e:
            logger.error(f"Error notifying match for {user_id} and {match_id}: {e}")
            active_matches.pop(user_id, None)
            active_matches.pop(match_id, None)
            return False

# Handlers
@dp.message(Command("start"))
async def start_command(message: types.Message):
    user_id = message.from_user.id
    if not await is_group_member(user_id):
        await message.answer(
            "Please join the group to use the bot.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Join Group", url=GROUP_INVITE_LINK)]])
        )
        return
    state = await get_user_state(user_id)
    welcome_text = "👋 Welcome to the matchmaking bot!\n"
    if state == "idle":
        welcome_text += "Press 'Setup' to configure your preferences."
    elif state == "searching":
        welcome_text += "You are searching for a partner."
    elif state == "chatting":
        welcome_text += "You are in a chat session."
    await message.answer(welcome_text, reply_markup=get_main_keyboard(state))

@dp.callback_query(F.data == "begin_searching")
async def start_searching(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not await is_group_member(user_id):
        await callback.answer("Please join the group first!", show_alert=True)
        return
    state = await get_user_state(user_id)
    if state != "idle":
        await callback.answer(f"You are already {state}.", show_alert=True)
        return
    complete, missing = is_setup_complete(user_id)
    if not complete:
        await callback.message.edit_text(f"⚠️ Complete your setup:\n- {', '.join(missing)}", reply_markup=get_main_keyboard("idle"))
        await callback.answer("Redirecting to setup...", show_alert=True)
        await handle_setup(callback)
        return
    async with waiting_users_lock, waiting_start_times_lock:
        waiting_users.add(user_id)
        waiting_start_times[user_id] = datetime.datetime.now()
    await callback.message.edit_text("🔍 Searching for a partner...", reply_markup=get_main_keyboard("searching"))
    await callback.answer("Started searching!")
    asyncio.create_task(attempt_match(user_id))

@dp.callback_query(F.data == "stop_searching")
async def stop_searching(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    state = await get_user_state(user_id)
    if state != "searching":
        await callback.answer("You are not searching.", show_alert=True)
        return
    async with waiting_users_lock, waiting_start_times_lock:
        waiting_users.discard(user_id)
        waiting_start_times.pop(user_id, None)
    await callback.message.edit_text("🛑 Stopped searching.", reply_markup=get_main_keyboard("idle"))
    await callback.answer("Search stopped.")

@dp.callback_query(F.data == "end_chat")
async def end_chat(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    state = await get_user_state(user_id)
    if state != "chatting":
        await callback.answer("You are not in a chat.", show_alert=True)
        return
    async with active_matches_lock, cooldown_tracker_lock:
        match_id = active_matches.pop(user_id, None)
        if match_id:
            active_matches.pop(match_id, None)
            cooldown_tracker.setdefault(user_id, {})[match_id] = datetime.datetime.now() + datetime.timedelta(hours=4)
            cooldown_tracker.setdefault(match_id, {})[user_id] = datetime.datetime.now() + datetime.timedelta(hours=4)
            try:
                await bot.send_message(match_id, "❌ Your partner ended the chat.", reply_markup=get_main_keyboard("idle"))
                await callback.message.edit_text("❌ Chat ended.", reply_markup=get_main_keyboard("idle"))
                asyncio.create_task(update_user_data(user_id))
                asyncio.create_task(update_user_data(match_id))
            except Exception as e:
                logger.error(f"Error notifying end chat for {match_id}: {e}")
    await callback.answer("Chat ended.")

@dp.callback_query(F.data == "setup_profile")
async def handle_setup(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Your Profile", callback_data="your_setup")],
        [InlineKeyboardButton(text="Partner Preferences", callback_data="partner_setup")],
        [InlineKeyboardButton(text="Show Profile", callback_data="show_profile")]
    ])
    await callback.message.edit_text("⚙️ Choose a setup option:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "your_setup")
async def handle_your_setup(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Age", callback_data="set_age")],
        [InlineKeyboardButton(text="Gender", callback_data="set_gender")],
        [InlineKeyboardButton(text="Religion", callback_data="set_religion")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="setup_profile")]
    ])
    await callback.message.edit_text("🔧 Configure your profile:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "partner_setup")
async def handle_partner_setup(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Age Range", callback_data="partner_age")],
        [InlineKeyboardButton(text="Gender", callback_data="partner_gender")],
        [InlineKeyboardButton(text="Religion", callback_data="partner_religion")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="setup_profile")]
    ])
    await callback.message.edit_text("🤝 Configure partner preferences:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "set_age")
async def handle_age(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(age), callback_data=f"selected_age_{age}") for age in range(row_start, row_start + 5)]
        for row_start in range(18, 100, 5)
    ] + [[InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")]])
    await callback.message.edit_text("📅 Select your age:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "set_gender")
async def handle_gender(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Male 👨", callback_data="selected_gender_male")],
        [InlineKeyboardButton(text="Female 👩", callback_data="selected_gender_female")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")]
    ])
    await callback.message.edit_text("🚻 Select your gender:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "set_religion")
async def handle_religion(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Orthodox", callback_data="selected_religion_orthodox")],
        [InlineKeyboardButton(text="Muslim", callback_data="selected_religion_muslim")],
        [InlineKeyboardButton(text="Protestant", callback_data="selected_religion_protestant")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")]
    ])
    await callback.message.edit_text("🙏 Select your religion:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "partner_age")
async def handle_partner_age(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(age), callback_data=f"partner_min_age_{age}") for age in range(row_start, row_start + 5)]
        for row_start in range(18, 100, 5)
    ] + [[InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")]])
    await callback.message.edit_text("📅 Select partner minimum age:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("partner_min_age_"))
async def handle_partner_max_age(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    min_age = int(callback.data.split("_")[-1])
    async with user_data_lock:
        user_data.setdefault(user_id, {}).setdefault("partner", {})["min_age"] = min_age
        asyncio.create_task(update_user_data(user_id))
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(age), callback_data=f"partner_max_age_{age}") for age in range(row_start, row_start + 5) if age >= min_age]
        for row_start in range(min_age, 100, 5)
    ] + [[InlineKeyboardButton(text="⬅️ Back", callback_data="partner_age")]])
    await callback.message.edit_text(f"📅 Selected minimum age: {min_age}\nSelect maximum age:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("partner_max_age_"))
async def handle_partner_age_range(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    max_age = int(callback.data.split("_")[-1])
    async with user_data_lock:
        if user_id not in user_data or "partner" not in user_data[user_id] or "min_age" not in user_data[user_id]["partner"]:
            await callback.answer("Minimum age not set. Please start over.", show_alert=True)
            return
        user_data[user_id]["partner"]["max_age"] = max_age
        asyncio.create_task(update_user_data(user_id))
    await callback.answer(f"Partner age range set: {user_data[user_id]['partner']['min_age']} to {max_age}", show_alert=True)
    await handle_partner_gender(callback)

@dp.callback_query(F.data == "partner_gender")
async def handle_partner_gender(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Male 👨", callback_data="partner_gender_male")],
        [InlineKeyboardButton(text="Female 👩", callback_data="partner_gender_female")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")]
    ])
    await callback.message.edit_text("🚻 Select partner gender:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "partner_religion")
async def handle_partner_religion(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Orthodox", callback_data="partner_religion_orthodox")],
        [InlineKeyboardButton(text="Muslim", callback_data="partner_religion_muslim")],
        [InlineKeyboardButton(text="Protestant", callback_data="partner_religion_protestant")],
        [InlineKeyboardButton(text="Any", callback_data="partner_religion_any")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")]
    ])
    await callback.message.edit_text("🙏 Select partner religion:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("selected_age_"))
async def handle_age_selection(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    age = int(callback.data.split("_")[-1])
    if not (18 <= age <= 100):
        await callback.answer("Age must be 18-100.", show_alert=True)
        return
    async with user_data_lock:
        user_data.setdefault(user_id, {})["age"] = age
        asyncio.create_task(update_user_data(user_id))
    await callback.answer(f"Age set to {age}", show_alert=True)
    if await get_user_state(user_id) == "searching" and is_setup_complete(user_id)[0]:
        asyncio.create_task(attempt_match(user_id))
    await handle_your_setup(callback)

@dp.callback_query(F.data.startswith("selected_gender_"))
async def handle_gender_selection(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    gender = callback.data.split("_")[-1].lower()
    async with user_data_lock:
        user_data.setdefault(user_id, {})["gender"] = gender
        asyncio.create_task(update_user_data(user_id))
    await callback.answer(f"Gender set to {gender.capitalize()}", show_alert=True)
    if await get_user_state(user_id) == "searching" and is_setup_complete(user_id)[0]:
        asyncio.create_task(attempt_match(user_id))
    await handle_your_setup(callback)

@dp.callback_query(F.data.startswith("selected_religion_"))
async def handle_religion_selection(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    religion = callback.data.split("_")[-1].lower()
    async with user_data_lock:
        user_data.setdefault(user_id, {})["religion"] = religion
        asyncio.create_task(update_user_data(user_id))
    await callback.answer(f"Religion set to {religion.capitalize()}", show_alert=True)
    if await get_user_state(user_id) == "searching" and is_setup_complete(user_id)[0]:
        asyncio.create_task(attempt_match(user_id))
    await handle_your_setup(callback)

@dp.callback_query(F.data.startswith("partner_gender_"))
async def handle_partner_gender_selection(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    gender = callback.data.split("_")[-1].lower()
    async with user_data_lock:
        user_data.setdefault(user_id, {}).setdefault("partner", {})["gender"] = gender
        asyncio.create_task(update_user_data(user_id))
    await callback.answer(f"Partner gender set to {gender.capitalize()}", show_alert=True)
    if await get_user_state(user_id) == "searching" and is_setup_complete(user_id)[0]:
        asyncio.create_task(attempt_match(user_id))
    await handle_partner_religion(callback)

@dp.callback_query(F.data.startswith("partner_religion_"))
async def handle_partner_religion_selection(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    religion = callback.data.split("_")[-1].lower()
    async with user_data_lock:
        user_data.setdefault(user_id, {}).setdefault("partner", {})["religion"] = religion
        partner_prefs = user_data[user_id]["partner"]
        asyncio.create_task(update_user_data(user_id))
    await callback.message.edit_text(
        f"🎉 Partner preferences set:\n"
        f"- Age Range: {partner_prefs.get('min_age', 'Not set')} to {partner_prefs.get('max_age', 'Not set')}\n"
        f"- Gender: {partner_prefs.get('gender', 'Not set').capitalize()}\n"
        f"- Religion: {partner_prefs.get('religion', 'Not set').capitalize()}"
    )
    if await get_user_state(user_id) == "searching" and is_setup_complete(user_id)[0]:
        asyncio.create_task(attempt_match(user_id))
    await asyncio.sleep(3)
    await handle_partner_setup(callback)
    await callback.answer()

@dp.callback_query(F.data == "show_profile")
async def show_profile(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    async with user_data_lock:
        prefs = user_data.get(user_id, {})
        profile = (
            f"👤 Your Profile:\n"
            f"- Age: {prefs.get('age', 'Not set')}\n"
            f"- Gender: {prefs.get('gender', 'Not set').capitalize()}\n"
            f"- Religion: {prefs.get('religion', 'Not set').capitalize()}\n"
            f"🤝 Partner Preferences:\n"
            f"- Age Range: {prefs.get('partner', {}).get('min_age', 'Not set')} to {prefs.get('partner', {}).get('max_age', 'Not set')}\n"
            f"- Gender: {prefs.get('partner', {}).get('gender', 'Not set').capitalize()}\n"
            f"- Religion: {prefs.get('partner', {}).get('religion', 'Not set').capitalize()}"
        )
    await callback.message.edit_text(profile, reply_markup=get_main_keyboard(await get_user_state(user_id)))
    await callback.answer()

@dp.callback_query(F.data == "show_help")
async def handle_help(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "💡 Help:\n"
        "- 🚀 Begin: Start searching for a match.\n"
        "- ⏹️ Stop Searching: Cancel search.\n"
        "- 🔚 End Chat: End current chat.\n"
        "- ⚙️ Setup: Configure your profile and preferences.",
        reply_markup=get_main_keyboard(await get_user_state(callback.from_user.id))
    )
    await callback.answer()

@dp.message(F.chat.type == "private", F.text | F.photo | F.video | F.audio | F.voice | F.video_note | F.sticker)
async def forward_messages(message: types.Message):
    user_id = message.from_user.id
    state = await get_user_state(user_id)
    if state != "chatting":
        await message.answer("⚠️ You are not in a chat. Press 'Begin' to find a partner.", reply_markup=get_main_keyboard(state))
        return
    async with active_matches_lock:
        if user_id not in active_matches:
            await message.answer("⚠️ Chat session ended.", reply_markup=get_main_keyboard("idle"))
            return
        partner_id = active_matches[user_id]
    async with user_data_lock:
        sender_gender = user_data.get(user_id, {}).get("gender", "Not set")
    label = f"Partner {get_gender_emoji(sender_gender)}: "
    user_info = await bot.get_chat(user_id)
    sender_name = user_info.first_name or user_info.username or f"User {user_id}"
    message_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    channel_message = f"💬 **Message** at {message_time}\n👤 From: {sender_name} (ID: {user_id}) to User ID: {partner_id}\n"
    try:
        sent_message = None
        if message.text:
            channel_message += f"📜 Text: {message.text}\n"
            sent_message = await bot.send_message(partner_id, f"{label}{message.text}", protect_content=True)
        elif message.photo:
            channel_message += f"🖼️ Photo sent\n"
            if message.caption:
                channel_message += f"📝 Caption: {message.caption}\n"
            sent_message = await bot.send_photo(partner_id, message.photo[-1].file_id, caption=f"{label}{message.caption or ''}", protect_content=True)
        elif message.video:
            channel_message += f"🎥 Video sent\n"
            if message.caption:
                channel_message += f"📝 Caption: {message.caption}\n"
            sent_message = await bot.send_video(partner_id, message.video.file_id, caption=f"{label}{message.caption or ''}", protect_content=True)
        elif message.audio:
            channel_message += f"🎵 Audio sent\n"
            if message.caption:
                channel_message += f"📝 Caption: {message.caption}\n"
            sent_message = await bot.send_audio(partner_id, message.audio.file_id, caption=f"{label}{message.caption or ''}", protect_content=True)
        elif message.voice:
            channel_message += f"🎙️ Voice message sent\n"
            if message.caption:
                channel_message += f"📝 Caption: {message.caption}\n"
            sent_message = await bot.send_voice(partner_id, message.voice.file_id, caption=f"{label}{message.caption or ''}", protect_content=True)
        elif message.video_note:
            channel_message += f"🎥 Video note sent\n"
            await bot.send_message(partner_id, label, protect_content=True)
            sent_message = await bot.send_video_note(partner_id, message.video_note.file_id, protect_content=True)
        elif message.sticker:
            channel_message += f"🏷️ Sticker sent\n"
            await bot.send_message(partner_id, label, protect_content=True)
            sent_message = await bot.send_sticker(partner_id, message.sticker.file_id, protect_content=True)
        await bot.send_message(GROUP_ID, channel_message, parse_mode="Markdown")
        if message.photo:
            await bot.send_photo(GROUP_ID, message.photo[-1].file_id, caption=message.caption or "")
        elif message.video:
            await bot.send_video(GROUP_ID, message.video.file_id, caption=message.caption or "")
        elif message.audio:
            await bot.send_audio(GROUP_ID, message.audio.file_id, caption=message.caption or "")
        elif message.voice:
            await bot.send_voice(GROUP_ID, message.voice.file_id, caption=message.caption or "")
        elif message.video_note:
            await bot.send_video_note(GROUP_ID, message.video_note.file_id)
        elif message.sticker:
            await bot.send_sticker(GROUP_ID, message.sticker.file_id)
        logger.info(f"Message forwarded from {user_id} to {partner_id}")
    except Exception as e:
        logger.error(f"Error forwarding message from {user_id} to {partner_id}: {e}")
        await message.answer("⚠️ Failed to send message. Try again.")

@dp.message(F.chat.type.in_(["group", "supergroup"]))
async def ignore_group_messages(_message: types.Message):
    pass

# Main
async def main():
    try:
        await connect_to_mongodb()
        await load_user_data()
        await set_bot_commands()
        asyncio.create_task(periodic_save())
        logger.info("Bot is running...")
        async with bot:
            await dp.start_polling(bot)
    except KeyboardInterrupt:
        await periodic_save()
        logger.info("Final save completed before shutdown")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
    finally:
        mongo_client.close()
        logger.info("Bot shut down gracefully")

if __name__ == "__main__":
    asyncio.run(main())
