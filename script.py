from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    BotCommand,
    ReplyKeyboardMarkup,
    KeyboardButton
)
import asyncio
import json
import os
import datetime

# Bot token and channel ID setup
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')  # Private channel ID (e.g., -1001234567890)
if not BOT_TOKEN:
    raise ValueError("No BOT_TOKEN found in environment variables. Please set it securely.")
if not CHANNEL_ID:
    raise ValueError("No CHANNEL_ID found in environment variables. Please set it securely.")

bot = Bot(token=BOT_TOKEN)
router = Router()
dp = Dispatcher()
dp.include_router(router)

# Initialize data structures
user_data = {}  # Stores user preferences
USER_DATA_FILE = "user_data.json"
active_matches = {}  # Tracks active matches (user1: user2)
cooldown_tracker = {}  # Tracks cooldowns for specific user pairs {user_id: {partner_id: cooldown_end_time}}
waiting_users = set()  # Tracks users who pressed Begin but are unmatched
waiting_start_times = {}  # Tracks when each user started waiting {user_id: datetime.datetime}
message_id_map = {}  # Tracks message IDs: {sender_id: {original_message_id: forwarded_message_id}}

# Function to get gender emoji
def get_gender_emoji(gender):
    """Return gender emoji based on the provided gender."""
    if gender.lower() == "male":
        return "👨"
    elif gender.lower() == "female":
        return "👩"
    else:
        return "❓"  # Fallback for unset or unknown gender

# Function to save user data to a JSON file
async def save_user_data():
    """Save the user_data dictionary to a JSON file."""
    serializable_data = {str(user_id): data for user_id, data in user_data.items()}
    try:
        with open(USER_DATA_FILE, 'w', encoding='utf-8') as file:
            json.dump(serializable_data, file, indent=4, ensure_ascii=False)
        print(f"✅ User data saved to {USER_DATA_FILE}")
    except Exception as e:
        print(f"❌ Error saving user data: {e}")

# Function for immediate (non-awaited) saving
def save_user_data_now():
    """Immediately save user data without awaiting - for use after each data change."""
    asyncio.create_task(save_user_data())

# Function to load user data from a JSON file
def load_user_data():
    """Load the user_data dictionary from a JSON file if it exists."""
    global user_data
    try:
        if os.path.exists(USER_DATA_FILE):
            with open(USER_DATA_FILE, 'r', encoding='utf-8') as file:
                loaded_data = json.load(file)
                user_data = {int(user_id): data for user_id, data in loaded_data.items()}
            print(f"✅ User data loaded from {USER_DATA_FILE}")
            print(f"📊 Loaded data for {len(user_data)} users")
        else:
            print(f"ℹ️ No user data file found at {USER_DATA_FILE}")
    except Exception as e:
        print(f"❌ Error loading user data: {e}")

# Helper function to check if setup is complete
def is_setup_complete(user_id):
    """
    Check if the user has completed both 'Your Setup' and 'Partner Setup'.
    Returns a tuple: (is_complete, list_of_missing_fields)
    """
    if user_id not in user_data:
        return False, ["Age", "Gender", "Religion", "Partner Minimum Age", "Partner Maximum Age", "Partner Gender",
                       "Partner Religion"]

    user_prefs = user_data[user_id]
    missing_fields = []

    # Check Your Setup
    if "age" not in user_prefs or user_prefs["age"] == "Not set":
        missing_fields.append("Age")
    if "gender" not in user_prefs or user_prefs["gender"] == "Not set":
        missing_fields.append("Gender")
    if "religion" not in user_prefs or user_prefs["religion"] == "Not set":
        missing_fields.append("Religion")

    # Check Partner Setup
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

# Define the Reply Keyboard with dynamic "Begin" or "End"
def get_main_keyboard(is_begin: bool = True):
    """
    Returns a layout for the main menu.
    Dynamically switches between "Begin" and "End".
    """
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🚀 Begin" if is_begin else "🛑 End"),
                KeyboardButton(text="⚙️ Setup"),
            ],
            [
                KeyboardButton(text="❓ Help"),
            ],
        ],
        resize_keyboard=True
    )

# Define the Inline Keyboard for Setup options
def get_setup_inline_keyboard():
    """
    Returns an inline keyboard for the 'Setup' menu.
    Includes 'Your Setup', 'Partner Setup', and 'Show Setup'.
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Your Setup", callback_data="your_setup")],
            [InlineKeyboardButton(text="Partner Setup", callback_data="partner_setup")],
            [InlineKeyboardButton(text="Show Setup", callback_data="show_setup")],
        ]
    )

# Define the /start command
@router.message(F.text == "/start")
async def start_command(message: Message):
    """
    Handle the /start command:
    - Send welcome message.
    - Automatically redirect to Setup.
    """
    await message.answer(
        text="👋 Welcome to the bot!\n\nLet's get started by configuring your setup!",
        reply_markup=get_main_keyboard()
    )
    await show_setup_menu(message)

# Handle "Setup" button or command
@router.message(F.text.in_({"⚙️ Setup", "/setup"}))
async def handle_setup(message: Message):
    """
    Handle both the 'Setup' reply button and the /setup command.
    """
    await show_setup_menu(message)

async def show_setup_menu(message_or_callback):
    """
    Show inline keyboard setup menu either from a message or callback action.
    """
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

# Handle "Your Setup" inline button
@router.callback_query(F.data == "your_setup")
async def handle_your_setup(callback: CallbackQuery):
    """
    Handle "Your Setup" inline keyboard button.
    Replaces the current inline keyboard with specific options: Age, Gender, Religion.
    """
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

# Handle "Partner Setup" inline button
@router.callback_query(F.data == "partner_setup")
async def handle_partner_setup(callback: CallbackQuery):
    """
    Handle "Partner Setup" inline keyboard button.
    """
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
    """
    Return to the main Setup menu.
    """
    await callback.message.edit_text(
        text="⚙️ Please choose your setup option:",
        reply_markup=get_setup_inline_keyboard()
    )
    await callback.answer()

# Modified to prioritize users waiting longer and handle "Any" religion explicitly
def find_match(user_id):
    """
    Find a match for the user based on their preferences.
    Prioritize users who have been waiting longer by sorting based on waiting_start_times in ascending order.
    Apply 4-hour cooldown only for rematching with the same user.
    Explicitly handle "Any" religion to match with Orthodox, Muslim, or Protestant.
    """
    if user_id not in user_data:
        return None

    now = datetime.datetime.now()
    user_prefs = user_data[user_id]

    # Sort waiting users by waiting time (earliest first, i.e., longest wait first)
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

        # Check if candidate is a previous match and within cooldown
        if user_id in cooldown_tracker and candidate_id in cooldown_tracker[user_id]:
            cooldown_end = cooldown_tracker[user_id][candidate_id]
            if now < cooldown_end:
                continue  # Skip this candidate due to cooldown

        # Check for match compatibility
        partner_criteria = candidate_prefs.get("partner", {})
        user_partner_prefs = user_prefs.get("partner", {})

        # Religion compatibility: "Any" matches Orthodox, Muslim, or Protestant
        user_religion = user_prefs.get("religion", "Not set")
        candidate_religion = candidate_prefs.get("religion", "Not set")
        partner_religion_pref = partner_criteria.get("religion", "any")
        user_partner_religion_pref = user_prefs.get("partner", {}).get("religion", "any")

        # Candidate's partner religion preference must match user's religion
        candidate_religion_ok = (
            partner_religion_pref.lower() == "any" or
            partner_religion_pref == user_religion
        )
        # User's partner religion preference must match candidate's religion
        user_religion_ok = (
            user_partner_religion_pref.lower() == "any" or
            user_partner_religion_pref == candidate_religion
        )

        if (
            # Age compatibility
            (partner_criteria.get("min_age", 0) <= int(user_prefs.get("age", 0)) <= partner_criteria.get("max_age", 100))
            and (user_partner_prefs.get("min_age", 0) <= int(candidate_prefs.get("age", 0)) <= user_partner_prefs.get("max_age", 100))
            # Gender compatibility
            and (partner_criteria.get("gender", "any") in ("any", user_prefs.get("gender", "any")))
            and (user_partner_prefs.get("gender", "any") in ("any", candidate_prefs.get("gender", "any")))
            # Religion compatibility
            and candidate_religion_ok
            and user_religion_ok
        ):
            return candidate_id

    return None

async def attempt_match(user_id):
    """
    Attempt to match a user with another waiting user, notify both, and log match in private channel.
    """
    match_id = find_match(user_id)
    if match_id:
        active_matches[user_id] = match_id
        active_matches[match_id] = user_id
        waiting_users.discard(user_id)
        waiting_users.discard(match_id)

        # Remove from waiting start times
        waiting_start_times.pop(user_id, None)
        waiting_start_times.pop(match_id, None)

        user_data_1 = user_data[user_id]
        user_data_2 = user_data[match_id]

        # Fetch user names (first_name or username)
        user_1_info = await bot.get_chat(user_id)
        user_2_info = await bot.get_chat(match_id)
        user_1_name = user_1_info.first_name or user_1_info.username or f"User {user_id}"
        user_2_name = user_2_info.first_name or user_2_info.username or f"User {match_id}"

        # Notify users
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
            reply_markup=get_main_keyboard(is_begin=False),
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
            reply_markup=get_main_keyboard(is_begin=False),
        )

        # Log match to private channel
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

@router.message(F.text.in_({"🚀 Begin", "🛑 End", "/end"}))
async def handle_matching_button(message: Message):
    """
    Handle dynamic 'Begin' (start matchmaking), 'End' button, and '/end' command.
    Check if setup is complete before matching; apply 4-hour cooldown only for rematching with the same user.
    Record waiting start time when beginning a match.
    """
    user_id = message.from_user.id

    if message.text == "🚀 Begin":
        if user_id in active_matches:
            await message.answer("⚠️ You are already in a session. Please end it first.")
            return

        # Check if setup is complete
        is_complete, missing_fields = is_setup_complete(user_id)
        if not is_complete:
            await message.answer(
                text=(
                    f"⚠️ Please complete your setup before starting a match. Missing fields:\n"
                    f"- {', '.join(missing_fields)}\n"
                    "Redirecting to setup menu..."
                ),
                reply_markup=get_main_keyboard(is_begin=True)
            )
            await show_setup_menu(message)
            return

        # Record the waiting start time
        waiting_start_times[user_id] = datetime.datetime.now()

        # Attempt to find a match
        if await attempt_match(user_id):
            return
        else:
            waiting_users.add(user_id)
            await message.answer(
                "🔍 Waiting for a compatible partner. You'll be matched automatically when one is found.",
                reply_markup=get_main_keyboard(is_begin=True)
            )

    elif message.text in {"🛑 End", "/end"}:
        if user_id in active_matches:
            match_id = active_matches.pop(user_id)
            active_matches.pop(match_id, None)

            # Set 4-hour cooldown for this specific user-partner pair
            cooldown_period = datetime.timedelta(hours=4)
            now = datetime.datetime.now()
            cooldown_tracker.setdefault(user_id, {})[match_id] = now + cooldown_period
            cooldown_tracker.setdefault(match_id, {})[user_id] = now + cooldown_period

            # Clear message ID mappings for both users
            print(f"🗑️ Clearing message_id_map for user {user_id} and {match_id}")
            message_id_map.pop(user_id, None)
            message_id_map.pop(match_id, None)

            # Remove from waiting users and waiting start times
            waiting_users.discard(user_id)
            waiting_start_times.pop(user_id, None)

            await message.answer(
                "❌ You have ended the session. You can 'Begin' again to find a new partner immediately.",
                reply_markup=get_main_keyboard(is_begin=True))
            await bot.send_message(
                chat_id=match_id,
                text=(
                    "❌ Your partner has ended the session. You can 'Begin' again to find a new partner immediately."
                ),
                reply_markup=get_main_keyboard(is_begin=True))
        else:
            await message.answer("⚠️ You are not in an active session.")
        waiting_users.discard(user_id)
        waiting_start_times.pop(user_id, None)

# Handle "Help" button or command
@router.message(F.text.in_({"❓ Help", "/help"}))
async def handle_help(message: Message):
    """
    Handle both the 'Help' button and the /help command.
    """
    await message.answer(
        text=(
            "💡 Need assistance? Here's what you can do:\n"
            " - 🚀 Begin: Start your journey (after completing setup).\n"
            " - 🛑 End: Stop chatting with your partner.\n"
            " - ⚙️ Setup: Configure your preferences.\n"
            " - ❓ Help: Get guidance and information.\n"
            " - 📩 Reply: Swipe left on a message to reply to it."
        )
    )

@router.message(F.text | F.document | F.photo | F.video | F.audio | F.voice | F.video_note | F.sticker)
async def forward_messages(message: Message):
    """
    Forward messages between matched users anonymously, with "Partner" label first, no hyphen, and message beside it.
    Supports replies, logs original messages to CHANNEL_ID, and protects content.
    """
    user_id = message.from_user.id
    print(f"📩 Received message from {user_id}, type: {message.content_type}")
    print(f"📋 Active matches: {active_matches}")
    print(f"🗂️ Current message_id_map: {message_id_map}")

    if user_id not in active_matches:
        print(f"⚠️ User {user_id} not in active_matches")
        await message.answer("⚠️ You are not currently chatting with anyone. Press 'Begin' to find a partner.")
        return

    partner_id = active_matches[user_id]
    # Initialize message ID map for both users
    message_id_map.setdefault(user_id, {})
    message_id_map.setdefault(partner_id, {})

    # Get sender's gender and create the label
    sender_gender = user_data.get(user_id, {}).get("gender", "Not set")
    gender_emoji = get_gender_emoji(sender_gender)
    label = f"Partner {gender_emoji}: "

    # Check if this is a reply
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

    # Fetch sender's name for logging
    user_info = await bot.get_chat(user_id)
    sender_name = user_info.first_name or user_info.username or f"User {user_id}"

    # Prepare channel log message
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

        # Store the message ID mapping for other message types
        if forwarded_message and hasattr(forwarded_message, 'message_id') and message.content_type not in ('video_note', 'sticker'):
            message_id_map[user_id][message.message_id] = forwarded_message.message_id
            message_id_map[partner_id][forwarded_message.message_id] = message.message_id
            print(f"📌 Mapped message ID {message.message_id} (user {user_id}) to {forwarded_message.message_id} (user {partner_id})")
        else:
            print(f"⚠️ Failed to map message ID for {user_id}: No valid forwarded_message")

    except Exception as e:
        print(f"❌ Error forwarding message from {user_id} to {partner_id}: {e}")
        await message.answer("⚠️ Failed to send message. Please try again.")

    # Log the original message to the private channel
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

async def set_bot_commands():
    """
    Registers default bot commands that appear in the Telegram menu.
    """
    commands = [
        BotCommand(command="start", description="Start the bot"),
        BotCommand(command="begin", description="Begin your journey"),
        BotCommand(command="setup", description="Set up your preferences"),
        BotCommand(command="help", description="Get help or assistance"),
        BotCommand(command="end", description="End your session")
    ]
    await bot.set_my_commands(commands)

@router.callback_query(F.data == "age")
async def handle_age(callback: CallbackQuery):
    """
    Handle "Age" inline keyboard button.
    """
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=str(age), callback_data=f"selected_age_{age}")
                for age in range(row_start, row_start + 5)
            ]
            for row_start in range(18, 100, 5)
        ]
    )
    age_keyboard.inline_keyboard.append(
        [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")]
    )
    await callback.message.edit_text(
        text="📅 Select your age:",
        reply_markup=age_keyboard
    )
    await callback.answer()

@router.callback_query(F.data == "gender")
async def handle_gender(callback: CallbackQuery):
    """
    Handle "Gender" inline keyboard button.
    """
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="selected_gender_male")],
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="selected_gender_female")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(
        text="🚻 Please indicate your Gender:",
        reply_markup=gender_keyboard
    )
    await callback.answer()

@router.callback_query(F.data == "religion")
async def handle_religion(callback: CallbackQuery):
    """
    Handle "Religion" inline keyboard button.
    """
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox", callback_data="selected_religion_orthodox")],
            [InlineKeyboardButton(text="Muslim", callback_data="selected_religion_muslim")],
            [InlineKeyboardButton(text="Protestant", callback_data="selected_religion_protestant")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="your_setup")],
        ]
    )
    await callback.message.edit_text(
        text="🙏 Please select your religion:",
        reply_markup=religion_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("selected_age_"))
async def handle_age_selection(callback: CallbackQuery):
    """
    Handle the selection of a specific age and prompt for Gender selection.
    Check for match if user is waiting.
    """
    user_id = callback.from_user.id
    selected_age = callback.data.split("_")[-1]
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["age"] = selected_age
    save_user_data_now()
    await callback.answer(text=f"Your age is {selected_age}", show_alert=True)

    # Check for match if user is waiting
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)

    await handle_gender(callback)

@router.callback_query(F.data.startswith("selected_gender_"))
async def handle_gender_selection(callback: CallbackQuery):
    """
    Handle the selection of a specific Gender and prompt for religion selection.
    Check for match if user is waiting.
    """
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1]
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["gender"] = selected_gender
    save_user_data_now()
    await callback.answer(text=f"You selected {selected_gender}", show_alert=True)

    # Check for match if user is waiting
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)

    await handle_religion(callback)

@router.callback_query(F.data.startswith("selected_religion_"))
async def handle_religion_selection(callback: CallbackQuery):
    """
    Handle the selection of a specific religion, summarize user data, and return to Setup menu.
    Check for match if user is waiting.
    """
    user_id = callback.from_user.id
    selected_religion = callback.data.split("_")[-1].replace("_", " ").capitalize()
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]["religion"] = selected_religion
    save_user_data_now()
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

    # Check for match if user is waiting
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)

    await asyncio.sleep(5)
    await handle_back_to_setup(callback)

@router.callback_query(F.data == "partner_age")
async def handle_partner_minimum_age(callback: CallbackQuery):
    """
    Handle the start of partner age selection by asking for the minimum age.
    """
    age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=str(age), callback_data=f"partner_min_age_{age}")
                for age in range(row_start, row_start + 5)
            ]
            for row_start in range(18, 100, 5)
        ]
    )
    age_keyboard.inline_keyboard.append(
        [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")]
    )
    await callback.message.edit_text(
        text="📅 Select the **minimum age** for the partner:",
        reply_markup=age_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("partner_min_age_"))
async def handle_partner_maximum_age(callback: CallbackQuery):
    """
    Handle the selection of the partner's minimum age and prompt for the maximum age.
    Check for match if user is waiting.
    """
    user_id = callback.from_user.id
    min_age = int(callback.data.split("_")[-1])
    user_data.setdefault(user_id, {}).setdefault("partner", {})["min_age"] = min_age
    save_user_data_now()

    # Check for match if user is waiting
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)

    max_age_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=str(age), callback_data=f"partner_max_age_{age}")
                for age in range(row_start, row_start + 5) if age >= min_age
            ]
            for row_start in range(18, 100, 5)
        ]
    )
    max_age_keyboard.inline_keyboard.append(
        [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_age")]
    )
    await callback.message.edit_text(
        text=f"📅 Selected minimum age: **{min_age}**\nNow, select the **maximum age** for the partner:",
        reply_markup=max_age_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("partner_max_age_"))
async def handle_partner_age_range(callback: CallbackQuery):
    """
    Handle the selection of the partner's maximum age and proceed to Gender selection.
    Check for match if user is waiting.
    """
    user_id = callback.from_user.id
    max_age = int(callback.data.split("_")[-1])
    min_age = user_data[user_id]["partner"].get("min_age", None)
    if min_age is None:
        await callback.message.answer("❌ Minimum age not set. Please start from minimum age selection.")
        return
    user_data[user_id]["partner"]["max_age"] = max_age
    save_user_data_now()
    await callback.answer(
        text=f"🎉 Partner age range set: From {min_age} to {max_age}",
        show_alert=True
    )

    # Check for match if user is waiting
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)

    await handle_partner_gender(callback)

@router.callback_query(F.data == "partner_gender")
async def handle_partner_gender(callback: CallbackQuery):
    """
    Handle "Partner's Gender" inline keyboard button.
    """
    gender_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Male 🧑🏽‍🦱", callback_data="partner_gender_male")],
            [InlineKeyboardButton(text="Female 👩🏽‍🦰", callback_data="partner_gender_female")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(
        text="🚻 Please select your partner's gender:",
        reply_markup=gender_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("partner_gender_"))
async def handle_partner_gender_selection(callback: CallbackQuery):
    """
    Handle the selection of partner's Gender and proceed to religion selection.
    Check for match if user is waiting.
    """
    user_id = callback.from_user.id
    selected_gender = callback.data.split("_")[-1]
    if user_id not in user_data:
        user_data[user_id] = {}
    if "partner" not in user_data[user_id]:
        user_data[user_id]["partner"] = {}
    user_data[user_id]["partner"]["gender"] = selected_gender
    save_user_data_now()
    await callback.answer(
        text=f"🎉 Partner's Gender set to: {selected_gender.capitalize()}",
        show_alert=True
    )

    # Check for match if user is waiting
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)

    await handle_partner_religion(callback)

@router.callback_query(F.data == "partner_religion")
async def handle_partner_religion(callback: CallbackQuery):
    """
    Display the Partner's religion selection menu.
    """
    religion_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Orthodox", callback_data="partner_religion_orthodox")],
            [InlineKeyboardButton(text="Muslim", callback_data="partner_religion_muslim")],
            [InlineKeyboardButton(text="Protestant", callback_data="partner_religion_protestant")],
            [InlineKeyboardButton(text="Any", callback_data="partner_religion_Any")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="partner_setup")],
        ]
    )
    await callback.message.edit_text(
        text="🙏 Please select your partner's religion:",
        reply_markup=religion_keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("partner_religion_"))
async def handle_partner_religion_selection(callback: CallbackQuery):
    """
    Handle the selection of a specific partner's religion and return to Setup menu.
    Check for match if user is waiting.
    """
    user_id = callback.from_user.id
    selected_partner_religion = callback.data.split("_")[-1].replace("_", " ").capitalize()
    if user_id not in user_data:
        user_data[user_id] = {}
    if "partner" not in user_data[user_id]:
        user_data[user_id]["partner"] = {}
    user_data[user_id]["partner"]["religion"] = selected_partner_religion
    save_user_data_now()
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

    # Check for match if user is waiting
    if user_id in waiting_users and is_setup_complete(user_id)[0]:
        await attempt_match(user_id)

    await asyncio.sleep(5)
    await handle_back_to_setup(callback)

@router.callback_query(F.data == "show_setup")
async def handle_show_setup(callback: CallbackQuery):
    """
    Handle the "Show Setup" button to display results of both setups.
    """
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
    asyncio.create_task(save_user_data())
    await callback.message.edit_text(
        text=result_text,
        reply_markup=get_setup_inline_keyboard()
    )
    await callback.answer()

async def periodic_save():
    """Periodically save user data to avoid data loss."""
    while True:
        await asyncio.sleep(60)
        await save_user_data()
        print("🔄 Performed periodic backup of user data")

async def main():
    load_user_data()
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