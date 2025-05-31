import asyncio
import threading
import os
import logging
from flask import Flask
import requests
import time
from aiogram import Bot, Dispatcher, Router, F
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
from motor.motor_asyncio import AsyncIOMotorClient

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Flask app setup
flask_app = Flask(__name__)

@flask_app.route('/')
def hello_world():
    logger.info("Received request to root endpoint")
    return 'Hello from Koyeb'

# Keep-alive thread
def keep_alive():
    url = os.getenv('APP_URL', 'https://your-app-name.koyeb.app')
    while True:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                logger.info(f"Successfully pinged {url} to keep alive")
            else:
                logger.warning(f"Ping to {url} returned status code {response.status_code}")
        except requests.RequestException as e:
            logger.error(f"Ping to {url} failed: {e}")
        time.sleep(300)  # Ping every 5 minutes

# Run Flask in a thread
def run_flask():
    port = int(os.getenv('PORT', 8080))
    logger.info(f"Starting Flask app on port {port}")
    flask_app.run(host='0.0.0.0', port=port)

# Telegram bot setup (copied from bot.py, abbreviated for brevity)
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')
GROUP_ID = os.getenv('GROUP_ID')
GROUP_INVITE_LINK = os.getenv('GROUP_INVITE_LINK')
MONGODB_URI = os.getenv('MONGODB_URI')

if not all([BOT_TOKEN, CHANNEL_ID, GROUP_ID, GROUP_INVITE_LINK, MONGODB_URI]):
    missing = [k for k, v in {'BOT_TOKEN': BOT_TOKEN, 'CHANNEL_ID': CHANNEL_ID, 'GROUP_ID': GROUP_ID,
                              'GROUP_INVITE_LINK': GROUP_INVITE_LINK, 'MONGODB_URI': MONGODB_URI}.items() if not v]
    raise ValueError(f"Missing environment variables: {', '.join(missing)}")

bot = Bot(token=BOT_TOKEN)
router = Router()
dp = Dispatcher()
dp.include_router(router)

client = AsyncIOMotorClient(MONGODB_URI)
db = client['bot_database']
users_collection = db['users']

# ... (Include all bot.py code: user_data, functions, handlers, etc.)

async def main():
    await load_user_data()
    logger.info("🤖 Bot is running...")
    logger.info("💾 Individual data points will be saved immediately upon change")
    logger.info("💾 Automatic backups will occur every minute")
    await set_bot_commands()
    periodic_save_task = asyncio.create_task(periodic_save())
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    # Start keep-alive thread
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    try:
        async with bot:
            await dp.start_polling(bot)
    except KeyboardInterrupt:
        await save_user_data()
        logger.info("💾 Final save completed before shutdown")
    finally:
        periodic_save_task.cancel()
        try:
            await periodic_save_task
        except asyncio.CancelledError:
            pass
        logger.info("👋 Bot has shut down gracefully")

if __name__ == "__main__":
    asyncio.run(main())
