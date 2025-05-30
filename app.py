from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient
import os

# MongoDB setup
MONGODB_URI = os.getenv("MONGODB_URI")
client = AsyncIOMotorClient(MONGODB_URI)
db = client["telegram_bot_db"]
users_collection = db["users"]

# Bot setup
bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()

# Telegram /start handler
@dp.message(Command(commands=["start"]))
async def start(message: types.Message):
    user_id = message.from_user.id
    user = await users_collection.find_one({"user_id": user_id})
    if not user:
        await users_collection.insert_one({"user_id": user_id, "message_count": 1})
        await message.reply("Welcome! You're now registered.")
    else:
        await users_collection.update_one({"user_id": user_id}, {"$inc": {"message_count": 1}})
        count = (await users_collection.find_one({"user_id": user_id}))["message_count"]
        await message.reply(f"You've sent {count} messages.")

# Webhook handler for Telegram updates
async def handle_webhook(request):
    update = await request.json()
    await dp.feed_raw_update(bot, update)
    return web.Response()

# Mimic Flask's / route
async def hello_world(request):
    return web.Response(text="Hello from KOYE!")

# Ping route for uptime monitoring
async def ping(request):
    return web.Response(text="Bot is alive")

app = web.Application()
app.router.add_post('/', handle_webhook)  # Telegram webhooks
app.router.add_get('/', hello_world)      # Mimics Flask's route
app.router.add_get('/ping', ping)         # Uptime monitoring

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8080)
