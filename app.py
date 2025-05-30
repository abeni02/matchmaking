from aiogram import Bot, Dispatcher, types
from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient
import os

MONGODB_URI = os.getenv("MONGODB_URI")
client = AsyncIOMotorClient(MONGODB_URI)
db = client["telegram_bot_db"]
users_collection = db["users"]

bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher(bot)

@dp.message_handler(commands=['start'])
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

async def handle_webhook(request):
    update = await request.json()
    await dp.process_update(types.Update.de_json(update))
    return web.Response()

app = web.Application()
app.router.add_post('/', handle_webhook)
app.router.add_get('/ping', lambda request: web.Response(text="Bot is alive"))

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8080)
