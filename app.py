from fastapi import FastAPI
import asyncio
import os
import logging
import httpx
from telegram.ext import Updater
import pymongo
from datetime import datetime, timedelta

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()
bot_started = False

def acquire_lock(client, instance_id):
    lock_collection = client[os.getenv("MONGO_DB_NAME", "your_database")]["instance_locks"]
    lock_collection.delete_many({"timestamp": {"$lt": datetime.utcnow() - timedelta(hours=1)}})
    try:
        result = lock_collection.insert_one({"instance_id": instance_id, "timestamp": datetime.utcnow()})
        return bool(result.inserted_id)
    except pymongo.errors.DuplicateKeyError:
        return False

async def set_webhook():
    bot_token = os.getenv("BOT_TOKEN")
    webhook_url = os.getenv("WEBHOOK_URL", "https://intelligent-bertina-abeni02-04f16b55.koyeb.app/webhook")
    async with httpx.AsyncClient() as client:
        response = await client.get(f"https://api.telegram.org/bot{bot_token}/setWebhook?url={webhook_url}")
        logger.info(f"Webhook set: {response.json()}")
        return response.json()

async def run_bot():
    global bot_started
    if bot_started:
        logger.warning("Bot already started, skipping")
        return
    try:
        # Initialize MongoDB client (example)
        mongo_client = pymongo.MongoClient(os.getenv("MONGO_URI"))
        if not acquire_lock(mongo_client, "bot_instance_1"):
            logger.error("Another bot instance is already running")
            raise RuntimeError("Another bot instance is already running")

        logger.info("Starting bot as webhook worker...")
        updater = Updater(token=os.getenv("BOT_TOKEN"), use_context=True)
        updater.start_webhook(
            listen="0.0.0.0",
            port=int(os.getenv("PORT", 8080)),
            url_path="/webhook",
            webhook_url="https://intelligent-bertina-abeni02-04f16b55.koyeb.app/webhook"
        )
        bot_started = True
        await updater.idle()
    except Exception as e:
        logger.error(f"Bot failed: {e}", exc_info=True)
        raise
    finally:
        mongo_client.close()

@app.on_event("startup")
async def startup_event():
    await set_webhook()
    asyncio.create_task(run_bot())

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.post("/webhook")
async def webhook(data: dict):
    logger.debug(f"Received webhook data: {data}")
    return {"status": "ok"}
