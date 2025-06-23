import os
import logging
from fastapi import FastAPI, Request
from aiogram import Bot
from bot import bot, dp, main as bot_main, acquire_instance_lock

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
lock_id = None

@app.on_event("startup")
async def on_startup():
    global lock_id
    logger.info("Starting bot as webhook worker")
    try:
        lock_id = await acquire_instance_lock()
        await bot_main()  # This contains bot.set_webhook + background tasks
    except Exception as e:
        logger.warning(f"Bot won't start (possibly already running): {e}")
        # Don't crash — just run FastAPI normally

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/webhook")
async def webhook(request: Request):
    try:
        update = await request.json()
        await dp.feed_raw_update(bot, update)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return {"status": "error"}
