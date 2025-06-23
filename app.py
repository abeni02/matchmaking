import os
import logging
from fastapi import FastAPI, Request
from aiogram import Bot
from bot import bot, dp, main as bot_main

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.on_event("startup")
async def on_startup():
    logger.info("Starting bot as webhook worker")
    await bot_main()  # <-- runs the bot inside the FastAPI lifecycle

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
