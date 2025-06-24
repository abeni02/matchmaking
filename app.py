import os
import logging
import asyncio
from fastapi import FastAPI, Request
from bot import main, bot, dp

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Store the bot task globally so we can manage it if needed
bot_task = None

@app.on_event("startup")
async def on_startup():
    global bot_task
    logger.info("FastAPI startup - launching bot in background...")
    # Run your bot's main in the background (does NOT block FastAPI)
    bot_task = asyncio.create_task(main())

@app.on_event("shutdown")
async def on_shutdown():
    global bot_task
    logger.info("FastAPI shutting down...")
    if bot_task:
        bot_task.cancel()
        try:
            await bot_task
        except asyncio.CancelledError:
            logger.info("Bot task cancelled cleanly.")

@app.get("/health")
async def health_check():
    logger.info("Health check requested")
    return {"status": "OK"}

@app.post("/webhook")
async def webhook(request: Request):
    try:
        update = await request.json()
        logger.info("Received webhook update")
        await dp.feed_raw_update(bot, update)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return {"status": "error"}
