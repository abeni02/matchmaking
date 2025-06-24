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

# Store the bot task globally
bot_task = None

@app.on_event("startup")
async def on_startup():
    global bot_task
    logger.info("FastAPI startup - launching bot in background...")
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
    try:
        await bot.get_me()  # Check bot is responsive
        return {"status": "OK", "bot": "running"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "ERROR", "error": str(e)}

# Remove or disable webhook endpoint
# @app.post("/webhook")
# async def webhook(request: Request):
#     logger.warning("Webhook endpoint disabled as bot is using polling")
#     return {"status": "error", "message": "Bot is running in polling mode"}
