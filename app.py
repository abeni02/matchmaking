import uvicorn
import asyncio
import signal
import sys
import os
from fastapi import FastAPI, Request
from bot import main, bot, dp  # Import bot and dp from bot.py
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()
bot_task = None

# Health check endpoint for hosting platform
@app.get("/health")
async def health_check():
    return {"status": "OK"}

# Webhook endpoint for Telegram updates
@app.post("/webhook")
async def webhook(request: Request):
    try:
        update = await request.json()
        await dp.feed_raw_update(bot, update)
        return {"status": "OK"}
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return {"status": "error"}

# Handle shutdown signals
def signal_handler(sig, frame):
    logger.info("Received shutdown signal, stopping bot...")
    if bot_task:
        bot_task.cancel()
    sys.exit(0)

if __name__ == "__main__":
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Start bot as a single background task
    bot_task = asyncio.create_task(main())

    # Use environment variable for port
    port = int(os.getenv("PORT", 8080))

    # Run FastAPI server
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        workers=1,
        loop="asyncio"
    )
