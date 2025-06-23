import os
import signal
import sys
import asyncio
import logging
import uvicorn
from fastapi import FastAPI, Request
from bot import main, bot, dp

# Configure logging
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
bot_task = None

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

def signal_handler(sig, frame):
    logger.info(f"Received signal {sig}, initiating shutdown...")
    if bot_task:
        bot_task.cancel()
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Start bot's main function as a background task
    bot_task = asyncio.create_task(main())

    # Run FastAPI server
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        workers=1
    )
