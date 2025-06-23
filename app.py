import os
import logging
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

@app.on_event("startup")
async def on_startup():
    logger.info("FastAPI startup - launching bot...")
    try:
        await main()
    except Exception as e:
        logger.warning(f"Bot main() exited or another instance has lock: {e}")
        # Do not crash app — keep FastAPI server running

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
