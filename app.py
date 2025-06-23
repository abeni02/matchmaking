import uvicorn
from bot import main
from fastapi import FastAPI
import asyncio
import signal
import sys

app = FastAPI()
bot_task = None

@app.get("/health")
async def health_check():
    return {"status": "OK"}

def signal_handler(sig, frame):
    logger.info("Received shutdown signal, stopping bot...")
    if bot_task:
        bot_task.cancel()
    sys.exit(0)

if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Start the bot polling in a separate task
    bot_task = asyncio.create_task(main())
    
    # Run the FastAPI server
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8080,
        log_level="info",
        workers=1,
        loop="asyncio"
    )
