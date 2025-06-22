import uvicorn
from bot import main  # Import the main function from bot.py

if __name__ == "__main__":
    uvicorn.run(
        main,  # Reference the main coroutine directly
        host="0.0.0.0",
        port=8080,
        log_level="info",
        workers=1  # Single worker, as aiogram handles concurrency
    )
