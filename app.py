import asyncio
import uvicorn
from bot import main  # Import the main coroutine from bot.py

# Define an async ASGI application
async def app():
    try:
        # Run the bot's main coroutine
        await main()
    except Exception as e:
        print(f"Error running bot: {e}")
        # Keep the server running for health checks
        await asyncio.sleep(3600)  # Uvicorn entry point
if __name__ == "__main__":
    # Configure uvicorn server
    config = uvicorn.Config(
        app=app:app,
        host="0.0.0.0",
        port=8080,
        log_level="info",
        workers=1,  # Single worker to avoid multiprocessing issues
    )
    server = uvicorn.Server(config)
    server.run()
