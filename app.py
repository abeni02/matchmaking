import uvicorn
from bot import main  # Import the main coroutine from bot.py

# Define a minimal ASGI application
async def app(scope, receive, send):
    if scope["type"] == "http":
        # Respond to health checks with a simple 200 OK
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain")],
        })
        await send({
            "type": "http.response.body",
            "body": b"OK",
        })
    # Run the bot in the background
    await main()

if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8080,
        log_level="info",
        workers=1  # Single worker to avoid multiprocessing issues
    )
