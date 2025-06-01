import asyncio
import os
import logging
import time
from aiohttp import web, ClientSession

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name)

async def hello_world(request):
    logger.info("Received request to root endpoint")
    return web.Response(text="Hello from Koyeb")

async def keep_alive_task():
    url = os.getenv('APP_URL', 'https://your-app-name.koyeb.app')
    async with ClientSession() as session:
        while True:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        logger.info(f"Successfully pinged {url}")
                    else:
                        logger.warning(f"Ping to {url} returned status {response.status}")
            except Exception as e:
                logger.error(f"Ping to {url} failed: {e}")
            await asyncio.sleep(300)  # Ping every 5 minutes

async def main():
    app = web.Application()
    app.add_routes([web.get('/', hello_world)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Started aiohttp server on port {port}")
    # Start keep_alive task in the background
    asyncio.create_task(keep_alive_task())
    # Wait forever
    await asyncio.Event().wait()

if name == 'main':
    asyncio.run(main())
