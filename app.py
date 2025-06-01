from flask import Flask
import os
import threading
import requests
import time
import logging

# Set up logging for better debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name)

app = Flask(name)

@app.route('/')
def hello_world():
    logger.info("Received request to root endpoint")
    return 'Hello from Koyeb'

def keep_alive():
    # Use APP_URL environment variable or default to a placeholder (update after deployment)
    url = os.getenv('APP_URL', 'https://your-app-name.koyeb.app')
    while True:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                logger.info(f"Successfully pinged {url} to keep alive")
            else:
                logger.warning(f"Ping to {url} returned status code {response.status_code}")
        except requests.RequestException as e:
            logger.error(f"Ping to {url} failed: {e}")
        time.sleep(300)  # Ping every 5 minutes to prevent sleep

if name == 'main':
    # Start keep-alive thread
    logger.info("Starting keep-alive thread")
    threading.Thread(target=keep_alive, daemon=True).start()
    
    # Get port from environment variable (Koyeb sets PORT) or default to 8080
    port = int(os.getenv('PORT', 8080))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port)
