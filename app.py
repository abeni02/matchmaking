from flask import Flask
import os

# Initialize Flask app with __name__
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello from Koyeb'

if __name__ == '__main__':
    # Use the PORT environment variable provided by Koyeb, default to 8080 for local testing
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
