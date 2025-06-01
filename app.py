from flask import Flask
import os

app = Flask(name)

@app.route('/')
def hello_world():
    return 'Hello from Koyeb'

if name == 'main':
    # Use the PORT environment variable provided by Koyeb, default to 8000 for local testing
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
