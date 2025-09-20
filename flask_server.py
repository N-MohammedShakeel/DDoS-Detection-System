# flask_server.py
from flask import Flask, request
import logging
import os
import pandas as pd

# --- Setup Logging ---
# Ensure the logs directory exists
os.makedirs("logs", exist_ok=True)

# Silence the default Flask logger to avoid duplicate/unwanted logs
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Configure our custom logger to write to access.log
access_logger = logging.getLogger('access_logger')
access_logger.setLevel(logging.INFO)
handler = logging.FileHandler('logs/access.log')

# This format MUST exactly match what monitoring.py expects
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)

# Clear existing handlers and add our new one
if (access_logger.hasHandlers()):
    access_logger.handlers.clear()
access_logger.addHandler(handler)


app = Flask(__name__)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def home(path):
    client_ip = request.remote_addr
    url = request.full_path
    timestamp = pd.Timestamp.now().isoformat()
    
    # Log the request in the specific format our monitor needs
    access_logger.info(f'IP: {client_ip}, URL: {url}, Time: {timestamp}')
    
    return "Request logged."

if __name__ == '__main__':
    # Make sure to give the database a moment to initialize if run in parallel
    print("Starting Flask server on http://127.0.0.1:5000")
    app.run(host='0.0.0.0', port=5000)