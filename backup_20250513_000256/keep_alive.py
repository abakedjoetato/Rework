"""
Keep alive module to maintain the bot running on Replit.
Creates a simple HTTP server to handle pings.
"""
import threading
from flask import Flask
import os
import logging

logger = logging.getLogger(__name__)

# Set up a small Flask app to keep the bot alive
keep_alive_app = Flask(__name__)

@keep_alive_app.route('/')
def home():
    """Return a simple message for health checks"""
    return "Discord bot is running!"

def run():
    """Run the Flask app in a separate thread"""
    # Use the PORT environment variable provided by Replit, or default to 8080
    port = int(os.environ.get('PORT', 8080))
    keep_alive_app.run(host='0.0.0.0', port=port)

def keep_alive():
    """
    Start the Flask server in a separate thread to keep the bot alive.
    This function is called from main.py.
    """
    logger.info("Starting keep-alive web server...")
    thread = threading.Thread(target=run)
    thread.daemon = True  # The thread will exit when the main program exits
    thread.start()
    logger.info(f"Keep-alive server started on port {os.environ.get('PORT', 8080)}")