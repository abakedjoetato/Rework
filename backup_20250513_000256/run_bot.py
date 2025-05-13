#!/usr/bin/env python3
"""
Bot execution script - runs the Discord bot
"""
import asyncio
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        RotatingFileHandler("bot.log", maxBytes=10000000, backupCount=5),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("run_bot")

async def run_bot():
    """Run the Discord bot with proper error handling"""
    try:
        # Import the bot here to avoid circular imports
        from main import main
        
        # Run the main function from main.py
        logger.info("Starting bot...")
        exit_code = await main()
        return exit_code
    except Exception as e:
        logger.critical(f"Failed to run bot: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    try:
        # Check if the required environment variables are set
        if not os.environ.get("DISCORD_TOKEN"):
            logger.error("DISCORD_TOKEN is not set. Please set it before running the bot.")
            sys.exit(1)
            
        if not os.environ.get("MONGODB_URI"):
            logger.error("MONGODB_URI is not set. Please set it before running the bot.")
            sys.exit(1)
            
        # Run the bot
        exit_code = asyncio.run(run_bot())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)