#!/usr/bin/env python3
"""
Tower of Temptation PvP Statistics Bot - Main Entry Point

This script first applies all necessary fixes, then initializes and starts 
the Discord bot with enhanced error handling and MongoDB connection validation.
"""

import os
import sys
import asyncio
import logging
import traceback
import subprocess
from dotenv import load_dotenv
from bot import Bot

# Load environment variables from .env file if present
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log")
    ]
)
logger = logging.getLogger("main")

async def main():
    """
    Main entry point for the Discord bot
    """
    # Check environment variables
    logger.info("Checking environment variables...")
    # Directly check required environment variables
    required_vars = ["MONGODB_URI", "DISCORD_TOKEN"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        for var in missing_vars:
            logger.critical(f"Missing required environment variable: {var}")
        logger.critical("Bot cannot start without required environment variables")
        return
    
    logger.info("All required environment variables are present")
    # Initialize the bot
    bot = Bot(production=True)

    # Set up additional error handlers
    @bot.event
    async def on_error(event, *args, **kwargs):
        """Global error handler for all Discord events"""
        logger.error(f"Error in event {event}: {sys.exc_info()[1]}")
        logger.error(traceback.format_exc())

    # Initialize database connection
    db_success = await bot.init_db(max_retries=3, retry_delay=5)
    if not db_success:
        logger.critical("Failed to initialize database. Bot cannot start!")
        return

    # Load core extensions
    extensions = [
        'cogs.admin',
        'cogs.analytics',
        'cogs.auto_bounty',
        'cogs.commands',
        'cogs.csv_processor',
        'cogs.debug',
        'cogs.match_history',
        'cogs.player_stats',
        'cogs.premium',
        'cogs.server_management',
        'cogs.stats'
    ]

    for extension in extensions:
        try:
            await bot.load_extension(extension)
        except Exception as e:
            logger.error(f"Failed to load extension {extension}: {e}")
            logger.error(traceback.format_exc())

    # Get Discord token from environment
    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        logger.critical("DISCORD_TOKEN not found in environment variables")
        return

    # Start the bot
    try:
        logger.info("Starting bot...")
        await bot.start(token)
    except Exception as e:
        logger.critical(f"Failed to start bot: {e}")
        logger.critical(traceback.format_exc())
    finally:
        logger.info("Bot is shutting down...")
        await bot.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by keyboard interrupt")
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)