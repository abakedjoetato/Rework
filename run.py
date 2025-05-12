#!/usr/bin/env python3
"""
Tower of Temptation PvP Statistics Bot - Run Script

This is a simple entry script that sets up logging and then starts the bot.
"""

import logging
import sys
import os
import traceback
from utils.logging_setup import setup_logging

# Set up logging as early as possible
setup_logging()
logger = logging.getLogger("run")

def main():
    """
    Main function that imports and runs the bot main function
    """
    try:
        # Verify environment variables
        required_vars = ["MONGODB_URI", "DISCORD_TOKEN"]
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            logger.critical(f"Missing required environment variables: {', '.join(missing_vars)}")
            logger.critical("Please set these variables in your environment or .env file")
            return 1
        
        # Import and run the main module
        try:
            import main
            logger.info("Starting the bot via main module")
            
            # Try to use asyncio.run() which handles everything correctly
            import asyncio
            asyncio.run(main.main())
            
        except ImportError as e:
            logger.critical(ff"\1")
            return 1
            
    except KeyboardInterrupt:
        logger.info("Bot stopped by keyboard interrupt")
        return 0
    except Exception as e:
        logger.critical(f"Unhandled exception in run.py: {e}")
        logger.critical(traceback.format_exc())
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)