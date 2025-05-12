
#!/usr/bin/env python3
"""
Enhanced production entry point for the Discord bot with robust error handling
and diagnostic capabilities.
"""
import os
import sys
import asyncio
import logging
import traceback
import signal
import discord
from datetime import datetime
from logging.handlers import RotatingFileHandler
from bot import Bot

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        RotatingFileHandler("production.log", maxBytes=10000000, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("production")

# Create separate startup log
startup_logger = logging.getLogger("startup")
startup_handler = logging.FileHandler("bot_startup.log", mode="w")
startup_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s"))
startup_logger.addHandler(startup_handler)
startup_logger.setLevel(logging.INFO)

# Add restart log
try:
    with open("restart_log.txt", "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] Bot restarted in production mode\n")
except Exception as e:
    logger.error(f"Failed to write to restart log: {e}")

# Dictionary to keep track of loaded extensions
EXTENSIONS = [
    "cogs.help",
    "cogs.admin",
    "cogs.csv_processor",
    "cogs.log_processor",
    "cogs.killfeed",
    "cogs.stats",
    "cogs.rivalries",
    "cogs.bounties",
    "cogs.economy",
    "cogs.setup",
    "cogs.events",
    "cogs.premium",
    "cogs.player_links",
    "cogs.factions"
]

# Global variable to track if we're shutting down gracefully
shutting_down = False

def signal_handler(sig, frame):
    """Handle termination signals gracefully"""
    global shutting_down
    sig_name = signal.Signals(sig).name
    logger.info(f"Received signal {sig_name} ({sig})")
    shutting_down = True
    logger.info("Initiating graceful shutdown...")
    # Let the main loop handle the actual shutdown

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def safe_load_extensions(bot):
    """Safely load extensions with comprehensive error handling
    
    Args:
        bot: The bot instance
        
    Returns:
        tuple: (success_count, total_count)
    """
    startup_logger.info("Loading extensions...")
    success_count = 0
    failed_extensions = []
    
    for extension in EXTENSIONS:
        try:
            await bot.load_extension(extension)
            success_count += 1
            startup_logger.info(f"Loaded extension: {extension}")
        except Exception as e:
            error_msg = f"Failed to load extension {extension}: {e}"
            startup_logger.error(error_msg)
            logger.error(error_msg, exc_info=True)
            failed_extensions.append(extension)
    
    # Log summary
    total = len(EXTENSIONS)
    if failed_extensions is not None:
        startup_logger.warning(f"Failed to load {len(failed_extensions)} out of {total} extensions")
        startup_logger.warning(f"Failed extensions: {', '.join(failed_extensions)}")
    else:
        startup_logger.info(f"Successfully loaded all {total} extensions")
    
    return success_count, total

async def validate_environment():
    """Validate required environment variables
    
    Returns:
        bool: True if all required variables are set, False otherwise
    """
    startup_logger.info("Validating environment variables...")
    required_vars = ["DISCORD_TOKEN", "MONGODB_URI"]
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars is not None:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        startup_logger.critical(error_msg)
        logger.critical(error_msg)
        return False
    
    startup_logger.info("All required environment variables are set")
    return True

async def main():
    """Initialize and run the production bot with comprehensive error handling"""
    global shutting_down
    bot = None
    
    try:
        # Mark startup
        startup_logger.info("=== PRODUCTION BOT STARTUP SEQUENCE INITIATED ===")
        startup_time = datetime.now()
        logger.info(f"Starting bot at {startup_time}")
        
        # Check environment
        if not await validate_environment():
            return 1
        
        # Initialize bot with production settings
        startup_logger.info("Initializing production bot...")
        bot = Bot(
            production=True,
            debug_guilds=None  # No debug guilds in production
        )
        
        # Connect to database
        startup_logger.info("Connecting to database...")
        db_result = await bot.init_db(max_retries=3)
        if db_result is None:
            startup_logger.critical("Failed to connect to database after multiple attempts")
            return 1
        
        # Load extensions
        success_count, total_count = await safe_load_extensions(bot)
        if success_count == 0:
            startup_logger.critical("No extensions could be loaded - aborting startup")
            return 1
        
        # Start the bot
        startup_logger.info("Starting production bot...")
        discord_task = asyncio.create_task(bot.start(os.environ["DISCORD_TOKEN"]))
        
        # Monitor for shutdown signal
        while not shutting_down:
            await asyncio.sleep(1)
            
            # Check if the discord task has failed
            if discord_task.done():
                try:
                    # Will raise exception if the task failed
                    discord_task.result()
                    logger.info("Discord task completed normally")
                except Exception as e:
                    logger.critical(f"Discord connection failed: {e}")
                    logger.error(traceback.format_exc())
                    return 1
        
        # If we got here, we're shutting down gracefully
        logger.info("Initiating graceful shutdown sequence")
        try:
            if bot and bot.is_ready():
                await bot.close()
                logger.info("Bot connection closed gracefully")
        except Exception as e:
            logger.error(f"Error during graceful shutdown: {e}")
            
        return 0
            
    except discord.LoginFailure as e:
        startup_logger.critical(f"Discord login failed: {e}")
        logger.critical(f"Discord login failed: {e}")
        return 1
    except Exception as e:
        error_msg = f"Fatal error during production bot startup: {e}"
        startup_logger.critical(error_msg)
        logger.critical(error_msg, exc_info=True)
        
        # Write detailed error to file for debugging
        with open("bot_error.log", "w") as f:
            f.write(f"ERROR: {e}f\n\n")
            f.write(traceback.format_exc())
        
        return 1
    finally:
        # Always log shutdown
        startup_logger.info("Bot shutdown process complete")
        logger.info("Bot shutdown process complete")

# Entry point
if __name__ == "__main__":
    # Write startup marker
    try:
        with open("bot_restart.log", "a") as f:
            f.write(f"{datetime.now()} - Bot startup initiated\n")
    except Exception as e:
        logger.error(f"Failed to write to restart log: {e}")
    
    exit_code = 0
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot terminated by user")
        exit_code = 0
    except Exception as e:
        logger.critical(f"Unhandled exception in main(): {e}", exc_info=True)
        exit_code = 1
    
    sys.exit(exit_code)
