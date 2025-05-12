#!/usr/bin/env python
"""
Direct script to run a historical parse on all CSV files
"""
import asyncio
import logging
from datetime import datetime

from bot import Bot
from cogs.csv_processor import CSVProcessorCog

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("historical_parse")

async def run_historical_parse():
    """Run historical parse directly"""
    logger.info("Starting historical parse test")
    
    # Create a bot instance
    bot = Bot(production=False)
    
    # Initialize the database connection
    logger.info("Initializing database...")
    await bot.init_db()
    logger.info("Database initialized")
    
    # Create CSV processor cog
    logger.info("Creating CSV processor cog")
    cog = CSVProcessorCog(bot)
    
    # Run historical parse
    server_id = "5251382d-8bce-4abd-8bcb-cdef73698a46"
    days = 30
    
    logger.info(f"Starting historical parse for server_id {server_id} for the last {days} days")
    start_time = datetime.now()
    
    try:
        files_processed, events_processed = await cog.run_historical_parse(
            server_id=server_id, 
            days=days
        )
        
        logger.info(f"Historical parse completed in {(datetime.now() - start_time).total_seconds()} seconds")
        logger.info(f"Processed {files_processed} files with {events_processed} events")
    except Exception as e:
        logger.error(f"Error running historical parse: {e}", exc_info=True)
    
    # Close any open connections
    if hasattr(bot, 'db') and bot.db:
        logger.info("Closing database connection")
        # Close any open connections here if needed
    
    logger.info("Historical parse test completed")

if __name__ == "__main__":
    asyncio.run(run_historical_parse())