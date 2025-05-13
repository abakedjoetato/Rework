"""
Script to manually trigger the CSV processor for testing.
"""
import logging
import asyncio
import sys
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_test.log')
    ]
)
logger = logging.getLogger("csv_test")

async def main():
    try:
        # Import Discord.py commands
        from discord.ext import commands
        
        # Import our CSV processor cog for direct access
        sys.path.insert(0, os.path.abspath('.'))
        from cogs.csv_processor import CSVProcessorCog
        
        logger.info(f"Manually triggering CSV processor at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Create a mock bot class
        class MockBot(commands.Bot):
            def __init__(self):
                super().__init__(command_prefix="!", intents=None)
                self._db = None
                self._background_tasks = {}
                self._sftp_connections = {}
                self._home_guild_id = None
                
            @property
            def db(self):
                return self._db
                
            @db.setter
            def db(self, value):
                self._db = value
                
            @property
            def background_tasks(self):
                return self._background_tasks
                
            @background_tasks.setter
            def background_tasks(self, value):
                self._background_tasks = value
                
            @property
            def sftp_connections(self):
                return self._sftp_connections
                
            @sftp_connections.setter
            def sftp_connections(self, value):
                self._sftp_connections = value
                
            @property
            def home_guild_id(self):
                return self._home_guild_id
                
            @home_guild_id.setter
            def home_guild_id(self, value):
                self._home_guild_id = value
                
        # Initialize a mock bot
        bot = MockBot()
        
        # Create a dummy context
        class MockContext:
            pass
            
        ctx = MockContext()
        
        # Create the CSV processor cog
        cog = CSVProcessorCog(bot)
        
        # Force run the process_csv_files method to test our fixes
        logger.info("Running CSV processor directly")
        await cog.process_csv_files()
        
        logger.info("CSV processor test complete")
    except Exception as e:
        logger.error(f"Error running CSV processor: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main())