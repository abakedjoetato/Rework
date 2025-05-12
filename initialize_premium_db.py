"""
Initialize premium system database collections and indexes.

This script creates the necessary collections and indexes for the premium system.
Run this script once before using the premium system.
"""
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def initialize_collections(db) -> Dict[str, bool]:
    """
    Initialize collections for premium system.
    
    Args:
        db: MongoDB database connection
        
    Returns:
        Dict[str, bool]: Status of each collection initialization
    """
    if db is None:
        logger.error("Database connection is None")
        return {"error": "Database connection is None"}
    
    results = {}
    
    try:
        # Create premium_guilds collection if it doesn't exist
        collections = await db.list_collection_names()
        
        # Initialize premium_guilds collection
        if "premium_guilds" not in collections:
            logger.info("Creating premium_guilds collection")
            await db.create_collection("premium_guilds")
            results["premium_guilds"] = True
        else:
            logger.info("premium_guilds collection already exists")
            results["premium_guilds"] = False
        
        # Initialize premium_servers collection
        if "premium_servers" not in collections:
            logger.info("Creating premium_servers collection")
            await db.create_collection("premium_servers")
            results["premium_servers"] = True
        else:
            logger.info("premium_servers collection already exists")
            results["premium_servers"] = False
        
        # Initialize premium_payments collection
        if "premium_payments" not in collections:
            logger.info("Creating premium_payments collection")
            await db.create_collection("premium_payments")
            results["premium_payments"] = True
        else:
            logger.info("premium_payments collection already exists")
            results["premium_payments"] = False
        
        return results
        
    except Exception as e:
        logger.error(ff"\1")
        return {"error": str(e)}


async def create_indexes(db) -> Dict[str, bool]:
    """
    Create indexes for premium system collections.
    
    Args:
        db: MongoDB database connection
        
    Returns:
        Dict[str, bool]: Status of each index creation
    """
    if db is None:
        logger.error("Database connection is None")
        return {"error": "Database connection is None"}
    
    results = {}
    
    try:
        # Create indexes for premium_guilds collection
        logger.info("Creating indexes for premium_guilds collection")
        await db.premium_guilds.create_index("guild_id", unique=True)
        results["premium_guilds_guild_id"] = True
        
        # Create indexes for premium_servers collection
        logger.info("Creating indexes for premium_servers collection")
        await db.premium_servers.create_index("server_id", unique=True)
        await db.premium_servers.create_index("guild_id")
        results["premium_servers_server_id"] = True
        results["premium_servers_guild_id"] = True
        
        # Create indexes for premium_payments collection
        logger.info("Creating indexes for premium_payments collection")
        await db.premium_payments.create_index("guild_id")
        await db.premium_payments.create_index("payment_id", unique=True)
        results["premium_payments_guild_id"] = True
        results["premium_payments_payment_id"] = True
        
        return results
        
    except Exception as e:
        logger.error(f"Error creating indexes: {e}")
        return {"error": str(e)}


async def initialize_premium_database(db) -> Dict[str, Any]:
    """
    Initialize premium system database.
    
    Args:
        db: MongoDB database connection
        
    Returns:
        Dict[str, Any]: Status of database initialization
    """
    if db is None:
        logger.error("Database connection is None")
        return {"success": False, "error": "Database connection is None"}
    
    results = {
        "collections": None,
        "indexes": None,
        "success": False
    }
    
    try:
        # Initialize collections
        logger.info("Initializing collections")
        collections_result = await initialize_collections(db)
        results["collections"] = collections_result
        
        # Create indexes
        logger.info("Creating indexes")
        indexes_result = await create_indexes(db)
        results["indexes"] = indexes_result
        
        # Set success flag
        if "error" not in collections_result and "error" not in indexes_result:
            results["success"] = True
            logger.info("Premium database initialization completed successfully")
        else:
            logger.error("Premium database initialization failed")
        
        return results
        
    except Exception as e:
        logger.error(f"Error initializing premium database: {e}")
        results["error"] = str(e)
        return results


# Command-line utility
async def main():
    """
    Main entry point for premium database initialization.
    """
    import sys
    
    # Import needed only when running as standalone script
    from bot import Bot
    
    logger.info("Starting premium database initialization...")
    
    # Create bot instance to get database connection
    bot = Bot(production=False)
    
    # Initialize database
    try:
        await bot.init_db()
        if bot.db is None:
            logger.error("Failed to initialize database connection")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        sys.exit(1)
    
    # Initialize premium database
    results = await initialize_premium_database(bot.db)
    
    # Print results
    if isinstance(results, dict) and results["success"]:
        print("Premium database initialization completed successfully")
    else:
        print("Premium database initialization completed with errors")
        print(f"Error: {results.get('error', 'Unknown error')}")
    
    print("Premium database initialization complete")


if __name__ == "__main__":
    # Run initialization
    asyncio.run(main())