#!/usr/bin/env python3
"""
Premium System Trace Tool

This script provides targeted tracing of premium system function calls.
"""
import asyncio
import inspect
import logging
import traceback
from typing import Dict, Any, List, Optional, Union, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("premium_trace")

# Test guild ID for premium verification
TEST_GUILD_ID = "123456789012345678"  # Replace with an actual guild ID when testing

# List of premium features to check
FEATURES = [
    "advanced_stats",
    "custom_charts",
    "data_export",
    "leaderboard_customization",
    "rivalry_tracking",
    "server",
    "leaderboard"
]

async def trace_premium_checks():
    """Trace premium check functions directly"""
    logger.info("Starting premium system trace...")
    
    # Declare variables at the top level to ensure they're accessible throughout
    db = None
    stats_cog = None
    stats_cog_found = False
    StatsCog = None
    guild_doc = None
    
    try:
        ###########################################
        # PART 1: DATABASE INITIALIZATION SECTION
        ###########################################
        try:
            # Import database module
            import database
            
            # Initialize database based on whether init_db is async or not
            if hasattr(database, 'init_db'):
                if inspect.iscoroutinefunction(database.init_db):
                    # Async function
                    db_initialized = await database.init_db()
                else:
                    # Sync function
                    db_initialized = database.init_db()
                
                if not db_initialized:
                    logger.error("Failed to initialize database")
                    return
            else:
                logger.error("Database module does not have init_db function")
                return
            
            # Get database connection
            db = getattr(database, 'db', None)
            if db is None:
                logger.error("Database connection not available after initialization")
                return
                
            logger.info(f"Database initialized successfully")
        except ImportError:
            logger.error("Could not import database module")
            return
        except Exception as db_error:
            logger.error(f"Error initializing database: {db_error}")
            traceback.print_exc()
            return

        ###########################################
        # PART 2: GUILD PREMIUM STATUS CHECK
        ###########################################
        logger.info(f"Checking premium status for guild {TEST_GUILD_ID}")
        
        # Check guild in database
        try:
            if db is not None and hasattr(db, 'guilds'):
                guild_doc = await db.guilds.find_one({"guild_id": TEST_GUILD_ID})
                if guild_doc:
                    logger.info(f"Found guild document: {guild_doc.get('name', 'Unknown')}")
                    logger.info(f"Premium tier: {guild_doc.get('premium_tier', 0)}")
                else:
                    logger.info(f"Guild {TEST_GUILD_ID} not found in database")
            else:
                logger.error("Database connection not established or guilds collection not accessible.")
                guild_doc = None
        except Exception as e:
            logger.error(f"Error accessing database: {e}")
            traceback.print_exc()
            guild_doc = None

        ###########################################
        # PART 3: PREMIUM UTILS FUNCTIONS TEST
        ###########################################
        logger.info("\nTesting premium utility functions...")
        try:
            # Import premium utils
            import utils.premium_utils as premium_utils
            
            # Test get_guild_premium_tier
            if hasattr(premium_utils, 'get_guild_premium_tier'):
                if inspect.iscoroutinefunction(premium_utils.get_guild_premium_tier):
                    tier = await premium_utils.get_guild_premium_tier(db, TEST_GUILD_ID)
                    logger.info(f"Guild premium tier from utils: {tier}")
                else:
                    logger.error("get_guild_premium_tier is not an async function")
            else:
                logger.error("premium_utils module does not have get_guild_premium_tier function")
                
            # Test check_premium_feature_access
            if hasattr(premium_utils, 'check_premium_feature_access'):
                if inspect.iscoroutinefunction(premium_utils.check_premium_feature_access):
                    for feature in FEATURES:
                        has_access = await premium_utils.check_premium_feature_access(db, TEST_GUILD_ID, feature)
                        logger.info(f"Feature '{feature}' access: {has_access}")
                else:
                    logger.error("check_premium_feature_access is not an async function")
            else:
                logger.error("premium_utils module does not have check_premium_feature_access function")
        except ImportError:
            logger.error("Could not import premium_utils module")
        except Exception as util_error:
            logger.error(f"Error testing premium utility functions: {util_error}")
            traceback.print_exc()

        ###########################################
        # PART 4: PREMIUM COMPATIBILITY LAYER TEST
        ###########################################
        logger.info("\nTesting premium compatibility layer...")
        try:
            # Import premium compatibility
            import premium_compatibility
            
            # Test get_premium_tier
            if hasattr(premium_compatibility, 'get_premium_tier'):
                if inspect.iscoroutinefunction(premium_compatibility.get_premium_tier):
                    tier = await premium_compatibility.get_premium_tier(db, TEST_GUILD_ID)
                    logger.info(f"Guild premium tier from compatibility layer: {tier}")
                else:
                    logger.error("get_premium_tier in compatibility layer is not an async function")
            else:
                logger.error("premium_compatibility module does not have get_premium_tier function")
                
            # Test check_feature_access
            if hasattr(premium_compatibility, 'check_feature_access'):
                if inspect.iscoroutinefunction(premium_compatibility.check_feature_access):
                    for feature in FEATURES:
                        has_access = await premium_compatibility.check_feature_access(db, TEST_GUILD_ID, feature)
                        logger.info(f"Feature '{feature}' access from compatibility layer: {has_access}")
                else:
                    logger.error("check_feature_access in compatibility layer is not an async function")
            else:
                logger.error("premium_compatibility module does not have check_feature_access function")
        except ImportError:
            logger.error("Could not import premium_compatibility module")
        except Exception as compat_error:
            logger.error(f"Error testing premium compatibility layer: {compat_error}")
            traceback.print_exc()

        ###########################################
        # PART 5: PREMIUM FEATURE ACCESS TEST
        ###########################################
        logger.info("\nTesting PremiumFeature class...")
        try:
            # Import premium feature access
            from premium_feature_access import PremiumFeature
            
            # Test check_access
            if hasattr(PremiumFeature, 'check_access'):
                if inspect.iscoroutinefunction(PremiumFeature.check_access):
                    for feature in FEATURES:
                        has_access = await PremiumFeature.check_access(db, TEST_GUILD_ID, feature)
                        logger.info(f"Feature '{feature}' access from PremiumFeature: {has_access}")
                else:
                    logger.error("PremiumFeature.check_access is not an async function")
            else:
                logger.error("PremiumFeature class does not have check_access method")
                
            # Test get_guild_tier
            if hasattr(PremiumFeature, 'get_guild_tier'):
                if inspect.iscoroutinefunction(PremiumFeature.get_guild_tier):
                    tier = await PremiumFeature.get_guild_tier(db, TEST_GUILD_ID)
                    logger.info(f"Guild premium tier from PremiumFeature: {tier}")
                else:
                    logger.error("PremiumFeature.get_guild_tier is not an async function")
            else:
                logger.error("PremiumFeature class does not have get_guild_tier method")
        except ImportError:
            logger.error("Could not import PremiumFeature from premium_feature_access")
        except Exception as feature_error:
            logger.error(f"Error testing PremiumFeature class: {feature_error}")
            traceback.print_exc()

        ###########################################
        # PART 6: STATS COG PREMIUM CHECKS
        ###########################################
        logger.info("\nChecking StatsCog premium checks...")
        stats_cog_found = False
        try:
            # Try to import the StatsCog
            try:
                from cogs.stats import StatsCog
                stats_cog_found = True
            except ImportError:
                logger.error("Could not import StatsCog from cogs.stats")
                
            # Alternative approach if direct import fails
            if not stats_cog_found:
                try:
                    import sys
                    import importlib.util
                    
                    spec = importlib.util.spec_from_file_location("stats_cog", "./cogs/stats.py")
                    if spec and spec.loader:
                        stats_module = importlib.util.module_from_spec(spec)
                        sys.modules["stats_cog"] = stats_module
                        spec.loader.exec_module(stats_module)
                        
                        if hasattr(stats_module, "StatsCog"):
                            StatsCog = stats_module.StatsCog
                            stats_cog_found = True
                        else:
                            logger.error("StatsCog class not found in stats module")
                except Exception as import_error:
                    logger.error(f"Error importing StatsCog: {import_error}")
            
            # If we successfully found StatsCog, check its methods
            if stats_cog_found and StatsCog:
                logger.info("Found StatsCog, checking methods for premium checks...")
                
                # Create a mock bot for initializing the cog
                class MockBot:
                    def __init__(self):
                        self.db = db
                
                # Initialize the StatsCog
                stats_cog = StatsCog(MockBot())
                
                # Get all command methods from the cog
                for attr_name in dir(stats_cog):
                    if attr_name.startswith("_"):
                        continue
                        
                    attr = getattr(stats_cog, attr_name)
                    if callable(attr):
                        cmd_name = attr_name
                        
                        # Check if the method's source code contains premium checks
                        try:
                            source = inspect.getsource(attr)
                            premium_lines = []
                            
                            for line in source.split("\n"):
                                if ("premium" in line.lower() or "tier" in line.lower()) and not line.strip().startswith("#"):
                                    premium_lines.append(line.strip())
                                    
                            if premium_lines:
                                logger.info(f"Premium check lines in {cmd_name}:")
                                for line in premium_lines:
                                    logger.info(f"  {line}")
                            else:
                                logger.info(f"Method {cmd_name} does not contain premium checks")
                        except Exception as source_error:
                            logger.error(f"Error examining source of {cmd_name}: {source_error}")
            else:
                logger.error("Could not find or initialize StatsCog")
        except Exception as stats_cog_error:
            logger.error(f"Error examining StatsCog: {stats_cog_error}")
            traceback.print_exc()
        
    except Exception as outer_error:
        logger.error(f"Error in premium system trace: {outer_error}")
        traceback.print_exc()
    finally:
        logger.info("\nPremium system trace completed")

if __name__ == "__main__":
    asyncio.run(trace_premium_checks())