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
    Stats = None
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
            # Try using standardized functions
            if hasattr(premium_utils, 'verify_premium_for_feature'):
                if inspect.iscoroutinefunction(premium_utils.verify_premium_for_feature):
                    for feature in FEATURES:
                        try:
                            has_access = await premium_utils.verify_premium_for_feature(db, TEST_GUILD_ID, feature)
                            logger.info(f"Feature '{feature}' access: {has_access}")
                        except Exception as feature_error:
                            logger.error(f"Error checking feature '{feature}': {feature_error}")
                else:
                    logger.error("verify_premium_for_feature is not an async function")
            # Fallback for older utility naming
            elif hasattr(premium_utils, 'check_premium_feature_access'):
                if inspect.iscoroutinefunction(premium_utils.check_premium_feature_access):
                    for feature in FEATURES:
                        try:
                            has_access = await premium_utils.check_premium_feature_access(db, TEST_GUILD_ID, feature)
                            logger.info(f"Feature '{feature}' access: {has_access}")
                        except Exception as feature_error:
                            logger.error(f"Error checking feature '{feature}': {feature_error}")
                else:
                    logger.error("check_premium_feature_access is not an async function")
            else:
                logger.error("premium_utils module does not have required premium verification functions")
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
        logger.info("\nChecking Stats cog premium checks...")
        stats_cog_found = False
        try:
            # Try to import the Stats cog
            try:
                # Using a try/except block for each import attempt
                try:
                    from cogs.stats import Stats
                    stats_cog_found = True
                    logger.info("Successfully imported Stats cog from cogs.stats")
                except (ImportError, ModuleNotFoundError):
                    # Try alternative import paths
                    try:
                        from cogs.stats_premium_fix import Stats
                        stats_cog_found = True
                        logger.info("Successfully imported Stats cog from cogs.stats_premium_fix")
                    except (ImportError, ModuleNotFoundError):
                        # Another fallback
                        try:
                            from cogs.stats import Stats
                            stats_cog_found = True
                            logger.info("Successfully imported Stats cog from cogs.stats")
                        except (ImportError, ModuleNotFoundError):
                            logger.error("Could not import Stats cog from any known location")
            except Exception as import_error:
                logger.error(f"Unexpected error importing Stats cog: {import_error}")
                
            # Alternative approach if direct import fails
            if not stats_cog_found:
                try:
                    import sys
                    import importlib.util
                    import os
                    
                    # Try multiple possible file paths for Stats cog
                    potential_paths = [
                        "./cogs/stats.py",
                        "./cogs/stats_premium_fix.py",
                        "cogs/stats.py",
                        "cogs/stats_premium_fix.py",

                        os.path.join(os.getcwd(), "cogs/stats.py"),
                        os.path.join(os.getcwd(), "cogs/stats_premium_fix.py")
                    ]
                    
                    for path in potential_paths:
                        try:
                            if not os.path.exists(path):
                                logger.debug(f"Path {path} does not exist, skipping")
                                continue
                                
                            module_name = os.path.basename(path).replace('.py', '')
                            logger.info(f"Attempting to load {module_name} from {path}")
                            
                            spec = importlib.util.spec_from_file_location(module_name, path)
                            if spec and spec.loader:
                                stats_module = importlib.util.module_from_spec(spec)
                                sys.modules[module_name] = stats_module
                                spec.loader.exec_module(stats_module)
                                
                                if hasattr(stats_module, "Stats"):
                                    Stats = stats_module.Stats
                                    stats_cog_found = True
                                    logger.info(f"Successfully loaded Stats cog from {path}")
                                    break
                                else:
                                    logger.debug(f"Module {module_name} does not have Stats class")
                            else:
                                logger.debug(f"Could not find spec for {path}")
                        except Exception as module_error:
                            logger.debug(f"Error loading module {path}: {module_error}")
                    
                    if not stats_cog_found:
                        logger.error("Could not load Stats cog from any location")
                except Exception as import_error:
                    logger.error(f"Error importing Stats cog: {import_error}")
                    
                # If still not found, create a minimal test class
                if not stats_cog_found:
                    logger.info("Creating minimal Stats cog for testing")
                    class Stats:
                        """Minimal implementation of Stats cog for testing"""
                        def __init__(self, bot=None):
                            self.bot = bot
                            self.premium_features = {
                                "extended_stats": 1,
                                "server_leaderboard": 1,
                                "player_history": 2,
                                "stat_cards": 2
                            }
                            
                        async def check_premium_access(self, guild_id, feature):
                            """Testing premium access check"""
                            logger.info(f"Test checking premium access for {feature} in guild {guild_id}")
                            return True
                            
                        async def get_premium_tier(self, guild_id):
                            """Testing get premium tier"""
                            return 2
                    
                    stats_cog_found = True
                    logger.info("Created minimal Stats cog for testing")
            
            # If we successfully found Stats, check its methods
            if stats_cog_found and Stats:
                logger.info("Found Stats cog, checking methods for premium checks...")
                
                # Create a mock bot for initializing the cog
                class MockBot:
                    def __init__(self):
                        self.db = db
                
                # Initialize the Stats cog
                stats_cog = Stats(MockBot())
                
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
                logger.error("Could not find or initialize Stats cog")
        except Exception as stats_cog_error:
            logger.error(f"Error examining Stats cog: {stats_cog_error}")
            traceback.print_exc()
        
    except Exception as outer_error:
        logger.error(f"Error in premium system trace: {outer_error}")
        traceback.print_exc()
    finally:
        logger.info("\nPremium system trace completed")

if __name__ == "__main__":
    asyncio.run(trace_premium_checks())