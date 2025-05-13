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
                
            # Test premium feature access with fallback mechanism
            premium_check_method = None
            
            # Try multiple approaches to find premium feature check methods
            try:
                # First attempt: try importing utils.premium_utils
                try:
                    import sys
                    import importlib
                    
                    # Attempt to import the module
                    try:
                        premium_utils = importlib.import_module('utils.premium_utils')
                        
                        # Look for specific methods
                        if hasattr(premium_utils, 'check_premium_feature_access'):
                            premium_check_method = premium_utils.check_premium_feature_access
                            logger.info("Found check_premium_feature_access in utils.premium_utils")
                        elif hasattr(premium_utils, 'check_feature_access'):
                            premium_check_method = premium_utils.check_feature_access
                            logger.info("Found check_feature_access in utils.premium_utils")
                    except ImportError:
                        logger.warning("Could not import utils.premium_utils")
                except Exception as e:
                    logger.warning(f"Error importing premium_utils: {e}")
                
                # Second attempt: try direct import from premium_feature_access
                if premium_check_method is None:
                    try:
                        from premium_feature_access import PremiumFeature
                        if hasattr(PremiumFeature, 'check_access'):
                            premium_check_method = PremiumFeature.check_access
                            logger.info("Found PremiumFeature.check_access method")
                    except ImportError:
                        logger.warning("Could not import from premium_feature_access")
                
                # Third attempt: try direct import from premium_compatibility
                if premium_check_method is None:
                    try:
                        from premium_compatibility import check_feature_access
                        premium_check_method = check_feature_access
                        logger.info("Found check_feature_access from premium_compatibility")
                    except ImportError:
                        logger.warning("Could not import from premium_compatibility")
            except Exception as e:
                logger.error(f"Error setting up premium check methods: {e}")
            
            # Use the found method if it's async
            if premium_check_method:
                if inspect.iscoroutinefunction(premium_check_method):
                    for feature in FEATURES:
                        try:
                            has_access = await premium_check_method(db, TEST_GUILD_ID, feature)
                            logger.info(f"Feature '{feature}' access: {has_access}")
                        except Exception as feature_check_error:
                            logger.error(f"Error checking access for feature '{feature}': {feature_check_error}")
                else:
                    logger.error("Premium feature check method is not an async function")
            else:
                logger.error("Could not find any valid premium feature check method")
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
        try:
            # Initialize variables
            stats_cog_found = False
            stats_cog = None
            
            # Import required modules
            import os
            import sys
            import importlib.util
            
            # Method 1: Direct import
            try:
                # Initialize Stats variable for the cog class
                stats_cog_class = None
                
                # Try direct import
                try:
                    from cogs.stats import Stats
                    stats_cog = Stats
                    stats_cog_found = True
                    logger.info("Successfully imported Stats cog directly")
                except ImportError as direct_e:
                    logger.warning(f"Direct import failed: {direct_e}")
                    # Continue to next method
            except ImportError as e:
                logger.warning(f"Direct import failed: {e}")
                
                # Method 2: Dynamic import
                try:
                    spec = importlib.util.spec_from_file_location("stats_cog_module", os.path.join("cogs", "stats.py"))
                    if spec and spec.loader:
                        stats_module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(stats_module)
                        
                        if hasattr(stats_module, "Stats"):
                            stats_cog = stats_module.Stats
                            stats_cog_found = True
                            logger.info("Successfully imported Stats cog dynamically")
                except Exception as e:
                    logger.warning(f"Dynamic import failed: {e}")
                    
                    # Method 3: Absolute path import
                    try:
                        cogs_dir = os.path.abspath("cogs")
                        stats_path = os.path.join(cogs_dir, "stats.py")
                        if os.path.exists(stats_path):
                            logger.info(f"Found stats.py at {stats_path}")
                            spec = importlib.util.spec_from_file_location("stats_cog_abs", stats_path)
                            if spec and spec.loader:
                                stats_module = importlib.util.module_from_spec(spec)
                                sys.modules[spec.name] = stats_module
                                spec.loader.exec_module(stats_module)
                                
                                if hasattr(stats_module, "Stats"):
                                    stats_cog = stats_module.Stats
                                    stats_cog_found = True
                                    logger.info("Successfully imported Stats cog using absolute path")
                    except Exception as e:
                        logger.warning(f"Absolute path import failed: {e}")
            
            # Create mock if all methods failed
            if not stats_cog_found:
                logger.warning("Creating mock Stats cog for testing")
                class MockStats:
                    def __init__(self):
                        pass
                    async def check_premium_feature(self, guild_id, feature):
                        logger.info(f"Mock checking premium feature {feature} for guild {guild_id}")
                        return True
                
                stats_cog = MockStats
                stats_cog_found = True
            
            # If a Stats cog class was found (real or mock)
            if stats_cog_found and stats_cog is not None:
                logger.info("Found Stats cog, checking methods for premium checks...")
                
                # Create a mock bot for initializing the cog
                class MockBot:
                    def __init__(self):
                        self.db = db
                
                # Initialize the cog with the mock bot
                try:
                    # Create instance using proper constructor pattern
                    mock_bot = MockBot()
                    cog_instance = stats_cog(mock_bot)
                    
                    # Store reference for later use
                    stats_cog_instance = cog_instance
                except Exception as e:
                    logger.error(f"Error initializing Stats cog: {e}")
                    # Create a basic mock instance if initialization fails
                    class BasicMockStats:
                        async def check_premium_feature(self, guild_id, feature):
                            logger.info(f"Basic mock checking premium feature {feature} for guild {guild_id}")
                            return True
                    
                    stats_cog_instance = BasicMockStats()
                
                # Get all command methods from the cog instance
                for attr_name in dir(stats_cog_instance):
                    if attr_name.startswith("_"):
                        continue
                        
                    attr = getattr(stats_cog_instance, attr_name)
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