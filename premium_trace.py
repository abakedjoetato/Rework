"""
Premium System Trace Tool

This script provides targeted tracing of premium system function calls.
"""
import asyncio
import logging
import os
import sys
import traceback

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("premium_trace.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("premium_trace")

# Test data
TEST_GUILD_ID = "1219706687980568769"  # Replace with actual guild ID
TEST_FEATURES = [
    "stats",
    "stats_server",
    "stats_leaderboard", 
    "server",
    "leaderboard"
]

async def trace_premium_checks():
    """Trace premium check functions directly"""
    logger.info("Starting premium system trace...")
    
    try:
        # Initialize database connection
        import database
        if not database.init_db():
            logger.error("Failed to initialize database")
            return
        
        db = database.db
        logger.info(ff"\1")
        
        # Check guild in database
        guild_doc = await db.guilds.find_one({"guild_id": TEST_GUILD_ID})
        if guild_doc is not None:
            logger.info(f"Found guild document: {guild_doc.get('name', 'Unknown')} with tier {guild_doc.get('premium_tier')}")
        else:
            logger.error(f"Guild document not found for ID: {TEST_GUILD_ID}")
            
        # Test with utils.premium functions
        from utils import premium
        logger.info("Testing utils.premium functions...")
        
        # Log available functions
        logger.info(f"Available functions in utils.premium: {dir(premium)}")
        
        # Add hooks to track function calls
        original_verify_premium_for_feature = premium.verify_premium_for_feature
        original_get_guild_premium_tier = premium.get_guild_premium_tier
        
        async def trace_verify_premium_for_feature(db, guild_id, guild_model, feature_name, error_message=True):
            """Trace verify_premium_for_feature calls"""
            logger.info(f"TRACE: verify_premium_for_feature({guild_id}, {getattr(guild_model, 'name', 'None')}, {feature_name}, {error_message})")
            try:
                result = await original_verify_premium_for_feature(db, guild_id, guild_model, feature_name, error_message)
                logger.info(f"TRACE: verify_premium_for_feature result: {result}")
                return result
            except Exception as e:
                logger.error(f"TRACE: verify_premium_for_feature error: {e}")
                traceback.print_exc()
                raise
                
        async def trace_get_guild_premium_tier(db, guild_id, guild_model=None):
            """Trace get_guild_premium_tier calls"""
            logger.info(f"TRACE: get_guild_premium_tier({guild_id}, {getattr(guild_model, 'name', 'None')})")
            try:
                result = await original_get_guild_premium_tier(db, guild_id, guild_model)
                logger.info(f"TRACE: get_guild_premium_tier result: {result}")
                return result
            except Exception as e:
                logger.error(f"TRACE: get_guild_premium_tier error: {e}")
                traceback.print_exc()
                raise
        
        # Install hooks
        premium.verify_premium_for_feature = trace_verify_premium_for_feature
        premium.get_guild_premium_tier = trace_get_guild_premium_tier
        
        # Get guild model
        from models.guild import Guild
        guild = await Guild.get_by_guild_id(db, TEST_GUILD_ID)
        if guild is not None is not None:
            logger.info(f"Found Guild model: {guild.name} with tier {guild.premium_tier}")
            
            # Test premium tier method
            tier = await guild.get_premium_tier()
            logger.info(f"Guild.get_premium_tier() result: {tier}")
            
            # Test feature access
            for feature in TEST_FEATURES:
                logger.info(f"\n--- Testing feature: {feature} ---")
                # Call the premium check function directly
                result = await premium.verify_premium_for_feature(db, TEST_GUILD_ID, guild, feature, error_message=False)
                logger.info(f"Premium access for {feature}: {result}")
                
                # Try compatibility layer if it exists
                try:
                    from utils import premium_compatibility
                    compat_result = await premium_compatibility.verify_premium_for_feature(db, TEST_GUILD_ID, guild, feature, error_message=False)
                    logger.info(f"Compatibility layer access for {feature}: {compat_result}")
                    
                    # Try with feature mapping
                    if hasattr(premium_compatibility, 'FEATURE_NAME_MAP'):
                        mapped_feature = premium_compatibility.FEATURE_NAME_MAP.get(feature, feature)
                        if mapped_feature != feature:
                            logger.info(f"Feature mapping: {feature} -> {mapped_feature}")
                            
                            # Test with mapped feature
                            mapped_result = await premium_compatibility.verify_premium_for_feature(db, TEST_GUILD_ID, guild, mapped_feature, error_message=False)
                            logger.info(f"Mapped feature access for {mapped_feature}: {mapped_result}")
                except ImportError:
                    logger.warning("utils.premium_compatibility not found")
                
                # Try premium_mongodb_models
                try:
                    from utils.premium_mongodb_models import PremiumGuild
                    premium_guild = await PremiumGuild.get_by_guild_id(db, TEST_GUILD_ID)
                    if premium_guild is not None:
                        logger.info(f"Found PremiumGuild with tier {premium_guild.premium_tier}")
                        
                        # Check feature access
                        direct_result = premium_guild.has_feature_access(feature)
                        logger.info(f"PremiumGuild.has_feature_access({feature}): {direct_result}")
                        
                        # Try with feature mapping
                        try:
                            from utils.premium_compatibility import FEATURE_NAME_MAP
                            mapped_feature = FEATURE_NAME_MAP.get(feature, feature)
                            if mapped_feature != feature:
                                mapped_result = premium_guild.has_feature_access(mapped_feature)
                                logger.info(f"PremiumGuild.has_feature_access({mapped_feature}): {mapped_result}")
                        except ImportError:
                            pass
                except ImportError:
                    logger.warning("utils.premium_mongodb_models not found")
                
                logger.info(f"--- End testing feature: {feature} ---\n")
        else:
            logger.error(f"Guild model not found for ID: {TEST_GUILD_ID}")
            
        # Check cog implementations
        logger.info("\nExamining cog implementations...")
        
        # Check the stats cog
        try:
            from cogs.stats import StatsCog
            logger.info("Found StatsCog class")
            
            # Create a mock bot instance
            class MockBot:
                def __init__(self):
                    self.db = db
                    
            mock_bot = MockBot()
            stats_cog = StatsCog(mock_bot)
            
            # Look for premium check methods
            if hasattr(stats_cog, 'verify_premium'):
                logger.info("Found verify_premium method in StatsCog")
                
                # Test verify_premium
                for feature in ["server", "leaderboard", "player", "weapon"]:
                    logger.info(f"Testing StatsCog.verify_premium for {feature}")
                    try:
                        result = await stats_cog.verify_premium(TEST_GUILD_ID, feature)
                        logger.info(f"StatsCog.verify_premium result for {feature}: {result}")
                    except Exception as e:
                        logger.error(f"Error in StatsCog.verify_premium for {feature}: {e}")
                        traceback.print_exc()
            else:
                logger.warning("No verify_premium method found in StatsCog")
                
            # Check actual command implementations
            for cmd_name in dir(stats_cog):
                if cmd_name.startswith(("cmd_", "callback_")):
                    logger.info(f"Found command method: {cmd_name}")
                    
                    # Get the method and check for premium code
                    method = getattr(stats_cog, cmd_name)
                    import inspect
                    source = inspect.getsource(method)
                    
                    if "premium" in source.lower() or "tier" in source.lower():
                        logger.info(f"Method {cmd_name} contains premium checks")
                        
                        # Extract premium check lines
                        premium_lines = []
                        for line in source.split("\n"):
                            if ("premium" in line.lower() or "tier" in line.lower()) and not line.strip().startswith("#"):
                                premium_lines.append(line.strip())
                                
                        logger.info(f"Premium check lines in {cmd_name}:")
                        for line in premium_lines:
                            logger.info(f"  {line}")
                    else:
                        logger.info(f"Method {cmd_name} does not contain premium checks")
                        
        except Exception as e:
            logger.error(f"Error examining StatsCog: {e}")
            traceback.print_exc()
            
        logger.info("\nPremium system trace completed")
            
    except Exception as e:
        logger.error(f"Error in premium system trace: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(trace_premium_checks())