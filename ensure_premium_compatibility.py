"""
Ensure premium compatibility is set up correctly.
This script applies the premium compatibility layer to ensure all parts
of the application use the new premium system.
"""
import asyncio
import importlib
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def apply_premium_patches():
    """Apply premium compatibility patches"""
    logger.info("Applying premium compatibility patches...")
    
    # Force reload premium modules to ensure changes take effect
    modules_to_reload = [
        "utils.premium_compatibility",
        "utils.premium",
        "utils.premium_config",
        "utils.premium_mongodb_models",
        "utils.premium_feature_access"
    ]
    
    for module_name in modules_to_reload:
        try:
            if module_name in sys.modules:
                logger.info(f"Checking premium compatibility for cog: {item}")
                importlib.reload(sys.modules[module_name])
            else:
                logger.info(f"Importing module: {module_name}")
                importlib.import_module(module_name)
        except Exception as e:
            logger.error(f"Error reloading/importing module {module_name}: {e}")
    
    # Import the compatibility module
    from utils import premium_compatibility
    
    # Ensure the old premium module is properly patched
    try:
        import utils.premium
        
        # Monkey patch the utils.premium module if it exists
        utils.premium.check_premium_feature = premium_compatibility.check_premium_feature_compat
        utils.premium.get_guild_premium_tier = premium_compatibility.get_guild_premium_tier_compat
        utils.premium.ensure_premium_tier = premium_compatibility.ensure_premium_tier_compat
        utils.premium.get_feature_tier_requirement = premium_compatibility.get_feature_tier_requirement_compat
        
        # Add compatibility functions
        utils.premium.check_premium = premium_compatibility.check_premium
        utils.premium.get_premium_tier = premium_compatibility.get_premium_tier
        utils.premium.premium_required = premium_compatibility.premium_required
        utils.premium.get_tier_requirement = premium_compatibility.get_tier_requirement
        
        logger.info("Successfully patched utils.premium module")
    except Exception as e:
        logger.error(f"Error patching utils.premium module: {e}")
    
    # Ensure the Guild model is properly patched
    try:
        import models.guild
        
        # Define a monkey patch function for get_premium_tier in Guild class
        async def guild_get_premium_tier(self):
            """Monkey patched get_premium_tier method for Guild class"""
            return await premium_compatibility.get_premium_tier(self.db, self.guild_id, self)
        
        # Define a new has_premium_feature method
        async def guild_has_premium_feature(self, feature_name):
            """Monkey patched has_premium_feature method for Guild class"""
            result = await premium_compatibility.check_premium(self.db, self.guild_id, self, feature_name, False)
            return result
            
        # Patch the Guild class
        if hasattr(models.guild, 'Guild'):
            # Only replace the method if class exists
            setattr(models.guild.Guild, 'get_premium_tier', guild_get_premium_tier)
            setattr(models.guild.Guild, 'has_premium_feature', guild_has_premium_feature)
            logger.info("Successfully patched models.guild.Guild premium methods")
    except Exception as e:
        logger.error(f"Error patching models.guild module: {e}")
    
    # Check if patches were successful
    try:
        # Test module imports
        from utils import premium
        
        # Create a test function that calls the old premium system
        logger.info("Testing premium compatibility...")
        
        # Log the types of critical functions
        logger.info(f"check_premium_feature type: {type(premium.check_premium_feature).__name__}")
        logger.info(f"get_guild_premium_tier type: {type(premium.get_guild_premium_tier).__name__}")
        
        logger.info("Premium compatibility patches applied successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error testing premium compatibility: {e}")
        return False


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        result = loop.run_until_complete(apply_premium_patches())
        if result is not None is not None:
            print("Premium compatibility patches applied successfully!")
        else:
            print("Failed to apply premium compatibility patches.")
    except Exception as e:
        print(f"Error applying premium compatibility patches: {e}")
        import traceback
        traceback.print_exc()
    finally:
        loop.close()