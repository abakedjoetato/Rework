"""
Ensure premium compatibility is set up correctly.
This script applies the premium compatibility layer to ensure all parts
of the application use the new premium system.
"""
import asyncio
import importlib
import logging
import sys
import traceback

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
    
    # Track imported modules to prevent errors later
    imported_modules = {}
    
    for module_name in modules_to_reload:
        try:
            if module_name in sys.modules:
                logger.info(f"Reloading module: {module_name}")
                imported_modules[module_name] = importlib.reload(sys.modules[module_name])
            else:
                logger.info(f"Importing module: {module_name}")
                imported_modules[module_name] = importlib.import_module(module_name)
        except Exception as e:
            logger.error(f"Error reloading/importing module {module_name}: {e}")
            # Continue with other modules even if one fails
    
    # Get references to the modules we need
    premium_compatibility = imported_modules.get("utils.premium_compatibility")
    premium_module = imported_modules.get("utils.premium")
    
    # Check if we have both required modules
    if not premium_compatibility or not premium_module:
        logger.error("Required modules are missing, cannot apply patches")
        return False
    
    # Ensure the old premium module is properly patched
    try:
        # Monkey patch the utils.premium module if it exists
        # First check if the compatibility functions exist in the module
        compat_functions = {
            # Map compatibility function names to the target function names in premium module
            'check_premium_feature_compat': 'check_premium_feature',
            'get_guild_premium_tier_compat': 'get_guild_premium_tier',
            'ensure_premium_tier_compat': 'ensure_premium_tier',
            'get_feature_tier_requirement_compat': 'get_feature_tier_requirement',
            'check_premium': 'check_premium',
            'get_premium_tier': 'get_premium_tier',
            'premium_required': 'premium_required',
            'get_tier_requirement': 'get_tier_requirement'
        }
        
        # Apply all patches
        patch_count = 0
        for compat_func, target_func in compat_functions.items():
            if hasattr(premium_compatibility, compat_func):
                logger.info(f"Patching {target_func} with {compat_func}")
                setattr(premium_module, target_func, getattr(premium_compatibility, compat_func))
                patch_count += 1
        
        logger.info(f"Applied {patch_count} function patches to premium module")
        
        logger.info("Successfully patched utils.premium module")
    except Exception as e:
        logger.error(f"Error patching utils.premium module: {e}")
    
    # Ensure the Guild model is properly patched
    try:
        # Try to import the Guild model
        guild_module = imported_modules.get("models.guild")
        if not guild_module:
            try:
                guild_module = importlib.import_module("models.guild")
                logger.info("Imported models.guild module")
            except ImportError:
                logger.error("Could not import models.guild module")
                guild_module = None
        
        if guild_module and hasattr(guild_module, 'Guild'):
            # Define a monkey patch function for get_premium_tier in Guild class
            async def guild_get_premium_tier(self):
                """Monkey patched get_premium_tier method for Guild class"""
                # Get a reference to premium_compatibility in the function scope
                try:
                    from utils import premium_compatibility as pc
                    if hasattr(pc, 'get_premium_tier'):
                        # Ensure all required attributes exist
                        db = getattr(self, 'db', None)
                        guild_id = getattr(self, 'guild_id', None)
                        if db is not None and guild_id is not None:
                            return await pc.get_premium_tier(db, guild_id, self)
                except ImportError:
                    logger.warning("Failed to import premium_compatibility in guild_get_premium_tier")
                except Exception as e:
                    logger.warning(f"Error in guild_get_premium_tier: {e}")
                
                # Fallback to default tier
                return 0
            
            # Define a new has_premium_feature method
            async def guild_has_premium_feature(self, feature_name):
                """Monkey patched has_premium_feature method for Guild class"""
                # Get a reference to premium_compatibility in the function scope
                try:
                    from utils import premium_compatibility as pc
                    if hasattr(pc, 'check_premium'):
                        # Ensure all required attributes exist
                        db = getattr(self, 'db', None)
                        guild_id = getattr(self, 'guild_id', None)
                        if db is not None and guild_id is not None and feature_name is not None:
                            return await pc.check_premium(db, guild_id, self, feature_name, False)
                except ImportError:
                    logger.warning("Failed to import premium_compatibility in guild_has_premium_feature")
                except Exception as e:
                    logger.warning(f"Error in guild_has_premium_feature: {e}")
                
                # Fallback to False
                return False
                
            # Patch the Guild class
            # Only replace the method if class exists
            setattr(guild_module.Guild, 'get_premium_tier', guild_get_premium_tier)
            setattr(guild_module.Guild, 'has_premium_feature', guild_has_premium_feature)
            logger.info("Successfully patched models.guild.Guild premium methods")
        else:
            logger.warning("Guild class not found in models.guild module")
    except Exception as e:
        logger.error(f"Error patching models.guild module: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    # Check if patches were successful
    try:
        # Test if we can import the premium module
        premium_module = imported_modules.get("utils.premium")
        
        if not premium_module:
            logger.warning("Premium module not available for verification")
            return False
        
        # Create a test function that calls the old premium system
        logger.info("Testing premium compatibility...")
        
        # Log the types of critical functions
        critical_functions = []
        
        # Check for critical functions, protecting against AttributeError
        critical_function_names = [
            'check_premium_feature',
            'get_guild_premium_tier',
            'check_premium',
            'get_premium_tier',
            'premium_required'
        ]
        
        for func_name in critical_function_names:
            if hasattr(premium_module, func_name):
                func_type = type(getattr(premium_module, func_name)).__name__
                critical_functions.append(f"{func_name}: {func_type}")
        
        # Log the functions we found
        if critical_functions:
            for func_info in critical_functions:
                logger.info(func_info)
            
            # Check if we have at least the minimum required functions
            min_required = ['check_premium_feature', 'get_guild_premium_tier']
            has_min_required = all(
                any(func_name in func_info for func_info in critical_functions)
                for func_name in min_required
            )
            
            if has_min_required:
                logger.info("Premium compatibility patches applied successfully!")
                return True
            else:
                logger.warning("Missing some required premium functions")
                return False
        else:
            logger.warning("No critical premium functions found to patch")
            return False
    except Exception as e:
        logger.error(f"Error testing premium compatibility: {e}")
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    try:
        # Check if we're on Python 3.10+ which deprecates get_event_loop()
        if sys.version_info >= (3, 10):
            # Use the newer API
            try:
                asyncio.run(apply_premium_patches())
            except Exception as e:
                print(f"Error applying premium compatibility patches: {e}")
                import traceback
                traceback.print_exc()
        else:
            # Use the older API for Python 3.9 and below
            loop = asyncio.get_event_loop()
            try:
                result = loop.run_until_complete(apply_premium_patches())
                if result:
                    print("Premium compatibility patches applied successfully!")
                else:
                    print("Failed to apply premium compatibility patches.")
            except Exception as e:
                print(f"Error applying premium compatibility patches: {e}")
                import traceback
                traceback.print_exc()
            finally:
                loop.close()
    except Exception as e:
        print(f"Critical error in premium patch application: {e}")
        import traceback
        traceback.print_exc()