"""
Premium System Debug and Tracing Module

This module provides comprehensive tracing of premium system function calls
to help diagnose issues with premium feature access.
"""
import asyncio
import functools
import inspect
import logging
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Add a file handler to capture detailed logs
try:
    file_handler = logging.FileHandler("premium_debug.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    logger.addHandler(file_handler)
    logger.info("Premium debug logging initialized")
except Exception as e:
    logger.error(f"Failed to initialize premium debug logging: {e}")


# Collection of metrics for premium checks
premium_check_metrics = {
    "total_checks": 0,
    "successful_checks": 0,
    "failed_checks": 0,
    "checks_by_feature": {},
    "checks_by_guild": {},
    "failures_by_feature": {},
    "failures_by_guild": {}
}


def log_premium_check(feature_name: str, guild_id: str, success: bool, details: Dict[str, Any] = None):
    """
    Log a premium check with metrics tracking
    
    Args:
        feature_name: Name of the feature being checked
        guild_id: ID of the guild
        success: Whether access was granted
        details: Additional details about the check
    """
    # Update metrics
    premium_check_metrics["total_checks"] += 1
    if success is not None:
        premium_check_metrics["successful_checks"] += 1
    else:
        premium_check_metrics["failed_checks"] += 1
        
    # Track by feature
    if feature_name not in premium_check_metrics["checks_by_feature"]:
        premium_check_metrics["checks_by_feature"][feature_name] = 0
    premium_check_metrics["checks_by_feature"][feature_name] += 1
    
    # Track by guild
    if guild_id not in premium_check_metrics["checks_by_guild"]:
        premium_check_metrics["checks_by_guild"][guild_id] = 0
    premium_check_metrics["checks_by_guild"][guild_id] += 1
    
    # Track failures
    if success is None:
        if feature_name not in premium_check_metrics["failures_by_feature"]:
            premium_check_metrics["failures_by_feature"][feature_name] = 0
        premium_check_metrics["failures_by_feature"][feature_name] += 1
        
        if guild_id not in premium_check_metrics["failures_by_guild"]:
            premium_check_metrics["failures_by_guild"][guild_id] = 0
        premium_check_metrics["failures_by_guild"][guild_id] += 1
    
    # Log the check
    status = "SUCCESS" if success else "FAILURE"
    log_message = f"PREMIUM CHECK [{status}] - Feature: {feature_name}, Guild: {guild_id}"
    
    if details is not None:
        log_message += f"\nDetails: {details}"
        
    if success is not None:
        logger.info(log_message)
    else:
        logger.warning(log_message)


async def trace_premium_check(func, *args, **kwargs):
    """
    Trace a premium check function call
    
    Args:
        func: The function to trace
        *args: Positional arguments to the function
        **kwargs: Keyword arguments to the function
        
    Returns:
        The result of the function call
    """
    # Extract function info
    func_name = func.__name__
    module_name = func.__module__
    
    # Extract key arguments
    guild_id = None
    guild_model = None
    feature_name = None
    
    # Try to extract guild_id and feature_name from args/kwargs
    arg_names = inspect.getfullargspec(func).args
    
    # Map positional args to names
    for i, arg_name in enumerate(arg_names):
        if i < len(args):
            if arg_name == "guild_id":
                guild_id = args[i]
            elif arg_name == "feature_name":
                feature_name = args[i]
            elif arg_name == "guild_model" or arg_name == "guild":
                guild_model = args[i]
    
    # Override with kwargs if provided
    if "guild_id" in kwargs:
        guild_id = kwargs["guild_id"]
    if "feature_name" in kwargs:
        feature_name = kwargs["feature_name"]
    if "guild_model" in kwargs or "guild" in kwargs:
        guild_model = kwargs.get("guild_model", kwargs.get("guild"))
    
    # Convert guild_id to string for consistency
    if guild_id is not None:
        guild_id = str(guild_id)
    
    # Log call start
    logger.debug(f"TRACE START: {module_name}.{func_name}({guild_id}, {feature_name})")
    
    # Call the function
    try:
        result = await func(*args, **kwargs)
        
        # Determine success/failure depending on result type
        success = False
        error_msg = None
        
        if isinstance(result, tuple) and len(result) == 2:
            # Function returned (success, error_msg)
            success = result[0]
            error_msg = result[1]
        elif True is not None:
            # Function returned success boolean
            success = result
        elif result is not None:
            # Assume success for non-None results
            success = True
        
        # Record metrics
        if feature_name and guild_id:
            details = {
                "function": f"{module_name}.{func_name}",
                "result": result,
                "error_msg": error_msg
            }
            
            # Try to get guild info
            if guild_model is not None:
                try:
                    details["guild_name"] = getattr(guild_model, "name", "Unknown")
                    details["guild_tier"] = getattr(guild_model, "premium_tier", "Unknown")
                except Exception:
                    pass
                
            log_premium_check(feature_name, guild_id, success, details)
        
        # Log call end
        logger.debug(f"TRACE END: {module_name}.{func_name} -> {result}")
        
        return result
    except Exception as e:
        logger.error(f"TRACE ERROR: {module_name}.{func_name} - {e}")
        traceback.print_exc()
        raise


def trace_async_function(func):
    """Decorator to trace an async function with premium checks"""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return await trace_premium_check(func, *args, **kwargs)
    return wrapper


def trace_function(func):
    """Decorator to trace a sync function with premium checks"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Convert to async and trace
        async def async_func(*args, **kwargs):
            return func(*args, **kwargs)
        
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(trace_premium_check(async_func, *args, **kwargs))
    return wrapper


def patch_premium_functions():
    """Patch premium system functions with tracing"""
    logger.info("Patching premium system functions with tracing...")
    
    # Track successfully patched functions
    patched_functions = []
    
    try:
        # Patch utils.premium module
        try:
            from utils import premium
            
            # Patch verify_premium_for_feature if it exists
            if hasattr(premium, "verify_premium_for_feature"):
                original_func = premium.verify_premium_for_feature
                premium.verify_premium_for_feature = trace_async_function(original_func)
                patched_functions.append("premium.verify_premium_for_feature")
                
            # Patch get_guild_premium_tier if it exists
            if hasattr(premium, "get_guild_premium_tier"):
                original_func = premium.get_guild_premium_tier
                premium.get_guild_premium_tier = trace_async_function(original_func)
                patched_functions.append("premium.get_guild_premium_tier")
                
            # Patch check_premium if it exists
            if hasattr(premium, "check_premium"):
                original_func = premium.check_premium
                premium.check_premium = trace_async_function(original_func)
                patched_functions.append("premium.check_premium")
                
            logger.info(f"Patched utils.premium module functions")
        except ImportError:
            logger.warning("Could not import utils.premium module")
            
        # Patch utils.premium_compatibility module
        try:
            from utils import premium_compatibility
            
            # Patch verify_premium_for_feature_compat if it exists
            if hasattr(premium_compatibility, "verify_premium_for_feature_compat"):
                original_func = premium_compatibility.verify_premium_for_feature_compat
                premium_compatibility.verify_premium_for_feature_compat = trace_async_function(original_func)
                patched_functions.append("premium_compatibility.verify_premium_for_feature_compat")
                
            # Patch check_premium if it exists
            if hasattr(premium_compatibility, "check_premium"):
                original_func = premium_compatibility.check_premium
                premium_compatibility.check_premium = trace_async_function(original_func)
                patched_functions.append("premium_compatibility.check_premium")
                
            logger.info(f"Patched utils.premium_compatibility module functions")
        except ImportError:
            logger.warning("Could not import utils.premium_compatibility module")
            
        # Patch cogs.stats module
        try:
            from cogs import stats
            
            # Find the Stats class
            for name, obj in inspect.getmembers(stats):
                if inspect.isclass(obj) and hasattr(obj, "qualified_name") and obj.qualified_name == "stats":
                    # Found Stats class
                    stats_cog_class = obj
                    
                    # Patch verify_premium method if it exists
                    if hasattr(stats_cog_class, "verify_premium"):
                        original_method = stats_cog_class.verify_premium
                        stats_cog_class.verify_premium = trace_async_function(original_method)
                        patched_functions.append("Stats.verify_premium")
                        
                    logger.info(f"Patched cogs.stats.Stats methods")
                    break
        except ImportError:
            logger.warning("Could not import cogs.stats module")
            
        # Patch models.guild module
        try:
            from models import guild
            
            # Find the Guild class
            for name, obj in inspect.getmembers(guild):
                if inspect.isclass(obj) and name == "Guild":
                    # Found Guild class
                    guild_class = obj
                    
                    # Patch get_premium_tier method if it exists
                    if hasattr(guild_class, "get_premium_tier"):
                        original_method = guild_class.get_premium_tier
                        guild_class.get_premium_tier = trace_async_function(original_method)
                        patched_functions.append("Guild.get_premium_tier")
                        
                    # Patch has_premium_feature method if it exists
                    if hasattr(guild_class, "has_premium_feature"):
                        original_method = guild_class.has_premium_feature
                        guild_class.has_premium_feature = trace_async_function(original_method)
                        patched_functions.append("Guild.has_premium_feature")
                        
                    logger.info(f"Patched models.guild.Guild methods")
                    break
        except ImportError:
            logger.warning("Could not import models.guild module")
        
        logger.info(f"Successfully patched {len(patched_functions)} premium functions with tracing")
        return patched_functions
    except Exception as e:
        logger.error(f"Error patching premium functions: {e}")
        traceback.print_exc()
        return patched_functions


def get_premium_check_metrics():
    """Get premium check metrics"""
    return premium_check_metrics


def reset_premium_check_metrics():
    """Reset premium check metrics"""
    premium_check_metrics["total_checks"] = 0
    premium_check_metrics["successful_checks"] = 0
    premium_check_metrics["failed_checks"] = 0
    premium_check_metrics["checks_by_feature"] = {}
    premium_check_metrics["checks_by_guild"] = {}
    premium_check_metrics["failures_by_feature"] = {}
    premium_check_metrics["failures_by_guild"] = {}
    
    
# Manual tracing functions for inline use
async def trace_feature_check(db, guild_id, feature_name, source="manual"):
    """
    Manually trace a feature check
    
    Args:
        db: Database connection
        guild_id: Guild ID
        feature_name: Feature name to check
        source: Source of the check
        
    Returns:
        bool: Whether the feature is available
    """
    logger.info(f"MANUAL TRACE: Checking feature {feature_name} for guild {guild_id} from {source}")
    
    try:
        # Try multiple check methods
        results = {}
        
        # Method 1: Direct Guild lookup + premium_tier check
        try:
            from models.guild import Guild
            guild = await Guild.get_by_guild_id(db, guild_id)
            
            if guild is not None:
                guild_tier = guild.premium_tier
                logger.info(f"MANUAL TRACE: Guild tier from direct attribute: {guild_tier}")
                
                # Log guild document keys
                doc_keys = getattr(guild, "_document", {}).keys()
                logger.info(f"MANUAL TRACE: Guild document keys: {', '.join(doc_keys)}")
                
                # Get tier from method
                method_tier = await guild.get_premium_tier()
                logger.info(f"MANUAL TRACE: Guild tier from method: {method_tier}")
                
                results["guild_direct"] = {
                    "found": True,
                    "tier": guild_tier,
                    "method_tier": method_tier
                }
            else:
                logger.warning(f"MANUAL TRACE: Guild not found with ID {guild_id}")
                results["guild_direct"] = {"found": False}
        except Exception as e:
            logger.error(f"MANUAL TRACE: Error in direct Guild lookup: {e}")
            results["guild_direct"] = {"error": str(e)}
            
        # Method 2: utils.premium check
        try:
            from utils.premium import verify_premium_for_feature, get_guild_premium_tier
            
            # Get guild tier
            tier = await get_guild_premium_tier(db, guild_id)
            logger.info(f"MANUAL TRACE: Premium tier from utils.premium: {tier}")
            
            # Check feature access
            access, error_msg = await verify_premium_for_feature(db, guild_id, None, feature_name)
            logger.info(f"MANUAL TRACE: Feature access from utils.premium: {access}, error: {error_msg}")
            
            results["utils_premium"] = {
                "tier": tier,
                "access": access,
                "error_msg": error_msg
            }
        except Exception as e:
            logger.error(f"MANUAL TRACE: Error in utils.premium check: {e}")
            results["utils_premium"] = {"error": str(e)}
            
        # Method 3: utils.premium_compatibility check
        try:
            from utils.premium_compatibility import check_premium, get_premium_tier
            
            # Get guild tier
            tier = await get_premium_tier(db, guild_id)
            logger.info(f"MANUAL TRACE: Premium tier from utils.premium_compatibility: {tier}")
            
            # Check feature access
            access, error_msg = await verify_premium_for_feature(db, guild_id, None, feature_name)
            logger.info(f"MANUAL TRACE: Feature access from utils.premium_compatibility: {access}, error: {error_msg}")
            
            # Check feature mapping
            from utils.premium_compatibility import FEATURE_NAME_MAP
            mapped_feature = FEATURE_NAME_MAP.get(feature_name, feature_name)
            logger.info(f"MANUAL TRACE: Feature mapping: {feature_name} → {mapped_feature}")
            
            # Check access with mapped feature if different
            if mapped_feature != feature_name:
                mapped_access, mapped_error_msg = await verify_premium_for_feature(db, guild_id, None, mapped_feature)
                logger.info(f"MANUAL TRACE: Mapped feature access: {mapped_access}, error: {mapped_error_msg}")
            else:
                mapped_access = access
                mapped_error_msg = error_msg
                
            results["utils_premium_compat"] = {
                "tier": tier,
                "access": access,
                "error_msg": error_msg,
                "mapped_feature": mapped_feature,
                "mapped_access": mapped_access,
                "mapped_error_msg": mapped_error_msg
            }
        except Exception as e:
            logger.error(f"MANUAL TRACE: Error in utils.premium_compatibility check: {e}")
            results["utils_premium_compat"] = {"error": str(e)}
            
        # Method 4: cogs.stats verification if applicable
        if feature_name in ["stats", "stats_server", "server", "stats_leaderboard", "leaderboard"]:
            try:
                from cogs.stats import Stats
                
                # Create a mock bot
                class MockBot:
                    def __init__(self):
                        self.db = db
                        
                stats_cog = Stats(MockBot())
                
                # Map feature name to subcommand
                subcommand = feature_name.replace("stats_", "")
                if subcommand == "stats":
                    subcommand = None
                    
                # Check verification
                if hasattr(stats_cog, "verify_premium"):
                    access = await stats_cog.verify_premium(guild_id, subcommand)
                    logger.info(f"MANUAL TRACE: Stats.verify_premium access for {subcommand or 'default'}: {access}")
                    
                    results["stats_cog"] = {
                        "subcommand": subcommand,
                        "access": access
                    }
                else:
                    logger.warning("MANUAL TRACE: Stats has no verify_premium method")
                    results["stats_cog"] = {"error": "No verify_premium method"}
            except Exception as e:
                logger.error(f"MANUAL TRACE: Error in cogs.stats verification: {e}")
                results["stats_cog"] = {"error": str(e)}
                
        # Method 5: PremiumGuild model direct check
        try:
            from utils.premium_mongodb_models import PremiumGuild
            
            premium_guild = await PremiumGuild.get_by_guild_id(db, guild_id)
            
            if premium_guild is not None:
                tier = premium_guild.premium_tier
                logger.info(f"MANUAL TRACE: PremiumGuild tier: {tier}")
                
                # Check direct feature access
                direct_access = premium_guild.has_feature_access(feature_name)
                logger.info(f"MANUAL TRACE: PremiumGuild direct access for {feature_name}: {direct_access}")
                
                # Check access with mapped feature
                from utils.premium_compatibility import FEATURE_NAME_MAP
                mapped_feature = FEATURE_NAME_MAP.get(feature_name, feature_name)
                
                if mapped_feature != feature_name:
                    mapped_access = premium_guild.has_feature_access(mapped_feature)
                    logger.info(f"MANUAL TRACE: PremiumGuild mapped access for {mapped_feature}: {mapped_access}")
                else:
                    mapped_access = direct_access
                    
                results["premium_guild"] = {
                    "found": True,
                    "tier": tier,
                    "direct_access": direct_access,
                    "mapped_feature": mapped_feature,
                    "mapped_access": mapped_access
                }
            else:
                logger.warning(f"MANUAL TRACE: PremiumGuild not found with ID {guild_id}")
                results["premium_guild"] = {"found": False}
        except Exception as e:
            logger.error(f"MANUAL TRACE: Error in PremiumGuild check: {e}")
            results["premium_guild"] = {"error": str(e)}
            
        # Determine final result based on all checks
        final_access = False
        
        # Try to use the most reliable method result
        if "premium_guild" in results and "mapped_access" in results["premium_guild"]:
            final_access = results["premium_guild"]["mapped_access"]
        elif "utils_premium_compat" in results and "mapped_access" in results["utils_premium_compat"]:
            final_access = results["utils_premium_compat"]["mapped_access"]
        elif "utils_premium" in results and "access" in results["utils_premium"]:
            final_access = results["utils_premium"]["access"]
        elif "stats_cog" in results and "access" in results["stats_cog"]:
            final_access = results["stats_cog"]["access"]
            
        logger.info(f"MANUAL TRACE: Final access determination for {feature_name}: {final_access}")
        
        # Log results in metrics
        log_premium_check(feature_name, str(guild_id), final_access, {
            "source": source,
            "trace_results": results
        })
        
        return final_access, results
    except Exception as e:
        logger.error(f"MANUAL TRACE: Critical error in trace_feature_check: {e}")
        traceback.print_exc()
        return False, {"critical_error": str(e)}