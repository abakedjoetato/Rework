"""
Compatibility layer for the premium system.
This module provides backwards compatibility for the old premium system.
It intercepts calls to the old system and redirects them to the new one.
"""
import logging
import functools
from typing import Any, Optional, Union, Tuple, Dict, List

# Import both old and new premium modules
try:
    # Import from old system for compatibility
    from utils.premium import verify_premium_for_feature, get_guild_premium_tier
    from utils.premium import ensure_premium_tier, get_feature_tier_requirement
    from utils.premium import PREMIUM_TIERS as OLD_PREMIUM_TIERS
except ImportError:
    # Create stub functions if old system is already removed
    verify_premium_for_feature = None
    get_guild_premium_tier = None
    ensure_premium_tier = None
    get_feature_tier_requirement = None
    OLD_PREMIUM_TIERS = {}

# Import from new system
from utils.premium_config import get_tier_name, get_tier_features, get_feature_tier, PREMIUM_TIERS
from utils.premium_mongodb_models import PremiumGuild
from utils.premium_feature_access import PremiumFeature

# Configure logging
logger = logging.getLogger(__name__)

# Maps of old feature names to new feature names
FEATURE_NAME_MAP = {
    "stats": "basic_stats",  # Updated by comprehensive fix
    "stats_server": "basic_stats",  # Updated by comprehensive fix
    "stats_player": "basic_stats",  # Updated by comprehensive fix
    "stats_weapon": "basic_stats",  # Updated by comprehensive fix
    "stats_leaderboard": "leaderboards",  # Updated by comprehensive fix
    "stats_weapon_categories": "basic_stats",  # Updated by comprehensive fix
    "server": "basic_stats",  # Updated by comprehensive fix
    "player": "basic_stats",  # Updated by comprehensive fix
    "weapon": "basic_stats",  # Updated by comprehensive fix
    "weapon_categories": "basic_stats",  # Updated by comprehensive fix
    "leaderboard": "leaderboards",  # Updated by comprehensive fix
    "leaderboards": "leaderboards",  # Updated by comprehensive fix
    "rivalry": "rivalries",  # Updated by comprehensive fix
    "bounty": "bounties",  # Updated by comprehensive fix
    "faction": "factions",  # Updated by comprehensive fix
    "event": "events",  # Updated by comprehensive fix
    "premium": "basic_stats",  # Updated by comprehensive fix
}

# Cache for premium tier lookups
_premium_tier_cache = {}


async def verify_premium_for_feature_compat(
    db, guild_id: Union[str, int], guild_model: Any, feature_name: str, 
    error_message: bool = True
) -> Union[bool, Tuple[bool, Optional[str]]]:
    """
    Compatibility wrapper for verify_premium_for_feature from the old system.
    
    Args:
        db: MongoDB database connection
        guild_id: Discord guild ID
        guild_model: Guild model instance from old system (may be None)
        feature_name: Name of the feature to check
        error_message: Whether to include error message in return value
        
    Returns:
        If error_message is True, returns (has_access, error_msg)
        If error_message is False, returns has_access
    """
    # Convert feature name if needed
    mapped_feature = FEATURE_NAME_MAP.get(feature_name, feature_name)
    
    # Log the check
    logger.info(f"[COMPAT] Checking premium feature: {feature_name} → {mapped_feature}")
    
    # Handle custom checks for certain features
    if feature_name == "killfeed" or mapped_feature == "killfeed":
        # Killfeed is always available
        if error_message is not None:
            return True, None
        return True
    
    try:
        # Get guild from new system
        str_guild_id = str(guild_id)
        premium_guild = await PremiumGuild.get_by_guild_id(db, str_guild_id)
        
        if premium_guild is None:
            # Create guild if it doesn't exist
            logger.info(f"[COMPAT] Creating new premium guild for {str_guild_id}")
            
            # Get guild name from model if available
            guild_name = getattr(guild_model, 'name', 'Unknown Guild')
            
            # Create new guild
            premium_guild = await PremiumGuild.get_or_create(db, str_guild_id, guild_name)
        
        # Check feature access
        has_access = premium_guild.has_feature_access(mapped_feature)
        logger.info(f"[COMPAT] Premium feature access for {str_guild_id}: {mapped_feature} = {has_access}")
        
        if error_message is not None:
            # If feature is not accessible, provide error message
            if has_access is None:
                feature_tier = get_feature_tier(mapped_feature)
                tier_name = get_tier_name(feature_tier) if feature_tier is not None else "Unknown"
                
                error_msg = (
                    f"This feature requires the **{tier_name}** tier or higher.\n"
                    f"Your server is currently on the **{get_tier_name(premium_guild.premium_tier)}** tier.\n"
                    f"Use `/premium upgrade` for more information."
                )
                return False, error_msg
            return True, None
        else:
            return has_access
            
    except Exception as e:
        logger.error(f"[COMPAT] Error checking premium feature: {e}")
        
        # Handle failure gracefully
        if error_message is not None:
            return False, "An error occurred while checking premium access."
        return False


async def get_guild_premium_tier_compat(db, guild_id: Union[str, int], guild_model: Any = None) -> int:
    """
    Compatibility wrapper for get_guild_premium_tier from the old system.
    
    Args:
        db: MongoDB database connection
        guild_id: Discord guild ID
        guild_model: Guild model instance (may be None)
        
    Returns:
        int: Premium tier (0-4)
    """
    # Try to use cache
    str_guild_id = str(guild_id)
    if str_guild_id in _premium_tier_cache:
        logger.info(f"[COMPAT] Using cached premium tier for {str_guild_id}: {_premium_tier_cache[str_guild_id]}")
        return _premium_tier_cache[str_guild_id]
    
    try:
        # Get guild from new system
        premium_guild = await PremiumGuild.get_by_guild_id(db, str_guild_id)
        
        if premium_guild is None:
            # Create guild if it doesn't exist
            logger.info(f"[COMPAT] Creating new premium guild for {str_guild_id}")
            
            # Get guild name from model if available
            guild_name = getattr(guild_model, 'name', 'Unknown Guild')
            
            # Create new guild
            premium_guild = await PremiumGuild.get_or_create(db, str_guild_id, guild_name)
        
        # Get premium tier
        tier = premium_guild.check_premium_status()
        
        # Cache result
        _premium_tier_cache[str_guild_id] = tier
        
        logger.info(f"[COMPAT] Premium tier for {str_guild_id}: {tier}")
        return tier
            
    except Exception as e:
        logger.error(f"[COMPAT] Error getting premium tier: {e}")
        return 0


def ensure_premium_tier_compat(required_tier: int):
    """
    Compatibility decorator for ensure_premium_tier from the old system.
    
    Args:
        required_tier: Required premium tier (0-4)
        
    Returns:
        Command decorator function
    """
    # Use the new system's decorator
    return PremiumFeature.require_tier(required_tier)


def get_feature_tier_requirement_compat(feature_name: str) -> int:
    """
    Compatibility function for get_feature_tier_requirement from the old system.
    
    Args:
        feature_name: Name of the feature
        
    Returns:
        int: Tier requirement (0-4)
    """
    # Convert feature name if needed
    mapped_feature = FEATURE_NAME_MAP.get(feature_name, feature_name)
    
    # Get tier requirement from new system
    tier = get_feature_tier(mapped_feature)
    
    # Default to highest tier if feature not found
    if tier is None:
        logger.warning(f"[COMPAT] Feature not found: {feature_name} → {mapped_feature}")
        return 4
    
    return tier


# Override the old functions with the new ones
if verify_premium_for_feature is not None:
    verify_premium_for_feature = verify_premium_for_feature_compat
    
if get_guild_premium_tier is not None:
    get_guild_premium_tier = get_guild_premium_tier_compat
    
if ensure_premium_tier is not None:
    ensure_premium_tier = ensure_premium_tier_compat
    
if get_feature_tier_requirement is not None:
    get_feature_tier_requirement = get_feature_tier_requirement_compat

# Define functions globally for import compatibility
async def verify_premium_for_feature(db, guild_id, guild_model=None, feature_name="premium", error_message=True):
    """Global compatibility function for check_premium"""
    return await verify_premium_for_feature_compat(db, guild_id, guild_model, feature_name, error_message)

async def get_premium_tier(db, guild_id, guild_model=None):
    """Global compatibility function for get_premium_tier"""
    return await get_guild_premium_tier_compat(db, guild_id, guild_model)

def premium_required(tier):
    """Global compatibility function for premium_required"""
    return ensure_premium_tier_compat(tier)

def get_tier_requirement(feature_name):
    """Global compatibility function for get_tier_requirement"""
    return get_feature_tier_requirement_compat(feature_name)

# Monkey patch the utils.premium module if it exists
try:
    import utils.premium
    
    # Replace functions in the module
    utils.premium.verify_premium_for_feature = verify_premium_for_feature_compat
    utils.premium.get_guild_premium_tier = get_guild_premium_tier_compat
    utils.premium.ensure_premium_tier = ensure_premium_tier_compat
    utils.premium.get_feature_tier_requirement = get_feature_tier_requirement_compat
    
    # Add compatibility functions
    utils.premium.check_premium = check_premium
    utils.premium.get_premium_tier = get_premium_tier
    utils.premium.premium_required = premium_required
    utils.premium.get_tier_requirement = get_tier_requirement
    
    logger.info("[COMPAT] Successfully monkey patched utils.premium module")
except Exception as e:
    logger.error(f"[COMPAT] Error monkey patching utils.premium module: {e}")

# Patch the models.guild module if it exists
try:
    import models.guild
    
    # Define a monkey patch function for get_premium_tier in Guild class
    async def guild_get_premium_tier(self):
        """Monkey patched get_premium_tier method for Guild class"""
        return await get_premium_tier(self.db, self.guild_id, self)
        
    # Patch the Guild class
    if hasattr(models.guild, 'Guild'):
        # Only replace the method if class exists
        setattr(models.guild.Guild, 'get_premium_tier', guild_get_premium_tier)
        logger.info("[COMPAT] Successfully monkey patched models.guild.Guild.get_premium_tier")
except Exception as e:
    logger.error(f"[COMPAT] Error monkey patching models.guild module: {e}")


# Clear cache (call this periodically)
def clear_cache():
    """Clear cached premium tier information"""
    _premium_tier_cache.clear()