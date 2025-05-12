"""
# module: premium
Premium Backward Compatibility Module

This module provides backward compatibility with older premium validation 
methods. It uses the new premium_utils and premium_feature_access modules
under the hood for consistent behavior.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple, Union, cast

from models.guild import Guild

logger = logging.getLogger(__name__)

# Import the mappings from premium_feature_access
from utils.premium_feature_access import PREMIUM_FEATURES, SERVER_LIMITS

# Define premium tier names for backward compatibility
PREMIUM_TIERS = {
    0: "Free",
    1: "Basic",
    2: "Standard",
    3: "Premium",
    4: "Enterprise"
}

# Import new utilities for implementation
from utils.premium_utils import (
    verify_premium_for_feature, 
    standardize_premium_check,
    get_guild_tier
)

from utils.premium_feature_access import (
    get_feature_tier_requirement,
    get_guild_premium_tier
)

# Backwards compatibility function
async def validate_premium_feature(guild, feature_name: str) -> Tuple[bool, Optional[str]]:
    """
    Legacy method to validate if a guild has access to a premium feature.
    
    Args:
        guild: Guild model instance
        feature_name: Name of the feature to check
        
    Returns:
        Tuple of (has_access, error_message)
    """
    # Handle possible None guild
    if guild is None:
        logger.warning(f"validate_premium_feature called with None guild for feature {feature_name}")
        return (False, "Server not found. Please set up the bot with /setup first.")
    
    # Get database from guild
    db = guild.db if hasattr(guild, 'db') else None
    if db is None:
        logger.error(f"No database connection in validate_premium_feature for guild {getattr(guild, 'guild_id', 'unknown')}")
        return (True, None)  # Default to allowing access if we can't check
    
    # Use standardize_premium_check for consistent behavior
    result = await standardize_premium_check(db, str(guild.guild_id), feature_name, error_message=True)
    
    # Ensure we return a properly typed tuple
    if isinstance(result, tuple) and len(result) == 2:
        has_access, error_msg = result
        return (bool(has_access), error_msg)
    elif isinstance(result, bool):
        return (result, None)
    else:
        logger.error(f"Unexpected result type from standardize_premium_check: {type(result)}")
        return (True, None)  # Default to allowing access on unexpected result

# Backwards compatibility function
async def get_minimum_tier_for_feature(feature_name: str) -> int:
    """
    Legacy method to get the minimum tier required for a feature.
    
    Args:
        feature_name: Name of the feature to check
        
    Returns:
        Minimum tier level (0-4)
    """
    return await get_feature_tier_requirement(feature_name)

# Backwards compatibility function
async def has_feature_access(guild, feature_name: str) -> bool:
    """
    Legacy method to check if a guild has access to a premium feature.
    
    Args:
        guild: Guild model instance
        feature_name: Name of the feature to check
        
    Returns:
        True if the guild has access, False otherwise
    """
    # Handle possible None guild
    if guild is None:
        logger.warning(f"has_feature_access called with None guild for feature {feature_name}")
        return False
    
    # Get database from guild
    db = guild.db if hasattr(guild, 'db') else None
    if db is None:
        logger.error(f"No database connection in has_feature_access for guild {getattr(guild, 'guild_id', 'unknown')}")
        return True  # Default to allowing access if we can't check
    
    # Use verify_premium_for_feature for consistent behavior
    return await verify_premium_for_feature(db, str(guild.guild_id), feature_name)

# Backwards compatibility function
async def check_tier_access(db, guild_id: Union[str, int], required_tier: int) -> Tuple[bool, Optional[str]]:
    """
    Legacy method to check if a guild has the required premium tier.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        required_tier: Minimum tier level required (0-4)
        
    Returns:
        Tuple of (has_access, error_message)
    """
    # Use standardize_premium_check with tier feature name
    result = await standardize_premium_check(db, str(guild_id), f"tier_{required_tier}", error_message=True)
    
    # Ensure we return a properly typed tuple
    if isinstance(result, tuple) and len(result) == 2:
        has_access, error_msg = result
        return (bool(has_access), error_msg)
    elif isinstance(result, bool):
        return (result, None)
    else:
        logger.error(f"Unexpected result type from standardize_premium_check: {type(result)}")
        return (True, None)  # Default to allowing access on unexpected result

# Backwards compatibility function
async def validate_server_limit(guild) -> Tuple[bool, Optional[str]]:
    """
    Legacy method to validate if a guild has reached its server limit.
    
    Args:
        guild: Guild model instance
        
    Returns:
        Tuple of (has_capacity, error_message)
    """
    # Handle possible None guild
    if guild is None:
        logger.warning("validate_server_limit called with None guild")
        return (False, "Server not found. Please set up the bot with /setup first.")
    
    # Get database from guild
    db = guild.db if hasattr(guild, 'db') else None
    if db is None:
        logger.error(f"No database connection in validate_server_limit for guild {getattr(guild, 'guild_id', 'unknown')}")
        return (True, None)  # Default to allowing access if we can't check
    
    try:
        # Get the guild's current tier
        guild_tier = await get_guild_tier(db, str(guild.guild_id))
        
        # Get the server limit for this tier
        server_limit = SERVER_LIMITS.get(guild_tier, 1)
        
        # Count the guild's current servers
        server_count = await db.servers.count_documents({"guild_id": str(guild.guild_id)})
        
        # Check if the limit is reached
        has_capacity = server_count < server_limit
        
        if not has_capacity:
            error_message = (
                f"You have reached your server limit ({server_limit} servers). "
                f"Upgrade your premium tier to add more servers."
            )
            return (False, error_message)
        
        return (True, None)
    except Exception as e:
        logger.error(f"Error in validate_server_limit: {e}")
        return (True, None)  # Default to allowing access on error