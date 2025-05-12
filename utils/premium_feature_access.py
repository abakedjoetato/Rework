"""
# module: premium_feature_access
Premium Feature Access Utilities

This module provides core functions for verifying premium feature access.
It is used by both the legacy premium module and the new premium_utils module.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

async def get_premium_features(guild_id: str) -> Dict[str, Any]:
    """
    Get all premium features available to a guild.
    
    Args:
        guild_id: Guild ID to check
        
    Returns:
        Dict of premium features
    """
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
        from utils.db_connection import get_database
        
        # Get database connection
        db = await get_database()
        if db is None:
            return {}
        
        # Get guild document
        guild_doc = await db.guilds.find_one({"guild_id": guild_id})
        if guild_doc is None:
            return {}
            
        # Return premium features if they exist
        if "premium_features" in guild_doc and guild_doc["premium_features"] is not None:
            return guild_doc["premium_features"]
        
        return {}
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error getting premium features for guild {guild_id}: {e}")
        return {}

# Premium feature to tier mapping
# Tier 0 = Free, 1 = Basic, 2 = Standard, 3 = Premium, 4 = Enterprise
PREMIUM_FEATURES = {
    # Free tier (0) features
    "stats": 0,
    "basic_commands": 0,
    "help": 0,
    "info": 0,
    "setup": 0,
    "player_search": 0,
    "basic_settings": 0,
    
    # Basic tier (1) features 
    "leaderboard": 1,
    "scheduled_updates": 1,
    "weapon_stats": 1,
    "playtime_tracking": 1,
    "player_links": 1,
    "killfeed": 1,
    "multi_server": 1,
    
    # Standard tier (2) features
    "advanced_stats": 2,
    "rivalries": 2,
    "embeds": 2,
    "data_export": 2,
    "custom_commands": 2,
    "admin_tools": 2,
    "bot_logs": 2,
    
    # Premium tier (3) features
    "premium_embeds": 3,
    "custom_embeds": 3,
    "events": 3,
    "factions": 3,
    "bounties": 3,
    "economy": 3,
    
    # Enterprise tier (4) features
    "custom_features": 4,
    "dedicated_hosting": 4,
    "custom_leaderboards": 4,
    "priority_support": 4,
    "white_label": 4,
    
    # Special features for testing tier levels directly
    "tier_0": 0,
    "tier_1": 1,
    "tier_2": 2,
    "tier_3": 3,
    "tier_4": 4,
}

# Graceful feature tier fallbacks
TIER_FALLBACKS = {
    "custom_features": "custom_embeds",  # Fall back from tier 4 to tier 3
    "white_label": "premium_embeds",     # Fall back from tier 4 to tier 3
    "bounties": "advanced_stats",        # Fall back from tier 3 to tier 2
    "factions": "advanced_stats",        # Fall back from tier 3 to tier 2
    "custom_commands": "basic_commands", # Fall back from tier 2 to tier 0
}

# Maximum server limits by tier
SERVER_LIMITS = {
    0: 1,    # Free tier: 1 server
    1: 3,    # Basic tier: 3 servers
    2: 10,   # Standard tier: 10 servers
    3: 25,   # Premium tier: 25 servers
    4: 100,  # Enterprise tier: 100 servers
}

async def get_feature_tier_requirement(feature_name: str) -> int:
    """
    Get the premium tier required for a specific feature.
    
    Args:
        feature_name: The name of the feature
        
    Returns:
        int: The required tier level (0-4)
    """
    # Convert to lowercase for consistency
    feature_name = feature_name.lower().strip()
    
    # Use the mapped tier or default to tier 1
    return PREMIUM_FEATURES.get(feature_name, 1)

async def verify_premium_tier(db, guild_id: Union[str, int], required_tier: int) -> bool:
    """
    Check if a guild has the required premium tier level.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        required_tier: The minimum tier level required (0-4)
        
    Returns:
        bool: True if the guild has the required tier or higher, False otherwise
    """
    # Get the guild's current tier
    current_tier = await get_guild_premium_tier(db, str(guild_id))
    
    # Simple tier comparison
    return current_tier >= required_tier

async def verify_premium_feature(db, guild_id: Union[str, int], feature_name: str) -> bool:
    """
    Check if a guild has access to a specific premium feature.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        feature_name: The name of the feature
        
    Returns:
        bool: True if the guild has access to the feature, False otherwise
    """
    # Get the required tier for this feature
    required_tier = await get_feature_tier_requirement(feature_name)
    
    # Check if the guild meets the tier requirement
    return await verify_premium_tier(db, guild_id, required_tier)

async def check_feature_access(db, guild_id: Union[str, int], feature_name: str) -> bool:
    """
    Check if a guild has access to a feature with fallback to alternative features.
    
    This function implements "graceful degradation" by checking if a guild
    has access to alternative features when they don't have access to the
    requested feature.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        feature_name: The name of the feature
        
    Returns:
        bool: True if the guild has access to the feature or a fallback, False otherwise
    """
    # First check direct access
    has_access = await verify_premium_feature(db, guild_id, feature_name)
    
    # If no access, check fallbacks
    if not has_access and feature_name in TIER_FALLBACKS:
        fallback_feature = TIER_FALLBACKS[feature_name]
        logger.info(f"Guild {guild_id} doesn't have access to {feature_name}, checking fallback to {fallback_feature}")
        has_access = await verify_premium_feature(db, guild_id, fallback_feature)
    
    return has_access

async def get_guild_premium_tier(db, guild_id: str) -> int:
    """
    Get a guild's premium tier level.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        
    Returns:
        int: The guild's premium tier (0-4, default 0)
    """
    try:
        # Get the guild document
        guild_doc = await db.guilds.find_one({"guild_id": guild_id})
        
        if guild_doc is None and guild_id.isdigit():
            # Try numeric guild_id if string version not found
            guild_doc = await db.guilds.find_one({"guild_id": int(guild_id)})
        
        # Extract and validate premium_tier
        if guild_doc and "premium_tier" in guild_doc:
            tier = guild_doc["premium_tier"]
            
            # Convert to int if needed
            if isinstance(tier, str) and tier.isdigit():
                tier = int(tier)
            elif not isinstance(tier, int):
                try:
                    tier = int(float(tier))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid premium_tier value for guild {guild_id}: {tier}")
                    return 0
            
            # Ensure valid range
            return max(0, min(4, tier))
        else:
            # Default to free tier
            return 0
            
    except Exception as e:
        logger.error(f"Error getting premium tier for guild {guild_id}: {e}")
        return 0  # Default to free tier on error

async def check_tier_access(db, guild_id: Union[str, int], required_tier: int) -> Tuple[bool, Optional[str]]:
    """
    Check if a guild has the required premium tier with error message.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        required_tier: The minimum tier level required (0-4)
        
    Returns:
        Tuple[bool, Optional[str]]: (has_access, error_message)
    """
    # Get the guild's current tier
    current_tier = await get_guild_premium_tier(db, str(guild_id))
    
    # Check access
    has_access = current_tier >= required_tier
    
    # Generate error message if needed
    if has_access is None:
        tier_names = {
            0: "Free",
            1: "Basic",
            2: "Standard",
            3: "Premium",
            4: "Enterprise"
        }
        current_tier_name = tier_names.get(current_tier, f"Tier {current_tier}")
        required_tier_name = tier_names.get(required_tier, f"Tier {required_tier}")
        
        error_message = (
            f"This feature requires **{required_tier_name}** tier. "
            f"Your current tier is **{current_tier_name}**. "
            "Use `/premium` to upgrade."
        )
        return (False, error_message)
    
    return (True, None)