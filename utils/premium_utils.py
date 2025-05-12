"""
# module: premium_utils
Premium Feature Utilities

This module provides a standardized interface for premium feature validation.
It is the recommended interface for all new code that needs to validate premium features.
"""

import logging
import re
import asyncio
from typing import Dict, Any, List, Optional, Tuple, Union, TypeVar, cast

logger = logging.getLogger(__name__)

# Import core functionality from premium_feature_access
from utils.premium_feature_access import (
    get_feature_tier_requirement,
    verify_premium_tier,
    verify_premium_feature,
    check_feature_access,
    get_guild_premium_tier,
    check_tier_access,
    PREMIUM_FEATURES
)

# Feature name mapping for UI display
FEATURE_NAME_MAPPING = {
    "stats": "Player Statistics",
    "basic_commands": "Basic Commands",
    "help": "Help Commands",
    "info": "Server Information",
    "setup": "Server Setup",
    "player_search": "Player Search",
    "basic_settings": "Basic Settings",
    "leaderboard": "Leaderboards",
    "scheduled_updates": "Scheduled Updates",
    "weapon_stats": "Weapon Statistics",
    "playtime_tracking": "Playtime Tracking",
    "player_links": "Player Links",
    "killfeed": "Kill Feed",
    "multi_server": "Multi-Server Support",
    "advanced_stats": "Advanced Statistics",
    "rivalries": "Rivalries System",
    "embeds": "Discord Embeds",
    "data_export": "Data Export",
    "custom_commands": "Custom Commands",
    "admin_tools": "Admin Tools",
    "bot_logs": "Bot Logs",
    "premium_embeds": "Premium Embeds",
    "custom_embeds": "Custom Embeds",
    "events": "Events System",
    "factions": "Factions System",
    "bounties": "Bounties System",
    "economy": "Economy System",
    "custom_features": "Custom Features",
    "dedicated_hosting": "Dedicated Hosting",
    "custom_leaderboards": "Custom Leaderboards",
    "priority_support": "Priority Support",
    "white_label": "White Label Service",
    "tier_0": "Free Tier Access",
    "tier_1": "Basic Tier Access",
    "tier_2": "Standard Tier Access",
    "tier_3": "Premium Tier Access",
    "tier_4": "Enterprise Tier Access",
}

# Feature tier requirements
FEATURE_TIERS = PREMIUM_FEATURES

async def normalize_feature_name(feature_name: str) -> str:
    """
    Normalize a feature name for consistent lookup.
    
    Args:
        feature_name: Raw feature name
        
    Returns:
        Normalized feature name
    """
    # Convert to lowercase and remove spaces/underscores
    normalized = feature_name.lower().strip()
    
    # Replace spaces with underscores for consistency
    normalized = normalized.replace(" ", "_")
    
    # Handle common variations
    if normalized in ["player_stats", "player_statistics", "playerstats"]:
        normalized = "stats"
    elif normalized in ["leaderboards", "top_players", "rankings"]:
        normalized = "leaderboard"
    elif normalized in ["advanced_analytics", "advanced_statistics"]:
        normalized = "advanced_stats"
    elif normalized in ["premium_embed", "premium_embeds"]:
        normalized = "premium_embeds"
    elif normalized in ["custom_embed", "custom_embeds"]:
        normalized = "custom_embeds"
    elif normalized in ["log", "logs"]:
        normalized = "bot_logs"
    elif normalized in ["multi", "multi_servers"]:
        normalized = "multi_server"
    
    # Special case for tier checks
    tier_match = re.match(r"tier[_\s]*(\d+)", normalized)
    if tier_match is not None:
        tier_num = tier_match.group(1)
        normalized = f"tier_{tier_num}"
    
    logger.debug(f"Normalized feature name '{feature_name}' to '{normalized}'")
    
    return normalized

async def verify_premium_for_feature(db, guild_id: Union[str, int], feature_name: str) -> bool:
    """
    Verify if a guild has access to a premium feature.
    
    This is the recommended function to use for most premium checks.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        feature_name: Feature name to check (will be normalized)
        
    Returns:
        bool: True if the guild has access, False otherwise
    """
    # Normalize feature name for consistent lookup
    normalized_feature = await normalize_feature_name(feature_name)
    
    # Use the core feature verification
    return await verify_premium_feature(db, str(guild_id), normalized_feature)

async def standardize_premium_check(
    db, 
    guild_id: Union[str, int], 
    feature_name: str, 
    error_message: bool = False
) -> Union[bool, Tuple[bool, Optional[str]]]:
    """
    Standardized premium check with optional error message.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        feature_name: Feature name to check (will be normalized)
        error_message: Whether to return an error message for UI display
        
    Returns:
        If error_message is False: bool indicating access
        If error_message is True: Tuple of (bool access, Optional[str] error_message)
    """
    # Normalize feature name for consistent lookup
    normalized_feature = await normalize_feature_name(feature_name)
    
    # Get the required tier for this feature
    required_tier = await get_feature_tier_requirement(normalized_feature)
    
    # Handle special tier_N feature names directly
    tier_match = re.match(r"tier_(\d+)", normalized_feature)
    if tier_match is not None:
        required_tier = int(tier_match.group(1))
    
    # If error message is requested, use check_tier_access
    if error_message is not None:
        return await check_tier_access(db, str(guild_id), required_tier)
    
    # Otherwise, just return the boolean result
    return await verify_premium_tier(db, str(guild_id), required_tier)

async def get_guild_tier(db, guild_id: Union[str, int]) -> int:
    """
    Get a guild's premium tier.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        
    Returns:
        int: Guild's premium tier (0-4)
    """
    return await get_guild_premium_tier(db, str(guild_id))

# Alias for compatibility with verification script
get_premium_tier = get_guild_tier

# Add function aliases for compatibility with test scripts
async def has_premium_tier(db, guild_id: Union[str, int], tier: int) -> bool:
    """Check if a guild has a specific premium tier.
    
    Args:
        db: Database connection
        guild_id: Guild ID
        tier: Tier to check for
        
    Returns:
        True if guild has the specified tier or higher, False otherwise
    """
    return await verify_premium_tier(db, str(guild_id), tier)

async def has_premium_feature(db, guild_id: Union[str, int], feature_name: str) -> bool:
    """Check if a guild has access to a premium feature.
    
    Args:
        db: Database connection
        guild_id: Guild ID
        feature_name: Feature to check access for
        
    Returns:
        True if guild has access to the feature, False otherwise
    """
    return await verify_premium_for_feature(db, guild_id, feature_name)

async def check_guild_feature_access(db, guild_id: Union[str, int], feature_names: List[str]) -> Dict[str, bool]:
    """
    Check multiple features at once for a guild.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        feature_names: List of feature names to check
        
    Returns:
        Dict mapping feature names to access status
    """
    result = {}
    for feature in feature_names:
        result[feature] = await verify_premium_for_feature(db, guild_id, feature)
    
    return result

async def log_premium_access_attempt(
    db,
    guild_id: Union[str, int],
    feature_name: str, 
    granted: bool,
    user_id: Optional[Union[str, int]] = None
) -> None:
    """
    Log a premium access attempt for analytics and debugging.
    
    Args:
        db: Database connection
        guild_id: Discord guild ID
        feature_name: Feature name that was checked
        granted: Whether access was granted
        user_id: Optional Discord user ID who attempted to use the feature
    """
    try:
        # Skip logging for common features to avoid log spam
        common_features = ["stats", "help", "info", "basic_commands"]
        normalized = await normalize_feature_name(feature_name)
        if normalized in common_features:
            return
        
        # Log to console for debugging
        status = "granted" if granted else "denied"
        logger.debug(f"Premium access {status} for guild {guild_id}, feature '{feature_name}'")
        
        # Attempt to log to database if premium_access_logs collection exists
        if db is not None:
            try:
                # Check if collection exists first to avoid errors
                collections = await db.list_collection_names()
                if "premium_access_logs" in collections:
                    # Get current time safely
                    import datetime
                    current_time = datetime.datetime.utcnow()
                    
                    # Format the log entry
                    log_entry = {
                        "guild_id": str(guild_id),
                        "feature": feature_name,
                        "normalized_feature": normalized,
                        "access_granted": granted,
                        "timestamp": current_time,
                    }
                    
                    # Add user_id if provided
                    if user_id is not None:
                        log_entry["user_id"] = str(user_id)
                    
                    # Insert the log
                    await db.premium_access_logs.insert_one(log_entry)
            except Exception as inner_e:
                logger.warning(f"Failed to log to database: {inner_e}")
    except Exception as e:
        # Don't let logging failures affect the running application
        logger.error(f"Failed to log premium access attempt: {e}")