"""
Premium system configuration and constants.
This module provides centralized configuration for the premium system.
"""
from typing import Dict, List, Any

# Premium tier definitions
PREMIUM_TIERS: Dict[int, Dict[str, Any]] = {
    0: {
        "name": "Free",
        "max_servers": 1,
        "price_gbp": 0,
        "features": ["killfeed"]
    },
    1: {
        "name": "Survivor",
        "max_servers": 2,
        "price_gbp": 5,
        "features": ["killfeed", "basic_stats", "leaderboards"]
    },
    2: {
        "name": "Mercenary",
        "max_servers": 5,
        "price_gbp": 10,
        "features": ["killfeed", "basic_stats", "leaderboards", "rivalries", "bounties", 
                    "player_links", "economy", "advanced_analytics"]
    },
    3: {
        "name": "Warlord",
        "max_servers": 10,
        "price_gbp": 20,
        "features": ["killfeed", "basic_stats", "leaderboards", "rivalries", "bounties", 
                    "player_links", "economy", "advanced_analytics", "factions", "events"]
    },
    4: {
        "name": "Overlord",
        "max_servers": 25,
        "price_gbp": 50,
        "features": ["killfeed", "basic_stats", "leaderboards", "rivalries", "bounties", 
                    "player_links", "economy", "advanced_analytics", "factions", "events",
                    "custom_embeds", "full_automation"]
    }
}

# Feature descriptions for documentation
FEATURE_DESCRIPTIONS: Dict[str, str] = {
    "killfeed": "Real-time player kill notifications in Discord channels",
    "basic_stats": "Player kill/death statistics and leaderboards",
    "leaderboards": "Server-wide player rankings and statistics",
    "rivalries": "Track player vs player combat history",
    "bounties": "Place and collect bounties on other players",
    "player_links": "Link Discord accounts to in-game players",
    "economy": "In-server currency and reward system",
    "advanced_analytics": "Detailed combat and playstyle analysis",
    "factions": "Group-based gameplay and tracking",
    "events": "Scheduled and triggered server events",
    "custom_embeds": "Customized Discord message appearance",
    "full_automation": "Advanced scheduled tasks and automated reports"
}

# Feature to tier mapping (derived from PREMIUM_TIERS)
FEATURE_TIER_REQUIREMENTS: Dict[str, int] = {}

# Initialize FEATURE_TIER_REQUIREMENTS
for tier, data in PREMIUM_TIERS.items():
    for feature in data.get("features", []):
        # Only set if feature not already in map or current tier is lower
        if feature not in FEATURE_TIER_REQUIREMENTS or tier < FEATURE_TIER_REQUIREMENTS[feature]:
            FEATURE_TIER_REQUIREMENTS[feature] = tier

# Collections used by the premium system
PREMIUM_COLLECTIONS = [
    "premium_guilds",
    "premium_servers",
    "premium_payments"
]

# Subscription status values
SUBSCRIPTION_STATUS = {
    "ACTIVE": "active",
    "EXPIRED": "expired",
    "CANCELLED": "cancelled",
    "PENDING": "pending"
}


def get_feature_tier(feature_name: str) -> int:
    """
    Get the minimum tier required for a feature.
    
    Args:
        feature_name: Name of the feature
        
    Returns:
        int: Minimum tier required (0-4), or None if feature not found
    """
    return FEATURE_TIER_REQUIREMENTS.get(feature_name)


def get_tier_features(tier: int) -> List[str]:
    """
    Get all features available at a specific tier.
    
    Args:
        tier: Premium tier (0-4)
        
    Returns:
        List[str]: List of feature names available at the specified tier
    """
    if tier not in PREMIUM_TIERS:
        return []
        
    return PREMIUM_TIERS[tier].get("features", [])


def get_tier_name(tier: int) -> str:
    """
    Get the name of a premium tier.
    
    Args:
        tier: Premium tier (0-4)
        
    Returns:
        str: Name of the tier, or "Unknown" if tier not found
    """
    if tier not in PREMIUM_TIERS:
        return "Unknown"
        
    return PREMIUM_TIERS[tier].get("name", "Unknown")


def get_tier_price(tier: int) -> float:
    """
    Get the price of a premium tier in GBP.
    
    Args:
        tier: Premium tier (0-4)
        
    Returns:
        float: Price in GBP, or 0 if tier not found
    """
    if tier not in PREMIUM_TIERS:
        return 0
        
    return PREMIUM_TIERS[tier].get("price_gbp", 0)


def get_max_servers(tier: int) -> int:
    """
    Get the maximum number of servers allowed for a premium tier.
    
    Args:
        tier: Premium tier (0-4)
        
    Returns:
        int: Maximum number of servers, or 1 if tier not found
    """
    if tier not in PREMIUM_TIERS:
        return 1
        
    return PREMIUM_TIERS[tier].get("max_servers", 1)


def get_feature_description(feature_name: str) -> str:
    """
    Get the description of a feature.
    
    Args:
        feature_name: Name of the feature
        
    Returns:
        str: Description of the feature, or empty string if feature not found
    """
    return FEATURE_DESCRIPTIONS.get(feature_name, "")