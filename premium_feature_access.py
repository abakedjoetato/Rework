"""
Premium feature access decorators and utility functions.
This module provides decorators for checking premium feature access in Discord commands.
"""
import logging
import functools
from typing import Optional, Callable, Dict, List, Any, Union

import discord
from discord.ext import commands

from premium_mongodb_models import PremiumGuild, validate_premium_feature
from premium_config import get_feature_tier, get_tier_name, get_tier_features

logger = logging.getLogger(__name__)

# Cache to store feature check results temporarily
_feature_check_cache = {}
_guild_tier_cache = {}

# Cache invalidation function (called periodically)
def invalidate_caches():
    """Invalidate all feature check caches"""
    _feature_check_cache.clear()
    _guild_tier_cache.clear()


class PremiumFeature:
    """
    Decorator class for premium feature requirement.
    This class provides decorators for checking premium feature access in Discord commands.
    """
    
    @staticmethod
    def require(feature_name: str):
        """
        Decorator for commands that require a premium feature.
        
        Args:
            feature_name: Name of the premium feature required
            
        Returns:
            Command decorator function
        """
        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                # Get command context and bot instance
                ctx = None
                bot = None
                guild_id = None
                
                # Handle different command types
                if len(args) > 0:
                    if isinstance(args[0], commands.Context):
                        # Traditional command
                        ctx = args[0]
                        bot = ctx.bot
                        guild_id = ctx.guild.id if ctx.guild else None
                        
                        # Skip check in DMs
                        if guild_id is None:
                            return await func(*args, **kwargs)
                        
                        # Get database connection
                        db = getattr(bot, 'db', None)
                        if db is None:
                            logger.error("Cannot check premium feature: bot.db is not available")
                            return await func(*args, **kwargs)
                        
                        # Check feature access
                        guild = await PremiumGuild.get_by_guild_id(db, str(guild_id))
                        has_access, error_message = await validate_premium_feature(db, guild, feature_name)
                        
                        # If no access, send error message
                        if has_access is None:
                            if error_message is not None:
                                await ctx.send(error_message)
                            return None
                        
                        # Access granted, continue with command
                        return await func(*args, **kwargs)
                        
                    elif len(args) > 1 and isinstance(args[1], discord.Interaction):
                        # Application command
                        interaction = args[1]
                        cog = args[0]
                        guild_id = interaction.guild.id if interaction.guild else None
                        
                        # Skip check in DMs
                        if guild_id is None:
                            return await func(*args, **kwargs)
                        
                        # Get database connection
                        bot = getattr(cog, 'bot', None) or getattr(cog, 'client', None)
                        if bot is None:
                            logger.error("Cannot check premium feature: bot reference not found in cog")
                            return await func(*args, **kwargs)
                        
                        db = getattr(bot, 'db', None)
                        if db is None:
                            logger.error("Cannot check premium feature: bot.db is not available")
                            return await func(*args, **kwargs)
                        
                        # Check feature access
                        guild = await PremiumGuild.get_by_guild_id(db, str(guild_id))
                        has_access, error_message = await validate_premium_feature(db, guild, feature_name)
                        
                        # If no access, send error message
                        if has_access is None:
                            if error_message is not None:
                                try:
                                    # Try to respond if interaction not responded to yet
                                    await interaction.response.send_message(error_message, ephemeral=True)
                                except discord.errors.InteractionResponded:
                                    # Fallback if interaction already responded to
                                    await interaction.followup.send(error_message, ephemeral=True)
                            return None
                        
                        # Access granted, continue with command
                        return await func(*args, **kwargs)
                
                # If we couldn't determine context, just run the command
                logger.warning(f"Could not determine Discord context for premium feature '{feature_name}' check, allowing command execution")
                return await func(*args, **kwargs)
            
            # Add attribute to help with command documentation
            setattr(wrapper, "premium_feature", feature_name)
            return wrapper
        
        return decorator
    
    @staticmethod
    def require_tier(tier: int):
        """
        Decorator for commands that require a specific premium tier.
        
        Args:
            tier: Minimum premium tier required (0-4)
            
        Returns:
            Command decorator function
        """
        # Get all features available at this tier
        tier_features = get_tier_features(tier)
        
        # Use the first feature from the tier's features list
        if tier_features is not None:
            feature_name = tier_features[0]
            return PremiumFeature.require(feature_name)
        else:
            # Fallback to a special feature name for tier check
            feature_name = f"premium_tier_{tier}"
            return PremiumFeature.require(feature_name)
    
    @staticmethod
    async def check_access(db, guild_id: Union[str, int], feature_name: str) -> bool:
        """
        Check if a guild has access to a specific premium feature.
        
        Args:
            db: MongoDB database connection
            guild_id: Discord guild ID
            feature_name: Name of the feature to check
            
        Returns:
            bool: True if guild is not None has access to the feature
        """
        # Create cache key
        str_guild_id = str(guild_id)
        cache_key = f"premium_feature_access:{str_guild_id}:{feature_name}"
        
        # Check cache first (optimization)
        if cache_key in _feature_check_cache:
            return _feature_check_cache[cache_key]
        
        # Perform actual check
        try:
            guild = await PremiumGuild.get_by_guild_id(db, str_guild_id)
            if guild is not None:
                has_access = guild.has_feature_access(feature_name)
                # Update cache
                _feature_check_cache[cache_key] = has_access
                return has_access
            
            # Guild not found, default to no access
            return False
            
        except Exception as e:
            logger.error(f"Error checking feature access: {e}")
            return False
    
    @staticmethod
    async def get_guild_tier(db, guild_id: Union[str, int]) -> int:
        """
        Get the premium tier for a guild.
        
        Args:
            db: MongoDB database connection
            guild_id: Discord guild ID
            
        Returns:
            int: Premium tier (0-4)
        """
        # Create cache key
        str_guild_id = str(guild_id)
        
        # Check cache first (optimization)
        if str_guild_id in _guild_tier_cache:
            return _guild_tier_cache[str_guild_id]
        
        # Perform actual check
        try:
            guild = await PremiumGuild.get_by_guild_id(db, str_guild_id)
            if guild is not None:
                tier = guild.check_premium_status()
                # Update cache
                _guild_tier_cache[str_guild_id] = tier
                return tier
            
            # Guild not found, default to free tier
            return 0
            
        except Exception as e:
            logger.error(f"Error getting guild tier: {e}")
            return 0
    
    @staticmethod
    async def get_guild_feature_list(db, guild_id: Union[str, int]) -> Dict[str, bool]:
        """
        Get a list of all features and their access status for a guild.
        
        Args:
            db: MongoDB database connection
            guild_id: Discord guild ID
            
        Returns:
            Dict[str, bool]: Dictionary of feature names and access status
        """
        # Get guild
        str_guild_id = str(guild_id)
        guild = await PremiumGuild.get_by_guild_id(db, str_guild_id)
        
        # Initialize result with all features set to False
        result = {}
        
        # If guild not found, return all features as False
        if guild is None:
            # Collect all unique features from all tiers
            all_features = set()
            for tier in range(5):  # Tiers 0-4
                all_features.update(get_tier_features(tier))
                
            # Set all features to False
            for feature in all_features:
                result[feature] = False
                
            return result
        
        # Get current tier and features
        tier = guild.check_premium_status()
        
        # Collect all features from all tiers and mark them as accessible based on current tier
        for check_tier in range(5):  # Tiers 0-4
            for feature in get_tier_features(check_tier):
                # A feature is accessible if its tier is less than or equal to the guild's tier
                result[feature] = check_tier <= tier
        
        return result