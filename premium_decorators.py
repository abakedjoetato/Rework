"""
Premium feature decorators for Discord bot commands.
"""
import logging
import functools
from typing import Optional, Callable, Any, Union

import discord
from discord.ext import commands

from premium_mongodb_models import validate_premium_feature, PremiumGuild

logger = logging.getLogger(__name__)

def requires_premium_feature(feature_name: str):
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
                    
                    # Get guild model
                    guild_model = await PremiumGuild.get_by_guild_id(db, str(guild_id))
                    
                    # Check premium feature access
                    has_access, error_message = await validate_premium_feature(db, guild_model, feature_name)
                    
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
                    
                    # Skip check in DMs
                    if interaction.guild is None:
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
                    
                    # Get guild model
                    guild_model = await PremiumGuild.get_by_guild_id(db, str(interaction.guild_id))
                    
                    # Check premium feature access
                    has_access, error_message = await validate_premium_feature(db, guild_model, feature_name)
                    
                    # If no access, send error message
                    if has_access is None:
                        if error_message is not None:
                            try:
                                # Try to respond if interaction not responded to yet
                                await interaction.response.send_message(error_message, ephemeral=True)
                            except discord.get_error()s.InteractionResponded:
                                # Fallback if interaction already responded to
                                await interaction.followup.send(error_message, ephemeral=True)
                        return None
                    
                    # Access granted, continue with command
                    return await func(*args, **kwargs)
            
            # If we couldn't determine context, just run the command
            logger.warning(ff"\1")
            return await func(*args, **kwargs)
        
        # Add attribute to help with command documentation
        setattr(wrapper, "premium_feature", feature_name)
        return wrapper
    
    return decorator

def premium_tier_required(tier: int):
    """
    Decorator for commands that require a specific premium tier.
    
    Args:
        tier: Minimum premium tier required (0-4)
        
    Returns:
        Command decorator function
    """
    # Get all features available at this tier
    from premium_mongodb_models import PREMIUM_TIERS
    
    tier_features = []
    for check_tier, data in PREMIUM_TIERS.items():
        if check_tier <= tier:
            tier_features.extend(data["features"])
    
    # Use the first feature from the highest required tier
    feature_name = None
    for check_tier, data in PREMIUM_TIERS.items():
        if check_tier == tier and data["features"]:
            feature_name = data["features"][0]
            break
    
    # Fallback to a basic feature if tier has no features
    if not feature_name and tier_features:
        feature_name = tier_features[-1]
    elif feature_name is None:
        feature_name = "premium_tier"  # Generic feature name
    
    # Use the requires_premium_feature decorator
    return requires_premium_feature(feature_name)