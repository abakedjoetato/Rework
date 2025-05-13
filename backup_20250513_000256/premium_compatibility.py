"""
Compatibility layer for transitioning from old premium system to new system.
This module provides seamless integration during the migration period.
"""
import inspect
import logging
import functools
from typing import Optional, Callable, Dict, Any, Union, Tuple

import discord
from discord.ext import commands

from premium_mongodb_models import PremiumGuild, validate_premium_feature

logger = logging.getLogger(__name__)

# Cache for quick lookups during transition period
_guild_cache = {}
_feature_cache = {}

async def get_premium_tier(db, guild_id: str) -> int:
    """
    Get premium tier for a guild with compatibility handling.
    Checks both old and new systems, prioritizing new system.
    
    Args:
        db: MongoDB database connection
        guild_id: Discord guild ID
        
    Returns:
        int: Premium tier (0-4)
    """
    try:
        # Try new system first
        guild = await PremiumGuild.get_by_guild_id(db, guild_id)
        if guild is not None:
            try:
                # Update cache - safely access attributes with proper null checks
                premium_tier = getattr(guild, 'premium_tier', 0)
                premium_expires_at = getattr(guild, 'premium_expires_at', None)
                _guild_cache[guild_id] = {"tier": premium_tier, "expires_at": premium_expires_at}
                
                # Call check_premium_status safely
                if hasattr(guild, 'check_premium_status') and callable(guild.check_premium_status):
                    return guild.check_premium_status()
                else:
                    # Fallback if method doesn't exist
                    return premium_tier
            except Exception as e:
                logger.error(f"Error handling PremiumGuild object: {e}")
                # Continue to fallback approach
            
        # Fall back to old system
        guild_doc = await db.guilds.find_one({"guild_id": guild_id})
        if guild_doc and "premium_tier" in guild_doc:
            tier_raw = guild_doc.get("premium_tier", 0)
            
            # Convert to integer with safe handling
            tier = 0
            try:
                if isinstance(tier_raw, int):
                    tier = tier_raw
                elif isinstance(tier_raw, str) and tier_raw.strip().isdigit():
                    tier = int(tier_raw.strip())
                elif isinstance(tier_raw, float):
                    tier = int(tier_raw)
                else:
                    # Last attempt
                    tier = int(float(str(tier_raw)))
            except (ValueError, TypeError):
                tier = 0
                
            # Ensure tier is in valid range (0-4)
            tier = max(0, min(4, tier))
            
            # Check if premium has expired
            if "premium_expires" in guild_doc and guild_doc["premium_expires"]:
                from datetime import datetime
                if isinstance(guild_doc, dict) and guild_doc["premium_expires"] < datetime.utcnow():
                    tier = 0
            
            # Update cache
            _guild_cache[guild_id] = {"tier": tier, "expires_at": guild_doc.get("premium_expires")}
            return tier
            
        # Default to free tier
        return 0
        
    except Exception as e:
        logger.error(f"Error checking premium tier for guild {guild_id}: {e}")
        return 0


async def check_feature_access(db, guild_id: str, feature_name: str) -> bool:
    """
    Check if a guild has access to a specific premium feature.
    Checks both old and new systems, prioritizing new system.
    
    Args:
        db: MongoDB database connection
        guild_id: Discord guild ID
        feature_name: Name of the feature to check
        
    Returns:
        bool: True if guild has access to the feature
    """
    try:
        # Create cache key
        cache_key = f"premium_access:{guild_id}:{feature_name}"
        
        # Try new system first
        guild = await PremiumGuild.get_by_guild_id(db, guild_id)
        if guild is not None:
            try:
                # Safely access the method with proper null checks
                if hasattr(guild, 'has_feature_access') and callable(guild.has_feature_access):
                    has_access = guild.has_feature_access(feature_name)
                    # Update cache
                    _feature_cache[cache_key] = has_access
                    return has_access
                else:
                    logger.warning(f"PremiumGuild missing has_feature_access method for {guild_id}")
                    # Continue to fallback approach
            except Exception as e:
                logger.error(f"Error checking feature access with PremiumGuild: {e}")
                # Continue to fallback approach
            
        # Fall back to old system
        from utils.premium import has_feature_access
        
        # Get guild from old system
        guild_doc = await db.guilds.find_one({"guild_id": guild_id})
        if guild_doc is not None:
            # Old system uses various formats, so try the most direct access first
            try:
                has_access = await has_feature_access(guild_doc, feature_name)
                # Update cache
                _feature_cache[cache_key] = has_access
                return has_access
            except Exception as e:
                logger.warning(f"Error using old has_feature_access for {feature_name}: {e}")
                
            # Fall back to direct tier comparison
            try:
                from utils.premium import get_minimum_tier_for_feature
                # Get the premium tier for the guild
                tier = await get_premium_tier(db, guild_id)
                tier_int = int(tier) if isinstance(tier, (int, str, float)) else 0
                
                # Get the minimum tier required for the feature
                min_tier = None
                try:
                    if inspect.iscoroutinefunction(get_minimum_tier_for_feature):
                        min_tier = await get_minimum_tier_for_feature(feature_name)
                    else:
                        min_tier = get_minimum_tier_for_feature(feature_name)
                except Exception as e:
                    logger.error(f"Error getting minimum tier for {feature_name}: {e}")
                    min_tier = None
                
                # Convert to int safely
                min_tier_int = int(min_tier) if isinstance(min_tier, (int, str, float)) else 0
                
                # Check if the guild's tier is sufficient
                if min_tier_int is not None:
                    has_access = tier_int >= min_tier_int
                    # Update cache
                    _feature_cache[cache_key] = has_access
                    return has_access
            except Exception as e:
                logger.warning(f"Error in tier comparison for {feature_name}: {e}")
        
        # Default to no access for unknown features/guilds
        return False
        
    except Exception as e:
        logger.error(f"Error checking feature access for guild {guild_id}, feature {feature_name}: {e}")
        return False


def requires_premium_feature_compat(feature_name: str):
    """
    Compatibility decorator for commands that require a premium feature.
    Works with both old and new premium systems during transition.
    
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
                    
                    # Check feature access
                    has_access = await check_feature_access(db, str(guild_id), feature_name)
                    
                    # If no access, send error message
                    if not has_access:
                        try:
                            # Try to get a detailed error message from new system
                            guild = await PremiumGuild.get_by_guild_id(db, str(guild_id))
                            
                            # Check if validate_premium_feature exists and is callable
                            if guild is not None and hasattr(guild, "validate_premium_feature") and inspect.iscoroutinefunction(validate_premium_feature):
                                _, error_message = await validate_premium_feature(db, guild, feature_name)
                                
                                if error_message is not None:
                                    await ctx.send(error_message)
                                    return None
                        except Exception as e:
                            logger.error(f"Error validating premium feature access: {e}")
                        
                        # Generic message if detailed error not available or an exception occurred
                        await ctx.send("This command requires premium tier. Use `/premium upgrade` for more information.")
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
                    
                    # Check feature access
                    has_access = await check_feature_access(db, str(interaction.guild_id), feature_name)
                    
                    # If no access, send error message
                    if not has_access:
                        try:
                            # Try to get a detailed error message from new system
                            guild = await PremiumGuild.get_by_guild_id(db, str(interaction.guild_id))
                            
                            error_message = None
                            # Check if validate_premium_feature exists and is callable
                            if guild is not None and hasattr(guild, "validate_premium_feature") and inspect.iscoroutinefunction(validate_premium_feature):
                                _, error_message = await validate_premium_feature(db, guild, feature_name)
                            
                            # Send specific error message if available
                            if error_message is not None:
                                try:
                                    if not interaction.response.is_done():
                                        await interaction.response.send_message(error_message, ephemeral=True)
                                    else:
                                        await interaction.followup.send(error_message, ephemeral=True)
                                    return None
                                except Exception as e:
                                    logger.error(f"Error sending premium access message: {e}")
                        except Exception as e:
                            logger.error(f"Error validating premium feature access: {e}")
                        
                        # Generic message as fallback
                        try:
                            if not interaction.response.is_done():
                                await interaction.response.send_message(
                                    "This command requires premium tier. Use `/premium upgrade` for more information.",
                                    ephemeral=True
                                )
                            else:
                                await interaction.followup.send(
                                    "This command requires premium tier. Use `/premium upgrade` for more information.",
                                    ephemeral=True
                                )
                        except Exception as e:
                            logger.error(f"Error sending premium generic message: {e}")
                        return None
                    
                    # Access granted, continue with command
                    return await func(*args, **kwargs)
            
            # If we couldn't determine context, just run the command
            logger.warning(f"Could not determine context for premium feature check: {feature_name}")
            return await func(*args, **kwargs)
        
        # Add attribute to help with command documentation
        setattr(wrapper, "premium_feature", feature_name)
        return wrapper
    
    return decorator