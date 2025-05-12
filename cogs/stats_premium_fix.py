"""
Premium verification fix for stats commands

This script adds a centralized premium verification method to stats cog,
replacing the individual decorators with a consistent check.
"""
import logging
import asyncio
import traceback
from typing import Optional, Dict, Any, Union

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("stats_premium_fix")

async def apply_stats_premium_fix():
    """Apply premium verification fix to stats cog"""
    logger.info("Applying premium verification fix to stats cog...")
    
    try:
        # Import our new premium utils
        from utils import premium_utils
        
        # Add verify_premium method to Stats class
        from cogs.stats import Stats
        
        # Define the verify_premium method
        async def verify_premium(self, guild_id: Union[str, int], subcommand: Optional[str] = None) -> bool:
            """
            Verify premium access for a subcommand
            
            Args:
                guild_id: Discord guild ID
                subcommand: The stats subcommand (server, leaderboard, etc.)
                
            Returns:
                bool: Whether access is granted
            """
            # Standardize guild_id to string
            guild_id_str = str(guild_id)
            
            # Determine feature name based on subcommand
            if subcommand is not None:
                # Use specific subcommand feature name
                feature_name = f"player_stats_premium"
            else:
                # Use generic stats feature
                feature_name = "stats"
                
            logger.info(f"Verifying premium for guild {guild_id_str}, feature: {feature_name}")
            
            try:
                # Use our standardized premium check
                has_access = await premium_utils.verify_premium_for_feature(
                    self.bot.db, guild_id_str, feature_name
                )
                
                # Log the result
                logger.info(f"Premium tier verification for {feature_name}: access={has_access}")
                return has_access
                
            except Exception as e:
                logger.error(f"Error verifying premium: {e}")
                traceback.print_exc()
                # Default to allowing access if there's an error
                return True
        
        # Add the method to the Stats class
        Stats.verify_premium = verify_premium
        logger.info("Added verify_premium method to Stats class")
        
        # Update command methods to use verify_premium
        original_server_stats = Stats.server_stats
        original_leaderboard = Stats.leaderboard
        
        # Replace server_stats implementation
        async def server_stats_wrapper(self, ctx, server_id: str):
            """Wrapped server_stats method with standardized premium check"""
            # Check premium access first
            if not await self.verify_premium(ctx.guild.id, "server"):
                await ctx.send("This command requires premium access. Use `/premium upgrade` for more information.")
                return
                
            # Call original method
            return await original_server_stats(self, ctx, server_id)
            
        # Replace leaderboard implementation
        async def leaderboard_wrapper(self, ctx, server_id: str, stat: str, limit: int = 10):
            """Wrapped leaderboard method with standardized premium check"""
            # Check premium access first
            if not await self.verify_premium(ctx.guild.id, "leaderboard"):
                await ctx.send("This command requires premium access. Use `/premium upgrade` for more information.")
                return
                
            # Call original method
            return await original_leaderboard(self, ctx, server_id, stat, limit)
            
        # Apply the wrappers
        Stats.server_stats = server_stats_wrapper
        Stats.leaderboard = leaderboard_wrapper
        
        logger.info("Updated Stats commands with standard premium checks")
        
        return True
    except Exception as e:
        logger.error(f"Error applying stats premium fix: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    asyncio.run(apply_stats_premium_fix())