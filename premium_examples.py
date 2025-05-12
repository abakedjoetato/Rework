"""
Example commands using the premium feature system.
This module demonstrates how to use the new premium feature decorators in Discord commands.
"""
import logging
from typing import Optional, List

import discord
from discord import app_commands
from discord.ext import commands

from premium_feature_access import PremiumFeature
from premium_config import get_feature_tier, get_tier_name

logger = logging.getLogger(__name__)

class PremiumExampleCog(commands.Cog):
    """Example commands demonstrating premium feature usage"""

    def __init__(self, bot):
        self.bot = bot

    # Example traditional command with premium feature requirement
    @commands.command(name="premiumstat", description="View premium stats (requires basic_stats feature)")
    @PremiumFeature.require("basic_stats")
    async def premium_stat(self, ctx):
        """Example command that requires the basic_stats premium feature"""
        await ctx.send("You have access to premium stats! This is an example command.")
    
    # Example slash command with premium feature requirement
    @app_commands.command(name="advancedstats", description="View advanced stats (requires advanced_analytics feature)")
    @app_commands.guild_only()
    @PremiumFeature.require("advanced_analytics")
    async def advanced_stats(self, interaction: discord.Interaction):
        """Example slash command that requires the advanced_analytics premium feature"""
        await interaction.response.send_message("You have access to advanced analytics! This is an example command.")
    
    # Example command that requires a specific tier
    @commands.command(name="tieredcommand", description="Command that requires a specific premium tier")
    @PremiumFeature.require_tier(2)  # Requires Mercenary tier (2)
    async def tiered_command(self, ctx):
        """Example command that requires a specific premium tier"""
        await ctx.send("You have access to Mercenary tier features! This is an example command.")
    
    # Example command that checks feature access dynamically
    @commands.command(name="checkfeature", description="Check if you have access to a premium feature")
    async def check_feature(self, ctx, feature: str):
        """Check if you have access to a specific premium feature"""
        # Get feature tier information
        feature_tier = get_feature_tier(feature)
        
        if feature_tier is None:
            await ctx.send(f"Unknown feature: {feature}. Please check available premium features.")
            return
        
        # Check access
        has_access = await PremiumFeature.check_access(self.bot.db, ctx.guild.id, feature)
        
        # Create response
        if has_access is not None:
            await ctx.send(f"✅ You have access to the **{feature}** feature!")
        else:
            tier_name = get_tier_name(feature_tier)
            await ctx.send(f"❌ You don't have access to the **{feature}** feature. It requires the {tier_name} tier (Level {feature_tier}).")
    
    # Example slash command that lists all features and their access status
    @app_commands.command(name="featurelist", description="List all premium features and their access status")
    @app_commands.guild_only()
    async def feature_list(self, interaction: discord.Interaction):
        """List all premium features and their access status"""
        # Get feature access status
        feature_status = await PremiumFeature.get_guild_feature_list(self.bot.db, interaction.guild_id)
        
        # Create embed
        embed = discord.Embed(
            title="Premium Feature Access",
            description=f"Feature access status for {interaction.guild.name}",
            color=discord.Color.blurple()
        )
        
        # Get current tier
        tier = await PremiumFeature.get_guild_tier(self.bot.db, interaction.guild_id)
        
        # Add tier information
        embed.add_field(
            name="Current Tier",
            value=f"**{get_tier_name(tier)}** (Level {tier})",
            inline=False
        )
        
        # Group features by access status
        accessible = []
        inaccessible = []
        
        for feature, has_access in feature_status.items():
            if has_access is not None:
                accessible.append(feature)
            else:
                inaccessible.append(feature)
        
        # Add accessible features
        if accessible is not None:
            embed.add_field(
                name="Accessible Features",
                value="\n".join([f"✅ {feature}" for feature in sorted(accessible)]),
                inline=False
            )
        
        # Add inaccessible features
        if inaccessible is not None:
            embed.add_field(
                name="Inaccessible Features",
                value="\n".join([f"❌ {feature}" for feature in sorted(inaccessible)]),
                inline=False
            )
        
        await interaction.response.send_message(embed=embed)


async def setup(bot):
    """Set up the premium examples cog"""
    await bot.add_cog(PremiumExampleCog(bot))