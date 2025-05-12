"""
Premium features and management commands for the Discord bot.
This module handles premium tier subscription management, feature access, and status commands.
"""
import logging
import traceback
from datetime import datetime, timedelta
from typing import Optional, Union, Dict, Any, List

import discord
from discord import app_commands
from discord.ext import commands

from premium_mongodb_models import PremiumGuild, PREMIUM_TIERS, validate_premium_feature
from premium_decorators import requires_premium_feature, premium_tier_required

logger = logging.getLogger(__name__)

class NewPremium(commands.Cog):
    """Premium features and management commands"""

    def __init__(self, bot):
        self.bot = bot

    @commands.hybrid_group(name="premium", description="Premium management commands")
    @commands.guild_only()
    async def premium(self, ctx):
        """Premium command group"""
        if ctx.invoked_subcommand is None:
            await ctx.send("Please specify a subcommand.")

    @premium.command(name="status", description="Check premium status")
    async def status(self, ctx):
        """Check guild's premium tier status"""
        try:
            # Get guild model
            guild = await PremiumGuild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            
            # Check current premium status (handles expiration)
            current_tier = guild.check_premium_status()
            
            # Get tier information
            tier_info = PREMIUM_TIERS.get(current_tier, PREMIUM_TIERS[0])
            tier_name = tier_info.get("name", "Unknown")
            max_servers = tier_info.get("max_servers", 0)
            
            # Create embed
            embed = discord.Embed(
                title="Premium Status",
                description=f"Check premium status for a server",
                color=discord.Color.blurple()
            )
            
            # Add tier information
            embed.add_field(
                name="Current Tier",
                value=f"**{tier_name}** (Level {current_tier})",
                inline=True
            )
            
            # Add server limit
            embed.add_field(
                name="Server Limit",
                value=f"{len(guild.servers)}/{max_servers} servers",
                inline=True
            )
            
            # Add expiration if premium
            if current_tier > 0 and guild.premium_expires_at:
                # Calculate days remaining
                days_remaining = (guild.premium_expires_at - datetime.utcnow()).days
                expires_str = f"<t:{int(guild.premium_expires_at.timestamp())}:F>"
                
                embed.add_field(
                    name="Premium Expires",
                    value=f"{expires_str}\n({days_remaining} days remaining)",
                    inline=False
                )
            
            # Add features list
            feature_list = tier_info.get("features", [])
            if feature_list is not None:
                embed.add_field(
                    name="Available Features",
                    value="\n".join([f"✅ {feature}" for feature in feature_list]),
                    inline=False
                )
            
            # Add upgrade information
            if current_tier < 4:  # Not at max tier
                next_tier = current_tier + 1
                next_tier_info = PREMIUM_TIERS.get(next_tier, {})
                next_tier_name = next_tier_info.get("name", "Unknown")
                next_tier_price = next_tier_info.get("price_gbp", 0)
                
                embed.add_field(
                    name="Upgrade Available",
                    value=f"Upgrade to **{next_tier_name}** tier for £{next_tier_price}/month.\nUse `/premium upgrade` to upgrade.",
                    inline=False
                )
            
            # Set footer
            embed.set_footer(text="Premium status as of")
            embed.timestamp = datetime.utcnow()
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error checking premium status: {e}", exc_info=True)
            await ctx.send(f"An error occurred while checking premium status: {e}")

    @premium.command(name="features", description="View premium features")
    async def features(self, ctx, tier: Optional[int] = None):
        """View features available at each premium tier"""
        try:
            # Get guild model
            guild = await PremiumGuild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            
            # Check current premium status
            current_tier = guild.check_premium_status()
            
            # If tier is specified, show that tier's features
            # Otherwise, show current tier's features
            display_tier = tier if tier is not None else current_tier
            display_tier = max(0, min(4, display_tier))  # Ensure tier is in valid range
            
            # Get tier information
            tier_info = PREMIUM_TIERS.get(display_tier, PREMIUM_TIERS[0])
            tier_name = tier_info.get("name", "Unknown")
            
            # Create embed
            embed = discord.Embed(
                title=f"{tier_name} Tier Features",
                description=f"These are the features available at Premium Tier {display_tier}",
                color=discord.Color.blurple()
            )
            
            # Add tier information
            embed.add_field(
                name="Tier Level",
                value=f"**{tier_name}** (Level {display_tier})",
                inline=True
            )
            
            # Add price information
            price = tier_info.get("price_gbp", 0)
            embed.add_field(
                name="Price",
                value=f"£{price}/month" if price > 0 else "Free",
                inline=True
            )
            
            # Add server limit
            embed.add_field(
                name="Server Limit",
                value=f"{tier_info.get('max_servers', 1)} servers",
                inline=True
            )
            
            # Add features list
            feature_list = tier_info.get("features", [])
            if feature_list is not None:
                embed.add_field(
                    name="Available Features",
                    value="\n".join([f"✅ {feature}" for feature in feature_list]),
                    inline=False
                )
            
            # If viewing a different tier, show comparison
            if display_tier != current_tier:
                embed.add_field(
                    name="Your Current Tier",
                    value=f"You are currently on the **{PREMIUM_TIERS.get(current_tier, {}).get('name', 'Unknown')}** tier (Level {current_tier})",
                    inline=False
                )
                
                # Add upgrade/downgrade button logic here if needed
            
            # Set footer
            embed.set_footer(text="Premium features as of")
            embed.timestamp = datetime.utcnow()
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error viewing premium features: {e}", exc_info=True)
            await ctx.send(f"An error occurred while viewing premium features: {e}")

    @premium.command(name="upgrade", description="Request a premium upgrade")
    async def upgrade(self, ctx):
        """Upgrade to a premium tier"""
        try:
            # Get guild model
            guild = await PremiumGuild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            
            # Check current premium status
            current_tier = guild.check_premium_status()
            
            # If already at max tier, show message
            if current_tier >= 4:
                await ctx.send("You are already at the highest premium tier!")
                return
            
            # Get next tier information
            next_tier = current_tier + 1
            next_tier_info = PREMIUM_TIERS.get(next_tier, {})
            next_tier_name = next_tier_info.get("name", "Unknown")
            next_tier_price = next_tier_info.get("price_gbp", 0)
            
            # Create embed
            embed = discord.Embed(
                title="Premium Upgrade",
                description=f"Upgrade to the **{next_tier_name}** tier (Level {next_tier}) for £{next_tier_price}/month.",
                color=discord.Color.blurple()
            )
            
            # Add current tier information
            current_tier_info = PREMIUM_TIERS.get(current_tier, {})
            current_tier_name = current_tier_info.get("name", "Unknown")
            
            embed.add_field(
                name="Current Tier",
                value=f"**{current_tier_name}** (Level {current_tier})",
                inline=True
            )
            
            # Add new tier information
            embed.add_field(
                name="New Tier",
                value=f"**{next_tier_name}** (Level {next_tier})",
                inline=True
            )
            
            # Add price information
            embed.add_field(
                name="Price",
                value=f"£{next_tier_price}/month",
                inline=True
            )
            
            # Add new features list
            current_features = set(current_tier_info.get("features", []))
            next_features = set(next_tier_info.get("features", []))
            new_features = next_features - current_features
            
            if new_features is not None:
                embed.add_field(
                    name="New Features",
                    value="\n".join([f"✅ {feature}" for feature in new_features]),
                    inline=False
                )
            
            # Add payment information
            # In a real implementation, this could link to a payment page
            embed.add_field(
                name="How to Upgrade",
                value="Contact the bot owner to purchase a premium upgrade.\nMention this server's ID in your message.",
                inline=False
            )
            
            # Set footer
            embed.set_footer(text="Premium upgrade information as of")
            embed.timestamp = datetime.utcnow()
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error requesting premium upgrade: {e}", exc_info=True)
            await ctx.send(f"An error occurred while requesting a premium upgrade: {e}")

    @premium.command(name="verify", description="Verify premium feature access")
    async def verify(self, ctx, feature_name: Optional[str] = None):
        """Verify access to premium features"""
        try:
            # Get guild model
            guild = await PremiumGuild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            
            # Check current premium status
            current_tier = guild.check_premium_status()
            
            # Create embed
            embed = discord.Embed(
                title="Premium Feature Verification",
                description="Checking access to premium features",
                color=discord.Color.blurple()
            )
            
            # Add tier information
            tier_info = PREMIUM_TIERS.get(current_tier, {})
            tier_name = tier_info.get("name", "Unknown")
            
            embed.add_field(
                name="Current Tier",
                value=f"**{tier_name}** (Level {current_tier})",
                inline=True
            )
            
            # If specific feature provided, check access
            if feature_name is not None:
                # Test access using the model's direct check
                has_access = guild.has_feature_access(feature_name)
                
                embed.add_field(
                    name=f"Feature Access: {feature_name}",
                    value=f"Result: {'✅ Access Granted' if has_access else '❌ Access Denied'}",
                    inline=False
                )
                
                # Test via central validation function
                access_result, error_msg = await validate_premium_feature(self.bot.db, guild, feature_name)
                embed.add_field(
                    name=f"Validation Result: {feature_name}",
                    value=f"Result: {'✅ Valid' if access_result else '❌ Invalid'}\nMessage: {error_msg if error_msg else 'No error'}",
                    inline=False
                )
            else:
                # List all features with access status
                features_status = []
                for feature, tier_features in PREMIUM_TIERS.items():
                    for feature_name in tier_features.get("features", []):
                        has_access = guild.has_feature_access(feature_name)
                        features_status.append(f"✅ {feature}: Available (Tier {tier})")
                
                embed.add_field(
                    name="Feature Access Status",
                    value="\n".join(features_status) if features_status else "No features to display",
                    inline=False
                )
            
            # Set footer
            embed.set_footer(text="Premium verification as of")
            embed.timestamp = datetime.utcnow()
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error verifying premium access: {e}", exc_info=True)
            await ctx.send(f"An error occurred while verifying premium access: {e}")

    @premium.command(name="testupdate", description="Test premium tier update (Owner Only)")
    @commands.is_owner()
    async def testupdate(self, ctx, tier: int, days: Optional[int] = 30):
        """Test updating the premium tier (Bot Owner only)"""
        try:
            # Get guild model
            guild = await PremiumGuild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            
            if guild is not None is None:
                await ctx.send("Failed to get guild model")
                return
                
            # Log before state
            logger.info(f"TEST: Premium tier before update: {guild.premium_tier}")
            
            # Set expiration date
            expires_at = datetime.utcnow() + timedelta(days=days)
            
            # Update premium tier
            result = await guild.set_premium_tier(tier, expires_at=expires_at, reason="Test update via command")
            
            # Log after state
            logger.info(f"TEST: Premium tier update result: {result}")
            
            # Re-fetch guild to verify
            updated_guild = await PremiumGuild.get_by_guild_id(self.bot.db, str(ctx.guild.id))
            tier_after = updated_guild.premium_tier if updated_guild else 'None'
            logger.info(f"TEST: Premium tier after update: {tier_after}")
            
            # Create embed
            embed = discord.Embed(
                title="Premium Tier Update Test",
                description=f"Premium tier update test completed",
                color=discord.Color.blurple()
            )
            
            embed.add_field(
                name="Results",
                value=f"Update successful: {result}\nBefore: Tier {guild.premium_tier}\nAfter: Tier {tier_after}",
                inline=False
            )
            
            if updated_guild and updated_guild.premium_expires_at:
                embed.add_field(
                    name="Expiration",
                    value=f"<t:{int(updated_guild.premium_expires_at.timestamp())}:F>",
                    inline=False
                )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in test update: {e}", exc_info=True)
            await ctx.send(f"Error: {e}")

    @commands.Cog.listener()
    async def on_ready(self):
        """Handle bot ready event"""
        logger.info("Premium cog ready")


async def setup(bot):
    """Set up the Premium cog"""
    await bot.add_cog(NewPremium(bot))