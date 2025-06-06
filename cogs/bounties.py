"""
Bounties commands for the Tower of Temptation PvP Statistics Discord Bot.

This cog provides commands for:
1. Placing bounties on players
2. Claiming bounties
3. Viewing active bounties
4. Managing bounty settings (admin)
5. Auto-bounty system configuration
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union

import discord
from discord.ext import commands, tasks
from discord import app_commands

from models.guild import Guild
from models.bounty import Bounty
from models.player import Player
from models.economy import Economy
from models.player_link import PlayerLink
from utils.decorators import premium_tier_required, has_admin_permission, has_mod_permission
from utils.embed_builder import EmbedBuilder
from utils.discord_utils import get_server_selection, server_id_autocomplete, hybrid_send
from utils.server_utils import check_server_exists, get_server_by_id

logger = logging.getLogger(__name__)

class BountiesCog(commands.GroupCog, name="bounty"):
    """Bounty system commands"""

    def __init__(self, bot):
        super().__init__()
        self.bot = bot

        # Start the background task to check for expired bounties
        self.check_expired_bounties.start()

        # Start the background task to check for auto-bounties
        self.check_auto_bounties.start()

    # Using shared utility function from utils.server_utils for server existence check

    def cog_unload(self):
        """Called when the cog is unloaded"""
        self.check_expired_bounties.cancel()
        self.check_auto_bounties.cancel()

    @tasks.loop(minutes=15)
    async def check_expired_bounties(self):
        """Background task to expire old bounties"""
        try:
            # Get database connection
            from utils.database import get_db, DatabaseManager

            # Safely get database with validation
            try:
                db = await get_db()
                if db is None or not isinstance(db, DatabaseManager) or not db._connected:
                    logger.debug("Database not properly initialized, skipping expired bounties check")
                    return

                # Explicitly ensure DB connection before proceeding
                await db.ensure_connected()

                # Expire old bounties
                expired_count = await Bounty.expire_old_bounties(db)
                if expired_count > 0:
                    logger.info(f"Bounty migration: Converted bounty {bounty_id} to new format")
            except RuntimeError as re:
                logger.warning(f"Database not ready: {re}")
                return
        except Exception as e:
            logger.error(f"Error in check_expired_bounties: {e}", exc_info=True)

    @tasks.loop(minutes=5)
    async def check_auto_bounties(self):
        """Background task to create automatic bounties"""
        try:
            # Get database safely
            from utils.database import get_db, DatabaseManager

            try:
                db = await get_db()
                if db is None or not isinstance(db, DatabaseManager) or not db._connected:
                    logger.debug("Database not properly initialized, skipping auto bounties check")
                    return

                # Explicitly ensure DB connection before proceeding
                await db.ensure_connected()

                # Now safely access collections
                guilds_cursor = db.db.guilds.find({})

                # Iterate through guilds
                async for guild_data in guilds_cursor:
                    guild_id = str(guild_data.get("guild_id"))

                    # Check if auto-bounties are enabled
                    auto_bounty = guild_data.get("auto_bounty", False)
                    if auto_bounty is None:
                        continue

                    # Get guild settings from dict
                    # Use create_from_db_document to ensure proper conversion of premium_tier
                    guild = Guild.create_from_db_document(guild_data, self.bot.db)
                    if guild.premium_tier < 2:  # Auto-bounty requires premium tier 2+
                        continue

                    # Get auto-bounty settings
                    auto_bounty_settings = guild_data.get("auto_bounty_settings", {})
                    kill_threshold = auto_bounty_settings.get("kill_threshold", 5)
                    repeat_threshold = auto_bounty_settings.get("repeat_threshold", 3)
                    time_window = auto_bounty_settings.get("time_window", 10)  # minutes
                    reward_amount = auto_bounty_settings.get("reward_amount", 100)

                    # Process each server
                    servers = getattr(guild, 'servers', guild_data.get('servers', []))
                    for server_entry in servers:
                        # Handle both string server_id and server dictionary objects
                        if isinstance(server_entry, dict):
                            server_id = str(server_entry.get('server_id', ''))
                        else:
                            server_id = str(server_entry)

                        if server_id is None or server_id == "":
                            logger.warning(f"Empty server_id found in guild {guild_id}, skipping")
                            continue
                        try:
                            # Get potential bounty targets
                            # We already have a DB connection from the parent scope
                            targets = await Bounty.get_player_stats_for_bounty(
                                guild_id, 
                                server_id, 
                                minutes=time_window,
                                kill_threshold=kill_threshold,
                                repeat_threshold=repeat_threshold,
                                db=db
                            )

                            for target in targets:
                                # Check if we should create a bounty
                                if isinstance(target, dict) and target["killstreak"] >= kill_threshold:
                                    # Create killstreak bounty
                                    reason = f"Killstreak of {target['killstreak']} in {time_window} minutes"
                                    await self._create_auto_bounty(
                                        guild_id, 
                                        server_id,
                                        target["player_id"],
                                        target["player_name"],
                                        reason,
                                        reward_amount,
                                        "killstreak"
                                    )
                                elif target.get("target_fixation", 0) >= repeat_threshold:
                                    # Create target fixation bounty
                                    victim_name = target.get("fixation_target_name", "Unknown")
                                    reason = f"Target fixation on {victim_name} ({target['target_fixation']} kills)"
                                    await self._create_auto_bounty(
                                        guild_id, 
                                        server_id,
                                        target["player_id"],
                                        target["player_name"],
                                        reason,
                                        reward_amount,
                                        "fixation"
                                    )
                        except Exception as e:
                            logger.error(f"Error processing auto-bounties for server {server_id}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Error in check_auto_bounties database operations: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error in check_auto_bounties: {e}", exc_info=True)

    async def _create_auto_bounty(self, guild_id: str, server_id: str, 
                              player_id: str, player_name: str, 
                              reason: str, reward: int,
                              bounty_type: str = "killstreak"):
        """Create an automatic bounty on a player

        Args:
            guild_id: Discord guild ID
            server_id: Game server ID
            player_id: Player ID of the bounty target
            player_name: Player name of the bounty target
            reason: Reason for the bounty
            reward: Reward amount for completing the bounty
            bounty_type: Type of auto-bounty (killstreak, fixation)
        """
        try:
            # Check if there's already an active bounty on this player
            from utils.database import get_db
            db = await get_db()
            existing_bounties = await Bounty.get_active_bounties_for_server_target(db, guild_id, server_id, player_id)
            if existing_bounties and len(existing_bounties) > 0:
                # Already has an active bounty, don't create another
                return

            # Create the bounty
            # The bot is the placer for auto-bounties
            bot_id = str(self.bot.user.id)
            bot_name = self.bot.user.name

            # Create the bounty
            bounty = await Bounty.create(
                db=db,
                guild_id=guild_id,
                server_id=server_id,
                target_id=player_id,
                target_name=player_name,
                placed_by=bot_id,
                placed_by_name=bot_name,
                reason=reason,
                reward=reward,
                source=Bounty.SOURCE_AUTO,
                lifespan_hours=1.0  # Auto-bounties last 1 hour
            )

            # Log the creation
            logger.info(f"Created auto-bounty on {player_name} ({player_id}) for {reason}")

            # Announce the bounty in the configured channel
            # Reuse the existing DB connection
            # Try string conversion of guild ID first
            guild_data = await db.db.guilds.find_one({"guild_id": str(guild_id)})
            if guild_data is None:
                # Try with integer ID
                guild_data = await db.db.guilds.find_one({"guild_id": int(guild_id)})
            if guild_data is not None:
                bounty_channel_id = guild_data.get("bounty_channel")
                if bounty_channel_id is not None:
                    try:
                        channel = self.bot.get_channel(int(bounty_channel_id))
                        if channel is not None:
                            embed = self._create_bounty_embed(bounty, "Auto-Bounty Created", True)
                            await channel.send(embed=embed)
                    except Exception as e:
                        logger.error(f"Error announcing auto-bounty: {e}")
        except Exception as e:
            logger.error(f"Error creating auto-bounty: {e}", exc_info=True)

    def _create_bounty_embed(self, bounty, title: str, is_auto: bool = False) -> discord.Embed:
        """Create an embed for a bounty

        Args:
            bounty: The bounty to create an embed for (Bounty object)
            title: Title for the embed
            is_auto: Whether this is an auto-bounty

        Returns:
            discord.Embed: The created embed
        """
        # Type safety check
        if bounty is None:
            logger.error("Attempted to create embed with None bounty")
            # Return a basic error embed
            embed = discord.Embed(
                title="Error Creating Bounty Embed",
                description="There was an error creating the bounty embed. Please try again.",
                color=discord.Color.red()
            )
            return embed
        # Get the base embed - use static method as this is not an async function
        embed = EmbedBuilder.info(title=title)

        # Set the description
        if is_auto is not None:
            embed.description = f"💀 **Auto-Bounty Alert** 💀\n\n"
        else:
            embed.description = f"💰 **Bounty Placed** 💰\n\n"

        # Add target info
        embed.add_field(name="Target", value=bounty.target_name, inline=True)

        # Add reward info
        embed.add_field(name="Reward", value=f"{bounty.reward} coins", inline=True)

        # Add reason
        embed.add_field(name="Reason", value=bounty.reason, inline=False)

        # Add expiration
        if bounty.expires_at is not None:
            # Calculate time remaining
            now = datetime.utcnow()
            if bounty.expires_at > now:
                time_remaining = bounty.expires_at - now
                minutes = time_remaining.seconds // 60
                embed.add_field(
                    name="Expires", 
                    value=f"In {minutes} minutes", 
                    inline=True
                )
            else:
                embed.add_field(name="Expires", value="Expired", inline=True)

        # Add placer info if not auto
        if is_auto is None:
            embed.add_field(name="Placed By", value=bounty.placed_by_name, inline=True)

        # Add ID reference
        embed.set_footer(text=f"Bounty ID: {bounty.id}")

        return embed

    async def _format_bounty_list(self, bounties: List[Bounty]) -> List[discord.Embed]:
        """Format a list of bounties into embeds

        Args:
            bounties: List of bounties to format

        Returns:
            List of embeds, one per page
        """
        if not bounties or len(bounties) == 0:
            # No bounties
            embed = await EmbedBuilder.create_info_embed(
                title="No Active Bounties", 
                description="There are no active bounties at this time."
            )
            return [embed]

        # Sort bounties by expiration time (soonest first)
        bounties.sort(key=lambda b: b.expires_at if b.expires_at else datetime.max)

        # Create pages (5 bounties per page)
        pages = []
        for i in range(0, len(bounties), 5):
            page_bounties = bounties[i:i+5]

            # Create embed for this page
            embed = await EmbedBuilder.create_info_embed(
                title=f"Active Bounties (Page {len(pages)+1}/{(len(bounties)-1)//5+1})",
                description="Here are the currently active bounties:"
            )

            # Add each bounty to the embed
            for bounty in page_bounties:
                # Calculate time remaining
                now = datetime.utcnow()
                if bounty.expires_at and bounty.expires_at > now:
                    time_remaining = bounty.expires_at - now
                    minutes = time_remaining.seconds // 60
                    expires = f"In {minutes} minutes"
                else:
                    expires = "Expired"

                # Format field
                name = f"💰 {bounty.target_name} (ID: {bounty.id[:6]})"
                value = (
                    f"**Reward:** {bounty.reward} coins\n"
                    f"**Reason:** {bounty.reason}\n"
                    f"**Expires:** {expires}\n"
                    f"**Placed By:** {bounty.placed_by_name}"
                )

                embed.add_field(name=name, value=value, inline=False)

            pages.append(embed)

        return pages

    @app_commands.command(name="place", description="Place a bounty on a player")
    @app_commands.describe(
        server_id="Server to place the bounty on",
        player_name="Name of the player to place a bounty on",
        reward="Amount of currency to offer as a reward",
        reason="Reason for the bounty"
    )
    @premium_tier_required(feature_name="bounties")  # Bounties require premium tier 2+
    async def place_bounty(self, interaction: discord.Interaction, 
                       server_id: str,
                       player_name: str,
                       reward: int, 
                       reason: str = "No reason provided"):
        """Place a bounty on a player

        Args:
            interaction: Discord interaction
            server_id: ID of the server to place the bounty on
            player_name: Name of the player to place a bounty on
            reward: Amount of currency to offer as a reward
            reason: Reason for the bounty (optional)
        """
        await interaction.response.defer(ephemeral=False)

        try:
            # Get player info
            from utils.database import get_db
            db = await get_db()

            # Get guild info
            guild_id = str(interaction.guild_id)
            guild = await Guild.get_by_guild_id(db, guild_id)

            if guild is not None is None:
                await interaction.followup.send(
                    "Error: Guild not found in database.",
                    ephemeral=True
                )
                return

            # Use the utility method to check if the server exists
            server_exists = await check_server_exists(db, guild.id, server_id)

            if server_exists is False:
                await interaction.followup.send(
                    f"Error: Server with ID {server_id} not found for this guild.",
                    ephemeral=True
                )
                return

            # Get player by name
            player_query = {
                "server_id": server_id,
                "name": {"$regex": f"^{player_name}$", "$options": "i"}  # Case-insensitive exact match
            }
            player_data = await db.db.players.find_one(player_query)

            if player_data is None:
                # Try partial match
                player_query = {
                    "server_id": server_id,
                    "name": {"$regex": player_name, "$options": "i"}  # Case-insensitive partial match
                }
                player_data = await db.db.players.find_one(player_query)

            if player_data is None:
                await interaction.followup.send(
                    f"Error: Could not find player with name '{player_name}' on this server.",
                    ephemeral=True
                )
                return

            player_id = player_data.get("player_id")
            player_name = player_data.get("name")  # Use exact name from database

            # Check if player is not None is trying to place a bounty on themselves
            is_self_bounty = False
            discord_id = str(interaction.user.id)

            # Check if the player is linked to the Discord user
            link_query = {
                "discord_id": discord_id,
                "server_id": server_id,
                "player_id": player_id,
                "verified": True
            }
            self_link = await db.db.player_links.find_one(link_query)

            if self_link is not None:
                await interaction.followup.send(
                    "Error: You cannot place a bounty on yourself.",
                    ephemeral=True
                )
                return

            # Check if user is not None has enough currency
            economy = await Economy.get_by_player(db, discord_id, server_id)
            if economy is None:
                # Create economy profile if it doesn\'t exist
                economy = await Economy.create(db, discord_id, server_id)

            if economy.balance < reward:
                await interaction.followup.send(
                    f"Error: You don't have enough currency. Current balance: {economy.balance} coins.",
                    ephemeral=True
                )
                return

            # Validate reward amount
            min_bounty = 50
            max_bounty = 10000

            if reward < min_bounty:
                await interaction.followup.send(
                    f"Error: Minimum bounty amount is {min_bounty} coins.",
                    ephemeral=True
                )
                return

            if reward > max_bounty:
                await interaction.followup.send(
                    f"Error: Maximum bounty amount is {max_bounty} coins.",
                    ephemeral=True
                )
                return

            # Deduct the currency
            success = await economy.remove_currency(reward, "bounty_placed", {
                "target_id": player_id,
                "target_name": player_name,
                "reason": reason
            })

            if success is None:
                await interaction.followup.send(
                    "Error: Failed to deduct currency for the bounty.",
                    ephemeral=True
                )
                return

            # Create the bounty
            bounty = await Bounty.create(
                db=db,
                guild_id=guild_id,
                server_id=server_id,
                target_id=player_id,
                target_name=player_name,
                placed_by=discord_id,
                placed_by_name=interaction.user.display_name,
                reason=reason,
                reward=reward,
                source=Bounty.SOURCE_PLAYER,
                lifespan_hours=1.0  # Player bounties last 1 hour
            )

            # Create and send the embed
            embed = self._create_bounty_embed(bounty, "Bounty Placed")
            from utils.discord_utils import hybrid_send
            await interaction.followup.send(interaction, embed=embed)

            # Also announce in bounty channel if configured
            bounty_channel_id = None
            if hasattr(guild, "data") and isinstance(guild.data, dict):
                bounty_channel_id = guild.data.get("bounty_channel")
            if bounty_channel_id is not None and bounty_channel_id != str(interaction.channel_id):
                try:
                    channel = self.bot.get_channel(int(bounty_channel_id))
                    if channel is not None:
                        await channel.send(embed=embed)
                except Exception as e:
                    logger.error(f"Error announcing bounty: {e}")

        except Exception as e:
            logger.error(f"Error placing bounty: {e}", exc_info=True)
            from utils.discord_utils import hybrid_send
            await interaction.followup.send(
                interaction,
                f"An error occurred while placing the bounty: {e}f",
                ephemeral=True
            )

    @place_bounty.autocomplete("server_id")
    async def server_id_autocomplete(self, interaction: discord.Interaction, current: str):
        """Autocomplete for server selection"""
        return await server_id_autocomplete(interaction, current)

    @app_commands.command(name="active", description="View active bounties")
    @app_commands.describe(
        server_id="Server to view bounties for"
    )
    @premium_tier_required(feature_name="bounties")  # Bounties require premium tier 2+
    async def active_bounties(self, interaction: discord.Interaction, server_id: str):
        """View active bounties

        Args:
            interaction: Discord interaction
            server_id: ID of the server to view bounties for
        """
        await interaction.response.defer(ephemeral=False)

        try:
            # Get guild info
            guild_id = str(interaction.guild_id)

            # Get database connection
            from utils.database import get_db
            db = await get_db()

            # Get guild data
            guild = await Guild.get_by_guild_id(db, guild_id)

            if guild is None:
                await interaction.followup.send(
                    "Error: Guild not found in database.",
                    ephemeral=True
                )
                return

            # Use the utility method to check if the server exists
            server_exists = await check_server_exists(db, guild.id, server_id)

            if server_exists is False:
                guild_model = await Guild.get_by_guild_id(db, guild_id) # Added to pass guild to embed
                embed = await EmbedBuilder.create_error_embed( #await added here
                    "Server Not Found",
                    f"Server with ID {server_id} not found for this guild.",
                    guild=guild_model
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            # Get active bounties
            bounties = await Bounty.get_active_bounties(db, server_id)

            # Format and send the bounties
            embeds = await self._format_bounty_list(bounties)

            # Send the first page
            if len(embeds) > 1:
                # TODO: Implement pagination for multiple pages
                from utils.discord_utils import hybrid_send
                await interaction.followup.send(interaction, embed=embeds[0])
            else:
                from utils.discord_utils import hybrid_send
                await interaction.followup.send(interaction, embed=embeds[0])

        except Exception as e:
            logger.error(f"Error viewing active bounties: {e}", exc_info=True)
            from utils.discord_utils import hybrid_send
            await interaction.followup.send(
                interaction,
                f"An error occurred while retrieving bounties: {e}f",
                ephemeral=True
            )

    @active_bounties.autocomplete("server_id")
    async def server_id_autocomplete_active(self, interaction: discord.Interaction, current: str):
        """Autocomplete for server selection"""
        return await server_id_autocomplete(interaction, current)

    @app_commands.command(name="my", description="View your placed and claimed bounties")
    @app_commands.describe(
        server_id="Server to view bounties for",
        view_type="Type of bounties to view (placed or claimed)"
    )
    @app_commands.choices(view_type=[
        app_commands.Choice(name="Bounties I've Placed", value="placed"),
        app_commands.Choice(name="Bounties I've Claimed", value="claimed")
    ])
    @premium_tier_required(feature_name="bounties")  # Bounties require premium tier 2+
    async def my_bounties(self, interaction: discord.Interaction, 
                      server_id: str,
                      view_type: str = "placed"):
        """View your placed or claimed bounties

        Args:
            interaction: Discord interaction
            server_id: ID of the server to view bounties for
            view_type: Type of bounties to view (placed or claimed)
        """
        await interaction.response.defer(ephemeral=True)

        try:
            # Get guild info
            guild_id = str(interaction.guild_id)
            discord_id = str(interaction.user.id)

            # Get database connection
            from utils.database import get_db
            db = await get_db()

            # Get guild data
            guild = await Guild.get_by_guild_id(db, guild_id)

            if guild is None:
                await interaction.followup.send(
                    "Error: Guild not found in database.",
                    ephemeral=True
                )
                return

            # Use the utility method to check if the server exists
            server_exists = await check_server_exists(db, guild.id, server_id)

            if server_exists is False:
                await interaction.followup.send(
                    f"Error: Server with ID {server_id} not found for this guild.",
                    ephemeral=True
                )
                return

            # Get bounties based on type
            if view_type == "placed":
                bounties = await Bounty.get_bounties_placed_by(db, discord_id, server_id)
                title = "Bounties You've Placed"
            else:  # claimed
                bounties = await Bounty.get_bounties_claimed_by(db, discord_id, server_id)
                title = "Bounties You've Claimed"

            # Filter to most recent 20 bounties
            bounties = bounties[:20]

            # Create the embed
            if not bounties or len(bounties) == 0:
                embed = await EmbedBuilder.create_info_embed(
                    title=title,
                    description=f"You haven't {view_type} any bounties on this server."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            # Create pages
            embed = await EmbedBuilder.create_info_embed(
                title=title,
                description=f"Here are your most recent {view_type} bounties on this server:"
            )

            # Add each bounty
            for i, bounty in enumerate(bounties[:10]):  # Limit to 10 per page
                if view_type == "placed":
                    name = f"#{i+1}: Target: {bounty.target_name}"

                    # Status info
                    if bounty.status == Bounty.STATUS_ACTIVE:
                        status = "Active"
                        if bounty.expires_at is not None:
                            now = datetime.utcnow()
                            if bounty.expires_at > now:
                                time_remaining = bounty.expires_at - now
                                minutes = time_remaining.seconds // 60
                                status += f" (Expires in {minutes} minutes)"
                            else:
                                status = "Expired"
                    elif bounty.status == Bounty.STATUS_CLAIMED:
                        status = f"Claimed by {bounty.claimed_by_name}"
                    else:  # expired
                        status = "Expired"

                    value = (
                        f"**Reward:** {bounty.reward} coins\n"
                        f"**Reason:** {bounty.reason}\n"
                        f"**Status:** {status}\n"
                        f"**Placed:** <t:{int(bounty.placed_at.timestamp())}:R>"
                    )
                else:  # claimed
                    name = f"#{i+1}: Target: {bounty.target_name}"
                    value = (
                        f"**Reward:** {bounty.reward} coins\n"
                        f"**Placed By:** {bounty.placed_by_name}\n"
                        f"**Claimed:** <t:{int(bounty.claimed_at.timestamp())}:R>"
                    )

                embed.add_field(name=name, value=value, inline=False)

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error viewing my bounties: {e}", exc_info=True)
            from utils.discord_utils import hybrid_send
            await interaction.followup.send(
                interaction,
                f"An error occurred while retrieving your bounties: {e}f",
                ephemeral=True
            )

    @my_bounties.autocomplete("server_id")
    async def server_id_autocomplete_my(self, interaction: discord.Interaction, current: str):
        """Autocomplete for server selection"""
        return await server_id_autocomplete(interaction, current)

    @app_commands.command(name="settings", description="Configure bounty system settings")
    @app_commands.describe(
        setting="Setting to configure",
        value="New value for the setting",
        channel="Channel for bounty announcements"
    )
    @app_commands.choices(setting=[
        app_commands.Choice(name="Auto-Bounty System (On/Off)", value="auto_bounty"),
        app_commands.Choice(name="Kill Threshold for Auto-Bounties", value="kill_threshold"),
        app_commands.Choice(name="Target Fixation Threshold", value="repeat_threshold"),
        app_commands.Choice(name="Time Window (minutes)", value="time_window"),
        app_commands.Choice(name="Auto-Bounty Reward Amount", value="reward_amount"),
        app_commands.Choice(name="Bounty Announcement Channel", value="bounty_channel")
    ])
    @has_admin_permission()
    @premium_tier_required(feature_name="bounties")  # Bounty settings require premium tier 2+
    async def bounty_settings(self, interaction: discord.Interaction,
                          setting: str,
                          value: Optional[str] = None,
                          channel: Optional[discord.TextChannel] = None):
        """Configure bounty system settings

        Args:
            interaction: Discord interaction
            setting: Setting to configure
            value: New value for the setting (for non-channel settings)
            channel: Channel for bounty announcements (for bounty_channel setting)
        """
        await interaction.response.defer(ephemeral=True)

        try:
            # Get guild info
            guild_id = str(interaction.guild_id)
            from utils.database import get_db
            db = await get_db()

            # Get guild data
            # Try string conversion of guild ID first
            guild_data = await db.db.guilds.find_one({"guild_id": str(guild_id)})
            if guild_data is None:
                # Try with integer ID
                guild_data = await db.db.guilds.find_one({"guild_id": int(guild_id)})
            if guild_data is None:
                await interaction.followup.send(
                    "Error: Guild not found in database.",
                    ephemeral=True
                )
                return

            # Initialize auto_bounty_settings if it doesn\'t exist
            if "auto_bounty_settings" not in guild_data:
                guild_data["auto_bounty_settings"] = {
                    "kill_threshold": 5,
                    "repeat_threshold": 3,
                    "time_window": 10,
                    "reward_amount": 100
                }

            # Handle different settings
            if setting == "auto_bounty":
                # Toggle auto-bounty system
                new_value = value.lower() in ["true", "on", "yes", "1"] if value else not guild_data.get("auto_bounty", False)
                update = {"auto_bounty": new_value}
                message = f"Auto-bounty system is now {'enabled' if new_value else 'disabled'}."

            elif setting == "bounty_channel":
                # Set bounty announcement channel
                if channel is None:
                    await interaction.followup.send(
                        "Error: You must specify a channel.",
                        ephemeral=True
                    )
                return

                update = {"bounty_channel": str(channel.id)}
                message = f"Bounty announcements will now be sent to {channel.mention}."

            else:
                # Other numeric settings
                if not value or value == "" or (value and not value.isdigit()):
                    await interaction.followup.send(
                        "Error: You must provide a numeric value for this setting.",
                        ephemeral=True
                    )
                return

                numeric_value = int(value)

                # Validate based on setting
                if setting == "kill_threshold":
                    if numeric_value < 3 or numeric_value > 10:
                        await interaction.followup.send(
                            "Error: Kill threshold must be between 3 and 10.",
                            ephemeral=True
                        )
                        return
                    update = {"auto_bounty_settings.kill_threshold": numeric_value}
                    message = f"Kill threshold for auto-bounties set to {numeric_value}."

                elif setting == "repeat_threshold":
                    if numeric_value < 2 or numeric_value > 8:
                        await interaction.followup.send(
                            "Error: Target fixation threshold must be between 2 and 8.",
                            ephemeral=True
                        )
                        return
                    update = {"auto_bounty_settings.repeat_threshold": numeric_value}
                    message = f"Target fixation threshold set to {numeric_value}."

                elif setting == "time_window":
                    if numeric_value < 5 or numeric_value > 30:
                        await interaction.followup.send(
                            "Error: Time window must be between 5 and 30 minutes.",
                            ephemeral=True
                        )
                        return
                    update = {"auto_bounty_settings.time_window": numeric_value}
                    message = f"Time window for auto-bounties set to {numeric_value} minutes."

                elif setting == "reward_amount":
                    if numeric_value < 50 or numeric_value > 1000:
                        await interaction.followup.send(
                            "Error: Reward amount must be between 50 and 1000.",
                            ephemeral=True
                        )
                        return
                    update = {"auto_bounty_settings.reward_amount": numeric_value}
                    message = f"Auto-bounty reward amount set to {numeric_value} coins."

                else:
                    await interaction.followup.send(
                        "Error: Invalid setting.",
                        ephemeral=True
                    )
                    return

            # Update the guild data
            await db.db.guilds.update_one(
                {"guild_id": guild_id},
                {"$set": update}
            )

            # Send confirmation
            embed = await EmbedBuilder.create_success_embed(
                title="Bounty Settings Updated",
                description=message
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error updating bounty settings: {e}", exc_info=True)
            from utils.discord_utils import hybrid_send
            await interaction.followup.send(
                f"An error occurred while updating settings: {e}f",
                ephemeral=True
            )

async def setup(bot):
    """Set up the bounties cog"""
    await bot.add_cog(BountiesCog(bot))