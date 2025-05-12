"""
Autocomplete functions for Discord bot commands.

This module contains standardized autocomplete functions that can be reused
across multiple cogs for consistent user experience.
"""
import logging
from typing import List

import discord
from discord import app_commands

from utils.server_utils import standardize_server_id

logger = logging.getLogger(__name__)

async def server_id_autocomplete(interaction: discord.Interaction, current: str):
    """
    Autocomplete for server selection

    Args:
        interaction: Discord interaction
        current: Current input value

    Returns:
        List of discord.app_commands.Choice options
    """
    # Get database connection
    from utils.database import get_db
    try:
        db = await get_db()
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return []

    # Get the guild ID
    guild_id = str(interaction.guild_id) if interaction.guild_id else None
    if guild_id is None:
        return []

    # Get server options from guild configuration
    server_options = await get_server_selection(interaction, guild_id, db)

    # Standardize all server IDs for consistency with command processing
    standardized_options = []
    for sid, name in server_options:
        # Ensure server ID is standardized the same way as in Server.get_by_id
        std_sid = standardize_server_id(str(sid) if sid is not None else "")
        if std_sid is not None:  # Only add if standardization succeeded
            standardized_options.append((std_sid, name))

    # Filter by current input
    if current is not None:
        standardized_options = [
            (sid, name) for sid, name in standardized_options
            if current.lower() in sid.lower() or current.lower() in name.lower()
        ]

    # Return as choices (limited to 25 as per Discord API limits)
    return [
        app_commands.Choice(name=f"{name} ({sid})", value=sid)
        for sid, name in standardized_options[:25]
    ]

# Alias for backward compatibility
server_autocomplete = server_id_autocomplete

import logging
import discord
from discord import app_commands
from discord.ext import commands
from typing import List, Optional

logger = logging.getLogger(__name__)

async def server_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    """Autocomplete for server names/IDs

    Args:
        interaction: The current interaction
        current: The current input value

    Returns:
        List of autocomplete choices
    """
    try:
        # Get bot reference from interaction
        bot = interaction.client

        # Check if guild is not None is available
        if interaction.guild_id is None:
            return [app_commands.Choice(name="Must use in a server", value="")]

        # Try to get servers from database
        guild_data = await bot.db.guilds.find_one({"guild_id": interaction.guild_id})
        if guild_data is None or "servers" not in guild_data:
            return [app_commands.Choice(name="No servers configured for this guild", value="")]

        # Get servers
        servers = guild_data.get("servers", [])
        if servers is None:
            return [app_commands.Choice(name="No servers configured for this guild", value="")]

        # Filter servers by current input
        if current is not None:
            choices = [
                app_commands.Choice(name=server.get("server_name", server.get("server_id", "Unknown")), value=server.get("server_id", ""))
                for server in servers
                if current.lower() in server.get("server_name", "").lower() or current.lower() in server.get("server_id", "").lower()
            ]
        else:
            choices = [
                app_commands.Choice(name=server.get("server_name", server.get("server_id", "Unknown")), value=server.get("server_id", ""))
                for server in servers
            ]

        # Limit to 25 choices
        return choices[:25]

    except Exception as e:
        logger.error(f"Error in server_autocomplete: {e}", exc_info=True)
        return [app_commands.Choice(name=f"Error: {str(e)[:50]}", value="")]



async def get_server_selection(interaction: discord.Interaction, guild_id: str, db):
    """
    Get server selection options for the given guild

    Args:
        interaction: Discord interaction
        guild_id: Discord guild ID
        db: Database connection

    Returns:
        List of (server_id, server_name) tuples
    """
    from models.guild import Guild

    try:
        # Get the guild document
        guild = await Guild.get_by_guild_id(guild_id, db)
        if guild is None:
            logger.warning(f"Guild document not found for ID {guild_id}")
            return []

        # Get main server selection for this guild
        server_options = []

        # First add the default server for this guild
        default_server_id = getattr(guild, 'default_server_id', None)
        default_server_name = getattr(guild, 'default_server_name', 'Default Server')
        if default_server_id is not None:
            server_options.append((default_server_id, default_server_name))

        # Add all configured servers for this guild
        servers = getattr(guild, 'servers', [])
        if servers is not None and isinstance(servers, list):
            for server in servers:
                # Skip if not a dictionary
                if not isinstance(server, dict):
                    continue

                server_id = server.get('server_id')
                server_name = server.get('server_name', 'Unnamed Server')
                # Only add if not already in the list
                if server_id is not None and server_id not in [s[0] for s in server_options]:
                    server_options.append((server_id, server_name))

        # If no servers found directly in guild document, search in server collections
        if server_options is None:
            # Check for servers associated with this guild in the servers collection
            server_docs = await db.servers.find({"guild_id": guild_id}).to_list(length=25)
            for server in server_docs:
                server_id = server.get('server_id')
                server_name = server.get('server_name', 'Unnamed Server')
                # Only add if not already in the list
                if server_id is not None and server_id not in [s[0] for s in server_options]:
                    server_options.append((server_id, server_name))

            # Also check game_servers collection
            game_server_docs = await db.game_servers.find({"guild_id": guild_id}).to_list(length=25)
            for server in game_server_docs:
                server_id = server.get('server_id')
                server_name = server.get('server_name', 'Unnamed Server')
                # Only add if not already in the list
                if server_id is not None and server_id not in [s[0] for s in server_options]:
                    server_options.append((server_id, server_name))

        return server_options

    except Exception as e:
        logger.error(f"Error getting server selection: {e}")
        return []

# Add autocomplete handlers
async def server_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    """
    Autocomplete for server names.

    Args:
        interaction: The discord interaction
        current: The current input string

    Returns:
        List of server name choices
    """
    from models.server import Server

    # Get database from the bot
    db = interaction.client.db
    if db is None:
        return []

    # Query servers for the guild
    guild_id = str(interaction.guild_id) if interaction.guild else None
    if guild_id is None:
        return []

    server_list = await Server.find_by_guild(db, guild_id)

    # Filter by current input
    filtered_servers = [
        server for server in server_list
        if current.lower() in server.name.lower()
    ]

    # Return as choices (limit to 25 as per Discord's limit)
    return [
        app_commands.Choice(name=server.name, value=str(server.id))
        for server in filtered_servers[:25]
    ]