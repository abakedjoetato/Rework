"""
Discord Library Adapter for Py-Cord to Discord.py Compatibility

This adapter makes the bot compatible with either py-cord or discord.py
by providing a compatibility layer that handles the differences between
the two libraries.
"""
import os
import sys
import logging
import importlib
from typing import Optional, Any, Dict, List, Tuple, Union

# Configure logger
logger = logging.getLogger(__name__)

# Detect which Discord library is installed and available
def detect_discord_library():
    """Detect which Discord library is available and usable"""
    library_info = {
        "name": None,
        "version": None,
        "has_app_commands": False,
        "has_bridge": False,
        "supports_slash_commands": False
    }
    
    try:
        import discord
        library_info["name"] = "discord"
        library_info["version"] = getattr(discord, "__version__", "unknown")
        
        # Check for bridge module (py-cord specific)
        library_info["has_bridge"] = hasattr(discord, "bridge")
        
        # Check for app_commands (Discord.py 2.0+)
        try:
            from discord import app_commands
            library_info["has_app_commands"] = True
        except (ImportError, AttributeError):
            pass
            
        # Check for slash commands in commands module
        try:
            from discord.ext.commands import slash_command
            library_info["supports_slash_commands"] = True
        except (ImportError, AttributeError):
            # py-cord has it in a different location
            try:
                from discord.commands import slash_command
                library_info["supports_slash_commands"] = True  
            except (ImportError, AttributeError):
                pass
        
        logger.info(f"Detected Discord library: {library_info['name']} v{library_info['version']}")
        
        # Try to be more specific about which library it is
        if isinstance(library_info, dict) and library_info["has_bridge"]:
            logger.info("Detected py-cord (has bridge module)")
            library_info["name"] = "py-cord"
        elif isinstance(library_info, dict) and library_info["has_app_commands"]:
            logger.info("Detected discord.py 2.0+ (has app_commands module)")
            library_info["name"] = "discord.py"
        
        return library_info
        
    except ImportError:
        logger.error("No Discord library found! Neither discord.py nor py-cord is installed.")
        return library_info

# Get library information
DISCORD_LIBRARY = detect_discord_library()

# Import appropriate modules based on library
def get_discord_imports():
    """Get the appropriate Discord imports based on detected library"""
    imports = {}
    
    try:
        import discord
        imports["discord"] = discord
        
        try:
            from discord.ext import commands
            imports["commands"] = commands
        except ImportError:
            logger.error("Failed to import discord.ext.commands")
        
        # Import app_commands if available
        try:
            if isinstance(DISCORD_LIBRARY, dict) and DISCORD_LIBRARY["has_app_commands"]:
                from discord import app_commands
                imports["app_commands"] = app_commands
            elif isinstance(DISCORD_LIBRARY, dict) and DISCORD_LIBRARY["has_bridge"]:
                # py-cord has integrated app commands
                try:
                    from discord.commands import SlashCommandGroup, slash_command
                    imports["slash_command"] = slash_command
                    imports["SlashCommandGroup"] = SlashCommandGroup
                except ImportError:
                    logger.error("Failed to import discord.commands in py-cord")
        except ImportError:
            logger.error("Failed to import app_commands")
        
        # Bridge for py-cord
        if isinstance(DISCORD_LIBRARY, dict) and DISCORD_LIBRARY["has_bridge"]:
            try:
                from discord import bridge
                imports["bridge"] = bridge
            except ImportError:
                logger.error("Discord bridge module detection failed")
        
    except ImportError:
        logger.critical("Failed to import Discord library")
    
    return imports

# Get Discord imports
discord_imports = get_discord_imports()

# Create alias for common imports
discord = discord_imports.get("discord")
commands = discord_imports.get("commands")
app_commands = discord_imports.get("app_commands")
bridge = discord_imports.get("bridge")
slash_command = discord_imports.get("slash_command")
SlashCommandGroup = discord_imports.get("SlashCommandGroup")

# Function to create a slash command that works with either library
def create_slash_command(*args, **kwargs):
    """Create a slash command compatible with either library"""
    if isinstance(DISCORD_LIBRARY, dict) and DISCORD_LIBRARY["name"] == "py-cord":
        if slash_command is not None:
            return slash_command(*args, **kwargs)
        else:
            logger.error("slash_command not available in py-cord")
            # Fallback to basic command
            return commands.command(*args, **kwargs)
    else:
        # discord.py
        if app_commands is not None:
            # Newer discord.py with app_commands
            return commands.hybrid_command(*args, **kwargs)
        else:
            # Older discord.py without app_commands
            return commands.command(*args, **kwargs)

# Export library info for diagnostic purposes
def get_library_info():
    """Return information about the detected Discord library"""
    return DISCORD_LIBRARY