"""
Utility functions for the Mukti Discord bot.
"""
import re
import logging

logger = logging.getLogger(__name__)

def hex_to_rgb(hex_color):
    """
    Convert hex color code to RGB tuple.
    
    Args:
        hex_color: Hex color code like "#RRGGBB" or "#RGB"
        
    Returns:
        Tuple of (R, G, B) values
    
    Raises:
        ValueError: If input is not a valid hex color
    """
    hex_color = hex_color.lstrip('#')
    
    if len(hex_color) == 3:
        # Convert 3-digit hex to 6-digit
        hex_color = ''.join(c + c for c in hex_color)
    
    if len(hex_color) != 6:
        raise ValueError(f"Invalid hex color code: {hex_color}. Must be 6 characters long.")
    
    # Convert to RGB
    try:
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    except ValueError:
        raise ValueError(f"Invalid hex color value: {hex_color}")

def normalize_oycoord(coord_str):
    """
    Normalize an OyCoord string to standard (x,y) format.
    OyCoord is an alternative coordinate notation used by the Mukti guild.
    
    Args:
        coord_str: String containing coordinates in OyCoord format
        
    Returns:
        Tuple of (x, y) values or None if invalid
    """
    # Remove whitespace and lowercase
    coord_str = coord_str.strip().lower()
    
    # Format: oyc(x,y) or oyc:x,y or x,y
    patterns = [
        r'oyc\((\d+),(\d+)\)',  # oyc(x,y)
        r'oyc:(\d+),(\d+)',     # oyc:x,y
        r'(\d+),(\d+)'          # x,y
    ]
    
    for pattern in patterns:
        match = re.match(pattern, coord_str)
        if match is not None:
            try:
                x = int(match.group(1))
                y = int(match.group(2))
                return (x, y)
            except ValueError:
                return None
    
    return None

def format_oycoord(x, y):
    """
    Format coordinates as OyCoord string.
    
    Args:
        x: X coordinate
        y: Y coordinate
        
    Returns:
        Formatted OyCoord string
    """
    return f"oyc({x},{y})"

def validate_mukti_guild(ctx):
    """
    Validate that a command is being used in the Mukti guild.
    
    Args:
        ctx: Command context
        
    Returns:
        True if in Mukti guild, False otherwise
    """
    from config import MUKTI_GUILD_ID
    
    # If no specific guild ID is configured, allow all guilds
    if MUKTI_GUILD_ID is None:
        return True
    
    return str(ctx.guild.id) == str(MUKTI_GUILD_ID)

def get_embed_color():
    """Get the standard color for Discord embeds."""
    return 0x3498db  # Blue color
