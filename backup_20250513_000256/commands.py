"""
Utility functions for command parsing and handling.
"""
import logging
import re
import discord

logger = logging.getLogger(__name__)

def parse_coord_args(args):
    """
    Parse coordinate arguments from command inputs.
    
    Args:
        args: String arguments to parse
        
    Returns:
        Tuple of (x, y) values or None if invalid
    """
    if args is None:
        return None
    
    # If it's a combined argument (like "10,20")
    # Use direct parsing instead of relying on external utilities
    try:
        if ',' in args:
            x, y = map(int, args.split(',', 1))
            return (x, y)
    except (ValueError, TypeError):
        pass
    
    # If it's separate arguments
    parts = args.replace(',', ' ').split()
    if len(parts) >= 2:
        try:
            x = int(parts[0])
            y = int(parts[1])
            return (x, y)
        except ValueError:
            return None
    
    return None

def parse_color_arg(arg):
    """
    Parse and validate a color argument.
    
    Args:
        arg: Color argument string
        
    Returns:
        Validated color string or None if invalid
    """
    if arg is None:
        return None
    
    try:
        # Clean the input
        color = arg.strip().upper()
        
        # Add # if missing for hex colors (6 digits)
        if len(color) == 6 and all(c in '0123456789ABCDEF' for c in color):
            color = f"#{color}"
        
        # Short form hex color (3 digits)
        elif len(color) == 3 and all(c in '0123456789ABCDEF' for c in color):
            color = f"#{color}"
        
        # Validate the color (basic hex validation)
        if color.startswith('#') and len(color) in (4, 7) and all(c in '0123456789ABCDEF' for c in color[1:]):
            return color
        
        # Color name mapping (limited set)
        color_map = {
            "RED": "#FF0000",
            "GREEN": "#00FF00",
            "BLUE": "#0000FF",
            "YELLOW": "#FFFF00",
            "PURPLE": "#800080",
            "ORANGE": "#FFA500",
            "BLACK": "#000000",
            "WHITE": "#FFFFFF",
            "GRAY": "#808080",
            "PINK": "#FFC0CB"
        }
        
        mapped_color = color_map.get(color)
        if mapped_color:
            return mapped_color
            
        # If we reach here, color is invalid
        return None
    except Exception as e:
        logger.error(f"Error parsing color argument: {e}")
        return None
