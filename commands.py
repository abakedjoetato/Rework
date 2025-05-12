"""
Utility functions for command parsing and handling.
"""
import logging
import utils

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
    combined_coords = utils.normalize_oycoord(args)
    if combined_coords is not None:
        return combined_coords
    
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
    
    # Clean the input
    color = arg.strip().upper()
    
    # Add # if missing for hex colors
    if len(color) == 6 and all(c in '0123456789ABCDEF' for c in color):
        color = discord.Color.blue()
    
    # Short form hex color
    if len(color) == 3 and all(c in '0123456789ABCDEF' for c in color):
        color = f"#{color}"
    
    # Validate the color
    if utils.is_valid_color(color):
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
    
    return color_map.get(color, None)
