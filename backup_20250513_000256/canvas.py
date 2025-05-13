"""
Canvas functionality module.
Handles canvas generation, drawing, and manipulation.
"""
import io
import logging
import discord
from PIL import Image, ImageDraw, ImageFont
from database import get_canvas, get_pixel
import utils

logger = logging.getLogger(__name__)

async def generate_canvas_image(guild_id, show_grid=True, highlight_coords=None):
    """
    Generate an image of the canvas for the specified guild.
    
    Args:
        guild_id: The ID of the guild
        show_grid: Whether to show grid lines
        highlight_coords: Optional (x,y) coordinates to highlight
        
    Returns:
        Discord file object with the canvas image
    """
    # Get canvas data from database
    canvas_data = get_canvas(guild_id)
    width = canvas_data.get("width", 100)
    height = canvas_data.get("height", 100)
    bg_color = canvas_data.get("background_color", "#FFFFFF")
    
    # Canvas rendering settings
    pixel_size = 10  # Size of each pixel square
    grid_color = "#CCCCCC"  # Color of grid lines
    highlight_color = "#FF0000"  # Color to highlight coordinates
    
    # Create canvas image
    img_width = width * pixel_size
    img_height = height * pixel_size
    
    # Create base image with background color
    image = Image.new("RGB", (img_width, img_height), bg_color)
    draw = ImageDraw.Draw(image)
    
    # Draw all pixels from the database
    for x in range(width):
        for y in range(height):
            pixel_data = get_pixel(canvas_data["_id"], x, y)
            if pixel_data and "color" in pixel_data:
                # Draw the pixel
                draw.rectangle(
                    [
                        x * pixel_size, 
                        y * pixel_size, 
                        (x + 1) * pixel_size - 1, 
                        (y + 1) * pixel_size - 1
                    ],
                    fill=pixel_data["color"]
                )
    
    # Draw grid if requested
    if show_grid is not None:
        # Draw vertical lines
        for x in range(0, img_width, pixel_size):
            draw.line([(x, 0), (x, img_height)], fill=grid_color, width=1)
        
        # Draw horizontal lines
        for y in range(0, img_height, pixel_size):
            draw.line([(0, y), (img_width, y)], fill=grid_color, width=1)
    
    # Highlight specific coordinates if provided
    if highlight_coords and len(highlight_coords) == 2:
        x, y = highlight_coords
        if 0 <= x < width and 0 <= y < height:
            # Draw a highlighted border around the pixel
            draw.rectangle(
                [
                    x * pixel_size, 
                    y * pixel_size, 
                    (x + 1) * pixel_size - 1, 
                    (y + 1) * pixel_size - 1
                ],
                outline=highlight_color, 
                width=2
            )
    
    # Add coordinates labels (every 10 units)
    try:
        font = ImageFont.load_default()
        # X-axis labels
        for x in range(0, width, 10):
            draw.text(
                (x * pixel_size + 2, 2), 
                str(x), 
                fill="#000000", 
                font=font
            )
        
        # Y-axis labels
        for y in range(0, height, 10):
            draw.text(
                (2, y * pixel_size + 2), 
                str(y), 
                fill="#000000", 
                font=font
            )
    except Exception as e:
        logger.error(f"Error generating canvas: {e}")
    
    # Convert image to bytes for Discord
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)
    
    # Create Discord file
    file = discord.File(buffer, filename="canvas.png")
    
    return file

def is_valid_color(color):
    """Check if a string is a valid color code."""
    # Check if it's a valid hex color code
    if color.startswith('#'):
        # Check if it's a valid 3 or 6 digit hex code
        if len(color) == 4 or len(color) == 7:
            try:
                # Convert to RGB to validate
                utils.hex_to_rgb(color)
                return True
            except ValueError:
                return False
    
    # Add other color format validations if needed
    return False

def is_valid_coordinates(x, y, canvas_data):
    """Check if coordinates are within canvas bounds."""
    width = canvas_data.get("width", 100)
    height = canvas_data.get("height", 100)
    
    return 0 <= x < width and 0 <= y < height
