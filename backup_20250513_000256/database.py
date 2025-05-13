"""
Database module for MongoDB connection and operations.
Handles all database-related functions for the Discord bot.
"""
import os
import logging
import pymongo
import datetime
from pymongo.errors import ConnectionFailure, ConfigurationError

logger = logging.getLogger(__name__)

# MongoDB client and database references
client = None
db = None

def init_db(max_retries=3, retry_delay=2):
    """Initialize MongoDB connection using credentials from environment variables.
    
    Args:
        max_retries: Maximum number of connection attempts
        retry_delay: Seconds to wait between retries
        
    Returns:
        bool: True if connected successfully, False otherwise
    """
    global client, db
    
    # If already initialized, just return True
    if client is not None and db is not None:
        try:
            # Quick connection test
            client.admin.command('ping')
            logger.info("Database already initialized and connected")
            return True
        except Exception:
            # Connection lost, will reinitialize
            logger.warning("Database was initialized but connection lost, reconnecting...")
            client = None
            db = None
    
    # Get MongoDB connection details from environment
    mongo_uri = os.getenv("MONGODB_URI")
    if not mongo_uri:
        logger.critical("MONGODB_URI environment variable not set")
        return False
    
    db_name = os.getenv("DB_NAME", "mukti_bot")
    logger.info(f"Using database: {db_name}")
    
    # Attempt to connect with retries
    for attempt in range(1, max_retries + 1):
        try:
            # Connect to MongoDB
            logger.info(f"Connecting to MongoDB (attempt {attempt}/{max_retries}): {mongo_uri}")
            client = pymongo.MongoClient(
                mongo_uri,
                serverSelectionTimeoutMS=5000,  # 5 second timeout for server selection
                connectTimeoutMS=5000,          # 5 second timeout for initial connection
                socketTimeoutMS=30000           # 30 second timeout for socket operations
            )
            
            # Verify connection
            client.admin.command('ping')
            logger.info("Connected to MongoDB successfully")
            
            # Set database
            db = client[db_name]
            
            # Create necessary collections and indexes if they don't exist
            if ensure_collections():
                logger.info("Collections and indexes verified")
            else:
                logger.warning("Failed to verify collections and indexes, but connection is established")
            
            return True
            
        except (ConnectionFailure, ConfigurationError) as e:
            logger.error(f"Failed to connect to MongoDB (attempt {attempt}/{max_retries}): {e}")
            
            if attempt < max_retries:
                # Wait before trying again
                logger.info(f"Retrying in {retry_delay} seconds...")
                import time
                time.sleep(retry_delay)
            else:
                logger.critical(f"Failed to connect to MongoDB after {max_retries} attempts")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB (attempt {attempt}/{max_retries}): {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            if attempt < max_retries:
                # Wait before trying again
                logger.info(f"Retrying in {retry_delay} seconds...")
                import time
                time.sleep(retry_delay)
            else:
                logger.critical(f"Failed to connect to MongoDB after {max_retries} attempts due to unexpected error")
                return False
    
    # This line should never be reached if the function is working correctly
    return False

def ensure_collections():
    """Create necessary collections and indexes if they don't exist.
    
    Returns:
        bool: True if all collections and indexes were verified or created successfully
    """
    global db
    
    # Make sure db is initialized
    if db is None:
        logger.error("Database not initialized, cannot ensure collections")
        return False
    
    try:
        # Get existing collections
        existing_collections = db.list_collection_names()
        logger.info(f"Found existing collections: {', '.join(existing_collections) if existing_collections else 'none'}")
        
        # Define collections to ensure with their indexes
        collections_to_ensure = {
            "canvases": [
                {"keys": [("guild_id", pymongo.ASCENDING)], "unique": True}
            ],
            "pixels": [
                {"keys": [
                    ("canvas_id", pymongo.ASCENDING),
                    ("x", pymongo.ASCENDING),
                    ("y", pymongo.ASCENDING)
                ], "unique": False}
            ],
            "users": [
                {"keys": [("user_id", pymongo.ASCENDING)], "unique": True}
            ],
            # Add any other collections and their indexes here
        }
        
        # Create collections and indexes as needed
        for collection_name, indexes in collections_to_ensure.items():
            try:
                # Create collection if it doesn't exist
                if collection_name not in existing_collections:
                    logger.info(f"Creating '{collection_name}' collection")
                    collection = db.create_collection(collection_name)
                    logger.info(f"Created '{collection_name}' collection")
                else:
                    collection = db[collection_name]
                    logger.info(f"Collection '{collection_name}' already exists")
                
                # Create indexes
                for index_config in indexes:
                    try:
                        keys = index_config["keys"]
                        unique = index_config.get("unique", False)
                        index_name = collection.create_index(keys, unique=unique)
                        logger.info(f"Created/verified index '{index_name}' on '{collection_name}'")
                    except Exception as index_error:
                        logger.error(f"Error creating index on '{collection_name}': {index_error}")
                        # Continue with other indexes even if one fails
            
            except Exception as coll_error:
                logger.error(f"Error ensuring collection '{collection_name}': {coll_error}")
                import traceback
                logger.error(traceback.format_exc())
                # Continue with other collections even if one fails
        
        return True
        
    except Exception as e:
        logger.error(f"Error ensuring collections: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def get_canvas(guild_id):
    """Get canvas data for a specific guild. Creates a new canvas if one doesn't exist.
    
    Args:
        guild_id: The ID of the guild
        
    Returns:
        dict or None: Canvas data dictionary or None if error
    """
    global db
    
    # Check database connection
    if db is None:
        logger.error("Database not initialized, cannot get canvas")
        return None
    
    # Validate guild_id
    if guild_id is None:
        logger.error("Cannot get canvas without a guild_id")
        return None
    
    try:
        # Try to find existing canvas
        canvas = None
        try:
            canvas = db.canvases.find_one({"guild_id": guild_id})
        except Exception as find_error:
            logger.error(f"Error finding canvas for guild {guild_id}: {find_error}")
            import traceback
            logger.error(traceback.format_exc())
            return None
        
        # If canvas doesn't exist, create a new one
        if canvas is None:
            try:
                # Define default canvas properties
                new_canvas = {
                    "guild_id": guild_id,
                    "width": 100,               # Default canvas width
                    "height": 100,              # Default canvas height
                    "background_color": "#FFFFFF",  # Default canvas background color
                    "created_at": datetime.datetime.utcnow(),
                    "last_modified": datetime.datetime.utcnow()
                }
                
                # Insert the new canvas
                result = db.canvases.insert_one(new_canvas)
                
                # Get the inserted document with the _id
                if result.inserted_id:
                    canvas = db.canvases.find_one({"_id": result.inserted_id})
                    logger.info(f"Created new canvas for guild {guild_id}")
                else:
                    logger.error(f"Failed to create new canvas for guild {guild_id}")
                    return None
                    
            except Exception as create_error:
                logger.error(f"Error creating new canvas for guild {guild_id}: {create_error}")
                import traceback
                logger.error(traceback.format_exc())
                return None
        
        return canvas
        
    except Exception as e:
        logger.error(f"Error retrieving canvas for guild {guild_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def get_pixel(canvas_id, x, y):
    """Get pixel data at specific coordinates.
    
    Args:
        canvas_id: ID of the canvas
        x: X coordinate
        y: Y coordinate
        
    Returns:
        dict or None: Pixel data dictionary or None if not found or error
    """
    global db
    
    # Check database connection
    if db is None:
        logger.error("Database not initialized, cannot get pixel")
        return None
    
    # Validate parameters
    if canvas_id is None:
        logger.error("Cannot get pixel without a canvas_id")
        return None
    
    # Validate coordinates
    try:
        x = int(x)
        y = int(y)
    except (ValueError, TypeError):
        logger.error(f"Invalid coordinates: x={x}, y={y}. Must be integers.")
        return None
    
    try:
        # Query the pixel data
        pixel = db.pixels.find_one({
            "canvas_id": canvas_id,
            "x": x,
            "y": y
        })
        
        return pixel
        
    except Exception as e:
        logger.error(f"Error retrieving pixel for canvas {canvas_id} at ({x}, {y}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def set_pixel(canvas_id, x, y, color, user_id):
    """Set pixel data at specific coordinates.
    
    Args:
        canvas_id: ID of the canvas
        x: X coordinate
        y: Y coordinate
        color: Color value for the pixel
        user_id: ID of the user setting the pixel
        
    Returns:
        bool: True if pixel was set successfully, False otherwise
    """
    global db
    
    # Check database connection
    if db is None:
        logger.error("Database not initialized, cannot set pixel")
        return False
    
    # Validate required parameters
    if canvas_id is None:
        logger.error("Cannot set pixel without a canvas_id")
        return False
    
    if color is None:
        logger.error("Cannot set pixel without a color")
        return False
        
    if user_id is None:
        logger.error("Cannot set pixel without a user_id")
        return False
    
    # Validate coordinates
    try:
        x = int(x)
        y = int(y)
    except (ValueError, TypeError):
        logger.error(f"Invalid coordinates: x={x}, y={y}. Must be integers.")
        return False
    
    # Perform pixel update with error handling
    try:
        # Update or insert the pixel
        now = datetime.datetime.utcnow()
        result = db.pixels.update_one(
            {"canvas_id": canvas_id, "x": x, "y": y},
            {"$set": {
                "color": color,
                "last_modified_by": user_id,
                "last_modified": now
            }},
            upsert=True
        )
        
        pixel_updated = result.modified_count > 0 or result.upserted_id is not None
        
        if not pixel_updated:
            logger.warning(f"Pixel for canvas {canvas_id} at ({x}, {y}) was not updated")
            return False
        
        # Also update the canvas last_modified timestamp
        try:
            db.canvases.update_one(
                {"_id": canvas_id},
                {"$set": {"last_modified": now}}
            )
        except Exception as canvas_update_error:
            # Non-critical, just log
            logger.warning(f"Failed to update canvas last_modified: {canvas_update_error}")
        
        # Update user stats
        try:
            db.users.update_one(
                {"user_id": user_id},
                {
                    "$inc": {"pixels_placed": 1},
                    "$set": {"last_active": now}
                },
                upsert=True
            )
        except Exception as user_update_error:
            # Non-critical, just log
            logger.warning(f"Failed to update user stats: {user_update_error}")
        
        logger.info(f"Pixel set on canvas {canvas_id} at ({x}, {y}) with color {color} by user {user_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error setting pixel for canvas {canvas_id} at ({x}, {y}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def get_user_stats(user_id):
    """Get statistics for a specific user.
    
    Args:
        user_id: ID of the user
        
    Returns:
        dict or None: User statistics dictionary or None if error
    """
    global db
    
    # Check database connection
    if db is None:
        logger.error("Database not initialized, cannot get user stats")
        return None
    
    # Validate user_id
    if user_id is None:
        logger.error("Cannot get user stats without a user_id")
        return None
    
    try:
        # Find the user in the database
        user = None
        try:
            user = db.users.find_one({"user_id": user_id})
        except Exception as find_error:
            logger.error(f"Error finding user stats for user {user_id}: {find_error}")
            import traceback
            logger.error(traceback.format_exc())
            return None
        
        # If user doesn't exist, create a new entry
        if user is None:
            try:
                # Initialize with default values
                new_user = {
                    "user_id": user_id,
                    "pixels_placed": 0,
                    "first_seen": datetime.datetime.utcnow(),
                    "last_active": datetime.datetime.utcnow()
                }
                
                # Insert the new user
                result = db.users.insert_one(new_user)
                
                # Get the inserted document with the _id
                if result.inserted_id:
                    user = db.users.find_one({"_id": result.inserted_id})
                    logger.info(f"Created new user stats for user {user_id}")
                else:
                    logger.error(f"Failed to create new user stats for user {user_id}")
                    return None
            except Exception as create_error:
                logger.error(f"Error creating new user stats for user {user_id}: {create_error}")
                import traceback
                logger.error(traceback.format_exc())
                return None
        
        # Check if we need to update the last_active timestamp
        if user and not user.get("last_active"):
            try:
                db.users.update_one(
                    {"_id": user["_id"]},
                    {"$set": {"last_active": datetime.datetime.utcnow()}}
                )
                # Update the in-memory user object too
                user["last_active"] = datetime.datetime.utcnow()
            except Exception as update_error:
                # Non-critical, just log
                logger.warning(f"Failed to update last_active timestamp: {update_error}")
        
        return user
        
    except Exception as e:
        logger.error(f"Error retrieving user stats for user {user_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def clear_canvas(guild_id):
    """Clear all pixels from a guild's canvas.
    
    Args:
        guild_id: The ID of the guild whose canvas should be cleared
        
    Returns:
        int: Number of pixels deleted from the canvas (0 if error)
    """
    global db
    
    # Check database connection
    if db is None:
        logger.error("Database not initialized, cannot clear canvas")
        return 0
    
    # Validate guild_id
    if guild_id is None:
        logger.error("Cannot clear canvas without a guild_id")
        return 0
    
    try:
        # Get the canvas for this guild
        canvas = get_canvas(guild_id)
        
        # Verify the canvas exists
        if canvas is None:
            logger.error(f"Canvas not found for guild {guild_id}")
            return 0
        
        # Get the canvas ID
        canvas_id = canvas.get("_id")
        if canvas_id is None:
            logger.error(f"Canvas ID not found for guild {guild_id}")
            return 0
        
        try:
            # Delete all pixels for this canvas
            result = db.pixels.delete_many({"canvas_id": canvas_id})
            
            # Log the result
            deleted_count = result.deleted_count if hasattr(result, 'deleted_count') else 0
            logger.info(f"Cleared {deleted_count} pixels from canvas for guild {guild_id}")
            
            # Also update the canvas last_cleared timestamp
            try:
                db.canvases.update_one(
                    {"_id": canvas_id},
                    {"$set": {"last_cleared": datetime.datetime.utcnow()}}
                )
                logger.info(f"Updated last_cleared timestamp for guild {guild_id}")
            except Exception as update_error:
                # This is non-critical, so just log it
                logger.warning(f"Failed to update last_cleared timestamp: {update_error}")
            
            return deleted_count
            
        except Exception as delete_error:
            logger.error(f"Error deleting pixels for guild {guild_id}: {delete_error}")
            import traceback
            logger.error(traceback.format_exc())
            return 0
            
    except Exception as e:
        logger.error(f"Error clearing canvas for guild {guild_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0
