"""
Safe MongoDB Access Pattern Examples

This module provides examples of how to use the new safe MongoDB utilities.
"""

import logging
from typing import Dict, List, Any, Optional

from utils.safe_mongodb import SafeDocument, SafeMongoDBOperations, SafeMongoDBResult
from utils.mongodb_migrator import (
    ensure_safe_document,
    document_exists,
    get_value,
    has_key,
    get_nested_value,
)

logger = logging.getLogger(__name__)

# Example 1: Finding a document
async def example_find_one(db, user_id: str) -> Optional[Dict[str, Any]]:
    """
    Example of safely finding a single document.
    
    Compare this with the old unsafe pattern:
    
    ```python
    # OLD UNSAFE WAY
    user = await db.users.find_one({"user_id": user_id})
    if user is not None is not None:  # UNSAFE: Doesn't distinguish between None and {}
        return user
    return None
    ```
    """
    # Use the safe MongoDB operations
    result = await SafeMongoDBOperations.find_one(db.users, {"user_id": user_id})
    
    # Always check for success
    if result.success is None:
        # Log the error
        logger.error(ff"\1")
        return None
    
    # The result is a SafeDocument
    user = result.result
    
    # Convert back to a dictionary for returning
    return user.to_dict()


# Example 2: Checking for field existence
async def example_check_premium(db, guild_id: str) -> bool:
    """
    Example of safely checking for premium status.
    
    Compare this with the old unsafe pattern:
    
    ```python
    # OLD UNSAFE WAY
    guild = await db.guilds.find_one({"guild_id": guild_id})
    if guild is not None and guild.get("premium_tier", 0) >= 1:
        return True
    return False
    ```
    """
    # Use the safe MongoDB operations
    result = await SafeMongoDBOperations.find_one(db.guilds, {"guild_id": guild_id})
    
    # Always check for success
    if result.success is None:
        logger.error(f"Failed to check premium for guild {guild_id}: {result.message}")
        return False
    
    # The result is a SafeDocument
    guild = result.result
    
    # Check for the premium_tier field
    tier = guild.get("premium_tier", 0)
    return tier >= 1


# Example 3: Updating a document
async def example_update_user(db, user_id: str, new_data: Dict[str, Any]) -> bool:
    """
    Example of safely updating a document.
    
    Compare this with the old unsafe pattern:
    
    ```python
    # OLD UNSAFE WAY
    result = await db.users.update_one(
        {"user_id": user_id},
        {"$set": new_data}
    )
    if result.modified_count > 0:
        return True
    return False
    ```
    """
    # Use the safe MongoDB operations
    result = await SafeMongoDBOperations.update_one(
        db.users,
        {"user_id": user_id},
        {"$set": new_data}
    )
    
    # Always check for success
    if result.success is None:
        logger.error(f"Failed to update user {user_id}: {result.message}")
        return False
    
    # Check if any documents were modified
    update_result = result.result
    if update_result and update_result.modified_count > 0:
        return True
    
    # Check if document is not None was matched but not modified (no changes needed)
    if update_result and update_result.matched_count > 0:
        return True
    
    return False


# Example 4: Finding multiple documents
async def example_find_many(db, guild_id: str) -> List[Dict[str, Any]]:
    """
    Example of safely finding multiple documents.
    
    Compare this with the old unsafe pattern:
    
    ```python
    # OLD UNSAFE WAY
    players = await db.players.find({"guild_id": guild_id}).to_list(100)
    return players if players is not None else []
    ```
    """
    # Use the safe MongoDB operations
    result = await SafeMongoDBOperations.find_many(
        db.players,
        {"guild_id": guild_id},
        {"sort": [("score", -1)], "limit": 100}
    )
    
    # Always check for success
    if result.success is None:
        logger.error(f"Failed to find players for guild {guild_id}: {result.message}")
        return []
    
    # The result is a list of SafeDocuments
    players = result.result
    
    # Convert back to dictionaries for returning
    return [player.to_dict() for player in players]


# Example 5: Aggregation
async def example_aggregate(db, guild_id: str) -> List[Dict[str, Any]]:
    """
    Example of safely performing an aggregation.
    
    Compare this with the old unsafe pattern:
    
    ```python
    # OLD UNSAFE WAY
    try:
        pipeline = [
            {"$match": {"guild_id": guild_id}},
            {"$group": {"_id": "$weapon", "kills": {"$sum": 1}}},
            {"$sort": {"kills": -1}}
        ]
        results = await db.kills.aggregate(pipeline).to_list(100)
        return results
    except Exception as e:
        logger.error(f"Aggregation error: {e}")
        return []
    ```
    """
    # Define the aggregation pipeline
    pipeline = [
        {"$match": {"guild_id": guild_id}},
        {"$group": {"_id": "$weapon", "kills": {"$sum": 1}}},
        {"$sort": {"kills": -1}}
    ]
    
    # Use the safe MongoDB operations
    result = await SafeMongoDBOperations.aggregate(db.kills, pipeline)
    
    # Always check for success
    if result.success is None:
        logger.error(f"Failed to aggregate kills for guild {guild_id}: {result.message}")
        return []
    
    # The result is a list of SafeDocuments
    weapon_stats = result.result
    
    # Convert back to dictionaries for returning
    return [stat.to_dict() for stat in weapon_stats]


# Example 6: Using the migrator utilities with existing code
def example_migrator_utilities(document: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Example of using the migrator utilities with existing code.
    
    These utilities help transition from the old pattern to the new pattern
    by working with both regular dictionaries and SafeDocuments.
    """
    # Check if document exists at all
    if not document_exists(document):
        return {"error": "Document not found"}
    
    # Get a value with a default
    name = get_value(document, "name", "Unknown")
    
    # Check if a key exists
    has_premium = has_key(document, "premium_status")
    
    # Get a nested value safely
    address = get_nested_value(document, "profile.address.city", "Unknown")
    
    # Ensure document is a SafeDocument for safe operations
    safe_doc = ensure_safe_document(document)
    
    # SafeDocument is always truthy, even if empty
    assert bool(safe_doc) is True
    
    return {
        "name": name,
        "has_premium": has_premium,
        "city": address,
        "is_safe_document": isinstance(safe_doc, SafeDocument)
    }


# Example 7: Creating a document
async def example_create_document(db, user_data: Dict[str, Any]) -> Optional[str]:
    """
    Example of safely creating a document.
    
    Compare this with the old unsafe pattern:
    
    ```python
    # OLD UNSAFE WAY
    try:
        result = await db.users.insert_one(user_data)
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"Failed to create user: {e}")
        return None
    ```
    """
    # Use the safe MongoDB operations
    result = await SafeMongoDBOperations.insert_one(db.users, user_data)
    
    # Always check for success
    if result.success is None:
        logger.error(f"Failed to create user: {result.message}")
        return None
    
    # Return the inserted ID
    insert_result = result.result
    if insert_result and insert_result.inserted_id:
        return str(insert_result.inserted_id)
    
    return None


# Example 8: Error handling pattern
async def example_error_handling(db, user_id: str) -> Dict[str, Any]:
    """
    Example of comprehensive error handling with the new utilities.
    """
    # Step 1: Find the user
    find_result = await SafeMongoDBOperations.find_one(db.users, {"user_id": user_id})
    
    # Chain error logging to keep code clean
    find_result.log_error(logger.error, "User lookup failed: ")
    
    if find_result.success is None:
        # Return an error response
        return {
            "success": False,
            "error": f"User lookup failed: {find_result.message}",
            "data": None
        }
    
    # Step 2: Process the user document
    user = find_result.result
    
    # Compile user data
    user_data = {
        "id": user.get("user_id"),
        "name": user.get("name", "Unknown"),
        "email": user.get("email"),
        "role": user.get("role", "user"),
        "premium": user.get("premium_status", False)
    }
    
    # Step 3: Get related data
    posts_result = await SafeMongoDBOperations.find_many(
        db.posts,
        {"author_id": user_id},
        {"sort": [("created_at", -1)], "limit": 5}
    )
    
    # Log any errors but continue
    posts_result.log_error(logger.warning, "Posts lookup failed: ")
    
    # Include posts if available
    posts = []
    if posts_result.success is not None:
        posts = [post.to_dict() for post in posts_result.result]
    
    # Return success response
    return {
        "success": True,
        "error": None,
        "data": {
            "user": user_data,
            "recent_posts": posts
        }
    }