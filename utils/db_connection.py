"""
Database Connection Utilities

This module provides functions for connecting to MongoDB with proper error handling,
SRV URI support, and comprehensive connection validation.
"""

import os
import re
import logging
import asyncio
from typing import Optional, Tuple, Dict, Any, Union
from urllib.parse import urlparse

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConfigurationError, ConnectionFailure, ServerSelectionTimeoutError

logger = logging.getLogger(__name__)

# Regular expressions for MongoDB URI validation
STANDARD_URI_PATTERN = r'^mongodb://(?:[^:@]+(?::[^:@]+)?@)?[^:@/]+(?::[0-9]+)?(?:/[^?]+)?(?:\?.*)?$'
SRV_URI_PATTERN = r'^mongodb\+srv://(?:[^:@]+(?::[^:@]+)?@)?[^:@/]+(?:/[^?]+)?(?:\?.*)?$'

class DatabaseConnectionError(Exception):
    """Exception raised for database connection errors."""
    pass

async def get_database_client(max_retries: int = 3, retry_delay: int = 2) -> AsyncIOMotorClient:
    """
    Get a MongoDB client with proper error handling and retry logic.
    
    Args:
        max_retries: Maximum number of connection attempts
        retry_delay: Seconds to wait between retries
        
    Returns:
        AsyncIOMotorClient: MongoDB client
        
    Raises:
        DatabaseConnectionError: If connection fails after all retries
    """
    uri = os.environ.get("MONGODB_URI")
    if not uri:
        error_msg = "MONGODB_URI environment variable is not set"
        logger.critical(error_msg)
        raise DatabaseConnectionError(error_msg)
    
    # Validate the URI format
    is_valid, error_message = validate_mongodb_uri(uri)
    if not is_valid:
        logger.critical(f"Invalid MongoDB URI: {error_message}")
        raise DatabaseConnectionError(f"Invalid MongoDB URI: {error_message}")
    
    # Try to connect with retries
    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to MongoDB (attempt {attempt}/{max_retries})...")
            client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000)
            
            # Verify the connection by executing a command
            await client.admin.command('ping')
            
            logger.info("Successfully connected to MongoDB")
            return client
            
        except (ConnectionFailure, ServerSelectionTimeoutError, ConfigurationError) as e:
            last_exception = e
            logger.error(f"Failed to connect to MongoDB (attempt {attempt}/{max_retries}): {e}")
            
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                error_msg = f"Failed to connect to MongoDB after {max_retries} attempts"
                logger.critical(error_msg)
                raise DatabaseConnectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error connecting to MongoDB: {e}"
            logger.critical(error_msg)
            raise DatabaseConnectionError(error_msg) from e
    
    # This code should never be reached because we either return a client or raise an exception
    # But we add it to satisfy the type checker that a value is always returned
    if last_exception:
        raise DatabaseConnectionError(f"Failed to connect to MongoDB: {last_exception}") from last_exception
    else:
        raise DatabaseConnectionError("Failed to connect to MongoDB for unknown reasons")
    # This line will never be reached but ensures the type checker is satisfied
    return AsyncIOMotorClient(uri)

def validate_mongodb_uri(uri: str) -> Tuple[bool, str]:
    """
    Validate a MongoDB URI string.
    
    Args:
        uri: MongoDB connection URI
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not uri:
        return False, "URI is empty"
    
    # Check for SRV format
    is_srv = uri.startswith("mongodb+srv://")
    
    # Validate against appropriate pattern
    if is_srv:
        if not re.match(SRV_URI_PATTERN, uri):
            return False, "Invalid MongoDB SRV URI format"
    else:
        if not re.match(STANDARD_URI_PATTERN, uri):
            return False, "Invalid MongoDB standard URI format"
    
    # Parse the URI to validate its components
    try:
        parsed = urlparse(uri)
        
        # Check scheme
        if parsed.scheme not in ("mongodb", "mongodb+srv"):
            return False, f"Invalid scheme: {parsed.scheme}"
        
        # Check host
        if not parsed.netloc:
            return False, "Missing hostname"
        
        # For SRV URIs, additional validation
        if is_srv:
            # SRV records should not include port
            if ":" in parsed.netloc:
                return False, "SRV URI should not include port number"
        
        return True, ""
    except Exception as e:
        return False, f"URI parsing error: {str(e)}"

async def test_database_connection() -> Tuple[bool, str]:
    """
    Test the database connection and return status.
    
    Returns:
        Tuple of (success, message)
    """
    try:
        client = await get_database_client()
        server_info = await client.admin.command('serverStatus')
        
        version = server_info.get("version", "Unknown")
        connections = server_info.get("connections", {}).get("current", 0)
        
        return True, f"Connected to MongoDB version {version} with {connections} current connections"
        
    except DatabaseConnectionError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

async def get_database():
    """
    Get a MongoDB database instance with proper error handling.
    
    Returns:
        MongoDB database instance
    
    Raises:
        DatabaseConnectionError: If connection fails
    """
    client = await get_database_client()
    db_name = os.environ.get("DB_NAME", "mukti_bot")
    return client[db_name]

# No fallback URIs - if the primary connection fails, it's a critical error