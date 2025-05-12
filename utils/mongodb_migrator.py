"""
# module: mongodb_migrator
MongoDB Migration Utilities

This module provides helper functions for safely migrating from direct MongoDB
document access to safer document handling using SafeDocument and related patterns.
"""

import logging
from typing import Any, Dict, Optional, List, Union, Callable, TypeVar

from utils.safe_mongodb import SafeDocument, SafeMongoDBResult

T = TypeVar('T')

logger = logging.getLogger(__name__)

def document_exists(document: Optional[Union[Dict[str, Any], SafeDocument, SafeMongoDBResult[SafeDocument]]]) -> bool:
    """
    Safely check if a document exists (is not None and is not empty).
    
    This is a safer replacement for direct truthiness checks like `if document is not None:`.
    
    Args:
        document: MongoDB document, SafeDocument, or SafeMongoDBResult to check
        
    Returns:
        bool: True if the document exists, False otherwise
    """
    # Handle None case
    if document is None:
        return False
    
    # Handle SafeMongoDBResult case
    if isinstance(document, SafeMongoDBResult):
        if document.success is None:
            return False
        return document_exists(document.result)
    
    # Handle SafeDocument case
    if isinstance(document, SafeDocument):
        return bool(document)  # SafeDocument handles truthiness correctly
    
    # Handle dictionary case
    # We intentionally return True for empty dicts because they are valid documents
    return True

def has_key(document: Optional[Union[Dict[str, Any], SafeDocument, SafeMongoDBResult[SafeDocument]]], key: str) -> bool:
    """
    Safely check if a document has a specific key.
    
    Args:
        document: MongoDB document, SafeDocument, or SafeMongoDBResult to check
        key: Key to check for
        
    Returns:
        bool: True if the document has the key, False otherwise
    """
    # Handle None case
    if document is None:
        return False
    
    # Handle SafeMongoDBResult case
    if isinstance(document, SafeMongoDBResult):
        if document.success is None:
            return False
        return has_key(document.result, key)
    
    # Handle SafeDocument case
    if isinstance(document, SafeDocument):
        return document.has(key)
    
    # Handle dictionary case
    return key in document and document[key] is not None

def get_value(document: Optional[Union[Dict[str, Any], SafeDocument, SafeMongoDBResult[SafeDocument]]], 
             key: str, 
             default: Any = None) -> Any:
    """
    Safely get a value from a document with a default fallback.
    
    Args:
        document: MongoDB document, SafeDocument, or SafeMongoDBResult to get the value from
        key: Key to get
        default: Default value to return if the key doesn't exist
        
    Returns:
        The value or default
    """
    # Handle None case
    if document is None:
        return default
    
    # Handle SafeMongoDBResult case
    if isinstance(document, SafeMongoDBResult):
        if document.success is None:
            return default
        return get_value(document.result, key, default)
    
    # Handle SafeDocument case
    if isinstance(document, SafeDocument):
        return document.get(key, default)
    
    # Handle dictionary case
    return document.get(key, default)

def get_nested_value(document: Optional[Union[Dict[str, Any], SafeDocument, SafeMongoDBResult[SafeDocument]]], 
                     path: str, 
                     default: Any = None) -> Any:
    """
    Safely get a nested value from a document using dot notation.
    
    Args:
        document: MongoDB document, SafeDocument, or SafeMongoDBResult to get the value from
        path: Path to the nested value (e.g., "user.profile.name")
        default: Default value to return if any part of the path doesn't exist
        
    Returns:
        The nested value or default
    """
    # Handle None case
    if document is None:
        return default
    
    # Handle SafeMongoDBResult case
    if isinstance(document, SafeMongoDBResult):
        if document.success is None:
            return default
        return get_nested_value(document.result, path, default)
    
    # Handle SafeDocument case
    if isinstance(document, SafeDocument):
        return document.get_nested(path, default)
    
    # Handle dictionary case with dot notation
    current = document
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return default
        current = current.get(part)
        if current is None:
            return default
    
    return current

def safe_cast(value: Any, cast_func: Callable, default: Any = None) -> Any:
    """
    Safely cast a value to another type with a default fallback.
    
    Args:
        value: Value to cast
        cast_func: Function to use for casting (e.g., int, float, str)
        default: Default value to return if casting fails
        
    Returns:
        The cast value or default
    """
    if value is None:
        return default
    
    try:
        return cast_func(value)
    except (ValueError, TypeError):
        logger.warning(f"Failed to cast {value} using {cast_func.__name__}")
        return default

def safe_int(value: Any, default: int = 0) -> int:
    """
    Safely convert a value to int with a default fallback.
    
    Args:
        value: Value to convert
        default: Default value to return if conversion fails
        
    Returns:
        int: The converted value or default
    """
    return safe_cast(value, int, default)

def safe_float(value: Any, default: float = 0.0) -> float:
    """
    Safely convert a value to float with a default fallback.
    
    Args:
        value: Value to convert
        default: Default value to return if conversion fails
        
    Returns:
        float: The converted value or default
    """
    return safe_cast(value, float, default)

def safe_str(value: Any, default: str = "") -> str:
    """
    Safely convert a value to string with a default fallback.
    
    Args:
        value: Value to convert
        default: Default value to return if conversion fails
        
    Returns:
        str: The converted value or default
    """
    if value is None:
        return default
    
    try:
        return str(value)
    except Exception:
        logger.warning(f"Failed to convert {value} to string")
        return default

def safe_bool(value: Any, default: bool = False) -> bool:
    """
    Safely convert a value to boolean with a default fallback.
    
    This handles strings like "true", "false", "yes", "no", etc.
    
    Args:
        value: Value to convert
        default: Default value to return if conversion fails
        
    Returns:
        bool: The converted value or default
    """
    if value is None:
        return default
    
    if isinstance(value, bool):
        return value
    
    if isinstance(value, (int, float)):
        return bool(value)
    
    if isinstance(value, str):
        value = value.lower().strip()
        if value in ("true", "yes", "y", "1", "t"):
            return True
        if value in ("false", "no", "n", "0", ""):
            return False
    
    return default

def ensure_safe_document(document: Optional[Union[Dict[str, Any], SafeDocument, SafeMongoDBResult[SafeDocument]]]) -> Optional[SafeDocument]:
    """
    Ensure a document is a SafeDocument.
    
    If the document is already a SafeDocument, it is returned as is.
    If the document is a dictionary, it is converted to a SafeDocument.
    If the document is a SafeMongoDBResult, its result is extracted and converted if successful.
    If the document is None, None is returned.
    
    Args:
        document: Document to ensure is safe
        
    Returns:
        SafeDocument or None
    """
    if document is None:
        return None
    
    # Handle SafeMongoDBResult case
    if isinstance(document, SafeMongoDBResult):
        if document.success is None:
            return None
        return ensure_safe_document(document.result)
        
    # Handle SafeDocument case
    if isinstance(document, SafeDocument):
        return document
        
    # Convert dictionary to SafeDocument
    return SafeDocument(document)

def get_document_dict(document: Optional[Union[Dict[str, Any], SafeDocument, SafeMongoDBResult[SafeDocument]]]) -> Optional[Dict[str, Any]]:
    """
    Extract a document dictionary from various document types.
    
    This is a convenience function that safely extracts the dictionary from a document,
    regardless of whether it's a SafeDocument, SafeMongoDBResult, or already a dictionary.
    
    Args:
        document: Document to extract dictionary from
        
    Returns:
        Dict or None if no valid document could be extracted
    """
    # First ensure we have a SafeDocument
    safe_doc = ensure_safe_document(document)
    
    # If we couldn't get a SafeDocument, return None
    if safe_doc is None:
        return None
    
    # Get the dictionary from the SafeDocument
    return safe_doc.to_dict()