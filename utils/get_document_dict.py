"""
MongoDB Document Dictionary Helper Functions

This module provides helper functions to safely get document properties
and dictionaries from MongoDB documents, addressing the "truthiness"
problem that can occur when evaluating MongoDB documents as booleans.
"""

import logging
from typing import Any, Dict, List, Optional, TypeVar, Union, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

@runtime_checkable
class HasToDict(Protocol):
    """Protocol for objects with to_dict method."""
    def to_dict(self) -> Dict[str, Any]: ...

def get_document_dict(document: Any) -> Dict[str, Any]:
    """
    Get a dictionary representation of a document, handling various document types.
    
    This function safely handles:
    - Regular dictionaries
    - Objects with to_dict() method
    - MongoDB documents
    - None values
    
    Args:
        document: The document to convert
        
    Returns:
        A dictionary representing the document, or an empty dict if None
    """
    if document is None:
        return {}
        
    # If it's already a dictionary, return a copy
    if isinstance(document, dict):
        return document.copy()
        
    # If it has a to_dict method, use that
    if hasattr(document, 'to_dict') and callable(getattr(document, 'to_dict')):
        return document.to_dict()
        
    # If it has a __dict__ attribute, use that
    if hasattr(document, '__dict__'):
        return document.__dict__.copy()
        
    # Try to convert to dictionary
    try:
        return dict(document)
    except (TypeError, ValueError):
        logger.warning(f"Could not convert {type(document)} to dictionary")
        return {}
        
def document_exists(document: Any) -> bool:
    """
    Check if a document exists and is not None.
    
    This function handles the MongoDB "truthiness" problem by explicitly
    checking if the document is None rather than using bool(document).
    
    Args:
        document: The document to check
        
    Returns:
        True if the document exists and is not None, False otherwise
    """
    return document is not None
    
def get_dict_value(data: Union[Dict[str, Any], None], key: str, default: Any = None) -> Any:
    """
    Safely get a value from a dictionary that might be None.
    
    Args:
        data: The dictionary to get the value from
        key: The key to get
        default: The default value if the key doesn't exist or the dictionary is None
        
    Returns:
        The value or the default
    """
    if data is None:
        return default
    return data.get(key, default)
    
def get_nested_dict_value(data: Union[Dict[str, Any], None], path: str, default: Any = None) -> Any:
    """
    Get a nested value from a dictionary using dot notation.
    
    Args:
        data: The dictionary to get the value from
        path: The path to the nested value (e.g., "user.profile.name")
        default: The default value if any part of the path doesn't exist
        
    Returns:
        The nested value or the default
    """
    if data is None:
        return default
        
    parts = path.split('.')
    current = data
    
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            return default
        current = current.get(part)
        if current is None:
            return default
    
    return current