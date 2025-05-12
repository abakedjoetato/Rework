"""
Base model for MongoDB in the Tower of Temptation PvP Statistics Bot

This module contains the BaseModel class that all other models inherit from.
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Type, TypeVar, ClassVar

logger = logging.getLogger(__name__)

T = TypeVar('T', bound='BaseModel')

class BaseModel:
    """Base model for MongoDB documents"""
    collection_name = None  # Will be overridden by subclasses
    
    def __init__(self, db=None, data=None):
        """Initialize the base model with default attributes
        
        Args:
            db: Database connection
            data: Model data dictionary
        """
        self._id = None
        self.db = db
        self.data = data or {}
    
    @classmethod
    def from_document(cls: Type[T], document: Dict[str, Any], db=None) -> Optional[T]:
        """Create a model instance from a MongoDB document
        
        Args:
            document: MongoDB document
            db: Optional database connection
            
        Returns:
            Model instance or None if document is None
        """
        if document is None:
            return None
            
        # Create instance with db and data
        instance = cls(db=db, data=document.copy())
        
        # Also set attributes for backward compatibility
        for key, value in document.items():
            # Convert MongoDB _id to id if needed
            if key == '_id':
                instance._id = value
                continue
                
            # Set attribute if it exists in the document
            if hasattr(instance, key):
                setattr(instance, key, value)
            else:
                # Add any additional fields from document
                setattr(instance, key, value)
                
        return instance
    
    def to_document(self) -> Dict[str, Any]:
        """Convert model instance to a MongoDB document
        
        Returns:
            MongoDB document
        """
        document = {}
        
        # Get all attributes that are not private/internal
        for key, value in self.__dict__.items():
            # Skip internal attributes
            if key.startswith('__'):
                continue
                
            # Skip MongoDB _id if it's None
            if key == '_id' and value is None:
                continue
            
            # Include all other attributes
            document[key] = value
            
        return document
    
    @property
    def id(self) -> Optional[str]:
        """Get the MongoDB document ID
        
        Returns:
            MongoDB document ID or None
        """
        return str(self._id) if self._id is not None else None
    
    def __str__(self) -> str:
        """String representation of the model
        
        Returns:
            String representation
        """
        if hasattr(self, 'name') and getattr(self, 'name') is not None:
            return f"{self.__class__.__name__} (name: {getattr(self, 'name')})"
        elif hasattr(self, '_id') and self._id is not None:
            return f"{self.__class__.__name__} (id: {self._id})"
        else:
            return f"{self.__class__.__name__} (New)"
    
    @staticmethod
    def is_not_none(obj) -> bool:
        """Safely check if an object is not None, for database objects
        
        This helper method is designed to be used when checking database objects
        instead of directly using them in boolean contexts. MongoDB objects
        cannot be used directly in boolean tests and must be explicitly compared
        with None.
        
        Args:
            obj: The object to check
            
        Returns:
            bool: True if the object is not None, False otherwise
        """
        return obj is not None