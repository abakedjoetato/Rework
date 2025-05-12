"""
Safe MongoDB Utilities

This module provides utilities for safely working with MongoDB documents
to avoid the "truthiness" problem and handle errors consistently.

The main classes are:
- SafeDocument: A wrapper for MongoDB documents that provides safe access to fields
- SafeMongoDBResult: A result object for MongoDB operations
- SafeMongoDBOperations: Static methods for safely performing MongoDB operations
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union, Generic, Callable, TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING is not None:
    from pymongo.results import InsertOneResult, UpdateResult, DeleteResult

logger = logging.getLogger(__name__)

@runtime_checkable
class HasInsertedId(Protocol):
    """Protocol for objects with inserted_id attribute."""
    inserted_id: Any

@runtime_checkable
class HasModifiedCount(Protocol):
    """Protocol for objects with modified_count attribute."""
    modified_count: int
    
@runtime_checkable
class HasToDict(Protocol):
    """Protocol for objects with to_dict method."""
    def to_dict(self) -> Dict[str, Any]: ...

T = TypeVar('T')


class SafeDocument:
    """
    A wrapper for MongoDB documents that provides safe access methods.
    
    This class addresses the MongoDB "truthiness" problem by:
    - Always evaluating to True if it exists, even if empty
    - Providing safe access to fields with .get() and attribute access
    - Offering explicit existence checks with .has()
    - Supporting nested field access with .get_nested()
    """
    
    def __init__(self, document: Optional[Dict[str, Any]] = None):
        """
        Initialize a SafeDocument with a MongoDB document.
        
        Args:
            document: A MongoDB document or None
        """
        self._document = document if document is not None else {}
        self._exists = document is not None
    
    def __bool__(self) -> bool:
        """
        Always return True if the document exists, even if it's an empty document.
        
        Returns:
            True if the document exists, False otherwise
        """
        return self._exists
    
    def __getattr__(self, name: str) -> Any:
        """
        Allow attribute access to dictionary keys.
        
        Args:
            name: The attribute/key name
            
        Returns:
            The value or None if it doesn't exist
        """
        return self._document.get(name)
    
    def __getitem__(self, key: str) -> Any:
        """
        Allow dictionary-style access with safe fallback to None.
        
        Args:
            key: The dictionary key
            
        Returns:
            The value or None if it doesn't exist
        """
        return self._document.get(key)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a value from the document with a default.
        
        Args:
            key: The key to retrieve
            default: The default value if the key doesn't exist
            
        Returns:
            The value or the default
        """
        return self._document.get(key, default)
    
    def has(self, key: str) -> bool:
        """
        Check if a key exists in the document and is not None.
        
        This is more explicit than truthiness checking.
        
        Args:
            key: The key to check
            
        Returns:
            True if the key exists and is not None
        """
        return key in self._document and self._document[key] is not None
    
    def get_nested(self, path: str, default: Any = None) -> Any:
        """
        Get a nested value using dot notation.
        
        Args:
            path: The path to the nested value (e.g., "user.profile.name")
            default: The default value if any part of the path doesn't exist
            
        Returns:
            The nested value or the default
        """
        parts = path.split('.')
        current = self._document
        
        for part in parts:
            if not isinstance(current, dict) or part not in current:
                return default
            current = current.get(part)
            if current is None:
                return default
        
        return current
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert back to a regular dictionary.
        
        Returns:
            The underlying document dictionary
        """
        return self._document.copy() if self._document else {}
    
    def keys(self) -> List[str]:
        """
        Get the keys in the document.
        
        Returns:
            List of keys
        """
        return list(self._document.keys()) if self._document else []
    
    def values(self) -> List[Any]:
        """
        Get the values in the document.
        
        Returns:
            List of values
        """
        return list(self._document.values()) if self._document else []
    
    def items(self) -> List[Tuple[str, Any]]:
        """
        Get the items in the document.
        
        Returns:
            List of (key, value) tuples
        """
        return list(self._document.items()) if self._document else []


class SafeMongoDBResult(Generic[T]):
    """
    A result object for MongoDB operations.
    
    This standardizes error handling and success/failure reporting
    for MongoDB operations.
    """
    
    def __init__(
        self, 
        success: bool, 
        result: Optional[T] = None, 
        error: Optional[Exception] = None,
        message: Optional[str] = None
    ):
        """
        Initialize a MongoDB result.
        
        Args:
            success: Whether the operation was successful
            result: The result of the operation
            error: Any exception that occurred
            message: A human-readable message
        """
        self.success = success
        self.result = result
        self._error = error  # Use private attribute for actual storage
        self.message = message or (str(error) if error else ("Success" if success else "Failed"))
        
    def get_error(self) -> Optional[Union[Exception, str]]:
        """Get the error if one occurred."""
        return self._error
    
    @classmethod
    def ok(cls, result: T, message: Optional[str] = None) -> 'SafeMongoDBResult[T]':
        """
        Create a successful result.
        
        Args:
            result: The operation result
            message: Optional success message
            
        Returns:
            A successful result object
        """
        return cls(True, result, None, message)
    
    @classmethod
    def create_error(cls, error: Union[Exception, str], result: Optional[T] = None) -> 'SafeMongoDBResult[T]':
        """
        Create an error result.
        
        Args:
            error: The error that occurred
            result: Optional partial result
            
        Returns:
            An error result object
        """
        if isinstance(error, str):
            return cls(False, result, Exception(error), error)
        return cls(False, result, error, str(error))
    
    def log_error(self, logger_func=None, prefix: str = "") -> 'SafeMongoDBResult[T]':
        """
        Log an error if the operation failed.
        
        Args:
            logger_func: The logging function to use (defaults to logger.error)
            prefix: A prefix for the log message
            
        Returns:
            Self for chaining
        """
        if self.success is None:
            if logger_func is None:
                logger_func = logger.error
            logger_func(f"{prefix}{self.message}")
        return self
        
    @property
    def inserted_id(self) -> Any:
        """
        Get the inserted_id from the result if available.
        
        Returns:
            The inserted_id or None if not available
        """
        if not self.success or self.result is None:
            return None
            
        # Check if result implements the HasInsertedId protocol
        if isinstance(self.result, HasInsertedId):
            return self.result.inserted_id
            
        return None
        
    @property
    def modified_count(self) -> int:
        """
        Get the modified_count from the result if available.
        
        Returns:
            The number of modified documents if documents is not None else 0 if not available
        """
        if not self.success or self.result is None:
            return 0
            
        # Check if result implements the HasModifiedCount protocol
        if isinstance(self.result, HasModifiedCount):
            return self.result.modified_count
            
        return 0
        
    def get_dict(self) -> Dict[str, Any]:
        """
        Get the document as a dictionary if the result is a SafeDocument.
        
        Returns:
            The document dictionary or empty dict if not available
        """
        if not self.success or self.result is None:
            return {}
            
        # Handle objects with to_dict method (like SafeDocument)
        if isinstance(self.result, HasToDict):
            return self.result.to_dict()
            
        # Handle direct document result
        if isinstance(self.result, dict):
            return self.result.copy()
            
        return {}

    def error(self) -> Optional[Exception]:
        """
        Get the error from this result.
        
        Returns:
            The exception if there was an error, None otherwise
        """
        return self._error


class SafeMongoDBOperations:
    """
    Helper class for safely performing MongoDB operations.
    
    Each method returns a SafeMongoDBResult with consistent error handling.
    
    This class can be used in two ways:
    1. Create an instance with a database connection: ops = SafeMongoDBOperations(db)
    2. Use the static methods directly: await SafeMongoDBOperations.find_one(collection, query)
    """
    
    def __init__(self, db=None):
        """
        Initialize with an optional database connection.
        
        Args:
            db: MongoDB database connection (optional)
        """
        self.db = db
        
    async def insert_one_to_collection(self, collection_name: str, document: Dict[str, Any], *args, **kwargs) -> SafeMongoDBResult[Any]:
        """
        Insert a document into a collection using the database connection.
        
        Args:
            collection_name: Name of the collection
            document: Document to insert
            *args: Additional positional arguments for insert_one
            **kwargs: Additional keyword arguments for insert_one
            
        Returns:
            SafeMongoDBResult containing the insert result or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available")
            
        try:
            collection = self.db[collection_name]
            result = await collection.insert_one(document, *args, **kwargs)
            return SafeMongoDBResult.ok(result)
        except Exception as e:
            logger.error(f"Error in insert_one_to_collection: {e}")
            return SafeMongoDBResult.create_error(e)
    
    async def find_one(
        self,
        collection, 
        query: Dict[str, Any], 
        *args, 
        **kwargs
    ) -> SafeMongoDBResult[SafeDocument]:
        """
        Safely perform a find_one operation.
        
        Args:
            collection: The MongoDB collection or collection name
            query: The query dictionary
            *args: Additional positional arguments for find_one
            **kwargs: Additional keyword arguments for find_one
            
        Returns:
            SafeMongoDBResult containing a SafeDocument or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available")
            
        try:
            # Handle both collection objects and collection names
            if isinstance(collection, str):
                collection_obj = self.db[collection]
            else:
                collection_obj = collection
                
            document = await collection_obj.find_one(query, *args, **kwargs)
            return SafeMongoDBResult.ok(SafeDocument(document))
        except Exception as e:
            logger.error(f"Error in find_one: {e}")
            return SafeMongoDBResult.create_error(e)
    
    async def find_many(
        self,
        collection, 
        query: Dict[str, Any], 
        *args, 
        **kwargs
    ) -> SafeMongoDBResult[List[SafeDocument]]:
        """
        Safely perform a find operation.
        
        Args:
            collection: The MongoDB collection
            query: The query dictionary
            *args: Additional positional arguments for find
            **kwargs: Additional keyword arguments for find
            
        Returns:
            SafeMongoDBResult containing a list of SafeDocuments or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available")
            
        try:
            # Handle both collection objects and collection names
            if isinstance(collection, str):
                collection_obj = self.db[collection]
            else:
                collection_obj = collection
                
            cursor = collection_obj.find(query, *args, **kwargs)
            documents = []
            async for document in cursor:
                documents.append(SafeDocument(document))
            return SafeMongoDBResult.ok(documents)
        except Exception as e:
            logger.error(f"Error in find_many: {e}")
            return SafeMongoDBResult.create_error(e)
    
    async def insert_one(
        self,
        collection, 
        document: Dict[str, Any], 
        *args, 
        **kwargs
    ) -> SafeMongoDBResult[Any]:
        """
        Safely perform an insert_one operation.
        
        Args:
            collection: The MongoDB collection
            document: The document to insert
            *args: Additional positional arguments for insert_one
            **kwargs: Additional keyword arguments for insert_one
            
        Returns:
            SafeMongoDBResult containing the insert result or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available")
            
        try:
            # Handle both collection objects and collection names
            if isinstance(collection, str):
                collection_obj = self.db[collection]
            else:
                collection_obj = collection
                
            result = await collection_obj.insert_one(document, *args, **kwargs)
            return SafeMongoDBResult.ok(result)
        except Exception as e:
            logger.error(f"Error in insert_one: {e}")
            return SafeMongoDBResult.create_error(e)
    
    async def insert_many(
        self,
        collection, 
        documents: List[Dict[str, Any]], 
        *args, 
        **kwargs
    ) -> SafeMongoDBResult[Any]:
        """
        Safely perform an insert_many operation.
        
        Args:
            collection: The MongoDB collection or collection name
            documents: The documents to insert
            *args: Additional positional arguments for insert_many
            **kwargs: Additional keyword arguments for insert_many
            
        Returns:
            SafeMongoDBResult containing the insert result or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available")
            
        try:
            # Handle both collection objects and collection names
            if isinstance(collection, str):
                collection_obj = self.db[collection]
            else:
                collection_obj = collection
                
            result = await collection_obj.insert_many(documents, *args, **kwargs)
            return SafeMongoDBResult.ok(result)
        except Exception as e:
            logger.error(f"Error in insert_many: {e}")
            return SafeMongoDBResult.create_error(e)
    
    async def update_one(
        self,
        collection, 
        query: Dict[str, Any], 
        update: Dict[str, Any], 
        *args, 
        **kwargs
    ) -> SafeMongoDBResult[Any]:
        """
        Safely perform an update_one operation.
        
        Args:
            collection: The MongoDB collection or collection name
            query: The query dictionary
            update: The update dictionary
            *args: Additional positional arguments for update_one
            **kwargs: Additional keyword arguments for update_one
            
        Returns:
            SafeMongoDBResult containing the update result or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available")
            
        try:
            # Handle both collection objects and collection names
            if isinstance(collection, str):
                collection_obj = self.db[collection]
            else:
                collection_obj = collection
                
            result = await collection_obj.update_one(query, update, *args, **kwargs)
            return SafeMongoDBResult.ok(result)
        except Exception as e:
            logger.error(f"Error in update_one: {e}")
            return SafeMongoDBResult.create_error(e)
    
    async def update_many(
        self,
        collection, 
        query: Dict[str, Any], 
        update: Dict[str, Any], 
        *args, 
        **kwargs
    ) -> SafeMongoDBResult[Any]:
        """
        Safely perform an update_many operation.
        
        Args:
            collection: The MongoDB collection or collection name
            query: The query dictionary
            update: The update dictionary
            *args: Additional positional arguments for update_many
            **kwargs: Additional keyword arguments for update_many
            
        Returns:
            SafeMongoDBResult containing the update result or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available")
            
        try:
            # Handle both collection objects and collection names
            if isinstance(collection, str):
                collection_obj = self.db[collection]
            else:
                collection_obj = collection
                
            result = await collection_obj.update_many(query, update, *args, **kwargs)
            return SafeMongoDBResult.ok(result)
        except Exception as e:
            logger.error(f"Error in update_many: {e}")
            return SafeMongoDBResult.create_error(e)
    
    async def delete_one(
        self,
        collection, 
        query: Dict[str, Any], 
        *args, 
        **kwargs
    ) -> SafeMongoDBResult[Any]:
        """
        Safely perform a delete_one operation.
        
        Args:
            collection: The MongoDB collection or collection name
            query: The query dictionary
            *args: Additional positional arguments for delete_one
            **kwargs: Additional keyword arguments for delete_one
            
        Returns:
            SafeMongoDBResult containing the delete result or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available")
            
        try:
            # Handle both collection objects and collection names
            if isinstance(collection, str):
                collection_obj = self.db[collection]
            else:
                collection_obj = collection
                
            result = await collection_obj.delete_one(query, *args, **kwargs)
            return SafeMongoDBResult.ok(result)
        except Exception as e:
            logger.error(f"Error in delete_one: {e}")
            return SafeMongoDBResult.create_error(e)
    
    async def delete_many(
        self,
        collection, 
        query: Dict[str, Any], 
        *args, 
        **kwargs
    ) -> SafeMongoDBResult[Any]:
        """
        Safely perform a delete_many operation.
        
        Args:
            collection: The MongoDB collection or collection name
            query: The query dictionary
            *args: Additional positional arguments for delete_many
            **kwargs: Additional keyword arguments for delete_many
            
        Returns:
            SafeMongoDBResult containing the delete result or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available")
            
        try:
            # Handle both collection objects and collection names
            if isinstance(collection, str):
                collection_obj = self.db[collection]
            else:
                collection_obj = collection
                
            result = await collection_obj.delete_many(query, *args, **kwargs)
            return SafeMongoDBResult.ok(result)
        except Exception as e:
            logger.error(f"Error in delete_many: {e}")
            return SafeMongoDBResult.create_error(e)
    
    async def count_documents(
        self,
        collection, 
        query: Dict[str, Any], 
        *args, 
        **kwargs
    ) -> SafeMongoDBResult[int]:
        """
        Safely perform a count_documents operation.
        
        Args:
            collection: The MongoDB collection
            query: The query dictionary
            *args: Additional positional arguments for count_documents
            **kwargs: Additional keyword arguments for count_documents
            
        Returns:
            SafeMongoDBResult containing the count or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available", 0)
            
        try:
            # Handle both collection objects and collection names
            if isinstance(collection, str):
                collection_obj = self.db[collection]
            else:
                collection_obj = collection
                
            count = await collection_obj.count_documents(query, *args, **kwargs)
            return SafeMongoDBResult.ok(count)
        except Exception as e:
            logger.error(f"Error in count_documents: {e}")
            return SafeMongoDBResult.create_error(e, 0)
    
    async def aggregate(
        self,
        collection, 
        pipeline: List[Dict[str, Any]], 
        *args, 
        **kwargs
    ) -> SafeMongoDBResult[List[SafeDocument]]:
        """
        Safely perform an aggregate operation.
        
        Args:
            collection: The MongoDB collection
            pipeline: The aggregation pipeline
            *args: Additional positional arguments for aggregate
            **kwargs: Additional keyword arguments for aggregate
            
        Returns:
            SafeMongoDBResult containing a list of SafeDocuments or error
        """
        if self.db is None:
            return SafeMongoDBResult.create_error("No database connection available", [])
            
        try:
            # Handle both collection objects and collection names
            if isinstance(collection, str):
                collection_obj = self.db[collection]
            else:
                collection_obj = collection
                
            cursor = collection_obj.aggregate(pipeline, *args, **kwargs)
            documents = []
            async for document in cursor:
                documents.append(SafeDocument(document))
            return SafeMongoDBResult.ok(documents)
        except Exception as e:
            logger.error(f"Error in aggregate: {e}")
            return SafeMongoDBResult.create_error(e, [])

def get_document_dict(result: SafeMongoDBResult) -> Dict[str, Any]:
    """
    Extract the document dictionary from a SafeMongoDBResult.
    
    Args:
        result: SafeMongoDBResult to extract document from
        
    Returns:
        Document dictionary, or empty dict if None
    """
    if result is None:
        return {}
    
    # Check if successful result exists
    if not result.success or result.result is None:
        return {}
    
    # Handle objects with to_dict method (like SafeDocument)
    if hasattr(result.result, "to_dict"):
        return result.result.to_dict()
        
    # Handle direct dictionary result
    if isinstance(result.result, dict):
        return result.result.copy()
    
    return {}
