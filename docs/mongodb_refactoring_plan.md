# MongoDB Command Pipeline Refactoring Plan

This document outlines the comprehensive plan for refactoring the MongoDB access patterns throughout the codebase. The goal is to standardize MongoDB access, eliminate truthiness issues, improve error handling, and provide a consistent interface for database operations.

## Phase 1: Infrastructure and Utilities (Completed)

- ✅ Create `safe_mongodb.py` with core classes:
  - `SafeDocument`: Wrapper for MongoDB documents that avoids truthiness issues
  - `SafeMongoDBResult`: Standardized result object for MongoDB operations
  - `SafeMongoDBOperations`: Static methods for safely performing MongoDB operations

- ✅ Create `mongodb_migrator.py` with transition helpers:
  - Functions for safely accessing document fields
  - Functions for checking document existence without truthiness issues
  - Utilities for working with both old-style dictionaries and SafeDocuments

## Phase 2: Premium System Refactoring (Current Phase)

- ✅ Create standardized premium feature access in `premium_feature_access.py`
- ✅ Create backward-compatible wrapper in `premium.py`
- ⬜ Update premium model classes to use safe MongoDB access patterns
- ⬜ Update premium commands and checks to use the new utilities
- ⬜ Standardize premium tier verification throughout the codebase

## Phase 3: Core Model Classes Refactoring

- ⬜ Refactor `models/guild.py` to use SafeMongoDBOperations
- ⬜ Refactor `models/server.py` to use SafeMongoDBOperations
- ⬜ Refactor `models/player.py` to use SafeMongoDBOperations
- ⬜ Refactor other core model classes to use SafeMongoDBOperations
- ⬜ Update all model classes to return SafeDocuments or properly wrapped results

## Phase 4: Command Pipeline Refactoring

- ⬜ Refactor CSV processing commands to use safe MongoDB access
- ⬜ Refactor statistics commands to use safe MongoDB access
- ⬜ Refactor killfeed commands to use safe MongoDB access
- ⬜ Refactor administrative commands to use safe MongoDB access
- ⬜ Refactor economy and faction-related commands to use safe MongoDB access

## Phase 5: Error Handling Standardization

- ⬜ Implement consistent error logging throughout the codebase
- ⬜ Add detailed error messages for users where appropriate
- ⬜ Create centralized error handling utilities
- ⬜ Add graceful degradation for premium features

## Phase 6: Testing and Validation

- ⬜ Create comprehensive test cases for MongoDB operations
- ⬜ Test premium system with different tier levels
- ⬜ Validate error handling in edge cases
- ⬜ Ensure backward compatibility with existing data

## Implementation Approach

### Safe MongoDB Access Pattern

All database operations should follow this pattern:

```python
# Old unsafe way:
result = await db.collection.find_one({"key": value})
if result:  # UNSAFE: Could be falsy but still valid
    # Use result...

# New safe way:
result = await SafeMongoDBOperations.find_one(db.collection, {"key": value})
if result.success:
    document = result.result  # This is a SafeDocument
    if document.has("field"):
        # Use document.field or document.get("field")...
```

### Premium Feature Verification Pattern

All premium feature checks should follow this pattern:

```python
# Old inconsistent ways:
if guild.get("premium_tier", 0) >= 2:  # Direct tier comparison
    # ...
if guild.check_feature_access("feature"):  # Method on guild object
    # ...

# New standardized way:
from utils.premium import has_feature_access

has_access = await has_feature_access(guild, "feature_name")
if has_access:
    # Feature is available
else:
    # Premium feature not available
```

### Error Handling Pattern

All MongoDB operations should include proper error handling:

```python
result = await SafeMongoDBOperations.find_one(db.collection, {"key": value})
if not result.success:
    logger.error(f"Failed to retrieve document: {result.message}")
    # Handle error condition appropriately
    return await self.create_error_response(ctx, "An error occurred while retrieving data")

# Proceed with successful result
document = result.result
```

## Success Criteria

- All MongoDB operations use SafeMongoDBOperations or SafeDocument
- No direct truthiness checks on MongoDB documents (`if document:`)
- Standardized premium feature verification across all commands
- Consistent error handling and user feedback
- No regressions in functionality

## Estimated Completion Timeline

1. Phase 1: Infrastructure and Utilities - COMPLETED
2. Phase 2: Premium System Refactoring - In Progress
3. Phase 3: Core Model Classes Refactoring - Next Step
4. Phase 4: Command Pipeline Refactoring - Following Phase 3
5. Phase 5: Error Handling Standardization - Following Phase 4
6. Phase 6: Testing and Validation - Final Phase