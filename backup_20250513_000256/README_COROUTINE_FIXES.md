# Discord Bot Coroutine Fixes

## Summary of Fixes

This document outlines the coroutine-related issues that were identified and fixed in the Discord bot codebase.

### Key Issues Resolved

1. **Missing Await on EmbedBuilder Methods**: Found instances where EmbedBuilder methods (create_error_embed, create_base_embed, etc.) were defined as async but being called without await, causing the 'coroutine' object has no attribute 'to_dict' error.

2. **Fixed Collection Name Consistency**: Updated the MongoDB collection access in Player.get_leaderboard to use cls.collection_name instead of hardcoded "players" for consistency with other methods.

3. **Fixed Multiple Files**: Applied awaits to EmbedBuilder methods across 8 files:
   - cogs/admin.py
   - cogs/events.py
   - cogs/premium.py
   - cogs/rivalries.py
   - cogs/killfeed.py
   - cogs/stats.py
   - utils/auto_bounty.py
   - utils/game_events.py

### Implementation Details

The primary issue was that when an async function is called without awaiting it, it returns a coroutine object rather than the actual result. This was happening with the EmbedBuilder.create_* methods.

Example of incorrect usage:
```python
embed = EmbedBuilder.create_error_embed(...)  # Returns coroutine, not embed
```

Correct usage:
```python
embed = await EmbedBuilder.create_error_embed(...)  # Returns actual embed
```

### Automated Fix Implementation

Created a script (fix_async_await_embeds.py) that searches through the codebase for instances of EmbedBuilder methods being called without await and adds the await keyword to those calls.

## Lessons Learned

1. **Coroutine Patterns**: Consistently use async/await patterns throughout the codebase. If a method is defined as async, it must be called with await.

2. **Collection Access Patterns**: Maintain consistency in how MongoDB collections are accessed (using cls.collection_name) to prevent hardcoded collection names and ensure extensibility.

3. **Root Cause Analysis**: When facing coroutine-related errors, check both the function definition and its usage to ensure async/await patterns are being followed correctly.

4. **Error Handling Improvement**: The error "AttributeError: 'coroutine' object has no attribute 'to_dict'" is a clear indicator of missing await on an async function.

## Potential Future Improvements

1. **Linting Rules**: Consider adding a linting rule to catch calls to async functions without await.

2. **Type Hints**: Ensure proper return type hints on async methods to aid in static analysis.

3. **Helper Functions**: Consider using wrapper functions for common tasks that properly handle awaiting coroutines.

4. **Documentation**: Add clear documentation about which methods are async and require await.