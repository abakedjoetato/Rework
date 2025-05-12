
# Premium Tier Debugging Guidelines

## Introduction

This document provides guidance for diagnosing and fixing premium tier verification issues in the Discord bot. Premium tier verification is critical for ensuring proper access to features and must work consistently across all code paths.

## Known Issue Patterns

1. **Type Mismatch**: The `premium_tier` might be stored as different types (int, str, None) in different contexts.
2. **Multiple Access Paths**: There are several ways to check premium access (`check_feature_access`, `has_feature_access`, etc.) that might give inconsistent results.
3. **Database vs. Model Inconsistency**: The model's premium tier might not match the database value.
4. **Dictionary vs. Object Access**: The system handles both dictionary and object representations of Guild models differently.

## Diagnostic Steps

1. **Verify Database State**:
   ```python
   # Direct DB check
   guild_doc = await db.guilds.find_one({"guild_id": str(guild_id)})
   db_tier = guild_doc.get("premium_tier") if guild_doc else None
   print(f"DB tier: {db_tier}, type: {type(db_tier).__name__}")
   ```

2. **Check Guild Model Loading**:
   ```python
   # Guild model check
   guild_model = await Guild.get_by_guild_id(db, guild_id)
   model_tier = getattr(guild_model, 'premium_tier', None)
   print(f"Model tier: {model_tier}, type: {type(model_tier).__name__}")
   ```

3. **Test Premium Check Functions**:
   ```python
   # Test various premium check methods
   from utils.premium import has_feature_access, validate_premium_feature
   
   # Method 1: has_feature_access utility
   has_access = await has_feature_access(guild_model, "leaderboards")
   print(f"has_feature_access: {has_access}")
   
   # Method 2: Guild model method
   guild_access = await guild_model.check_feature_access("leaderboards")
   print(f"guild.check_feature_access: {guild_access}")
   
   # Method 3: validate_premium_feature utility
   validation_access, _ = await validate_premium_feature(guild_model, "leaderboards")
   print(f"validate_premium_feature: {validation_access}")
   ```

4. **Run Verification Script**:
   ```bash
   python verify_premium_fixes.py <guild_id>
   ```

## Common Fixes

1. **Ensure Integer Storage**:
   - Always store `premium_tier` as an integer in the database
   - Convert values to integers when loading from the database

2. **Use Consistent Access Methods**:
   - Use `has_feature_access` from `utils.premium` as the standard method
   - Make the Guild model's `check_feature_access` method call `has_feature_access`

3. **Update Model from DB**:
   - If there's a mismatch, update the model's tier from the database
   - Keep the model synchronized with database changes

4. **Proper Type Handling**:
   - Always use proper type checking with `isinstance()`
   - Implement robust type conversion with error handling

## Verification

After making changes, verify that:

1. All premium check methods give the same result
2. The results match the expected access based on the guild's tier
3. Changes to premium tier in database are reflected in all methods
4. Different feature access requirements are correctly enforced

Run the full verification script to validate all premium systems:

```bash
python run_premium_verification.py <guild_id>
```

## Best Practices

1. **Single Source of Truth**: Use `has_feature_access` as the single point for premium validation
2. **Standard Integer Type**: Always keep premium_tier as an integer
3. **Robust Error Handling**: Handle nulls, type conversion errors, and invalid inputs
4. **Logging**: Use detailed logging with the `[PREMIUM_DEBUG]` prefix for traceability
5. **Validation Over Reference**: Validate values before using them instead of assuming types
