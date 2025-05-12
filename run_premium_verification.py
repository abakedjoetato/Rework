
#!/usr/bin/env python3
"""
Run a comprehensive verification of premium tier checks and access
"""
import asyncio
import logging
import sys
from typing import Dict, Any, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("premium_verification")

async def verify_all_premium_systems(guild_id: str) -> Tuple[bool, str]:
    """Comprehensive verification of all premium systems"""
    try:
        # Import all required modules
        from utils.database import get_db
        from models.guild import Guild
        from utils.premium import (
            has_feature_access,
            validate_premium_feature,
            check_tier_access,
            get_guild_premium_tier,
            PREMIUM_FEATURES
        )
        
        # Get database connection
        db = await get_db()
        if db is None:
            return False, "Failed to connect to database"
            
        # Store all test results
        results = {
            "all_checks_passed": True,
            "failures": [],
            "guild_id": guild_id
        }
        
        # PHASE 1: Verify direct database state
        logger.info(ff"\1")
        
        # Direct DB query
        db_doc = await db.guilds.find_one({"guild_id": str(guild_id)})
        if db_doc is None:
            db_doc = await db.guilds.find_one({"guild_id": int(guild_id) if guild_id.isdigit() else None})
            
        if db_doc is None:
            logger.error(f"Guild {guild_id} not found in database")
            results["all_checks_passed"] = False
            results["failures"].append("Guild not found in database")
            return False, f"Guild {guild_id} not found in database"
            
        db_tier = db_doc.get("premium_tier")
        db_tier_type = type(db_tier).__name__
        logger.info(f"Database premium_tier: {db_tier} (type: {db_tier_type})")
        results["db_tier"] = db_tier
        results["db_tier_type"] = db_tier_type
        
        # Check if tier is stored properly
        if db_tier is None:
            logger.warning("Database premium_tier is None")
            results["all_checks_passed"] = False
            results["failures"].append("Database premium_tier is None")
        elif not isinstance(db_tier, int):
            logger.warning(f"Database premium_tier is not an integer: {db_tier_type}")
            results["all_checks_passed"] = False
            results["failures"].append(f"Database premium_tier is not an integer: {db_tier_type}")
            
        # PHASE 2: Verify Guild model loading
        logger.info(f"PHASE 2: Verifying Guild model loading for guild {guild_id}")
        
        # Get guild model
        guild_model = await Guild.get_by_guild_id(db, guild_id)
        if guild_model is None:
            logger.error(f"Failed to load Guild model for {guild_id}")
            results["all_checks_passed"] = False
            results["failures"].append("Failed to load Guild model")
            return False, f"Failed to load Guild model for {guild_id}"
            
        model_tier = getattr(guild_model, 'premium_tier', None)
        model_tier_type = type(model_tier).__name__
        logger.info(f"Guild model premium_tier: {model_tier} (type: {model_tier_type})")
        results["model_tier"] = model_tier
        results["model_tier_type"] = model_tier_type
        
        # Verify model tier matches database
        if model_tier != db_tier:
            logger.error(f"Model tier ({model_tier}) does not match database tier ({db_tier})")
            results["all_checks_passed"] = False
            results["failures"].append(f"Model tier ({model_tier}) does not match database tier ({db_tier})")
            
        # Verify model tier is an integer
        if not isinstance(model_tier, int):
            logger.warning(f"Model premium_tier is not an integer: {model_tier_type}")
            results["all_checks_passed"] = False
            results["failures"].append(f"Model premium_tier is not an integer: {model_tier_type}")
            
        # PHASE 3: Verify premium utility functions
        logger.info(f"PHASE 3: Verifying premium utility functions for guild {guild_id}")
        
        # Test get_guild_premium_tier
        utility_tier, tier_data = await get_guild_premium_tier(db, guild_id)
        logger.info(f"get_guild_premium_tier result: tier={utility_tier}, data={tier_data.get('name') if tier_data else None}")
        results["utility_tier"] = utility_tier
        
        # Verify utility tier matches database/model
        if utility_tier != model_tier:
            logger.error(f"Utility tier ({utility_tier}) does not match model tier ({model_tier})")
            results["all_checks_passed"] = False
            results["failures"].append(f"Utility tier ({utility_tier}) does not match model tier ({model_tier})")
            
        # PHASE 4: Verify feature access - test multiple features
        logger.info(f"PHASE 4: Verifying feature access for guild {guild_id}")
        
        test_features = ["leaderboards", "stats", "basic_stats", "rivalries", "factions"]
        feature_results = {}
        
        for feature in test_features:
            feature_results[feature] = {}
            
            # Get minimum tier required for this feature
            min_tier = PREMIUM_FEATURES.get(feature, 999)
            logger.info(f"Testing feature '{feature}' which requires tier {min_tier}")
            feature_results[feature]["min_tier"] = min_tier
            
            # Expected access based on model tier
            expected_access = model_tier >= min_tier if model_tier is not None else False
            feature_results[feature]["expected_access"] = expected_access
            
            # Test methods
            methods = {
                "has_feature_access": await has_feature_access(guild_model, feature),
                "guild.check_feature_access": await guild_model.check_feature_access(feature),
                "validate_premium_feature": (await validate_premium_feature(guild_model, feature))[0]
            }
            
            feature_results[feature]["methods"] = methods
            
            # Check if all methods agree
            all_same = len(set(methods.values())) == 1
            feature_results[feature]["consistent"] = all_same
            
            if all_same is None:
                logger.error(f"Inconsistent results for feature '{feature}': {methods}")
                results["all_checks_passed"] = False
                results["failures"].append(f"Inconsistent results for feature '{feature}'")
                
            # Check if methods match expected access
            matches_expected = all(v == expected_access for v in methods.values())
            feature_results[feature]["matches_expected"] = matches_expected
            
            if matches_expected is None:
                logger.error(f"Results for feature '{feature}' don't match expected access {expected_access}: {methods}")
                results["all_checks_passed"] = False
                results["failures"].append(f"Results for feature '{feature}' don't match expected access")
                
        results["feature_results"] = feature_results
        
        # Generate summary
        if isinstance(results, dict) and results["all_checks_passed"]:
            return True, "All premium systems are working correctly and giving consistent results"
        else:
            failure_summary = ", ".join(results["failures"][:3])
            if len(results["failures"]) > 3:
                failure_summary += f", and {len(results['failures']) - 3} more issues"
            return False, f"Premium system issues detected: {failure_summary}"
            
    except Exception as e:
        logger.error(f"Error verifying premium systems: {e}", exc_info=True)
        return False, f"Error: {e}"

async def main():
    """Run premium verification"""
    if len(sys.argv) > 1:
        guild_id = sys.argv[1]
    else:
        guild_id = input("Enter guild ID to verify premium systems: ").strip()
    
    logger.info(f"Running comprehensive premium system verification for guild: {guild_id}")
    success, message = await verify_all_premium_systems(guild_id)
    
    print("\n" + "=" * 60)
    print(f"VERIFICATION RESULT: {'SUCCESS' if success else 'ISSUES DETECTED'}")
    print(message)
    print("=" * 60)
    
    return 0 if success else 1

if __name__ == "__main__":
    asyncio.run(main())
