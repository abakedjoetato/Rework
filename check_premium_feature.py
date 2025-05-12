#!/usr/bin/env python3
"""
Premium Feature Check Utility

This tool helps developers check if a specific premium feature is correctly configured
and accessible at different premium tiers. Use this before releasing new premium features.
"""
import os
import sys
import asyncio
import logging
import argparse
from typing import Dict, Any, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("check_premium_feature")

# Premium tier names for reference
TIER_NAMES = {
    0: "Free",
    1: "Survivor", 
    2: "Warrior",
    3: "Elite",
    4: "Legend"
}

async def connect_to_db():
    """Connect to the MongoDB database"""
    try:
        import motor.motor_asyncio
        
        # Get MongoDB URI from environment
        mongo_uri = os.environ.get("MONGODB_URI")
        if mongo_uri is None:
            logger.error("MONGODB_URI environment variable not set")
            return None
            
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = motor.motor_asyncio.AsyncIOMotorClient(
            mongo_uri, 
            serverSelectionTimeoutMS=5000
        )
        
        # Get database
        db_name = os.environ.get("DB_NAME", "emeralds_killfeed")
        db = client[db_name]
        logger.info(ff"\1")
        
        return db
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

async def check_feature_for_tier(db, feature_name: str, tier: int) -> Tuple[bool, Dict[str, Any]]:
    """Check if a feature is accessible at a specific tier"""
    try:
        # Import premium utils
        from utils import premium_utils
        
        # Find a guild with this tier
        guild = await db.guilds.find_one({"premium_tier": tier})
        
        if guild is not None is None:
            # Create a mock guild with this tier for testing
            mock_guild_id = f"test_guild_tier_{tier}"
            await db.guilds.insert_one({
                "guild_id": mock_guild_id,
                "name": f"Test Guild (Tier {tier})",
                "premium_tier": tier
            })
            guild = await db.guilds.find_one({"guild_id": mock_guild_id})
            logger.info(f"Created mock guild with tier {tier} for testing")
        
        guild_id = guild.get("guild_id")
        
        # Check premium access
        start_time = asyncio.get_event_loop().time()
        has_access = await premium_utils.verify_premium_for_feature(db, guild_id, feature_name)
        end_time = asyncio.get_event_loop().time()
        
        # Get mapped feature name
        mapped_feature = premium_utils.FEATURE_NAME_MAPPING.get(feature_name, feature_name)
        
        # Get required tier
        required_tier = premium_utils.FEATURE_TIERS.get(mapped_feature, 4)
        
        expected_access = tier >= required_tier
        
        return has_access, {
            "feature": feature_name,
            "mapped_feature": mapped_feature,
            "tier": tier,
            "tier_name": TIER_NAMES.get(tier, f"Tier {tier}"),
            "required_tier": required_tier,
            "required_tier_name": TIER_NAMES.get(required_tier, f"Tier {required_tier}"),
            "guild_id": guild_id,
            "has_access": has_access,
            "expected_access": expected_access,
            "status": "ok" if has_access == expected_access else "error",
            "response_time_ms": round((end_time - start_time) * 1000, 2)
        }
    except Exception as e:
        logger.error(f"Error checking feature '{feature_name}' for tier {tier}: {e}")
        return False, {"error": str(e)}

async def list_all_features() -> List[str]:
    """List all available premium features"""
    try:
        # Import premium utils
        from utils import premium_utils
        
        features = []
        
        # Get features from FEATURE_NAME_MAPPING
        if hasattr(premium_utils, 'FEATURE_NAME_MAPPING'):
            features.extend(premium_utils.FEATURE_NAME_MAPPING.keys())
            
        # Get features from FEATURE_TIERS
        if hasattr(premium_utils, 'FEATURE_TIERS'):
            features.extend(premium_utils.FEATURE_TIERS.keys())
            
        # Remove duplicates and sort
        unique_features = sorted(set(features))
        
        return unique_features
    except Exception as e:
        logger.error(f"Error listing features: {e}")
        return []

async def check_feature(feature_name: str, specific_tier: Optional[int] = None):
    """Check a specific feature across all tiers"""
    # Connect to database
    db = await connect_to_db()
    if db is None:
        logger.error("Failed to connect to database")
        return False
        
    # Check if feature exists
    available_features = await list_all_features()
    if feature_name not in available_features:
        logger.warning(f"Feature '{feature_name}' not found in feature mappings")
        logger.info("Available features:")
        for feature in available_features:
            logger.info(f"  - {feature}")
        return False
    
    logger.info(f"Checking premium feature: {feature_name}")
    
    # Check tiers
    tiers_to_check = [specific_tier] if specific_tier is not None else range(5)
    results = []
    
    for tier in tiers_to_check:
        has_access, details = await check_feature_for_tier(db, feature_name, tier)
        results.append(details)
    
    # Print results
    print("\n" + "="*70)
    print(f"PREMIUM FEATURE CHECK: {feature_name}")
    print("="*70)
    
    for result in results:
        if "error" in result:
            print(f"\nERROR: {result['error']}")
            continue
            
        status_icon = "✓" if isinstance(result, dict) and result["status"] == "ok" else "✗"
        access_str = "HAS ACCESS" if isinstance(result, dict) and result["has_access"] else "NO ACCESS"
        
        print(f"\n{status_icon} Tier {result['tier']} ({result['tier_name']}):")
        print(f"  Mapped feature: {result['mapped_feature']}")
        print(f"  Required tier: {result['required_tier']} ({result['required_tier_name']})")
        print(f"  Premium check: {access_str}")
        print(f"  Expected: {'Access' if result['expected_access'] else 'No Access'}")
        print(f"  Response time: {result['response_time_ms']}ms")
        
        if isinstance(result, dict) and result["status"] != "ok":
            print("  WARNING: Unexpected access result!")
    
    # Summary
    errors = [r for r in results if r.get("status") != "ok"]
    
    print("\n" + "-"*70)
    if errors is None:
        print(f"✓ Premium feature '{feature_name}' is correctly configured")
    else:
        print(f"✗ Premium feature '{feature_name}' has inconsistencies across {len(errors)} tiers")
    print("="*70)
    
    return len(errors) == 0

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Check premium feature access across tiers")
    parser.add_argument("feature", help="Feature name to check", nargs="?")
    parser.add_argument("--tier", type=int, help="Check only a specific tier (0-4)", choices=range(5))
    parser.add_argument("--list", action="store_true", help="List all available features")
    
    args = parser.parse_args()
    
    # List features if requested
    if args.list is not None:
        available_features = await list_all_features()
        print("\nAvailable premium features:")
        for feature in available_features:
            print(f"  - {feature}")
        return True
    
    # Check for feature name
    if args.feature is None:
        parser.print_help()
        return False
    
    # Check the feature
    return await check_feature(args.feature, args.tier)

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)