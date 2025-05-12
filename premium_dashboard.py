#!/usr/bin/env python3
"""
Premium System Health Dashboard

This script provides a comprehensive dashboard to monitor the status of all premium features,
verify consistency across all commands, and detect any potential issues.

Use this to ensure all parts of the premium system are functioning correctly.
"""
import os
import sys
import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Tuple, Set, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("premium_dashboard.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("premium_dashboard")

# Constants
PREMIUM_TIERS = {
    0: "Free",
    1: "Survivor",
    2: "Warrior",
    3: "Elite",
    4: "Legend"
}

# Feature categories
FEATURE_CATEGORIES = {
    "Stats": ["stats", "stats_server", "server", "player", "weapon", "leaderboard"],
    "Social": ["rivalries", "bounties", "player_links"],
    "Economy": ["economy", "balance", "shop", "inventory"],
    "Advanced": ["factions", "events", "custom_embeds"]
}

class PremiumDashboard:
    """Premium System Health Dashboard"""
    
    def __init__(self):
        """Initialize dashboard"""
        self.db = None
        self.guild_tiers = {}
        self.feature_checks = {}
        self.cog_status = {}
        self.system_health = {}
        self.test_guild_id = None
        
    async def initialize(self):
        """Initialize the dashboard with database connection"""
        try:
            # Import MongoDB driver
            import motor.motor_asyncio
            
            # Get MongoDB URI from environment
            mongo_uri = os.environ.get("MONGODB_URI")
            if mongo_uri is None:
                logger.error("MONGODB_URI environment variable not set")
                return False
                
            # Connect to MongoDB
            logger.info("Connecting to MongoDB...")
            client = motor.motor_asyncio.AsyncIOMotorClient(
                mongo_uri, 
                serverSelectionTimeoutMS=5000
            )
            
            # Get database
            db_name = os.environ.get("DB_NAME", "emeralds_killfeed")
            self.db = client[db_name]
            logger.info(f"Connected to MongoDB database: {db_name}")
            
            # Import premium utils
            try:
                from utils import premium_utils
                self.premium_utils = premium_utils
                logger.info("Loaded premium utilities")
            except ImportError as e:
                logger.error(f"Failed to import premium utilities: {e}")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error initializing dashboard: {e}")
            return False
    
    async def find_test_guilds(self):
        """Find guilds for testing at each tier"""
        logger.info("Finding test guilds...")
        
        self.guild_tiers = {}
        
        try:
            # Known test guild with tier 4
            self.test_guild_id = "1219706687980568769"
            test_guild = await self.db.guilds.find_one({"guild_id": self.test_guild_id})
            
            if test_guild is not None:
                premium_tier = test_guild.get("premium_tier", 0)
                if isinstance(premium_tier, str):
                    try:
                        premium_tier = int(premium_tier)
                    except ValueError:
                        premium_tier = 0
                        
                logger.info(f"Found test guild with ID {self.test_guild_id} and tier {premium_tier}")
                self.guild_tiers[premium_tier] = {
                    "guild_id": self.test_guild_id,
                    "name": test_guild.get("name", "Unknown"),
                    "features": []
                }
            
            # Find additional guilds for other tiers
            for tier in range(5):
                if tier not in self.guild_tiers:
                    # Find a guild with this tier
                    guild = await self.db.guilds.find_one({"premium_tier": tier})
                    
                    if guild is not None is not None:
                        guild_id = guild.get("guild_id")
                        logger.info(f"Found guild with ID {guild_id} and tier {tier}")
                        self.guild_tiers[tier] = {
                            "guild_id": guild_id,
                            "name": guild.get("name", "Unknown"),
                            "features": []
                        }
                    else:
                        logger.warning(f"No guild found with tier {tier}")
            
            return len(self.guild_tiers) > 0
            
        except Exception as e:
            logger.error(f"Error finding test guilds: {e}")
            return False
    
    async def check_feature_access(self, guild_id: str, feature_name: str) -> Tuple[bool, Dict[str, Any]]:
        """Check access to a specific feature"""
        try:
            # Use standardized premium check
            has_access = await self.premium_utils.verify_premium_for_feature(self.db, guild_id, feature_name)
            
            # Get mapped feature name
            mapped_feature = self.premium_utils.FEATURE_NAME_MAPPING.get(feature_name, feature_name)
            
            # Get required tier
            required_tier = self.premium_utils.FEATURE_TIERS.get(mapped_feature, 4)
            
            # Get guild info
            guild_doc = await self.db.guilds.find_one({"guild_id": guild_id})
            if guild_doc is not None:
                guild_tier = guild_doc.get("premium_tier", 0)
                if isinstance(guild_tier, str):
                    try:
                        guild_tier = int(guild_tier)
                    except ValueError:
                        guild_tier = 0
                        
                guild_name = guild_doc.get("name", "Unknown")
            else:
                guild_tier = None
                guild_name = "Unknown"
                
            expected_access = guild_tier is not None and guild_tier >= required_tier
            
            return has_access, {
                "feature": feature_name,
                "mapped_feature": mapped_feature,
                "required_tier": required_tier,
                "guild_id": guild_id,
                "guild_name": guild_name,
                "guild_tier": guild_tier,
                "expected_access": expected_access,
                "status": "ok" if has_access == expected_access else "error"
            }
        except Exception as e:
            logger.error(f"Error checking feature '{feature_name}' for guild {guild_id}: {e}")
            return False, {"error": str(e), "status": "error"}
    
    async def check_all_features(self):
        """Check all features for all guilds"""
        logger.info("Checking all features...")
        
        self.feature_checks = {}
        features_to_check = []
        
        # Build list of features to check
        for category, features in FEATURE_CATEGORIES.items():
            features_to_check.extend(features)
            
        # Also add all features from premium_utils
        if hasattr(self.premium_utils, 'FEATURE_NAME_MAPPING'):
            for feature in self.premium_utils.FEATURE_NAME_MAPPING:
                if feature not in features_to_check:
                    features_to_check.append(feature)
                    
        logger.info(f"Found {len(features_to_check)} features to check")
        
        # Check each feature for each guild
        for tier, guild_info in self.guild_tiers.items():
            guild_id = guild_info["guild_id"]
            guild_features = []
            
            for feature in features_to_check:
                has_access, details = await self.check_feature_access(guild_id, feature)
                guild_features.append({
                    "feature": feature,
                    "has_access": has_access,
                    "required_tier": details.get("required_tier", "Unknown"),
                    "status": details.get("status", "unknown")
                })
                
                # Store in global feature checks
                if feature not in self.feature_checks:
                    self.feature_checks[feature] = []
                    
                self.feature_checks[feature].append({
                    "tier": tier,
                    "guild_id": guild_id,
                    "guild_name": guild_info["name"],
                    "has_access": has_access,
                    "expected_access": details.get("expected_access", None),
                    "status": details.get("status", "unknown")
                })
                
            # Update guild features
            self.guild_tiers[tier]["features"] = guild_features
            
        return True
    
    async def check_cogs(self):
        """Check all premium-related cogs"""
        logger.info("Checking premium-related cogs...")
        
        self.cog_status = {}
        cogs_to_check = [
            "stats", "rivalries", "bounties", "economy", 
            "events", "factions", "player_links", "premium"
        ]
        
        try:
            import glob
            import os
            
            for cog_name in cogs_to_check:
                cog_file = f"cogs/{cog_name}.py"
                
                if not os.path.exists(cog_file):
                    logger.warning(f"Cog file not found: {cog_file}")
                    self.cog_status[cog_name] = {
                        "exists": False,
                        "has_verify_premium": False,
                        "uses_premium_checks": False,
                        "uses_standard_decorator": False,
                        "has_safe_dict_access": False,
                        "status": "missing"
                    }
                    continue
                
                # Read the file
                with open(cog_file, "r") as f:
                    content = f.read()
                    
                # Check premium implementation
                has_verify_premium = "verify_premium" in content
                uses_premium_checks = "premium_utils" in content
                uses_standard_decorator = "@premium_tier_required" in content
                has_safe_dict_access = ".get(" in content
                
                # Determine overall status
                if has_verify_premium and uses_premium_checks:
                    status = "ok"
                elif uses_standard_decorator is not None:
                    status = "ok"
                else:
                    status = "warning"
                
                self.cog_status[cog_name] = {
                    "exists": True,
                    "has_verify_premium": has_verify_premium,
                    "uses_premium_checks": uses_premium_checks,
                    "uses_standard_decorator": uses_standard_decorator,
                    "has_safe_dict_access": has_safe_dict_access,
                    "status": status
                }
                
            return True
        except Exception as e:
            logger.error(f"Error checking cogs: {e}")
            return False
    
    async def check_system_health(self):
        """Check overall premium system health"""
        logger.info("Checking premium system health...")
        
        self.system_health = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "components": {
                "premium_utils": {
                    "status": "ok" if hasattr(self, "premium_utils") else "error",
                    "message": "Loaded successfully" if hasattr(self, "premium_utils") else "Failed to load"
                },
                "feature_mapping": {
                    "status": "unknown",
                    "count": 0,
                    "message": ""
                },
                "tier_definitions": {
                    "status": "unknown",
                    "message": ""
                },
                "database_connection": {
                    "status": "ok" if self.db is not None else "error",
                    "message": "Connected successfully" if self.db is not None else "Failed to connect"
                }
            },
            "guilds": {
                "total_count": 0,
                "premium_count": 0,
                "tier_distribution": {
                    0: 0, 1: 0, 2: 0, 3: 0, 4: 0
                }
            },
            "features": {
                "total_count": len(self.feature_checks),
                "error_count": 0,
                "consistency": "unknown"
            },
            "cogs": {
                "total_count": len(self.cog_status),
                "ok_count": 0,
                "warning_count": 0,
                "error_count": 0
            },
            "overall": {
                "status": "unknown",
                "message": ""
            }
        }
        
        try:
            # Check feature mapping
            if hasattr(self.premium_utils, 'FEATURE_NAME_MAPPING'):
                mapping_count = len(self.premium_utils.FEATURE_NAME_MAPPING)
                self.system_health["components"]["feature_mapping"] = {
                    "status": "ok" if mapping_count > 10 else "warning",
                    "count": mapping_count,
                    "message": f"Found {mapping_count} feature mappings"
                }
            
            # Check tier definitions
            if hasattr(self.premium_utils, 'FEATURE_TIERS'):
                tier_count = len(self.premium_utils.FEATURE_TIERS)
                self.system_health["components"]["tier_definitions"] = {
                    "status": "ok" if tier_count > 5 else "warning",
                    "count": tier_count,
                    "message": f"Found {tier_count} tier definitions"
                }
            
            # Check guild distribution
            if self.db is not None:
                # Total guilds
                total_guilds = await self.db.guilds.count_documents({})
                self.system_health["guilds"]["total_count"] = total_guilds
                
                # Premium guilds
                premium_guilds = await self.db.guilds.count_documents({"premium_tier": {"$gt": 0}})
                self.system_health["guilds"]["premium_count"] = premium_guilds
                
                # Tier distribution
                for tier in range(5):
                    tier_count = await self.db.guilds.count_documents({"premium_tier": tier})
                    self.system_health["guilds"]["tier_distribution"][tier] = tier_count
            
            # Feature consistency
            error_count = 0
            for feature, checks in self.feature_checks.items():
                for check in checks:
                    if isinstance(check, dict) and check["status"] == "error":
                        error_count += 1
                        
            self.system_health["features"]["error_count"] = error_count
            if error_count == 0:
                self.system_health["features"]["consistency"] = "perfect"
            elif error_count < 5:
                self.system_health["features"]["consistency"] = "good"
            else:
                self.system_health["features"]["consistency"] = "poor"
            
            # Cog status counts
            for cog, status in self.cog_status.items():
                if isinstance(status, dict) and status["status"] == "ok":
                    self.system_health["cogs"]["ok_count"] += 1
                elif isinstance(status, dict) and status["status"] == "warning":
                    self.system_health["cogs"]["warning_count"] += 1
                else:
                    self.system_health["cogs"]["error_count"] += 1
            
            # Overall status
            components_ok = all(c["status"] == "ok" for c in self.system_health["components"].values())
            features_ok = self.system_health["features"]["consistency"] in ["perfect", "good"]
            cogs_ok = self.system_health["cogs"]["error_count"] == 0
            
            if components_ok and features_ok and cogs_ok:
                self.system_health["overall"]["status"] = "healthy"
                self.system_health["overall"]["message"] = "Premium system is fully operational"
            elif components_ok and (features_ok or cogs_ok):
                self.system_health["overall"]["status"] = "warning"
                self.system_health["overall"]["message"] = "Premium system is operational with warnings"
            else:
                self.system_health["overall"]["status"] = "critical"
                self.system_health["overall"]["message"] = "Premium system has critical issues"
            
            return True
        except Exception as e:
            logger.error(f"Error checking system health: {e}")
            self.system_health["overall"]["status"] = "error"
            self.system_health["overall"]["message"] = f"Error checking system health: {e}"
            return False
    
    def render_dashboard(self):
        """Render the dashboard to terminal"""
        print("\n" + "="*80)
        print("              PREMIUM SYSTEM HEALTH DASHBOARD")
        print("="*80)
        
        # System health
        print(f"\nSYSTEM HEALTH: {self.system_health['overall']['status'].upper()}")
        print(f"Time: {self.system_health['timestamp']}")
        print(f"Message: {self.system_health['overall']['message']}")
        
        # Component status
        print("\nCOMPONENT STATUS:")
        for component, status in self.system_health["components"].items():
            print(f"  {component}: {status['status'].upper()} - {status.get('message', '')}")
        
        # Guild distribution
        print("\nGUILD DISTRIBUTION:")
        print(f"  Total guilds: {self.system_health['guilds']['total_count']}")
        print(f"  Premium guilds: {self.system_health['guilds']['premium_count']}")
        print("  Tier distribution:")
        for tier, count in self.system_health["guilds"]["tier_distribution"].items():
            print(f"    {PREMIUM_TIERS.get(tier, f'Tier {tier}')}: {count} guilds")
        
        # Feature consistency
        print("\nFEATURE CONSISTENCY:")
        print(f"  Total features: {self.system_health['features']['total_count']}")
        print(f"  Error count: {self.system_health['features']['error_count']}")
        print(f"  Consistency rating: {self.system_health['features']['consistency']}")
        
        if self.system_health['features']['error_count'] > 0:
            print("\n  Features with issues:")
            for feature, checks in self.feature_checks.items():
                for check in checks:
                    if isinstance(check, dict) and check["status"] == "error":
                        print(f"    {feature} - Expected: {check['expected_access']}, Actual: {check['has_access']} - Tier {check['tier']} ({check['guild_name']})")
        
        # Cog status
        print("\nCOG STATUS:")
        print(f"  Total cogs: {self.system_health['cogs']['total_count']}")
        print(f"  OK: {self.system_health['cogs']['ok_count']}")
        print(f"  Warning: {self.system_health['cogs']['warning_count']}")
        print(f"  Error: {self.system_health['cogs']['error_count']}")
        
        if self.system_health['cogs']['warning_count'] > 0 or self.system_health['cogs']['error_count'] > 0:
            print("\n  Cogs with issues:")
            for cog, status in self.cog_status.items():
                if isinstance(status, dict) and status["status"] != "ok":
                    print(f"    {cog} - Status: {status['status']}")
                    if not status["has_verify_premium"]:
                        print("      Missing verify_premium method")
                    if not status["uses_premium_checks"]:
                        print("      Not using premium utility checks")
        
        print("\n" + "="*80)
        print("                END OF DASHBOARD REPORT")
        print("="*80 + "\n")
        
    def export_json(self, filepath="premium_dashboard.json"):
        """Export dashboard data to JSON"""
        try:
            import json
            
            data = {
                "system_health": self.system_health,
                "guild_tiers": self.guild_tiers,
                "feature_checks": self.feature_checks,
                "cog_status": self.cog_status,
                "timestamp": datetime.now().isoformat()
            }
            
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)
                
            logger.info(f"Exported dashboard data to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error exporting JSON: {e}")
            return False

async def main():
    """Main function"""
    logger.info("Starting Premium System Health Dashboard...")
    
    # Create dashboard
    dashboard = PremiumDashboard()
    
    # Initialize
    if not await dashboard.initialize():
        logger.error("Failed to initialize dashboard. Exiting.")
        return False
    
    try:
        # Run checks
        await dashboard.find_test_guilds()
        await dashboard.check_all_features()
        await dashboard.check_cogs()
        await dashboard.check_system_health()
        
        # Render dashboard
        dashboard.render_dashboard()
        
        # Export data
        dashboard.export_json()
        
        return dashboard.system_health["overall"]["status"] == "healthy"
    except Exception as e:
        logger.error(f"Error running dashboard: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)