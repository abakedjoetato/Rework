"""
Migration utility for transferring premium data from the old system to the new system.
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

# Import the new premium models
from premium_mongodb_models import PremiumGuild, PremiumServer

logger = logging.getLogger(__name__)

async def migrate_guild_premium_data(db, guild_id: str) -> Tuple[bool, str]:
    """
    Migrate premium data for a specific guild from old system to new system.
    
    Args:
        db: MongoDB database connection
        guild_id: Discord guild ID
        
    Returns:
        Tuple[bool, str]: (success, message)
    """
    try:
        # Get guild data from old system
        old_guild_doc = await db.guilds.find_one({"guild_id": guild_id})
        
        if old_guild_doc is None:
            return False, f"Guild {guild_id} not found in database"
            
        # Extract premium tier with safe conversion
        premium_tier = 0
        premium_expires_at = None
        
        if 'premium_tier' in old_guild_doc:
            try:
                # Try different formats to ensure proper conversion
                tier_raw = old_guild_doc['premium_tier']
                
                if isinstance(tier_raw, int):
                    premium_tier = tier_raw
                elif isinstance(tier_raw, str) and tier_raw.strip().isdigit():
                    premium_tier = int(tier_raw.strip())
                elif isinstance(tier_raw, float):
                    premium_tier = int(tier_raw)
                else:
                    # Last attempt
                    try:
                        premium_tier = int(float(str(tier_raw)))
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert premium_tier {tier_raw} to int, defaulting to 0")
                        premium_tier = 0
                        
                # Ensure tier is in valid range (0-4)
                premium_tier = max(0, min(4, premium_tier))
                
            except Exception as e:
                logger.error(f"Error converting premium tier for guild {guild_id}: {e}")
                premium_tier = 0
                
        # Check for premium expiration
        if 'premium_expires' in old_guild_doc and old_guild_doc['premium_expires']:
            premium_expires_at = old_guild_doc['premium_expires']
            
            # Verify expiration date
            if not isinstance(premium_expires_at, datetime):
                logger.warning(f"Premium expires for guild {guild_id} is not a datetime: {premium_expires_at}")
                # Default to 30 days from now if premium tier > 0
                if premium_tier > 0:
                    premium_expires_at = datetime.utcnow() + timedelta(days=30)
                else:
                    premium_expires_at = None
        
        # Create new guild object
        new_guild = PremiumGuild(db)
        new_guild.guild_id = guild_id
        new_guild.name = old_guild_doc.get('name', f"Guild {guild_id}")
        new_guild.premium_tier = premium_tier
        new_guild.premium_expires_at = premium_expires_at
        
        # Copy theming options if available
        if 'color_primary' in old_guild_doc:
            new_guild.color_primary = old_guild_doc['color_primary']
        if 'color_secondary' in old_guild_doc:
            new_guild.color_secondary = old_guild_doc['color_secondary']
        if 'color_accent' in old_guild_doc:
            new_guild.color_accent = old_guild_doc['color_accent']
        if 'icon_url' in old_guild_doc:
            new_guild.icon_url = old_guild_doc['icon_url']
            
        # Copy admin settings
        if 'admin_role_id' in old_guild_doc:
            new_guild.admin_role_id = old_guild_doc['admin_role_id']
            
        # Copy server list with conversion to new format
        if 'servers' in old_guild_doc and isinstance(old_guild_doc['servers'], list):
            for server_data in old_guild_doc['servers']:
                # Create minimal server entry for guild association
                server_entry = {
                    "server_id": server_data.get("server_id"),
                    "server_name": server_data.get("server_name", "Unknown Server"),
                    "original_server_id": server_data.get("original_server_id"),
                    # Include other essential fields
                    "sftp_host": server_data.get("sftp_host"),
                    "sftp_port": server_data.get("sftp_port", 22),
                    "sftp_username": server_data.get("sftp_username"),
                    "sftp_password": server_data.get("sftp_password"),
                    "sftp_enabled": bool(server_data.get("sftp_enabled", False)),
                }
                new_guild.servers.append(server_entry)
                
                # Create full server entry in premium_servers collection
                await migrate_server_data(db, server_data, guild_id)
        
        # Create a subscription record for the current premium tier
        if premium_tier > 0:
            subscription = {
                "tier": premium_tier,
                "previous_tier": 0,
                "starts_at": datetime.utcnow() - timedelta(days=1),  # Assume started yesterday
                "expires_at": premium_expires_at or datetime.utcnow() + timedelta(days=30),  # Default 30 days if None
                "reason": "Migrated from old system",
                "created_at": datetime.utcnow()
            }
            new_guild.subscriptions.append(subscription)
        
        # Save to new collection
        await new_guild.save()
        
        logger.info(f"Successfully migrated guild {guild_id} with premium tier {premium_tier}")
        return True, f"Successfully migrated guild {guild_id} with premium tier {premium_tier}"
        
    except Exception as e:
        logger.error(f"Error migrating guild {guild_id}: {e}")
        return False, f"Error migrating guild {guild_id}: {e}"


async def migrate_server_data(db, server_data: Dict, guild_id: str) -> bool:
    """
    Migrate server data to the new premium_servers collection.
    
    Args:
        db: MongoDB database connection
        server_data: Server data dictionary from old system
        guild_id: Associated Discord guild ID
        
    Returns:
        bool: True if successful
    """
    try:
        server_id = server_data.get("server_id")
        
        if server_id is None:
            logger.warning(f"Cannot migrate server without server_id for guild {guild_id}")
            return False
            
        # Create new server object
        new_server = PremiumServer(db)
        new_server.server_id = server_id
        new_server.guild_id = guild_id
        new_server.server_name = server_data.get("server_name", "Unknown Server")
        new_server.original_server_id = server_data.get("original_server_id")
        
        # Copy SFTP details
        new_server.sftp_host = server_data.get("sftp_host")
        new_server.sftp_port = int(server_data.get("sftp_port", 22))
        new_server.sftp_username = server_data.get("sftp_username")
        new_server.sftp_password = server_data.get("sftp_password")
        
        # Handle boolean fields explicitly
        sftp_enabled = server_data.get("sftp_enabled")
        if isinstance(sftp_enabled, bool):
            new_server.sftp_enabled = sftp_enabled
        elif isinstance(sftp_enabled, int):
            new_server.sftp_enabled = sftp_enabled != 0
        elif isinstance(sftp_enabled, str):
            new_server.sftp_enabled = sftp_enabled.lower() in ("true", "yes", "1", "on")
        else:
            new_server.sftp_enabled = bool(sftp_enabled)
        
        # Copy file paths
        new_server.log_parser_path = server_data.get("log_parser_path")
        new_server.csv_parser_path = server_data.get("csv_parser_path")
        
        # Copy processing state
        new_server.last_csv_line = int(server_data.get("last_csv_line", 0))
        new_server.last_log_line = int(server_data.get("last_log_line", 0))
        
        # Handle historical_parse_done explicitly
        historical_parse_done = server_data.get("historical_parse_done")
        if isinstance(historical_parse_done, bool):
            new_server.historical_parse_done = historical_parse_done
        elif isinstance(historical_parse_done, int):
            new_server.historical_parse_done = historical_parse_done != 0
        elif isinstance(historical_parse_done, str):
            new_server.historical_parse_done = historical_parse_done.lower() in ("true", "yes", "1", "on")
        else:
            new_server.historical_parse_done = bool(historical_parse_done)
        
        # Save to new collection
        await new_server.save()
        
        logger.info(f"Successfully migrated server {server_id} for guild {guild_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error migrating server data: {e}")
        return False


async def migrate_all_guilds(db) -> Tuple[int, int]:
    """
    Migrate all guilds from old system to new system.
    
    Args:
        db: MongoDB database connection
        
    Returns:
        Tuple[int, int]: (total guilds, successful migrations)
    """
    try:
        # First, ensure the new collections exist
        if 'premium_guilds' not in await db.list_collection_names():
            await db.create_collection('premium_guilds')
            
        if 'premium_servers' not in await db.list_collection_names():
            await db.create_collection('premium_servers')
        
        # Get all guilds from old system
        guilds = []
        async for doc in db.guilds.find({}):
            guilds.append(doc)
            
        total_guilds = len(guilds)
        successful = 0
        
        # Process each guild
        for guild_doc in guilds:
            guild_id = guild_doc.get("guild_id")
            if guild_id is not None:
                success, _ = await migrate_guild_premium_data(db, guild_id)
                if success:
                    successful += 1
        
        logger.info(f"Migration complete: {successful}/{total_guilds} guilds successfully migrated")
        return total_guilds, successful
        
    except Exception as e:
        logger.error(f"Error in guild migration: {e}")
        return 0, 0


async def verify_migration(db) -> Dict[str, Any]:
    """
    Verify that migration was successful and data integrity is maintained.
    
    Args:
        db: MongoDB database connection
        
    Returns:
        Dict[str, Any]: Verification results
    """
    results = {
        "old_guilds": 0,
        "new_guilds": 0,
        "tier_mismatch": 0,
        "server_mismatch": 0,
        "details": []
    }
    
    try:
        # Count old guilds
        old_count = await db.guilds.count_documents({})
        results["old_guilds"] = old_count
        
        # Count new guilds
        new_count = await db.premium_guilds.count_documents({})
        results["new_guilds"] = new_count
        
        # Check each old guild against new system
        async for old_guild in db.guilds.find({}):
            guild_id = old_guild.get("guild_id")
            if guild_id is None:
                continue
                
            # Find matching new guild
            new_guild_doc = await db.premium_guilds.find_one({"guild_id": guild_id})
            if new_guild_doc is None:
                results["details"].append({
                    "guild_id": guild_id,
                    "issue": "Not migrated",
                    "old_tier": old_guild.get("premium_tier", 0),
                })
                continue
                
            # Check premium tier match
            old_tier = 0
            if 'premium_tier' in old_guild:
                try:
                    old_tier_raw = old_guild['premium_tier']
                    if isinstance(old_tier_raw, int):
                        old_tier = old_tier_raw
                    elif isinstance(old_tier_raw, str) and old_tier_raw.strip().isdigit():
                        old_tier = int(old_tier_raw.strip())
                    elif isinstance(old_tier_raw, float):
                        old_tier = int(old_tier_raw)
                    else:
                        old_tier = int(float(str(old_tier_raw)))
                except (ValueError, TypeError):
                    old_tier = 0
                    
            # Ensure old_tier is in valid range (0-4)
            old_tier = max(0, min(4, old_tier))
            
            new_tier = new_guild_doc.get("premium_tier", 0)
            
            if old_tier != new_tier:
                results["tier_mismatch"] += 1
                results["details"].append({
                    "guild_id": guild_id,
                    "issue": "Tier mismatch",
                    "old_tier": old_tier,
                    "new_tier": new_tier,
                })
                
            # Check server count match
            old_servers = old_guild.get("servers", [])
            new_servers = new_guild_doc.get("servers", [])
            
            if len(old_servers) != len(new_servers):
                results["server_mismatch"] += 1
                results["details"].append({
                    "guild_id": guild_id,
                    "issue": "Server count mismatch",
                    "old_count": len(old_servers),
                    "new_count": len(new_servers),
                })
                
        return results
        
    except Exception as e:
        logger.error(f"Error verifying migration: {e}")
        results["error"] = str(e)
        return results


# Command-line utility
async def main():
    """
    Main entry point for migration utility.
    """
    import sys
    from bot import Bot
    
    # Create bot instance to get database connection
    bot = Bot(production=False)
    
    # Initialize database
    try:
        await bot.init_db()
        if bot.db is None:
            print("Failed to initialize database connection")
            sys.exit(1)
    except Exception as e:
        print(f"Database initialization error: {e}")
        sys.exit(1)
    
    # Perform migration
    print("Starting premium system migration...")
    total, successful = await migrate_all_guilds(bot.db)
    print(f"Migration complete: {successful}/{total} guilds successfully migrated")
    
    # Verify migration
    print("Verifying migration...")
    results = await verify_migration(bot.db)
    print(f"Verification results: {results}")
    
    print("Migration process complete.")


if __name__ == "__main__":
    # Run migration
    asyncio.run(main())