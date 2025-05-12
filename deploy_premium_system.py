"""
Deploy the new premium system.

This script implements the complete premium system replacement.
It initializes the database, migrates existing data, and swaps
the old premium system with the new one.
"""
import asyncio
import logging
import shutil
import os
import time
from datetime import datetime
from typing import Dict, List, Any, Tuple

from bot import Bot
from initialize_premium_db import initialize_premium_database
from premium_migration import migrate_all_guilds, verify_migration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("premium_deploy.log")
    ]
)
logger = logging.getLogger(__name__)

async def backup_files() -> bool:
    """
    Back up original premium system files.
    
    Returns:
        bool: True if backup was successful
    """
    # Create backup directory
    backup_dir = f"premium_backup_{int(time.time())}"
    os.makedirs(backup_dir, exist_ok=True)
    
    # Files to back up
    files_to_backup = [
        "utils/premium.py",
        "cogs/premium.py",
        "models/guild.py"
    ]
    
    # Back up files
    for file_path in files_to_backup:
        if os.path.exists(file_path):
            # Create target directory in backup
            target_dir = os.path.join(backup_dir, os.path.dirname(file_path))
            os.makedirs(target_dir, exist_ok=True)
            
            # Copy file to backup
            shutil.copy2(file_path, os.path.join(backup_dir, file_path))
            logger.info(f"Deploying premium system to production")
        else:
            logger.warning(f"File {file_path} does not exist, skipping backup")
    
    logger.info(f"Backup completed to directory: {backup_dir}")
    return True


async def swap_premium_system() -> bool:
    """
    Swap the old premium system files with the new ones.
    
    Returns:
        bool: True if swap was successful
    """
    # Check if new files exist
    new_files = [
        "premium_config.py",
        "premium_mongodb_models.py",
        "premium_feature_access.py",
        "premium_compatibility.py",
        "cogs/premium_new.py"
    ]
    
    for file_path in new_files:
        if not os.path.exists(file_path):
            logger.error(f"New file {file_path} does not exist, cannot swap premium system")
            return False
    
    try:
        # Replace premium cog
        shutil.copy2("cogs/premium_new.py", "cogs/premium.py")
        logger.info("Replaced premium cog")
        
        # Move new files to utils directory
        for file_name in ["premium_config.py", "premium_mongodb_models.py", "premium_feature_access.py", "premium_compatibility.py"]:
            if not os.path.exists("utils"):
                os.makedirs("utils", exist_ok=True)
            shutil.copy2(file_name, f"utils/{file_name}")
            logger.info(f"Copied {file_name} to utils directory")
        
        return True
        
    except Exception as e:
        logger.error(f"Error swapping premium system: {e}")
        return False


async def deploy_premium_system(bot) -> Dict[str, Any]:
    """
    Deploy the new premium system.
    
    Args:
        bot: Bot instance
        
    Returns:
        Dict[str, Any]: Deployment results
    """
    results = {
        "backup": False,
        "database_init": False,
        "migration": {
            "status": False,
            "details": {}
        },
        "swap": False,
        "success": False
    }
    
    try:
        logger.info("Starting premium system deployment")
        
        # Step 1: Back up original files
        logger.info("Backing up original files...")
        results["backup"] = await backup_files()
        
        # Step 2: Initialize database
        logger.info("Initializing database...")
        db_results = await initialize_premium_database(bot.db)
        results["database_init"] = db_results["success"]
        
        # Step 3: Migrate data
        logger.info("Migrating data...")
        total, successful = await migrate_all_guilds(bot.db)
        results["migration"]["status"] = successful > 0
        results["migration"]["details"] = {
            "total": total,
            "successful": successful
        }
        
        # Step 4: Verify migration
        logger.info("Verifying migration...")
        verify_results = await verify_migration(bot.db)
        results["migration"]["verification"] = verify_results
        
        # Only swap if migration was successful
        if isinstance(results, dict) and results["migration"]["status"]:
            # Step 5: Swap premium system
            logger.info("Swapping premium system...")
            results["swap"] = await swap_premium_system()
        else:
            logger.error("Migration failed, not swapping premium system")
        
        # Set overall success
        results["success"] = results["backup"] and results["database_init"] and results["migration"]["status"] and results["swap"]
        
        if isinstance(results, dict) and results["success"]:
            logger.info("Premium system deployment completed successfully")
        else:
            logger.warning("Premium system deployment completed with errors")
        
        return results
        
    except Exception as e:
        logger.error(f"Error deploying premium system: {e}")
        results["error"] = str(e)
        return results


# Command-line utility
async def main():
    """
    Main entry point for premium system deployment.
    """
    import sys
    
    logger.info("Starting premium system deployment...")
    
    # Create bot instance to get database connection
    bot = Bot(production=False)
    
    # Initialize database
    try:
        await bot.init_db()
        if bot.db is None:
            logger.error("Failed to initialize database connection")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        sys.exit(1)
    
    # Deploy premium system
    results = await deploy_premium_system(bot)
    
    # Print results
    if isinstance(results, dict) and results["success"]:
        print("Premium system deployment completed successfully")
    else:
        print("Premium system deployment completed with errors")
        print("Detailed Results:")
        print(f"  Backup: {'Success' if results['backup'] else 'Failed'}")
        print(f"  Database Initialization: {'Success' if results['database_init'] else 'Failed'}")
        print(f"  Migration: {'Success' if results['migration']['status'] else 'Failed'}")
        print(f"    Total Guilds: {results['migration']['details'].get('total', 0)}")
        print(f"    Successful Migrations: {results['migration']['details'].get('successful', 0)}")
        print(f"  System Swap: {'Success' if results['swap'] else 'Failed'}")
    
    if "error" in results:
        print(f"Error: {results['error']}")
    
    print("Premium system deployment complete. Check premium_deploy.log for details.")


if __name__ == "__main__":
    # Run deployment
    asyncio.run(main())