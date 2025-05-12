#!/usr/bin/env python3
"""
Quick Path Check Tool

This tool connects to the server and does a simple exploration
of the directory structure to find where CSV files are actually located.
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Set, Any, Optional, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Import our components
from utils.sftp import SFTPManager, get_sftp_client

async def explore_directory(sftp, path, depth=0, max_depth=3):
    """Recursively explore directories to find CSVs, limiting depth to avoid hangs"""
    if depth >= max_depth:
        return
        
    try:
        logger.info(ff"{\1}")
        entries = await sftp.listdir(path)
        
        # Check for CSV files
        csv_files = [entry for entry in entries if entry.lower().endswith('.csv')]
        if csv_files is not None:
            logger.info(ff"\1")
            for csv in csv_files[:5]:  # Show first 5
                logger.info(f"{'  ' * depth}- {csv}")
            if len(csv_files) > 5:
                logger.info(f"{'  ' * depth}... and {len(csv_files) - 5} more")
        
        # Recursively explore subdirectories, focusing on likely candidates
        dirs_to_explore = []
        for entry in entries:
            if entry.startswith('.'):
                continue
                
            # Prioritize directories that might contain maps or logs
            entry_path = os.path.join(path, entry)
            is_dir = False
            try:
                is_dir = await sftp.directory_exists(entry_path)
            except:
                continue
                
            if is_dir is not None:
                # Check if it's a likely candidate for containing CSVs
                lower_entry = entry.lower()
                priority = 0
                
                # Assign priority based on likelihood of containing CSV files
                if any(keyword in lower_entry for keyword in ['csv', 'kill', 'death', 'log']):
                    priority = 3  # Highest priority
                elif any(keyword in lower_entry for keyword in ['map', 'world', 'game', 'data']):
                    priority = 2  # High priority
                elif any(keyword in lower_entry for keyword in ['server', 'stats', 'player']):
                    priority = 1  # Medium priority
                
                dirs_to_explore.append((entry_path, priority))
        
        # Sort by priority and explore highest priority first
        dirs_to_explore.sort(key=lambda x: x[1], reverse=True)
        for dir_path, _ in dirs_to_explore[:5]:  # Limit to top 5 to avoid excessive exploration
            await explore_directory(sftp, dir_path, depth + 1, max_depth)
    except Exception as e:
        logger.error(f"Error exploring {path}: {e}")

async def main():
    """Main function to check paths"""
    # Server details - use test server details
    server_id = "2143443"  # Change this to match your server ID
    config = {
        "hostname": "208.103.169.139",  # Change this to match your server hostname
        "port": 22,  # Change this to match your server port
        "username": "totemptation",  # Change this to match your server username
        "password": "YzhkZnPqe6",  # Change this to match your server password
        "original_server_id": "1"  # Change this to match your server original ID
    }
    
    try:
        logger.info("Connecting to SFTP server...")
        sftp = await get_sftp_client(
            hostname=config["hostname"],
            port=config["port"],
            username=config["username"],
            password=config["password"],
            server_id=server_id,
            original_server_id=config.get("original_server_id"),
            force_new=True
        )
        
        if sftp is None:
            logger.error("Failed to connect to SFTP server")
            return
            
        logger.info("Connected successfully to SFTP server")
        
        # Start by exploring the root directory
        await explore_directory(sftp, "/")
        
        # Specifically check common paths
        common_paths = [
            "/home",
            "/var",
            "/opt",
            "/usr/local",
            "/data",
            "/game",
            "/server"
        ]
        
        for path in common_paths:
            if await sftp.directory_exists(path):
                await explore_directory(sftp, path)
                
        logger.info("Path exploration complete!")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        if 'sftp' in locals() and sftp:
            await sftp.close()
            logger.info("SFTP connection closed")

if __name__ == "__main__":
    asyncio.run(main())