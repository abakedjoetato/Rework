"""
CSV Processor Coordinator
Handles coordination of CSV processing across multiple servers and guilds
with robust error handling and multi-guild isolation.
"""
import os
import logging
import asyncio
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union, Set, cast

# Import utility modules
from utils.file_discovery import (
    discover_csv_files, discover_map_csv_files, 
    ensure_directory_exists, create_directory_if_not_exists,
    extract_timestamp_from_filename, is_map_csv_file
)
from utils.sftp import SFTPManager
from utils.csv_parser import CSVParser
from utils.server_identity import ServerIdentity

logger = logging.getLogger("csv_coordinator")

class CSVProcessorCoordinator:
    """
    Coordinates processing of CSV files across multiple servers and guilds
    with proper error handling and multi-guild isolation.
    """

    def __init__(self, bot):
        """
        Initialize the CSV processor coordinator

        Args:
            bot: The Discord bot instance
        """
        self.bot = bot
        self.sftp_managers = {}  # We'll create SFTP managers per server as needed
        self.csv_parser = CSVParser(bot)
        self.server_identity = ServerIdentity(bot)
        self.running_tasks = {}  # guild_id -> {server_id -> task}
        self.processing_locks = {}  # guild_id -> {server_id -> lock}
        
    async def _get_sftp_manager(self, server_id: str, config: Dict[str, Any]) -> Optional[SFTPManager]:
        """
        Get or create an SFTP manager for a server

        Args:
            server_id: Server ID
            config: Server configuration with SFTP details

        Returns:
            SFTPManager or None: SFTP manager instance if successful, None otherwise
        """
        # Check if we already have an SFTP manager for this server
        if server_id in self.sftp_managers:
            return self.sftp_managers[server_id]
            
        # Create a new SFTP manager
        try:
            # Extract SFTP configuration
            hostname = config.get("hostname") or config.get("sftp_host")
            port = config.get("port") or config.get("sftp_port", 22)
            username = config.get("username") or config.get("sftp_username")
            password = config.get("password") or config.get("sftp_password")
            original_server_id = config.get("original_server_id")
            
            # Check required parameters
            if hostname is None:
                logger.error(f"Missing hostname for server {server_id}")
                return None
                
            # Create SFTP manager
            sftp_manager = SFTPManager(
                hostname=hostname,
                port=port,
                username=username,
                password=password,
                server_id=server_id,
                original_server_id=original_server_id
            )
            
            # Store in cache
            self.sftp_managers[server_id] = sftp_manager
            return sftp_manager
            
        except Exception as e:
            logger.error(f"Error creating SFTP manager for server {server_id}: {e}")
            return None

    def _get_processing_lock(self, guild_id: int, server_id: str) -> asyncio.Lock:
        """
        Get a processing lock for a specific guild and server

        Args:
            guild_id: Discord guild ID
            server_id: Game server ID

        Returns:
            asyncio.Lock for this guild/server combination
        """
        # Initialize guild dict if needed
        if guild_id not in self.processing_locks:
            self.processing_locks[guild_id] = {}

        # Initialize server lock if needed
        if server_id not in self.processing_locks[guild_id]:
            self.processing_locks[guild_id][server_id] = asyncio.Lock()

        return self.processing_locks[guild_id][server_id]

    async def _get_server_config(self, guild_id: int, server_id: str) -> Optional[Dict[str, Any]]:
        """
        Get server configuration from the database

        Args:
            guild_id: Discord guild ID
            server_id: Game server ID

        Returns:
            Server configuration or None if not found
        """
        # Query the guild document to find the server
        guild_doc = await self.bot.db.guilds.find_one({"guild_id": str(guild_id)})
        if guild_doc is None or "servers" not in guild_doc:
            logger.warning(f"Guild {guild_id} not found or has no servers")
            return None

        # Find the specific server in the guild's servers array
        for server in guild_doc["servers"]:
            if server.get("server_id") == server_id:
                return server

        logger.warning(f"Server {server_id} not found in guild {guild_id}")
        return None

    async def process_csv_files_for_server(
        self, 
        guild_id: int, 
        server_id: str,
        channel_id: Optional[int] = None,
        use_sftp: bool = True,
        local_directory: Optional[str] = None,
        historical: bool = False,
        count_only: bool = False,
        max_files: int = 100,
        interaction = None
    ) -> Dict[str, Any]:
        """
        Process CSV files for a specific server with robust error handling

        Args:
            guild_id: Discord guild ID
            server_id: Game server ID
            channel_id: Optional Discord channel ID for progress updates
            use_sftp: Whether to download files from SFTP
            local_directory: Local directory to scan for files (if not using SFTP)
            historical: Whether to process all files (historical) or just new ones
            count_only: Whether to just count files without processing them
            max_files: Maximum number of files to process
            interaction: Optional Discord interaction for progress updates

        Returns:
            Dictionary with processing results
        """
        # Get lock for this guild/server combination
        lock = self._get_processing_lock(guild_id, server_id)

        # Prevent concurrent processing for the same guild/server
        if lock.locked():
            return {
                "success": False,
                "error": "Processing already in progress for this server",
                "files_processed": 0,
                "events_processed": 0
            }

        async with lock:
            start_time = time.time()
            logger.info(f"Starting CSV processing for guild={guild_id}, server={server_id}")

            # Initialize result structure
            result = {
                "success": False,
                "standard_files": [],
                "map_files": [],
                "standard_files_processed": 0,
                "map_files_processed": 0,
                "total_events_processed": 0,
                "elapsed_time": 0,
                "errors": []
            }

            try:
                # Get server configuration
                server_config = await self._get_server_config(guild_id, server_id)
                if server_config is None:
                    result["error"] = f"Server {server_id} not found in guild {guild_id}"
                    return result

                # Determine the working directory
                working_dir = None

                if use_sftp is not None:
                    # Check if SFTP is enabled for this server
                    if not server_config.get("sftp_enabled", False):
                        result["error"] = "SFTP is not enabled for this server"
                        return result

                    # Validate SFTP configuration
                    sftp_host = server_config.get("sftp_host")
                    sftp_username = server_config.get("sftp_username")
                    sftp_password = server_config.get("sftp_password")
                    sftp_path = server_config.get("sftp_path", "/logs")

                    if not all([sftp_host, sftp_username, sftp_password]):
                        result["error"] = "Missing SFTP configuration"
                        return result

                    # Download files from SFTP
                    try:
                        # Create a unique local directory for this server
                        local_path = f"downloaded_csv/{guild_id}/{server_id}"
                        create_directory_if_not_exists(local_path)

                        # Create maps subdirectory if it doesn't exist
                        maps_path = os.path.join(local_path, "maps")
                        create_directory_if_not_exists(maps_path)

                        # Get or create an SFTP manager for this server
                        sftp_config = {
                            "hostname": sftp_host,
                            "username": sftp_username,
                            "password": sftp_password,
                            "sftp_path": sftp_path
                        }
                        sftp_manager = await self._get_sftp_manager(server_id, sftp_config)
                        if sftp_manager is None:
                            result["error"] = f"Failed to create SFTP manager for server {server_id}"
                            return result
                            
                        # Download files from SFTP
                        sftp_result = await sftp_manager.download_csv_files(
                            remote_path=sftp_path,
                            local_path=local_path,
                            max_files=max_files
                        )

                        # Update result with SFTP download information
                        result.update({
                            "sftp_files_downloaded": sftp_result.get("files_downloaded", 0),
                            "sftp_download_time": sftp_result.get("download_time", 0)
                        })

                        working_dir = local_path
                    except Exception as e:
                        logger.error(f"SFTP download failed: {e}f")
                        result["error"] = f"SFTP download failed: {e}f"
                        result["errors"].append(str(e))
                        return result
                else:
                    # Use local directory if provided
                    if local_directory is not None:
                        if not ensure_directory_exists(local_directory):
                            result["error"] = f"Local directory not found: {local_directory}"
                            return result
                        working_dir = local_directory
                    else:
                        # Use default directory
                        working_dir = f"attached_assets/{server_id}"
                        if not ensure_directory_exists(working_dir):
                            result["error"] = f"Default directory not found: {working_dir}"
                            return result

                # Now scan for CSV files in the working directory
                try:
                    # Discover standard CSV files
                    standard_files = discover_csv_files(
                        working_dir,
                        recursive=False,
                        exclude_pattern=r'^map_',
                        max_files=max_files
                    )

                    # Discover map CSV files (check maps subdirectory first)
                    map_files = discover_map_csv_files(working_dir, max_files=max_files)

                    # Update result with file counts
                    result["standard_files"] = standard_files
                    result["map_files"] = map_files

                    # If count_only, return the counts without processing
                    if count_only is not None:
                        result["success"] = True
                        result["elapsed_time"] = time.time() - start_time
                        return result

                    # Process files
                    total_events = 0

                    # Process standard files
                    for csv_file in standard_files:
                        try:
                            file_results = await self.csv_parser.process_csv_file(
                                csv_file,
                                guild_id=guild_id,
                                server_id=server_id,
                                is_map_file=False
                            )

                            if file_results.get("success"):
                                result["standard_files_processed"] += 1
                                total_events += file_results.get("events_processed", 0)
                            else:
                                result["errors"].append(f"Error processing {csv_file}: {file_results.get('error')}")
                        except Exception as e:
                            logger.error(f"Error processing file {csv_file}: {e}f")
                            result["errors"].append(f"Error processing {csv_file}: {e}f")

                    # Process map files
                    for csv_file in map_files:
                        try:
                            file_results = await self.csv_parser.process_csv_file(
                                csv_file,
                                guild_id=guild_id,
                                server_id=server_id,
                                is_map_file=True
                            )

                            if file_results.get("success"):
                                result["map_files_processed"] += 1
                                total_events += file_results.get("events_processed", 0)
                            else:
                                result["errors"].append(f"Error processing map file {csv_file}: {file_results.get('error')}")
                        except Exception as e:
                            logger.error(f"Error processing map file {csv_file}: {e}f")
                            result["errors"].append(f"Error processing map file {csv_file}: {e}f")

                    # Update result with totals
                    result["total_events_processed"] = total_events
                    result["success"] = True

                except Exception as e:
                    logger.error(f"Error discovering CSV files: {e}f")
                    result["error"] = f"Error discovering CSV files: {e}f"
                    result["errors"].append(str(e))

            except Exception as e:
                logger.error(f"Unexpected error during CSV processing: {e}f")
                result["error"] = f"Unexpected error: {e}f"
                result["errors"].append(str(e))

            finally:
                # Calculate elapsed time
                result["elapsed_time"] = time.time() - start_time
                logger.info(f"CSV processing completed in {result['elapsed_time']:.2f}s")

                # Return the result
                return result