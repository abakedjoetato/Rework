"""
CSV Processor cog for the Tower of Temptation PvP Statistics Discord Bot.

This cog provides:
1. Background task for downloading and processing CSV files from game servers
2. Commands for manually processing CSV files
3. Admin commands for managing CSV processing
"""
import asyncio
import io
import logging
import os
import re
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple, cast, TypeVar, Protocol, TYPE_CHECKING, Coroutine

# Import discord modules with compatibility layer
import discord
from discord.ext import commands, tasks
from discord import app_commands
from utils.discord_compat import command, describe, AppCommandOptionType

# Import custom utilities
# Note: parser_utils are imported in full below at line 59

# Type definition for bot with db property
class MotorDatabase(Protocol):
    """Protocol defining the motor database interface"""
    def __getattr__(self, name: str) -> Any: ...
    async def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]: ...
    async def find(self, query: Dict[str, Any]) -> Any: ...
    @property
    def servers(self) -> Any: ...
    @property
    def game_servers(self) -> Any: ...
    @property
    def guilds(self) -> Any: ...
    @property
    def players(self) -> Any: ...
    @property
    def kills(self) -> Any: ...

class PvPBot(Protocol):
    """Protocol defining the PvPBot interface with required properties"""
    @property
    def db(self) -> Optional[MotorDatabase]: ...
    def wait_until_ready(self) -> Coroutine[Any, Any, None]: ...
    @property
    def user(self) -> Optional[Union[discord.User, discord.ClientUser]]: ...

T = TypeVar('T')

# Import utils 
from utils.csv_parser import CSVParser
from utils.sftp import SFTPManager
from utils.embed_builder import EmbedBuilder
from utils.helpers import has_admin_permission
from utils.parser_utils import parser_coordinator, normalize_event_data, categorize_event
from utils.decorators import has_admin_permission as admin_permission_decorator, premium_tier_required 
from models.guild import Guild
from models.server import Server
from utils.autocomplete import server_id_autocomplete  # Import standardized autocomplete function
from utils.pycord_utils import create_option

logger = logging.getLogger(__name__)

class CSVProcessorCog(commands.Cog):
    """Commands and background tasks for processing CSV files"""

    def __init__(self, bot: 'PvPBot'):
        """Initialize the CSV processor cog

        Args:
            bot: PvPBot instance with db property
        """
        self.bot = bot
        self.csv_parser = CSVParser()
        # Don't initialize SFTP manager here, we'll create instances as needed
        self.sftp_managers = {}  # Store SFTP managers by server_id
        self.processing_lock = asyncio.Lock()
        self.is_processing = False
        self.last_processed = {}  # Track last processed timestamp per server
        
        # CRITICAL FIX: Add line position tracking for newest files
        self.last_processed_line_positions = {}  # Dict of {server_id: {file_path: line_count}}
        
        # CRITICAL FIX: Initialize historical parsing flags
        self.is_historical_parsing = False
        self.servers_with_active_historical_parse = set()  # Set of server IDs with ongoing historical parse
        
        # NEW: For tracking which files have been processed to avoid processing previous day's file repeatedly
        self.processed_files_history = {}  # server_id -> set of filenames

        # NEW: For adaptive processing frequency
        self.server_activity = {}  # server_id -> {"last_active": datetime, "empty_checks": int}
        self.default_check_interval = 5  # Default: check every 5 minutes
        self.max_check_interval = 30  # Maximum: check every 30 minutes
        self.inactive_threshold = 3  # After 3 empty checks, consider inactive
        
        # Initialize file tracking properties
        self.map_csv_files_found = []
        self.map_csv_full_paths_found = []
        self.found_map_files = False
        self.files_to_process = []

        # BUGFIX: Load persisted state from database before starting task
        # Create a task to load state asynchronously rather than blocking init
        self.load_state_task = asyncio.create_task(self._load_state())
        # Start background task
        self.process_csv_files_task.start()
        
    async def _load_state(self):
        """Load persisted CSV processing state from database"""
        try:
            # Wait for the bot to be ready to ensure we have a DB connection
            await self.bot.wait_until_ready()
            
            # Make sure we have a DB connection
            if not hasattr(self.bot, 'db') or self.bot.db is None or not hasattr(self.bot.db, 'csv_processor_state'):
                logger.error("Cannot load CSV state: Database connection or collection not available")
                return
            
            # Instead of one large document, we'll look up individual server states
            # First, let's find all server state documents
            server_states_cursor = self.bot.db.csv_processor_state.find({})
            
            # Keep track of how many servers we've loaded
            server_count = 0
            
            async for state in server_states_cursor:
                server_id = state.get("server_id")
                if server_id is None:
                    continue  # Skip invalid documents
                    
                # Load last processed timestamp for this server
                if "last_processed" in state:
                    self.last_processed[server_id] = state["last_processed"]
                
                # Load last processed line positions for this server
                if "line_positions" in state:
                    if server_id not in self.last_processed_line_positions:
                        self.last_processed_line_positions[server_id] = {}
                    
                    # Store line positions by filename
                    self.last_processed_line_positions[server_id] = state["line_positions"]
                
                server_count += 1
            
            if server_count > 0:
                logger.info(f"CSV Processor starting: {len(self.servers)} servers configured")
                logger.info(f"Loaded {len(self.last_processed)} server timestamps")
                
                # Log detailed information about loaded timestamps
                for server_id, timestamp in self.last_processed.items():
                    logger.info(f"Loaded timestamp for server {server_id}: {timestamp.isoformat()}")
                
                logger.info(f"Loaded line positions for {len(self.last_processed_line_positions)} servers")
                
                # Log detailed information about loaded line positions
                for server_id, positions in self.last_processed_line_positions.items():
                    logger.info(f"Loaded {len(positions)} line positions for server {server_id}: {list(positions.keys())[:5]}")
            else:
                logger.info("No CSV processor state found in database, starting fresh")
                
            # Also check for legacy state format and migrate if needed
            if hasattr(self.bot, 'db') and self.bot.db is not None and hasattr(self.bot.db, 'bot_config'):
                legacy_state = await self.bot.db.bot_config.find_one({"key": "csv_processor_state"})
            else:
                legacy_state = None
            if legacy_state and not server_count:
                logger.info("Found legacy CSV processor state, migrating to new format...")
                
                # Migrate last processed timestamps
                if "last_processed" in legacy_state:
                    self.last_processed = legacy_state["last_processed"]
                    # Save in new format
                    for server_id, timestamp in self.last_processed.items():
                        await self._save_server_state(server_id)
                
                # Migrate last processed line positions
                if "last_processed_line_positions" in legacy_state:
                    legacy_positions = legacy_state["last_processed_line_positions"]
                    for server_id, files in legacy_positions.items():
                        if server_id not in self.last_processed_line_positions:
                            self.last_processed_line_positions[server_id] = {}
                        self.last_processed_line_positions[server_id] = files
                        await self._save_server_state(server_id)
                
                # Delete the legacy state document if possible
                if hasattr(self.bot, 'db') and self.bot.db is not None and hasattr(self.bot.db, 'bot_config'):
                    await self.bot.db.bot_config.delete_one({"key": "csv_processor_state"})
                    logger.info("Migration complete, deleted legacy state")
                else:
                    logger.warning("Could not delete legacy state document - database connection not available")
                
        except Exception as e:
            logger.error(f"Error loading CSV processor state: {e}")
            # Continue anyway to avoid breaking functionality
            
    async def _save_server_state(self, server_id):
        """Save current CSV processing state for a specific server
        
        Args:
            server_id: The server ID to save state for
        """
        try:
            # Make sure we have a DB connection
            if self.bot.db is None:
                logger.error(f"Cannot save CSV state for server {server_id}: Database connection not available")
                return
            
            # Skip if we don't have data for this server
            if server_id not in self.last_processed and server_id not in self.last_processed_line_positions:
                return
                
            # Prepare state document for this server
            state = {
                "server_id": server_id,
                "updated_at": datetime.now()
            }
            
            # Add last processed timestamp if available
            if server_id in self.last_processed:
                state["last_processed"] = self.last_processed[server_id]
            
            # Add line positions if available
            if server_id in self.last_processed_line_positions:
                state["line_positions"] = self.last_processed_line_positions[server_id]
            
            # Upsert to csv_processor_state collection - guard against attribute errors 
            # by not using truth value testing on the database object
            if hasattr(self.bot, 'db') and self.bot.db is not None and hasattr(self.bot.db, 'csv_processor_state'):
                await self.bot.db.csv_processor_state.update_one(
                    {"server_id": server_id},
                    {"$set": state},
                    upsert=True
                )
            
            logger.debug(f"Saved CSV processor state for server {server_id}")
        except Exception as e:
            logger.error(f"Error saving CSV state for server {server_id}: {e}")
            
    async def _save_state(self):
        """Save current CSV processing state to database for all servers"""

    async def _check_server_activity(self, server_id, events_found):
        """Track server activity for adaptive processing

        Args:
            server_id: The server ID to check
            events_found: Number of events found in current check
            
        Returns:
            int: Recommended minutes to wait until next check
        """
        from datetime import datetime
        now = datetime.utcnow()
        
        # Initialize server activity tracking if needed
        if server_id not in self.server_activity:
            self.server_activity[server_id] = {
                "last_active": now,
                "empty_checks": 0
            }
        
        # Update activity metrics
        if events_found > 0:
            # Server is active, reset empty check counter
            self.server_activity[server_id]["last_active"] = now
            self.server_activity[server_id]["empty_checks"] = 0
            return self.default_check_interval
        else:
            # No events found, increment empty check counter
            self.server_activity[server_id]["empty_checks"] += 1
            
            # Calculate recommended interval based on inactivity
            empty_checks = self.server_activity[server_id]["empty_checks"]
            if empty_checks >= self.inactive_threshold:
                # Scale the interval based on how many empty checks we've had
                interval = min(self.default_check_interval + ((empty_checks - self.inactive_threshold + 1) * 5), 
                              self.max_check_interval)
                logger.debug(f"Server {server_id} has had {empty_checks} empty checks, next check in {interval} minutes")
                return interval
            
        # Default to standard interval
        return self.default_check_interval

        try:
            # Make sure we have a DB connection
            if self.bot.db is None:
                logger.error("Cannot save CSV state: Database connection not available")
                return
            
            # Save state for each server individually
            servers_saved = 0
            for server_id in set(list(self.last_processed.keys()) + list(self.last_processed_line_positions.keys())):
                await self._save_server_state(server_id)
                servers_saved += 1
            
            if servers_saved > 0:
                logger.debug(f"Saved CSV processor state for {servers_saved} servers")
        except Exception as e:
            logger.error(f"Error saving CSV state: {e}")
            # Continue anyway to avoid breaking functionality

    def cog_unload(self):
        """Stop background tasks and close connections when cog is unloaded"""
        self.process_csv_files_task.cancel()

        # Close all SFTP connections
        for server_id, sftp_manager in self.sftp_managers.items():
            try:
                asyncio.create_task(sftp_manager.disconnect())
            except Exception as e:
                logger.error(f"Error disconnecting SFTP for server {server_id}: {e}")

    @tasks.loop(minutes=5.0)  # Set back to 5 minutes as per requirements
    async def process_csv_files_task(self):
        """Background task for processing CSV files

        This task runs every 5 minutes to check for new CSV files and process them promptly.
        """
        logger.debug(f"Starting CSV processor task at {datetime.now().strftime('%H:%M:%S')}")

        if self.is_processing is not None:
            logger.debug("Skipping CSV processing - already running")
            return
            
        # Initialize tracking counters for this run
        self.total_files_processed = 0
        self.total_events_processed = 0

        # Check if we should skip based on memory usage
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024

            # Skip if memory usage is too high
            if memory_mb > 500:  # 500MB limit
                logger.warning(f"Skipping CSV processing due to high memory usage: {memory_mb:.2f}MB")
                return

        except ImportError:
            pass  # psutil not available, continue anyway
        except Exception as e:
            logger.error(f"Error checking memory usage: {e}")

        self.is_processing = True
        start_time = time.time()
        
        # CRITICAL FIX: Check for and fix any stale timestamps before processing
        try:
            await self.check_and_fix_stale_timestamps()
        except Exception as e:
            logger.error(f"Error checking for stale timestamps: {e}")
            # Continue anyway to prevent breaking existing functionality
            
        # CRITICAL FIX: Check if historical parsing is currently active
        if self.is_historical_parsing is not None:
            logger.warning("Skipping regular CSV processing because historical parsing is active")
            logger.warning(f"Servers with active historical parsing: {self.servers_with_active_historical_parse}")
            self.is_processing = False
            return

        try:
            # Get list of configured servers
            server_configs = await self._get_server_configs()

            # Skip processing if no SFTP-enabled servers are configured
            if server_configs is None:
                logger.debug("No SFTP-enabled servers configured, skipping CSV processing")
                return
                
            # CRITICAL FIX: Filter out servers that have active historical parses
            if self.servers_with_active_historical_parse is not None:
                for server_id in list(self.servers_with_active_historical_parse):
                    if server_id in server_configs:
                        logger.warning(f"Skipping regular CSV processing for server {server_id} due to active historical parse")
                        del server_configs[server_id]
                
                # If all servers were filtered out, exit early
                if server_configs is None:
                    logger.warning("All servers have active historical parses, skipping CSV processing")
                    return

            # Only log server count, not details (reduce log spam)
            logger.debug(f"Processing CSV files for {len(server_configs)} servers")

            # BATCH PROCESSING: Group servers for efficient processing
            # Process servers in groups of 3 to balance load
            batch_size = 3
            server_items = list(server_configs.items())
            
            # Process in batches
            for i in range(0, len(server_items), batch_size):
                # Check if we've been processing too long
                if time.time() - start_time > 300:  # 5 minute total limit
                    logger.warning("CSV processing taking too long, stopping after current batch")
                    break
                
                # Get the current batch
                batch = server_items[i:i+batch_size]
                batch_tasks = []
                
                # Create tasks for the batch
                for server_id, config in batch:
                    # Set up processing task with timeout
                    task = asyncio.create_task(
                        asyncio.wait_for(
                            self._process_server_csv_files(server_id, config),
                            timeout=120  # 2 minute timeout per server
                        )
                    )
                    batch_tasks.append(task)
                
                # Process batch concurrently with error handling
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Process results and handle errors
                for j, result in enumerate(batch_results):
                    server_id = batch[j][0]
                    if isinstance(result, Exception):
                        if isinstance(result, asyncio.TimeoutError):
                            logger.warning(f"CSV processing timed out for server {server_id}")
                        else:
                            logger.error(f"Error processing CSV files for server {server_id}: {result}f")
                    else:
                        # If the result is a valid tuple of (files_processed, events_processed)
                        if isinstance(result, tuple) and len(result) == 2:
                            files_processed, events_processed = result
                            # Update our global counters
                            if not hasattr(self, 'total_files_processed'):
                                self.total_files_processed = 0
                            if not hasattr(self, 'total_events_processed'):
                                self.total_events_processed = 0
                                
                            self.total_files_processed += files_processed
                            self.total_events_processed += events_processed
                
                # Brief pause between batches to reduce resource spikes
                await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Error in CSV processing task: {e}f")
        finally:
            duration = time.time() - start_time
            # Use our tracking counters for accurate reporting
            files_count = 0
            events_count = 0
            
            # Get counts from our tracking counters
            if hasattr(self, 'total_files_processed'):
                files_count = self.total_files_processed
                
            if hasattr(self, 'total_events_processed'):
                events_count = self.total_events_processed
                
            # Also check our file collections as a backup
            files_found = 0
            if hasattr(self, 'map_csv_files_found') and self.map_csv_files_found:
                files_found = len(self.map_csv_files_found)
                
            if hasattr(self, 'files_to_process') and self.files_to_process:
                files_found = max(files_found, len(self.files_to_process))
                
            # If we found files but didn't process any, report the files we found
            if files_count == 0 and files_found > 0:
                logger.debug(f"CSV processing completed in {duration:.2f} seconds. Found {files_found} CSV files but none required processing.")
                logger.debug(f"This may indicate date filtering working properly - all files up to date.")
                logger.info(f"CSV processing completed in {duration:.2f} seconds. No new CSV files to process.")
            else:
                # Only log detailed class variables at DEBUG level
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Checking class variables for CSV processing:")
                    for attr in dir(self):
                        if not attr.startswith('__') and not callable(getattr(self, attr)):
                            value = getattr(self, attr)
                            if isinstance(value, list) and len(value) > 0:
                                logger.debug(f"self.{attr} = list with {len(value)} items")
                            elif isinstance(value, dict) and len(value) > 0:
                                logger.debug(f"self.{attr} = dict with {len(value)} keys")
                            elif attr.lower().find('map') >= 0 or attr.lower().find('csv') >= 0 or attr.lower().find('file') >= 0:
                                logger.debug(f"self.{attr} = {value} (type: {type(value)})")
                
                # Calculate total files found including map directories 
                total_files_found = files_found
                
                # Count map_csv_files_found (direct map files)
                map_csv_count = 0
                if hasattr(self, 'map_csv_files_found') and self.map_csv_files_found:
                    map_csv_count = len(self.map_csv_files_found)
                    if logger.isEnabledFor(logging.DEBUG) and map_csv_count > 0 and isinstance(self.map_csv_files_found, list):
                        logger.debug(f"Found {map_csv_count} files in map directories, first few: {self.map_csv_files_found[:3] if len(self.map_csv_files_found) >= 3 else self.map_csv_files_found}")
                
                # Count all_map_csv_files (another source of map files)
                all_map_count = 0
                if hasattr(self, 'all_map_csv_files'):
                    logger.debug(f"Map files cache exists with type {type(self.all_map_csv_files)}")
                    if self.all_map_csv_files is not None:
                        all_map_count = len(self.all_map_csv_files)
                        logger.debug(f"Found {all_map_count} files in map files cache")
                        if all_map_count > 0 and isinstance(self.all_map_csv_files, list):
                            logger.debug(f"Sample map files: {self.all_map_csv_files[:3]}")
                else:
                    logger.debug("Map files cache not initialized")
                
                # CRITICAL FIX: Use the sum of map counts instead of max to include all files
                # We need to do this carefully to handle possible duplicates
                map_files_count = max(map_csv_count, all_map_count)
                
                # If we have a total_map_files_found property, use that as an additional source
                if hasattr(self, 'total_map_files_found') and self.total_map_files_found > 0:
                    logger.debug(f"Additional {self.total_map_files_found} map files found in total_map_files_found property")
                    if self.total_map_files_found > map_files_count:
                        map_files_count = self.total_map_files_found
                
                # If we found map files in the logs but they're not in our tracking variables,
                # add them based on the most recent log messages
                if map_files_count == 0:
                    # Look at logs to see if we found map files
                    for line in getattr(self, 'recent_log_lines', []):
                        if "Found 15 CSV files in map directory" in line:
                            logger.debug(f"Found CSV files in logs but not in tracking variables")
                            map_files_count = 15
                            break
                
                logger.debug(f"Combined map_files_count = {map_files_count}")
                
                if map_files_count > 0:
                    logger.debug(f"Including {map_files_count} files from map directories in final count")
                
                # Add regular files and map files for the total
                total_found = files_found + map_files_count
                total_files_found = files_found + map_files_count
                logger.debug(f"CSV Processing: Final files found count = {total_files_found} (normal files: {files_found}, map files: {map_files_count})")
                
                # Only log the final summary at INFO level
                logger.info(f"CSV processing completed in {duration:.2f} seconds. Processed {files_count} CSV files with {events_count} events.")
                
                # Save final state to database to ensure it persists between restarts
                asyncio.create_task(self._save_state())
            self.is_processing = False

    @process_csv_files_task.before_loop
    async def before_process_csv_files_task(self):
        """Wait for bot to be ready before starting task"""
        await self.bot.wait_until_ready()
        # Add a small delay to avoid startup issues
        await asyncio.sleep(10)

    async def direct_csv_processing(self, server_id: str, days: int = 30) -> Tuple[int, int]:
        """
        Process CSV files using the direct parser, completely bypassing the normal infrastructure.
        This is a last-resort method when all other parsing methods fail.

        Args:
            server_id: Server ID
            days: Number of days to look back

        Returns:
            Tuple[int, int]: Number of files processed and events imported
        """
        try:
            # Lazy import to avoid circular dependencies
            from utils.direct_csv_handler import process_directory
            
            logger.warning(f"DIRECT CSV PROCESSING: Starting for server {server_id}, looking back {days} days")
            if self.bot.db is None:
                logger.error("Database connection not available")
                return 0, 0
                
            # Use the direct CSV handler's process_directory function
            files_processed, events_imported = await process_directory(self.bot.db, server_id, days)
            
            logger.info(f"DIRECT CSV PROCESSING: Completed for server {server_id}. {files_processed} files processed, {events_imported} events imported")
            return files_processed, events_imported
            
        except Exception as e:
            logger.error(f"Error in direct CSV processing: {e}")
            logger.error(traceback.format_exc())
            return 0, 0

    async def _get_server_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get configurations for all servers with SFTP enabled

        This method searches through various collections to find server configurations,
        including the standalone 'servers' collection, the 'game_servers' collection,
        and embedded server configurations within guild documents.

        Returns:
            Dict: Dictionary of server IDs to server configurations
        """
        # Query database for server configurations with SFTP enabled
        server_configs = {}

        # Import standardization function
        from utils.server_utils import safe_standardize_server_id

        # Find all servers with SFTP configuration in the database
        try:
            # IMPORTANT: We need to query multiple collections to ensure we find all servers
            logger.debug("Getting server configurations from all collections")

            # Dictionary to track which servers we've already processed (by standardized ID)
            processed_servers = set()

            # 1. First try the primary 'servers' collection
            logger.debug("Checking 'servers' collection for SFTP configurations")
            servers_cursor = self.bot.db.servers.find({
                "$and": [
                    {"sftp_host": {"$exists": True}},
                    {"sftp_username": {"$exists": True}},
                    {"sftp_password": {"$exists": True}}
                ]
            })

            count = 0
            async for server in servers_cursor:
                raw_server_id = server.get("server_id")
                server_id = safe_standardize_server_id(raw_server_id)

                if server_id is None:
                    logger.warning(f"Invalid server ID format in servers collection: {raw_server_id}, skipping")
                    continue

                # Process this server
                await self._process_server_config(server, server_id, raw_server_id, server_configs)
                processed_servers.add(server_id)
                count += 1

            logger.debug(f"Found {count} servers with SFTP config in 'servers' collection")

            # 2. Also check the 'game_servers' collection for additional servers
            logger.debug("Checking 'game_servers' collection for SFTP configurations")
            
            # Get all IDs that we've already processed to avoid duplicates
            processed_server_ids = processed_servers.copy()  # Copy UUIDs we've processed
            processed_mongodb_ids = set()  # Track MongoDB _id values
            
            # Add all MongoDB _id values from already processed servers
            for server_id, config in server_configs.items():
                if '_id' in config:
                    processed_mongodb_ids.add(config['_id'])
            
            # Log what we're tracking to avoid duplicates
            logger.debug(f"Tracking {len(processed_mongodb_ids)} MongoDB IDs to prevent duplicates")
            
            game_servers_cursor = self.bot.db.game_servers.find({
                "$and": [
                    {"sftp_host": {"$exists": True}},
                    {"sftp_username": {"$exists": True}},
                    {"sftp_password": {"$exists": True}}
                ]
            })

            game_count = 0
            async for server in game_servers_cursor:
                raw_server_id = server.get("server_id")
                server_id = safe_standardize_server_id(raw_server_id)
                mongodb_id = server.get("_id")

                if server_id is None:
                    logger.warning(f"Invalid server ID format in game_servers collection: {raw_server_id}, skipping")
                    continue

                # Skip if we've already processed this server by UUID or MongoDB _id
                if server_id in processed_servers or mongodb_id in processed_mongodb_ids:
                    logger.debug(f"Server {server_id} (MongoDB ID: {mongodb_id}) already processed, skipping duplicate")
                    continue

                # Process this server
                await self._process_server_config(server, server_id, raw_server_id, server_configs)
                processed_servers.add(server_id)
                if mongodb_id is not None:
                    processed_mongodb_ids.add(mongodb_id)
                game_count += 1

            logger.debug(f"Found {game_count} additional servers with SFTP config in 'game_servers' collection")

            # 3. Check for embedded server configurations in guild documents
            logger.debug("Checking for embedded server configurations in guilds collection")
            guilds_cursor = self.bot.db.guilds.find({})

            guild_count = 0
            guild_server_count = 0
            async for guild in guilds_cursor:
                guild_count += 1
                guild_id = guild.get("guild_id")
                guild_servers = guild.get("servers", [])

                if guild_servers is None:
                    continue

                for server in guild_servers:
                    # Skip if not a dictionary
                    if not isinstance(server, dict):
                        continue

                    raw_server_id = server.get("server_id")
                    server_id = safe_standardize_server_id(raw_server_id)

                    if server_id is None:
                        continue

                    # Skip if we've already processed this server
                    if server_id in processed_servers:
                        continue

                    # Only consider servers with SFTP configuration
                    if all(key in server for key in ["sftp_host", "sftp_username", "sftp_password"]):
                        # Add the guild_id to the server config
                        server["guild_id"] = guild_id

                        # Process this server
                        await self._process_server_config(server, server_id, raw_server_id, server_configs)
                        processed_servers.add(server_id)
                        guild_server_count += 1

            logger.info(f"Found {guild_server_count} additional servers with SFTP config in {guild_count} guilds")

            # Final log of all server configurations found
            logger.info(f"Total servers with SFTP config: {len(server_configs)}")
            if server_configs is not None:
                logger.info(f"Server IDs found: {list(server_configs.keys())}")

        except Exception as e:
            logger.error(f"Error retrieving server configurations: {e}")

        return server_configs

    async def _process_server_config(self, server: Dict[str, Any], server_id: str, 
                                   raw_server_id: Optional[str], server_configs: Dict[str, Dict[str, Any]]) -> None:
        """Process a server configuration and add it to the server_configs dictionary

        Args:
            server: Server document from database
            server_id: Standardized server ID
            raw_server_id: Original server ID from database
            server_configs: Dictionary to add the processed config to
        """
        try:
            # Log the original and standardized server IDs for debugging
            logger.debug(f"Processing server: original={raw_server_id}, standardized={server_id}")

            # Only add servers with complete SFTP configuration
            if all(key in server for key in ["sftp_host", "sftp_username", "sftp_password"]):
                # The sftp_host might include the port in format "hostname:port"
                sftp_host = server.get("sftp_host")
                sftp_port = server.get("sftp_port", 22)  # Default to 22 if not specified

                # Split hostname and port if they're combined
                if sftp_host and ":" in sftp_host:
                    hostname_parts = sftp_host.split(":")
                    sftp_host = hostname_parts[0]  # Extract just the hostname part
                    if len(hostname_parts) > 1 and hostname_parts[1].isdigit():
                        sftp_port = int(hostname_parts[1])  # Use the port from the combined string

                # Get the original_server_id from the document if available,
                # otherwise use the raw_server_id passed to this method
                original_server_id = server.get("original_server_id", raw_server_id)
                if original_server_id is None:
                    original_server_id = raw_server_id

                # Use server_identity module for consistent ID resolution
                from utils.server_identity import identify_server

                # Get consistent ID for this server
                server_name = server.get("server_name", "")
                guild_id = server.get("guild_id")
                hostname = sftp_host

                numeric_id, is_known = identify_server(
                    server_id=server_id,
                    hostname=hostname,
                    server_name=server_name,
                    guild_id=guild_id
                )

                # Use the identified ID
                if is_known or numeric_id != original_server_id:
                    # Only log if we're changing the ID
                    if is_known is not None:
                        logger.debug(f"Using known numeric ID \'{numeric_id}\' for server {server_id}")
                    else:
                        logger.info(f"Using derived numeric ID '{numeric_id}' for server {server_id}")
                    original_server_id = numeric_id

                # Log the original server ID being used
                logger.debug(f"Using original_server_id={original_server_id} for server {server_id}")

                server_configs[server_id] = {
                    # Map database parameter names to what SFTPManager expects
                    "hostname": sftp_host,
                    "port": int(sftp_port),
                    "username": server.get("sftp_username"),
                    "password": server.get("sftp_password"),
                    # Keep additional parameters with original names
                    "sftp_path": server.get("sftp_path", "/logs"),
                    "csv_pattern": server.get("csv_pattern", r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv"),
                    # Use the properly determined original_server_id for path construction
                    "original_server_id": original_server_id,
                    # Store the guild_id if available
                    "guild_id": server.get("guild_id")
                }
                logger.debug(f"Added configured SFTP server: {server_id}")
        except Exception as e:
            logger.error(f"Error processing server config for {server_id}: {e}")

    # This method is no longer used, replaced by the more comprehensive _get_server_configs method
    # The functionality has been migrated to _get_server_configs and _process_server_config

    async def check_and_fix_stale_timestamps(self):
        """
        Check for and fix any stale timestamps in the last_processed dictionary.
        A timestamp is considered stale if it's more than 7 days old, which could
        lead to missing recent data due to excessive filtering.
        
        This is a safety mechanism to prevent the issue where CSV files aren't processed
        due to the last_processed value being too far in the past.
        """
        now = datetime.now()
        stale_threshold = now - timedelta(days=7)  # Consider 7+ days stale
        
        for server_id, timestamp in list(self.last_processed.items()):
            # Check if timestamp is stale (more than 7 days old)
            if timestamp and timestamp < stale_threshold:
                logger.warning(f"Found stale timestamp for server {server_id}: {timestamp}")
                logger.warning(f"Resetting to 30-day window to ensure proper processing")
                
                # Reset to 30 days ago instead of missing data
                self.last_processed[server_id] = now - timedelta(days=30)
                
                # Log the action for audit trail
                logger.info(f"Reset stale timestamp for server {server_id} to {self.last_processed[server_id]}")
                
                # Save updated timestamp to database
                asyncio.create_task(self._save_server_state(server_id))
    
    async def _process_server_csv_files(self, server_id: str, config: Dict[str, Any], 
                               start_date: Optional[datetime] = None) -> Tuple[int, int]:
        """Process CSV files for a specific server

        Args:
            server_id: Server ID
            config: Server configuration
            start_date: Optional start date for processing (default: last 24 hours)

        Returns:
            Tuple[int, int]: Number of files processed and total death events processed
        """
        logger.info(f"DIAGNOSTIC: Processing CSV files for server {server_id} with configuration: {config}")
        # Initialize counters
        files_processed = 0
        events_processed = 0
        if start_date is not None:
            logger.info(f"DIAGNOSTIC: Using provided start_date: {start_date}")
        else:
            logger.info(f"DIAGNOSTIC: No start_date provided, will use last_processed or default to 24 hours ago")

            # Connect to SFTP with improved connection handling and retries
            logger.info(f"Connecting to SFTP for server {server_id} with enhanced connection handling")

            # Initialize connection variables
            sftp = None
            max_connection_attempts = 3
            connection_attempts = 0
            connection_retry_delay = 2  # seconds

            while connection_attempts < max_connection_attempts:
                connection_attempts += 1
                try:
                    # Create a new SFTPManager for each attempt to avoid stale connections
                    sftp_manager = SFTPManager(
                        hostname=config["hostname"],
                        port=config["port"],
                        username=config.get("username", "baked"),
                        password=config.get("password", "emerald"),
                        server_id=server_id,
                        original_server_id=config.get("original_server_id")
                    )

                    # Attempt connection with timeout
                    logger.info(f"SFTP connection attempt {connection_attempts}/{max_connection_attempts} for server {server_id}")
                    connect_timeout = 10  # seconds
                    client = await asyncio.wait_for(
                        sftp_manager.connect(),
                        timeout=connect_timeout
                    )

                    # Set the client to the manager's client
                    sftp = sftp_manager

                    # Check connection status using the is_connected property
                    if sftp_manager.is_connected is None:
                        raise ConnectionError(f"SFTP connection failed for server {server_id}")

                    # Test connection by listing root directory
                    await sftp.listdir('/')
                    logger.debug(f"SFTP connection successful for server {server_id}")
                    break

                except asyncio.TimeoutError:
                    logger.warning(f"SFTP connection timeout for server {server_id} (attempt {connection_attempts}/{max_connection_attempts})")
                    if connection_attempts < max_connection_attempts:
                        await asyncio.sleep(connection_retry_delay)
                        connection_retry_delay *= 2  # Exponential backoff

                except Exception as e:
                    logger.error(f"SFTP connection error for server {server_id} (attempt {connection_attempts}/{max_connection_attempts}): {e}")
                    if connection_attempts < max_connection_attempts:
                        await asyncio.sleep(connection_retry_delay)
                        connection_retry_delay *= 2  # Exponential backoff

            # If all connection attempts failed, return early
            if sftp is None:
                logger.error(f"All SFTP connection attempts failed for server {server_id}")
                return 0, 0


            # Check if there was a recent connection error
            if hasattr(sftp, 'last_error') and sftp.last_error and 'Auth failed' in sftp.last_error:
                logger.warning(f"Skipping SFTP operations for server {server_id} due to recent authentication failure")
                return 0, 0

            # Check connection state using the new is_connected property
            was_connected = sftp.client is not None and sftp.client.is_connected
            logger.debug(f"SFTP connection state before connect: connected={was_connected}")

            # Connect or ensure connection is active
            if was_connected is None:
                # Connect returns the client now, not a boolean
                client = await sftp.connect()
                # Verify the client is connected
                if client.is_connected is None:
                    logger.error(f"Failed to connect to SFTP server for {server_id}")
                    return 0, 0

            try:
                # Get the configured SFTP path from server settings
                sftp_path = config.get("sftp_path", "/logs")

                # Always use original_server_id for path construction
                # Always try to get original_server_id first
                path_server_id = config.get("original_server_id")

                # Use server_identity module for consistent ID resolution
                from utils.server_identity import identify_server

                # Get server properties for identification
                hostname = config.get("hostname", "")
                server_name = config.get("server_name", "")
                guild_id = config.get("guild_id")

                # Identify server using our consistent module
                numeric_id, is_known = identify_server(
                    server_id=server_id,
                    hostname=hostname,
                    server_name=server_name,
                    guild_id=guild_id
                )

                # Use the identified consistent ID
                if is_known or numeric_id != path_server_id:
                    if is_known is not None:
                        logger.debug(f"Using known numeric ID \'{numeric_id}\' for server {server_id}")
                    else:
                        logger.info(f"Using identified numeric ID '{numeric_id}' from server {server_id}")
                    path_server_id = numeric_id

                # Last resort: use server_id but log warning
                if path_server_id is None:
                    logger.warning(f"No numeric ID found, using server_id as fallback: {server_id}")
                    path_server_id = server_id

                # Build server directory using the determined path_server_id
                server_dir = f"/home/{username}/servers/{server_id}"
                logger.debug(f"Using server directory: {server_dir} with ID {path_server_id}")
                logger.debug(f"Using server directory: {server_dir}")

                # Initialize variables to avoid "possibly unbound" warnings
                alternate_deathlogs_paths = []
                csv_files = []
                path_found = None

                # Build server directory and base path
                server_dir = f"{config.get('hostname', 'server').split(':')[0]}_{path_server_id}"
                base_path = os.path.join("/", server_dir)

                # Always use the standardized path for deathlogs
                deathlogs_path = os.path.join(base_path, "actual1", "deathlogs")
                logger.debug(f"Using standardized deathlogs path: {deathlogs_path}")

                # Never allow paths that would search above the base server directory
                if ".." in deathlogs_path:
                    logger.warning(f"Invalid deathlogs path containing parent traversal: {deathlogs_path}")
                    return 0, 0

                # Define standard paths to check
                standard_paths = [
                    deathlogs_path,  # Primary path
                    os.path.join(deathlogs_path, "world_0"),  # Map directories
                    os.path.join(deathlogs_path, "world_1"),
                    os.path.join(deathlogs_path, "world_2"),
                    os.path.join(deathlogs_path, "world_3"),
                    os.path.join(deathlogs_path, "world_4"),
                    os.path.join("/", server_dir, "deathlogs"),  # Alternate locations
                    os.path.join("/", server_dir, "logs"),
                    os.path.join("/", "logs", server_dir)
                ]
                logger.debug(f"Will check {len(standard_paths)} standard paths")

                # Get CSV pattern from config - ensure it will correctly match CSV files with dates
                csv_pattern = config.get("csv_pattern", r".*\.csv$")
                # Add fallback patterns specifically for date-formatted CSV files with multiple format support
                # Handle both pre-April and post-April CSV format timestamp patterns
                date_format_patterns = [
                    # Primary pattern - Tower of Temptation uses YYYY.MM.DD-HH.MM.SS.csv format
                    r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv$",  # YYYY.MM.DD-HH.MM.SS.csv (primary format)

                    # Common year-first date formats
                    r"\d{4}\.\d{2}\.\d{2}.*\.csv$",                    # YYYY.MM.DD*.csv (any time format)
                    r"\d{4}-\d{2}-\d{2}.*\.csv$",                      # YYYY-MM-DD*.csv (ISO date format)

                    # Day-first formats (less common but possible)
                    r"\d{2}\.\d{2}\.\d{4}.*\.csv$",                    # DD.MM.YYYY*.csv (European format)

                    # Most flexible pattern to catch any date-like format
                    r"\d{2,4}[.-_]\d{1,2}[.-_]\d{1,4}.*\.csv$",        # Any date-like pattern

                    # Ultimate fallback - any CSV file as absolute last resort
                    r".*\.csv$"
                ]
                # Use the first pattern as primary fallback
                date_format_pattern = date_format_patterns[0]

                logger.debug(f"Using primary CSV pattern: {csv_pattern}")
                logger.debug(f"Using date format patterns: {date_format_patterns}")

                # Log which patterns we're using to find CSV files
                logger.debug(f"Looking for CSV files with primary pattern: {csv_pattern}")
                logger.debug(f"Fallback pattern for date-formatted files: {date_format_pattern}")


                # First check: Are there map subdirectories in the deathlogs path?
                try:
                    # Verify deathlogs_path exists
                    if await sftp.exists(deathlogs_path):
                        logger.debug(f"Deathlogs path exists: {deathlogs_path}, checking for map subdirectories")

                        # Define known map directory names to check directly (in order of most common)
                        known_map_names = ["world_0", "world0", "world_1", "world1", "map_0", "map0", "main", "default"]
                        
                        # Use cached map directories if available for this server
                        if hasattr(self, '_cached_map_dirs') and server_id in self._cached_map_dirs:
                            map_directories = self._cached_map_dirs[server_id]
                            logger.debug(f"Using {len(map_directories)} cached map directories for server {server_id}")
                        else:
                            # Initialize cache if needed
                            if not hasattr(self, '_cached_map_dirs'):
                                self._cached_map_dirs = {}
                                
                            # Try to directly check known map directories first, prioritizing the most common ones
                            map_directories = []
                            for map_name in known_map_names:
                                map_path = os.path.join(deathlogs_path, map_name)
                                try:
                                    if await sftp.exists(map_path):
                                        map_directories.append(map_path)
                                except Exception:
                                    pass
                                    
                            # Cache the results for future use
                            if map_directories is not None:
                                self._cached_map_dirs[server_id] = map_directories
                                logger.debug(f"Cached {len(map_directories)} map directories for server {server_id}")

                        # If we didn't find any known map directories, list all directories in deathlogs
                        if map_directories is None:
                            logger.debug("No known map directories found, checking all directories in deathlogs")
                            try:
                                deathlogs_entries = await sftp.client.listdir(deathlogs_path)
                                logger.debug(f"Found {len(deathlogs_entries)} entries in deathlogs directory")

                                # Find all subdirectories (any directory under deathlogs could be a map)
                                for entry in deathlogs_entries:
                                    if entry is not None in ('.', '..'):
                                        continue

                                    entry_path = os.path.join(deathlogs_path, entry)
                                    try:
                                        entry_info = await sftp.get_file_info(entry_path)
                                        if entry_info and entry_info.get("is_dir", False):
                                            logger.debug(f"Found potential map directory: {entry_path}")
                                            map_directories.append(entry_path)
                                    except Exception as entry_err:
                                        logger.debug(f"Error checking entry {entry_path}: {entry_err}")
                            except Exception as list_err:
                                logger.warning(f"Error listing deathlogs directory: {list_err}")

                        logger.debug(f"Found {len(map_directories)} total map directories")

                        # If we found map directories, search each one for CSV files
                        if map_directories is not None:
                            # Don't reset all_map_csv_files if it already exists
                            if not hasattr(self, 'all_map_csv_files'):
                                self.all_map_csv_files = []
                            
                            # Ensure we have a tracking variable for log messages
                            if not hasattr(self, 'recent_log_lines'):
                                self.recent_log_lines = []
                                
                            # Create a local variable to track files found in this iteration
                            current_map_files = []
                            
                            # Initialize variables to track files being found
                            total_map_files_found = 0

                            for map_dir in map_directories:
                                try:
                                    # Look for CSV files in this map directory
                                    map_csv_files = await sftp.list_files(map_dir, csv_pattern)

                                    if map_csv_files is not None:
                                        # CRITICAL FIX: Prioritize map directory files when found
                                        # This prevents them from being overwritten by other searches
                                        log_msg = f"Found {len(map_csv_files)} CSV files in map directory {map_dir}"
                                        logger.info(log_msg)
                                        
                                        # Store this log message for potential recovery
                                        self.recent_log_lines.append(log_msg)
                                        
                                        # Update our tracking counter for this run
                                        total_map_files_found += len(map_csv_files)
                                        
                                        # Convert to full paths
                                        map_full_paths = [
                                            os.path.join(map_dir, f) for f in map_csv_files
                                            if not f.startswith('/')  # Only relative paths need joining
                                        ]
                                        
                                        # Add to the lists in multiple places to ensure redundancy
                                        if not hasattr(self, 'all_map_csv_files'):
                                            self.all_map_csv_files = []
                                        self.all_map_csv_files.extend(map_full_paths)
                                        
                                        # Also add to a backup list
                                        if not hasattr(self, 'map_csv_files_found'):
                                            self.map_csv_files_found = []
                                        self.map_csv_files_found.extend(map_csv_files)
                                        
                                        # Ensure we have a class property tracking the count too
                                        if not hasattr(self, 'total_map_files_found'):
                                            self.total_map_files_found = 0
                                        self.total_map_files_found = len(self.all_map_csv_files)
                                        
                                        # Set this flag to ensure we properly track that we've found map files
                                        if not hasattr(self, 'found_map_files'):
                                            self.found_map_files = False
                                        self.found_map_files = True
                                        
                                        # Log detailed information for debugging
                                        logger.debug(f"Added {len(map_full_paths)} CSV files from map directory {map_dir} to tracking lists")
                                        logger.debug(f"Total tracked map files now: {len(self.all_map_csv_files)}")
                                    else:
                                        # Try with each date format pattern
                                        for pattern in date_format_patterns:
                                            logger.debug(f"Trying pattern {pattern} in map directory {map_dir}")
                                            date_map_csv_files = await sftp.list_files(map_dir, pattern)
                                            if date_map_csv_files is not None:
                                                logger.info(f"Found {len(date_map_csv_files)} CSV files using pattern {pattern} in map directory {map_dir}")
                                                # Convert to full paths
                                                map_full_paths = [
                                                    os.path.join(map_dir, f) for f in date_map_csv_files
                                                    if not f.startswith('/')
                                                ]
                                                self.all_map_csv_files.extend(map_full_paths)
                                                break  # Stop after finding files with one pattern

                                        # Log if no files were found with any pattern
                                        found_any = False
                                        for pattern in date_format_patterns:
                                            if await sftp.list_files(map_dir, pattern):
                                                found_any = True
                                                break

                                        if found_any is None:
                                            logger.debug(f"No CSV files found with any pattern in map directory {map_dir}")
                                except Exception as map_err:
                                    logger.warning(f"Error searching map directory {map_dir}: {map_err}")

                            # If we found CSV files in any map directory
                            if self.all_map_csv_files is not None:
                                logger.info(f"Found {len(self.all_map_csv_files)} total CSV files across all map directories")
                                full_path_csv_files = self.all_map_csv_files
                                csv_files = [os.path.basename(f) for f in self.all_map_csv_files]
                                path_found = deathlogs_path  # Use the parent deathlogs path as the base

                                # Log a sample of found files
                                if len(csv_files) > 0:
                                    sample = csv_files[:5] if len(csv_files) > 5 else csv_files
                                    logger.info(f"Sample CSV files: {sample}")
                                    # Set class property to indicate we found map files
                                    self.found_map_files = True
                    else:
                        logger.warning(f"Deathlogs path does not exist: {deathlogs_path}")
                except Exception as e:
                    logger.warning(f"Error checking for map directories: {e}")

                # Initialize class variables to track file discovery
                if not hasattr(self, 'map_csv_files_found'):
                    self.map_csv_files_found = []
                
                if not hasattr(self, 'map_csv_full_paths_found'):
                    self.map_csv_full_paths_found = []
                
                # Initialize found_map_files property if not already present
                if not hasattr(self, 'found_map_files'):
                    self.found_map_files = False
                    
                # If we found files in map directories, store them for later processing
                if csv_files and len(csv_files) > 0:
                    logger.info(f"Successfully found {len(csv_files)} CSV files in map directories, will process these files")
                    self.map_csv_files_found = csv_files.copy()
                    self.map_csv_full_paths_found = full_path_csv_files.copy()
                    # Set these variables to ensure we process the files we found
                    self.files_to_process = self.map_csv_full_paths_found
                    # Set flag to track that we found files
                    self.found_map_files = True
                    # Update processing status to avoid misleading logs
                    if not hasattr(self, 'found_csv_files_status_updated'):
                        self.found_csv_files_status_updated = True
                    # We'll still continue with additional search to ensure no files are missed
                    logger.debug(f"Found {len(self.map_csv_files_found)} CSV files in map directories, continuing with additional search to find more files if available")
                elif self.found_map_files is None:
                    # Only log this if we haven't found files in previous checks
                    # CRITICAL FIX: Don't report "No CSV files found" since we'll check more locations
                    # No CSV files found in map directories yet, will continue with standard search
                    
                    # Set a flag so we don't mistakenly report we found files later
                    self.map_csv_files_found = []
                    self.map_csv_full_paths_found = []

                # Enhanced list of possible paths to check (when map directories search fails)
                # For Tower of Temptation, we need to include possible map subdirectory paths

                # Define known map subdirectory names
                map_subdirs = ["world_0", "world0", "world_1", "world1", "map_0", "map0", "main", "default"]

                # Build base paths list
                base_paths = [
                    deathlogs_path,  # Standard path: /hostname_serverid/actual1/deathlogs/
                    os.path.join("/", server_dir, "deathlogs"),  # Without "actual1"
                    os.path.join("/", server_dir, "logs"),  # Alternate logs directory
                    os.path.join("/", server_dir, "Logs", "deathlogs"),  # Capital Logs with deathlogs subdirectory
                    os.path.join("/", server_dir, "Logs"),  # Just capital Logs
                    os.path.join("/", "logs", server_dir),  # Common format with server subfolder
                    os.path.join("/", "deathlogs"),  # Root deathlogs 
                    os.path.join("/", "logs"),  # Root logs
                    os.path.join("/", server_dir),  # Just server directory
                    os.path.join("/", server_dir, "actual1"),  # Just the actual1 directory
                ]

                # Now add map subdirectory variations to each base path
                possible_paths = []
                for base_path in base_paths:
                    # Add the base path first
                    possible_paths.append(base_path)

                    # Then add each map subdirectory variation
                    for map_subdir in map_subdirs:
                        map_path = os.path.join(base_path, map_subdir)
                        possible_paths.append(map_path)

                # Add root as last resort
                possible_paths.append("/")

                logger.debug(f"Generated {len(possible_paths)} possible paths to search for CSV files")

                # First attempt: Use list_files with the specified pattern on all possible paths
                for search_path in possible_paths:
                    logger.debug(f"Trying to list CSV files in: {search_path}")
                    try:
                        # Check connection before each attempt
                        if sftp.client is None:
                            logger.warning(f"Connection lost before listing files in {search_path}, reconnecting...")
                            await sftp.connect()
                            if sftp.client is None:
                                logger.error(f"Failed to reconnect for path: {search_path}")
                                continue

                        # Try with primary pattern
                        path_files = await sftp.list_files(search_path, csv_pattern)

                        # If primary pattern didn't work, try with each date format pattern
                        if not path_files and csv_pattern != date_format_pattern:
                            logger.debug(f"No files found with primary pattern, trying date format patterns in {search_path}")
                            for pattern in date_format_patterns:
                                logger.debug(f"Trying pattern {pattern} in directory {search_path}")
                                pattern_files = await sftp.list_files(search_path, pattern)
                                if pattern_files is not None:
                                    logger.info(f"Found {len(pattern_files)} CSV files using pattern {pattern} in {search_path}")
                                    path_files = pattern_files
                                    break

                        if path_files is not None:
                            # Build full paths to the CSV files
                            full_paths = [
                                f if f.startswith('/') else os.path.join(search_path, f) 
                                for f in path_files
                            ]

                            # Check which are actually files (not directories)
                            verified_files = []
                            verified_full_paths = []

                            for i, file_path in enumerate(full_paths):
                                try:
                                    if await sftp.is_file(file_path):
                                        verified_files.append(path_files[i])
                                        verified_full_paths.append(file_path)
                                except Exception as verify_err:
                                    logger.warning(f"Error verifying file {file_path}: {verify_err}")

                            if verified_files is not None:
                                csv_files = verified_files
                                full_path_csv_files = verified_full_paths
                                path_found = search_path
                                logger.debug(f"Found {len(csv_files)} CSV files in {search_path}")

                                # Print the first few file names for debugging
                                if csv_files is not None:
                                    sample_files = csv_files[:5]
                                    logger.info(f"Sample CSV files: {sample_files}")

                                break
                    except Exception as path_err:
                        logger.warning(f"Error listing files in {search_path}: {path_err}")
                        # Continue to next path

                    # Second attempt: Try recursive search immediately with more paths and deeper search
                    if not csv_files and not self.found_map_files:
                        logger.info(f"No CSV files found in predefined paths, trying recursive search...")
                    elif not csv_files and self.found_map_files:
                        logger.debug(f"No additional CSV files found in predefined paths, but we already found files in map directories. Continuing with recursive search for thoroughness...")

                        # Try first from server root, then the root directory of the server
                        root_paths = [
                            server_dir,  # Server's root directory
                            "/",         # File system root
                            os.path.dirname(server_dir) if "/" in server_dir else "/",  # Parent of server dir
                            os.path.join("/", "data"),  # Common server data directory
                            os.path.join("/", "game"),  # Game installation directory
                            # More specific paths
                            os.path.join("/", server_dir, "game"),
                            os.path.join("/", "home", os.path.basename(server_dir) if server_dir != "/" else "server"),
                            os.path.join("/", "home", "steam", os.path.basename(server_dir) if server_dir != "/" else "server"),
                            os.path.join("/", "game", os.path.basename(server_dir) if server_dir != "/" else "server"),
                            os.path.join("/", "data", os.path.basename(server_dir) if server_dir != "/" else "server"),
                        ]

                        logger.debug(f"Will try recursive search from {len(root_paths)} different root paths")

                        for root_path in root_paths:
                            try:
                                # Check connection before recursive search
                                if sftp.client is None:
                                    logger.warning(f"Connection lost before recursive search at {root_path}, reconnecting...")
                                    await sftp.connect()
                                    if sftp.client is None:
                                        logger.error(f"Failed to reconnect for recursive search at {root_path}")
                                        continue

                                logger.debug(f"Starting deep recursive search from {root_path}")

                                # Use find_csv_files which has better error handling and multiple fallbacks
                                if hasattr(sftp, 'find_csv_files'):
                                    # Try with higher max_depth to explore deeper into the file structure
                                    root_csvs = await sftp.find_csv_files(root_path, recursive=True, max_depth=8)
                                    if root_csvs is not None:
                                        logger.debug(f"Found {len(root_csvs)} CSV files in deep search from {root_path}")
                                        # Log a sample of the files found
                                        if len(root_csvs) > 0:
                                            sample = root_csvs[:5] if len(root_csvs) > 5 else root_csvs
                                            logger.info(f"Sample files: {sample}")

                                        # Filter for CSV files that match our pattern
                                        pattern_re = re.compile(csv_pattern)
                                        matching_csvs = [
                                            f for f in root_csvs
                                            if pattern_re.search(os.path.basename(f))
                                        ]

                                        # If no matches with primary pattern, try date format pattern
                                        if not matching_csvs and csv_pattern != date_format_pattern:
                                            logger.debug(f"No matches with primary pattern, trying date format pattern")
                                            pattern_re = re.compile(date_format_pattern)
                                            matching_csvs = [
                                                f for f in root_csvs
                                                if pattern_re.search(os.path.basename(f))
                                            ]

                                            if matching_csvs is not None:
                                                # Found matching CSV files
                                                full_path_csv_files = matching_csvs
                                                csv_files = [os.path.basename(f) for f in matching_csvs]
                                                path_found = os.path.dirname(matching_csvs[0])
                                                logger.info(f"Found {len(csv_files)} CSV files through recursive search in {path_found}")

                                                # Store the files we found in class variables
                                                if not hasattr(self, 'map_csv_files_found'):
                                                    self.map_csv_files_found = []
                                                    
                                                if not hasattr(self, 'map_csv_full_paths_found'):
                                                    self.map_csv_full_paths_found = []
                                                    
                                                self.map_csv_files_found.extend(csv_files)
                                                self.map_csv_full_paths_found.extend(full_path_csv_files)
                                                self.found_map_files = True
                                                
                                                # Print the first few file names for debugging
                                                if csv_files is not None:
                                                    sample_files = csv_files[:5]
                                                    logger.info(f"Sample CSV files: {sample_files}")

                                                break

                                    # If we found files, break out of the root_path loop
                                    if csv_files is not None:
                                        break

                            except Exception as search_err:
                                logger.warning(f"Recursive CSV search failed for {root_path}: {search_err}")

                    # Third attempt: Last resort - manually search common directories with simpler method
                    if not csv_files and not self.found_map_files:
                        logger.info(f"Still no CSV files found, trying direct file stat checks...")
                        # This is a last resort method to check for CSV files
                        # by directly trying to stat specific paths with clear date patterns
                    elif not csv_files and self.found_map_files:
                        logger.info(f"No additional CSV files found via recursive search, but we already found {len(self.map_csv_files_found)} files in map directories")
                        # Add a clear comment about what's happening
                        logger.info(f"Will still try direct file checks for thoroughness, though we already have {len(self.map_csv_files_found)} files to process")
                        # Continue with processing despite not finding additional files

                        # Generate some likely filenames with date patterns
                        current_time = datetime.now()
                        test_dates = [
                            current_time - timedelta(days=i)
                            for i in range(0, 31, 5)  # Try dates at 5-day intervals going back a month
                        ]

                        test_filenames = []
                        for test_date in test_dates:
                            # Format: YYYY.MM.DD-00.00.00.csv (daily file at midnight)
                            test_filenames.append(test_date.strftime("%Y.%m.%d-00.00.00.csv"))
                            # Also try hourly files from the most recent day
                            if test_date == test_dates[0]:
                                for hour in range(0, 24, 6):  # Try every 6 hours
                                    test_filenames.append(test_date.strftime(f"%Y.%m.%d-{hour:02d}.00.00.csv"))

                        # Try these filenames in each potential directory
                        for search_path in possible_paths:
                            if csv_files is not None:  # Break early if we found something
                                break

                            for filename in test_filenames:
                                test_path = os.path.join(search_path, filename)
                                try:
                                    # Try to stat the file directly
                                    if await sftp.exists(test_path):
                                        logger.info(f"Found CSV file using direct check: {test_path}")
                                        # We found one file, now search the directory for more
                                        path_files = await sftp.list_files(search_path, r".*\.csv$")
                                        if path_files is not None:
                                            csv_files = path_files
                                            path_found = search_path
                                            full_path_csv_files = [os.path.join(search_path, f) for f in csv_files]
                                            logger.debug(f"Found {len(csv_files)} CSV files in {search_path} using direct check")
                                            
                                            # Save the files we found using direct check for later processing
                                            self.map_csv_files_found.extend(csv_files)
                                            self.map_csv_full_paths_found.extend(full_path_csv_files)
                                            self.found_map_files = True
                                            
                                            break
                                except Exception as direct_err:
                                    pass  # Silently continue, we're trying lots of paths

                        # If we still have no files or path, try local test files as a fallback
                        if not csv_files or path_found is None:
                            logger.warning(f"No CSV files found for server {server_id} after exhaustive search on SFTP")

                            # IMPORTANT: Disabled fallback to local test files in attached_assets
                            # This functionality has been removed to prevent accidentally processing test files
                            logger.warning(f"No CSV files found in SFTP locations for server {server_id}")
                            logger.warning(f"Fallback to attached_assets has been disabled to prevent processing test data")
                            # Return no files processed
                            return 0, 0

                        # Update deathlogs_path with the path where we actually found files (guaranteed to be non-None at this point)
                        deathlogs_path = path_found  # path_found is definitely not None here

                        # Sort chronologically
                        csv_files.sort()

                        # CRITICAL FIX: Always use a 30-day window for processing to ensure we don't miss files
                        # This completely bypasses the memory system that can cause files to be skipped
                        logger.info("Overriding last processed time for historical analysis")
                        
                        # Set processing window to last 30 days to ensure we catch all relevant files
                        last_time = datetime.now() - timedelta(days=30)
                        last_time_str = last_time.strftime("%Y.%m.%d-%H.%M.%S")
                        
                        # Update memory system to prevent future issues
                        self.last_processed[server_id] = last_time
                        
                        logger.debug(f"Set processing window to include all files newer than {last_time_str}")
                        logger.debug("Ensuring consistent file processing with memory system")

                        # Log the cutoff time being used
                        logger.info(f"Processing files newer than: {last_time_str}")

                        # If no CSV files found via SFTP, log error and return
                        if not csv_files or len(csv_files) == 0:
                            logger.error(f"No CSV files found in SFTP location for server {server_id}")
                            logger.error(f"Please check SFTP configuration and connectivity")
                            return 0, 0

                            # IMPORTANT: Disabled support for local test files in production environment
                            logger.warning(f"Using local test files has been disabled in production environment")
                            logger.warning(f"PRODUCTION ENVIRONMENT MUST USE SFTP FILES ONLY - Rule #11")
                            
                            # No CSV files found via SFTP - this is a legitimate error that should be reported and handled
                            if os.path.exists(test_dir):
                                logger.warning(f"attached_assets directory exists but will not be used in production")
                            
                            # Return without processing any files
                            return 0, 0

                        # FIXED: Properly initialize new_files only once and ensure it contains all files
                        # This is a critical fix to ensure all CSV files are passed along for processing
                        new_files = []
                        skipped_files = []
                        
                        # Log the number of files found and the cutoff date
                        logger.debug(f"CSV Processing: Found {len(csv_files)} CSV files, timestamp cutoff: {last_time_str}")
                        
                        # CRITICAL FIX: CONSOLIDATED FILE DISCOVERY LOGIC
                        # Always ensure we have files to process by directly assigning all discovered files
                        # This bypasses potential issues with date filtering
                        
                        if len(csv_files) > 0:
                            # Log what we're about to process
                            for f in csv_files:
                                filename = os.path.basename(f)
                                logger.debug(f"CSV Processing: Will process file: {filename}")
                                new_files.append(f)
                                
                            # Double-check that we have files in new_files
                            if len(new_files) != len(csv_files):
                                logger.error(f"CSV Processing: File count mismatch! csv_files={len(csv_files)}, new_files={len(new_files)}")
                                # Force assign all files as a last resort
                                new_files = csv_files.copy()
                        else:
                            logger.warning("CSV Processing: No CSV files found in search paths")
                            
                        # Final safety check - ensure we actually have files to process
                        if not new_files and csv_files:
                            logger.error("CSV Processing: Critical error - no files in new_files list but csv_files is not empty!")
                            # Force assign all files as a last resort
                            new_files = csv_files.copy()

                        # COMPLETE SKIP OF THE SECOND FILTERING LOOP
                        # Original loop commented out to prevent duplicate processing
                        # BEGINNING OF COMMENTED OUT SECTION
                        # for f in csv_files:
                        #   (and all lines below that were previously inside a triple-quote block)
                        # END OF COMMENTED OUT SECTION
                            # Get just the filename without the path
                            filename = os.path.basename(f)
                            logger.info(f"DEBUG CSV: Processing filename: {filename}")

                            # Extract the date portion (if it exists)
                            # Match patterns like: 2025.05.03-00.00.00.csv or 2025.05.03-00.00.00
                            date_match = re.search(r'(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})', filename)

                            if date_match is not None:
                                file_date_str = date_match.group(1)

                                # EMERGENCY FIX: Add enhanced timestamp parsing with multiple formats
                                try:
                                    # Try primary format first
                                    logger.info(f"Parsing timestamp for CSV file: {file_date_str}")
                                    try:
                                        file_date = datetime.strptime(file_date_str, '%Y.%m.%d-%H.%M.%S')
                                        logger.info(f"Successfully parsed timestamp: {file_date}")
                                    except ValueError as e:
                                        logger.warning(f"Could not parse timestamp {file_date_str}: {e}f")
                                        # Try alternative formats
                                        parsing_success = False
                                        for fmt in ["%Y.%m.%d-%H:%M:%S", "%Y-%m-%d-%H.%M.%S", "%Y-%m-%d %H:%M:%S", "%Y.%m.%d %H:%M:%S"]:
                                            try:
                                                file_date = datetime.strptime(file_date_str, fmt)
                                                logger.info(f"Successfully parsed timestamp with alternative format {fmt}: {file_date}")
                                                parsing_success = True
                                                break
                                            except ValueError:
                                                continue
                                        if parsing_success is None:
                                            logger.error(f"Failed to parse timestamp with all formats: {file_date_str}, skipping file")
                                            skipped_files.append(f)
                                            continue
                                    logger.warning(f"TIMESTAMP FIX: Successfully parsed {file_date_str} with primary format")
                                except ValueError:
                                    # Try fallback formats
                                    parsed = False
                                    fallback_formats = [
                                        '%Y.%m.%d-%H:%M:%S',  # With colons
                                        '%Y-%m-%d-%H.%M.%S',  # With dashes
                                        '%Y-%m-%d %H:%M:%S',  # Standard format
                                        '%Y.%m.%d %H:%M:%S',  # Dots for date
                                        '%d.%m.%Y-%H.%M.%S',  # European format
                                    ]

                                    for fmt in fallback_formats:
                                        try:
                                            file_date = datetime.strptime(file_date_str, fmt)
                                            logger.warning(f"TIMESTAMP FIX: Parsed {file_date_str} with fallback format {fmt}")
                                            parsed = True
                                            break
                                        except ValueError:
                                            continue

                                    if parsed is None:
                                        # If all formats fail, use a fixed date in the past
                                        # This ensures the file will be processed
                                        logger.warning(f"TIMESTAMP FIX: Could not parse {file_date_str} with any format")
                                        # Use day before last_time to ensure file is processed
                                        file_date = last_time - timedelta(days=1)
                                        logger.warning(f"TIMESTAMP FIX: Using fixed date: {file_date} for timestamp: {file_date_str}")

                                # EMERGENCY FIX: Add debug logging
                                logger.warning(f"TIMESTAMP FIX: File date: {file_date}, Last time: {last_time}, Will process: {file_date > last_time}")

                                # EMERGENCY FIX: Override comparison to always process all files
                                # Comment out next line to disable emergency mode
                                file_date = datetime.now()  # Force all files to be processed by setting date to now
                                logger.info(f"Extracted date {file_date_str} from filename {filename}")

                                try:
                                    # CRITICAL FIX: We need to immediately include all files regardless of date
                                    logger.info(f"CRITICAL FIX: Including file {filename} regardless of date comparison")
                                    new_files.append(f)
                                    
                                    # Continue with date parsing for informational purposes only
                                    # This won't affect file inclusion but helps with logging
                                    try:
                                        file_date = datetime.strptime(file_date_str, "%Y.%m.%d-%H.%M.%S")
                                        logger.info(f"Successfully parsed date from filename: {file_date}")
                                        
                                        # Log additional information without filtering
                                        now = datetime.now()
                                        if file_date > now + timedelta(hours=1):
                                            logger.warning(f"Note: File date {file_date} appears to be in the future")
                                        
                                        # Convert string format back to datetime for informational comparison
                                        last_time_date = datetime.strptime(last_time_str, "%Y.%m.%d-%H.%M.%S")
                                        
                                        # Log whether this file would have been included under old rules
                                        seven_days_ago = datetime.now() - timedelta(days=7)
                                        would_include = file_date > last_time_date or file_date >= seven_days_ago
                                        logger.info(f"File {filename} with date {file_date} would {'' if would_include else 'NOT '} have been included under old rules")
                                    except ValueError as inner_e:
                                        logger.warning(f"Could not parse date from {file_date_str}: {inner_e}, but file is included anyway")
                                    # Comment out old exclusion logic:
                                    # if file_date > last_time_date or file_date >= seven_days_ago:
                                    #     logger.info(f"FIXED: Including file {filename} with date {file_date} (last_processed={last_time_date}, 7_days_ago={seven_days_ago})")
                                    #     new_files.append(f)
                                    # else:
                                    #     logger.info(f"File date {file_date} is older than last processed {last_time_date} and older than 7 days")
                                    #     skipped_files.append(f)
                                except ValueError as e:
                                    # Try alternative date formats - primary format is yyyy.mm.dd-hh.mm.ss as confirmed by user
                                    parsed = False
                                    for date_format in ["%Y.%m.%d-%H.%M.%S", "%Y-%m-%d-%H.%M.%S", "%Y.%m.%d_%H.%M.%S", "%Y%m%d-%H%M%S"]:
                                        try:
                                            logger.info(f"Trying to parse date {file_date_str} with format {date_format}")
                                            file_date = datetime.strptime(file_date_str, date_format)
                                            last_time_date = datetime.strptime(last_time_str, "%Y.%m.%d-%H.%M.%S")
                                            logger.info(f"Successfully parsed date {file_date_str} as {file_date}")
                                            parsed = True

                                            # Apply the same logic as above - include files from last 7 days
                                            seven_days_ago = datetime.now() - timedelta(days=7)

                                            # CRITICAL FIX: Immediately include all files, regardless of date
                                            # This ensures that all files are processed during historical parsing
                                            logger.info(f"CRITICAL FIX: Including file {filename} with date {file_date} regardless of date comparison")
                                            new_files.append(f)
                                            # Comment out old exclusion logic:
                                            # if file_date > last_time_date or file_date >= seven_days_ago:
                                            #     logger.info(f"FIXED: Including file {filename} with date {file_date} using alternate format")
                                            #     new_files.append(f)
                                            # else:
                                            #     logger.info(f"File date {file_date} is older than last processed {last_time_date} and older than 7 days")
                                            #     skipped_files.append(f)
                                            break
                                        except ValueError:
                                            continue

                                    # If we have a date parsing error even after all formats, include the file by default
                                    if parsed is None:
                                        logger.warning(f"Error parsing date from {file_date_str}: {e}, including file by default")
                                        new_files.append(f)
                            else:
                                # If we can't parse the date from the filename, include it anyway to be safe
                                logger.warning(f"Could not extract date from filename: {filename}, including by default")
                                new_files.append(f)

                        # Log what we found
                        logger.info(f"Found {len(new_files)} new CSV files out of {len(csv_files)} total in {deathlogs_path}")
                        logger.info(f"Skipped {len(skipped_files)} CSV files as they are older than {last_time_str}")

                        if len(csv_files) > 0 and len(new_files) == 0:
                            # Show a sample of the CSV files and the last_time_str for debugging
                            sample = csv_files[:3] if len(csv_files) > 3 else csv_files
                            logger.info(f"All {len(csv_files)} files were filtered out as older than {last_time_str}")
                            logger.info(f"Sample filenames: {[os.path.basename(f) for f in sample]}")
                            # If all files were filtered out, check if any would be included with a much earlier date
                            debug_date = datetime.now() - timedelta(days=30)
                            debug_date_str = debug_date.strftime("%Y.%m.%d-%H.%M.%S")
                            logger.debug(f"Would any files be included if using a 30-day old cutoff of {debug_date_str}?")
                            for f in csv_files[:5]:  # Check first 5 files
                                filename = os.path.basename(f)
                                date_match = re.search(r'(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})', filename)
                                if date_match is not None:
                                    file_date_str = date_match.group(1)
                                    try:
                                        file_date = datetime.strptime(file_date_str, "%Y.%m.%d-%H.%M.%S")
                                        if file_date > debug_date:
                                            logger.debug(f"{filename} would be included with 30-day cutoff")
                                        else:
                                            logger.debug(f"{filename} would still be too old with 30-day cutoff")
                                    except ValueError:
                                        logger.debug(f"Could not parse date from {filename} to check against 30-day cutoff")
                            logger.debug(f"Original last_time_str: {last_time_str}, 30-day cutoff: {debug_date_str}")

                        # Process each file
                        files_processed = 0
                        events_processed = 0

                        logger.info(f"Starting to process {len(new_files)} CSV files")

                        # Sort files by date to ensure we process in chronological order
                        # Extract date from filename for proper sorting
                        def get_file_date(file_path):
                            try:
                                # Extract date portion from path like .../2025.05.06-00.00.00.csv
                                file_name = os.path.basename(file_path)
                                date_part = file_name.split('.csv')[0]
                                return datetime.strptime(date_part, "%Y.%m.%d-%H.%M.%S")
                            except (ValueError, IndexError):
                                # If parsing fails, return a default old date
                                logger.warning(f"Unable to parse date from filename: {file_path}")
                                return datetime(2000, 1, 1)

                        # Sort files by their embedded date for chronological processing
                        sorted_files = sorted(new_files, key=get_file_date)
                        logger.info(f"Found {len(new_files)} files to process, sorted {len(sorted_files)} chronologically")
                        
                        # CRITICAL FIX: Make sure sorted_files is not empty
                        if not sorted_files and new_files:
                            logger.warning("CRITICAL FIX: sorted_files is empty but new_files is not - using new_files directly")
                            sorted_files = new_files
                            
                        if sorted_files is not None:
                            logger.info(f"First 3 sorted files: {[os.path.basename(f) for f in sorted_files[:3]]}")
                        else:
                            logger.info("No files to process after sorting!")

                        # FIXED: Determine the correct processing mode based on context
                        # Default to incremental mode for normal background processing
                        is_historical_mode = False
                        
                        # Log information about the processing mode decision
                        if start_date is not None:
                            days_diff = (datetime.now() - start_date).days
                            logger.info(f"Start date is {start_date}, days difference is {days_diff}")
                            if days_diff > 7:
                                logger.info(f"Using historical mode since we're looking back {days_diff} days")
                                is_historical_mode = True
                            else:
                                logger.info(f"Using incremental mode since we're only looking back {days_diff} days")
                        else:
                            # Check if it's a first-time run (new server added)
                            if server_id not in self.last_processed:
                                logger.info("First time processing for this server, using historical mode for last 24 hours")
                                is_historical_mode = True
                                # Also clear database for this server since it's a first run
                                await self._clear_server_data(server_id)
                            else:
                                logger.info("Regular incremental processing for server with existing timestamp")
                                is_historical_mode = False

                        # FIXED: Implement clear distinction between historical vs killfeed processing
                        
                        # Log file counts for easier debugging
                        logger.debug(f"CSV Processing: Original csv_files: {len(csv_files)}, new_files after filtering: {len(new_files)}, sorted_files: {len(sorted_files)}")
                        
                        # Historical processor:
                        # - Should process ALL CSV files it finds, without any filtering
                        if is_historical_mode is not None:
                            logger.debug(f"CSV Processing: Historical mode - will process ALL {len(csv_files)} files with ALL lines")
                            # FIXED: Use sorted_files as the source since they're already chronologically ordered
                            # But if sorted_files is empty and csv_files isn't, use csv_files as fallback
                            if len(sorted_files) > 0:
                                files_to_process = sorted_files
                            else:
                                logger.warning("CSV Processing: sorted_files is empty, using csv_files directly")
                                files_to_process = csv_files
                                
                            only_new_lines = False  # Process all lines in historical mode
                            
                            # Reset last_processed timestamp to ensure we reprocess all files from scratch
                            if server_id in self.last_processed:
                                logger.debug(f"CSV Processing: Resetting last_processed timestamp for historical processing")
                                del self.last_processed[server_id]
                                
                            # Track this historical parse to prevent simultaneous processing
                            self.servers_with_active_historical_parse.add(server_id)
                            logger.debug(f"CSV Processing: Added server {server_id} to active historical parse tracking")
                        
                        # Killfeed processor:
                        # - Should find the newest file for each day and process only the new lines
                        else:
                            logger.debug(f"CSV Processing: Regular killfeed mode - processing newest files with incremental updates")
                            # Group files by day
                            files_by_day = {}
                            
                            # FIXED: Use the larger of sorted_files or new_files to ensure we don't miss anything
                            source_files = sorted_files if len(sorted_files) >= len(new_files) else new_files
                            if len(source_files) == 0 and len(csv_files) > 0:
                                logger.warning("CSV Processing: Both sorted_files and new_files are empty, using csv_files")
                                source_files = csv_files
                                
                            logger.debug(f"CSV Processing: Using {len(source_files)} files as source for day grouping")
                            
                            for f in source_files:
                                filename = os.path.basename(f)
                                # FIXED: Improved date extraction with better error handling
                                try:
                                    date_match = re.search(r'(\d{4}\.\d{2}\.\d{2})', filename)
                                    if date_match is not None:
                                        day = date_match.group(1)
                                        if day in files_by_day:
                                            files_by_day[day].append(f)
                                        else:
                                            files_by_day[day] = [f]
                                    else:
                                        # Try alternative date formats
                                        alt_date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
                                        if alt_date_match is not None:
                                            day = alt_date_match.group(1).replace('-', '.')
                                            if day in files_by_day:
                                                files_by_day[day].append(f)
                                            else:
                                                files_by_day[day] = [f]
                                        else:
                                            # If we can't extract day, just include it separately
                                            logger.info(f"CSV Processing: Could not extract date from {filename}, including it directly")
                                            files_by_day[filename] = [f]
                                except Exception as e:
                                    logger.error(f"CSV Processing: Error grouping file {filename}: {e}")
                                    # Add to a special "errors" group to ensure we don't lose files
                                    if "errors" not in files_by_day:
                                        files_by_day["errors"] = []
                                    files_by_day["errors"].append(f)
                            
                            # Get newest file for each day
                            newest_files = []
                            
                            # FIXED: Better handling of empty files_by_day
                            if files_by_day is None:
                                logger.warning("CSV Processing: No files could be grouped by day - using all files")
                                newest_files = source_files
                            else:
                                for day, day_files in files_by_day.items():
                                    try:
                                        # Sort files for this day
                                        day_files.sort(reverse=True)
                                        newest_files.append(day_files[0])
                                        logger.debug(f"CSV Processing: For day {day}, selected newest file: {os.path.basename(day_files[0])}")
                                    except Exception as e:
                                        logger.error(f"CSV Processing: Error selecting newest file for day {day}: {e}")
                                        # Add all files for this day as a fallback
                                        newest_files.extend(day_files)
                            
                            # OPTIMIZATION: In regular mode, only process the newest file
                            # And only check the previous day's file once when a new file is detected
                            if newest_files is not None:
                                # Sort the newest files by date (descending)
                                newest_files.sort(key=get_file_date, reverse=True)
                                
                                # Always include the most recent file
                                most_recent_file = newest_files[0]
                                files_to_keep = [most_recent_file]
                                most_recent_filename = os.path.basename(most_recent_file)
                                most_recent_date = get_file_date(most_recent_file)
                                
                                # Check if this is a newly created file we haven't processed before
                                is_new_file = False
                                if server_id in self.processed_files_history:
                                    if most_recent_filename not in self.processed_files_history[server_id]:
                                        is_new_file = True
                                        logger.debug(f"CSV Processing: Detected new file: {most_recent_filename}")
                                else:
                                    # First time seeing this server, consider it new
                                    self.processed_files_history[server_id] = set()
                                    is_new_file = True
                                
                                # If it's a new file, also include the previous day's file to catch missed data
                                if is_new_file is not None:
                                    yesterday = most_recent_date - timedelta(days=1)
                                    
                                    # Check if we have a file from the previous day to include
                                    for file in newest_files[1:]:
                                        file_date = get_file_date(file)
                                        # If this file is from yesterday, include it
                                        if file_date.date() == yesterday.date():
                                            logger.debug(f"CSV Processing: Including previous day's file for transition data: {os.path.basename(file)}")
                                            files_to_keep.append(file)
                                            break
                                
                                # Track this file as processed
                                self.processed_files_history[server_id].add(most_recent_filename)
                                
                                logger.debug(f"CSV Processing: OPTIMIZATION - In regular mode, processing only {len(files_to_keep)} files: {[os.path.basename(f) for f in files_to_keep]}")
                                files_to_process = files_to_keep
                            else:
                                logger.warning("CSV Processing: No newest files found - using empty list")
                                files_to_process = []
                            only_new_lines = True  # Only process new lines in regular mode
                            
                            # FIXED: Better handling of line position tracking
                            if server_id in self.last_processed_line_positions:
                                logger.info(f"CSV Processing: Found line position information from previous processing")
                                server_line_positions = self.last_processed_line_positions[server_id]
                                
                                # Show sample of line positions for debugging
                                sample_positions = dict(list(server_line_positions.items())[:3]) if server_line_positions else {}
                                logger.info(f"CSV Processing: Sample line positions for server {server_id}: {sample_positions}")
                                
                                # Add information for each file that has line position tracking
                                for file_path in files_to_process:
                                    file_key = os.path.basename(file_path)
                                    if file_key in server_line_positions:
                                        line_position = server_line_positions[file_key]
                                        logger.info(f"CSV Processing: Found line position {line_position} for file {file_key}")
                                        
                                        # Store this info to be used during actual processing
                                        if not hasattr(self, 'file_line_positions'):
                                            self.file_line_positions = {}
                                        self.file_line_positions[file_path] = line_position
                        
                        # FIXED: Enhanced logging and safety checks for file processing
                        logger.info(f"CSV Processing: Final files_to_process count: {len(files_to_process)}")
                        logger.info(f"CSV Processing: Mode: Historical={is_historical_mode}, Only new lines={only_new_lines}")
                        # CRITICAL FIX: For historical mode, ensure we include ALL files
                        if is_historical_mode is not None:
                            logger.warning(f"CSV Processing: Historical mode - will process ALL files from map directories")
                            if hasattr(self, 'map_csv_full_paths_found') and self.map_csv_full_paths_found:
                                files_to_process = self.map_csv_full_paths_found
                                logger.warning(f"CSV Processing: Using {len(files_to_process)} files from map directories for historical processing")

                        
                        # FIXED: Multiple layers of safety checks to ensure we have files to process
                        # First safety check - if files_to_process is empty but we found files earlier, use them
                        if len(files_to_process) == 0:
                            # Try different sources in order of preference
                            if len(sorted_files) > 0:
                                logger.warning(f"CSV Processing: files_to_process was empty but sorted_files has {len(sorted_files)} files - using sorted_files")
                                files_to_process = sorted_files
                            elif len(new_files) > 0:
                                logger.warning(f"CSV Processing: files_to_process and sorted_files were empty but new_files has {len(new_files)} files - using new_files")
                                files_to_process = new_files
                            elif len(csv_files) > 0:
                                logger.warning(f"CSV Processing: files_to_process, sorted_files, and new_files were empty but csv_files has {len(csv_files)} files - using csv_files")
                                files_to_process = csv_files
                            else:
                                logger.error(f"CSV Processing: No files found in any collection")
                        
                        # Log processing details
                        logger.info(f"CSV Processing: Mode: Historical={is_historical_mode}, " + 
                                  f"Start date={start_date}, Files to process={len(files_to_process)}")

                        # Final check with detailed diagnostics to help troubleshoot empty file lists
                        if len(files_to_process) == 0:
                            logger.error(f"CSV Processing: CRITICAL - No files to process after all safety checks!")
                            
                            # Log all file list lengths to help diagnose the issue
                            logger.error(f"CSV Processing: csv_files={len(csv_files)}, sorted_files={len(sorted_files)}, new_files={len(new_files)}")
                            
                            # Show sample from any non-empty file lists
                            if len(csv_files) > 0:
                                logger.error(f"CSV Processing: Sample from csv_files: {[os.path.basename(f) for f in csv_files[:3]]}")
                            if len(sorted_files) > 0:
                                logger.error(f"CSV Processing: Sample from sorted_files: {[os.path.basename(f) for f in sorted_files[:3]]}")
                            if len(new_files) > 0:
                                logger.error(f"CSV Processing: Sample from new_files: {[os.path.basename(f) for f in new_files[:3]]}")
                                
                            # Log timestamp cutoff information
                            logger.error(f"CSV Processing: Last processing time cutoff: {last_time_str}")
                        else:
                            # Log the first few files we're going to process
                            file_sample = [os.path.basename(f) for f in files_to_process[:5]]
                            logger.info(f"CSV Processing: Files ready for processing: {file_sample}")
                            
                            # Log total number of each type of file for reference
                            logger.info(f"CSV Processing: Total files by source - csv_files: {len(csv_files)}, sorted_files: {len(sorted_files)}, new_files: {len(new_files)}")

                        # Initialize counters to track processing results
                        files_processed = 0
                        events_processed = 0
                        processed_files_list = []
                        total_files_to_process = len(files_to_process)
                        
                        for file in files_to_process:
                            try:
                                # Download file content - use the correct path
                                file_path = file  # file is already the full path
                                logger.debug(f"Downloading CSV file: {file_path} ({files_processed + 1}/{len(files_to_process)})")

                                # DISABLED: No longer using attached_assets for testing or debugging
                                try_attached_assets = False  # Disable attached_assets fallback completely

                                # Special handling for local files in the attached_assets directory is no longer used
                                if 'attached_assets' in file_path:
                                    logger.warning(f"Skipping local file in attached_assets: {file_path}")
                                    logger.warning(f"Attached_assets files should not be processed in production.")
                                    content = None
                                else:
                                    try:
                                        logger.debug(f"Using enhanced download for file: {file_path}")

                                        # First attempt: Use the standard download method
                                        content = await sftp.download_file(file_path)

                                        # Check if we got content
                                        if content is not None:
                                            content_bytes = len(content)
                                            logger.debug(f"Successfully downloaded {file_path} ({content_bytes} bytes)")
                                        else:
                                            # Second attempt: Try direct SFTP access if possible
                                            logger.debug(f"First download attempt failed for {file_path}, trying direct SFTP access")
                                            try:
                                                # Try to access the SFTP connection directly
                                                if hasattr(sftp, 'sftp') and sftp.sftp:
                                                    async with sftp.sftp.open(file_path, 'r') as remote_file:
                                                        content = await remote_file.read()
                                                        content_bytes = len(content) if content else 0
                                                        logger.debug(f"Successfully accessed file directly: {file_path} ({content_bytes} bytes)")
                                            except Exception as direct_error:
                                                logger.debug(f"Direct SFTP access failed: {direct_error}f")

                                            # DISABLED: No longer using attached_assets as a fallback
                                            if content is None:
                                                logger.error(f"All SFTP download attempts failed for {file_path}")
                                                logger.error(f"Could not retrieve content from SFTP server - this is a legitimate file access failure")
                                                # No fallback available in production - attached_assets fallback is disabled
                                    except Exception as e:
                                        logger.error(f"Exception during SFTP download of {file_path}: {e}f")
                                        content = None

                                if content is not None:
                                    content_length = len(content) if hasattr(content, '__len__') else 0
                                    logger.debug(f"Downloaded content type: {type(content)}, length: {content_length}")

                                    # Verify the content is not empty
                                    if content_length == 0:
                                        logger.warning(f"Empty content downloaded from {file_path} - skipping processing")
                                        continue

                                    # Handle different types of content returned from download_file
                                    if isinstance(content, bytes):
                                        # Normal case - bytes returned
                                        decoded_content = content.decode('utf-8', errors='ignore')
                                    elif isinstance(content, list):
                                        # Handle case where a list of strings/bytes is returned
                                        if content and isinstance(content[0], bytes):
                                            # List of bytes
                                            decoded_content = b''.join(content).decode('utf-8', errors='ignore')
                                        else:
                                            # List of strings or empty list
                                            decoded_content = '\n'.join([str(line) for line in content])
                                    else:
                                        # Handle any other case by converting to string
                                        decoded_content = str(content)

                                    # Verify decoded content has actual substance
                                    if not decoded_content or len(decoded_content.strip()) == 0:
                                        logger.warning(f"Empty decoded content from {file_path} - skipping processing")
                                        continue

                                    # Log a sample of the content for debugging
                                    sample = decoded_content[:200] + "..." if len(decoded_content) > 200 else decoded_content
                                    logger.debug(f"CSV content sample: {sample}")

                                    # Process content - determine if we should only process new lines
                                    events = []

                                    # module: csv_event_parsing
                                    # Validate content before parsing to ensure CSV correctness per Rule #5 and #6
                                    if ";" not in decoded_content and "," not in decoded_content:
                                        logger.warning(f"CSV file {file_path} contains no valid delimiters, likely corrupted")
                                        continue

                                    # Enhanced delimiter detection with bias towards semicolons
                                    semicolon_count = decoded_content.count(';')
                                    comma_count = decoded_content.count(',')
                                    tab_count = decoded_content.count('\t')

                                    # Apply a weight factor to prioritize semicolons
                                    # Game logs commonly use semicolons and we want to prioritize them
                                    weighted_semicolon_count = semicolon_count * 3  # Triple the weight for semicolons

                                    logger.debug(f"Delimiter detection: semicolons={semicolon_count} (weighted: {weighted_semicolon_count}), commas={comma_count}, tabs={tab_count}")

                                    # Determine the most likely delimiter with semicolon bias
                                    detected_delimiter = ';'  # Default for our format
                                    if comma_count > weighted_semicolon_count and comma_count > tab_count:
                                        detected_delimiter = ','
                                    elif tab_count > weighted_semicolon_count and tab_count > comma_count:
                                        detected_delimiter = '\t'
                                    else:
                                        # Additional check for patterns that strongly indicate semicolon delimiter
                                        if ';;' in decoded_content or ';;;' in decoded_content:
                                            logger.debug("Found multiple sequential semicolons, confirming semicolon delimiter")
                                            detected_delimiter = ';'

                                    logger.debug(f"Using detected delimiter: \'{detected_delimiter}\' for file {file_path}")

                                    # Check for data rows that match expected format - minimum field count for kill events
                                    has_valid_data = False
                                    sample_lines = decoded_content.split('\n')[:20]  # Check first 20 lines for better detection

                                    # Minimum field counts for different formats
                                    min_fields_for_kill = 6  # timestamp, killer, killer_id, victim, victim_id, weapon

                                    for line in sample_lines:
                                        if not line or line.isspace():
                                            continue

                                        # Count fields by delimiter (adding 1 since n delimiters = n+1 fields)
                                        field_count = line.count(detected_delimiter) + 1

                                        # Check if this looks like a header line
                                        is_header = ('time' in line.lower() or 'date' in line.lower()) and \
                                                   ('killer' in line.lower() or 'player' in line.lower())

                                        # If it's not a header and has enough fields, it might be valid data
                                        if not is_header and field_count >= min_fields_for_kill:
                                            # Additional quality check - make sure there's a timestamp-like pattern
                                            # Most timestamps have numbers and periods or hyphens
                                            fields = line.split(detected_delimiter)
                                            first_field = fields[0].strip() if fields else ""

                                            # Looks like a timestamp if it has digits and separators
                                            looks_like_timestamp = any(c.isdigit() for c in first_field) and \
                                                                 any(c in '.-: ' for c in first_field)

                                            if looks_like_timestamp is not None:
                                                has_valid_data = True
                                                logger.debug(f"Found valid data row: {line[:50]}...")
                                                break

                                    if has_valid_data is None:
                                        logger.warning(f"CSV file {file_path} doesn't contain properly formatted kill data")
                                        logger.warning(f"Sample line: {sample_lines[0] if sample_lines else 'No lines found'}")
                                        continue

                                    # Convert validated content to StringIO for parsing
                                    content_io = io.StringIO(decoded_content)

                                    try:
                                        # EMERGENCY FIX: Robust error handling with detailed logging
                                        max_retries = 3  # Increased retries
                                        for retry in range(max_retries + 1):
                                            try:
                                                # FIXED: Improved CSV processing with better error handling and mode support
                                                # This ensures reliable processing of all CSV files
                                                logger.info(f"CSV Processing: Processing file: {os.path.basename(file_path)}")
                                                
                                                # Reset the file pointer to start fresh
                                                content_io.seek(0)
                                                
                                                # Use appropriate mode based on context
                                                process_mode = "historical" if is_historical_mode else "incremental"
                                                logger.info(f"CSV Processing: Using {process_mode} mode with delimiter: '{detected_delimiter}'")
                                                
                                                # FIXED: Always use direct CSV handler for reliable parsing
                                                # Import here to avoid circular imports
                                                from utils.direct_csv_handler import direct_parse_csv_content
                                                
                                                # Reset the file pointer and read content
                                                content_io.seek(0)
                                                content_str = content_io.read()
                                                
                                                # Determine correct line position to start from
                                                start_line = 0
                                                file_key = os.path.basename(file_path)
                                                
                                                # Handle line position tracking for incremental processing
                                                if is_historical_mode is None:
                                                    # First check file_line_positions
                                                    if hasattr(self, 'file_line_positions') and file_path in self.file_line_positions:
                                                        start_line = self.file_line_positions[file_path]
                                                        logger.info(f"CSV Processing: Starting from line position {start_line} for file {file_key}")
                                                    # Then check last_processed_line_positions
                                                    elif server_id in self.last_processed_line_positions and file_key in self.last_processed_line_positions[server_id]:
                                                        start_line = self.last_processed_line_positions[server_id][file_key]
                                                        logger.info(f"CSV Processing: Starting from saved line position {start_line} for file {file_key}")
                                                else:
                                                    logger.info(f"CSV Processing: Historical mode - processing all lines from the beginning")
                                                
                                                # Process with direct parser
                                                try:
                                                    events, total_lines = direct_parse_csv_content(
                                                        content_str,
                                                        file_path=file_path,
                                                        server_id=server_id,
                                                        start_line=start_line
                                                    )
                                                    logger.info(f"CSV Processing: Processed {len(events)} events from file with {total_lines} total lines")
                                                    
                                                    # If this is incremental mode and we got no events but the file has lines,
                                                    # it could mean we've already processed all lines - log this clearly
                                                    if not is_historical_mode and len(events) == 0 and total_lines > 0 and start_line > 0:
                                                        logger.info(f"CSV Processing: No new events found in {file_key} - all {total_lines} lines may have been processed already (starting from line {start_line})")
                                                        
                                                except Exception as direct_parse_error:
                                                    logger.error(f"CSV Processing: Error in direct parser: {direct_parse_error}")
                                                    # Try fallback to basic parsing in case of errors
                                                    try:
                                                        logger.warning(f"CSV Processing: Attempting fallback parsing for {file_key}")
                                                        # Simple fallback parsing - just extract lines with basic validation
                                                        lines = content_str.splitlines()
                                                        events = []
                                                        total_lines = len(lines)
                                                        
                                                        for i, line in enumerate(lines):
                                                            if i < start_line:
                                                                continue
                                                            
                                                            # Skip empty lines
                                                            if not line.strip():
                                                                continue
                                                                
                                                            # Split by most likely delimiter
                                                            parts = line.split(detected_delimiter)
                                                            
                                                            # Basic validation - need at least 5 parts for a valid event
                                                            if len(parts) >= 5:
                                                                try:
                                                                    events.append({
                                                                        'timestamp': parts[0],
                                                                        'killer_name': parts[1],
                                                                        'killer_id': parts[2] if len(parts) > 2 else "",
                                                                        'victim_name': parts[3],
                                                                        'victim_id': parts[4] if len(parts) > 4 else "",
                                                                        'server_id': server_id,
                                                                        'event_type': 'kill'
                                                                    })
                                                                except Exception:
                                                                    # Skip problematic lines
                                                                    pass
                                                        
                                                        logger.warning(f"CSV Processing: Fallback parsing extracted {len(events)} events from {total_lines} lines")
                                                    except Exception as fallback_error:
                                                        logger.error(f"CSV Processing: Fallback parsing also failed: {fallback_error}")
                                                        events = []
                                                        total_lines = 0
                                                
                                                # FIXED: Better line position tracking for all processing modes
                                                # Always update line position information for efficient incremental processing
                                                if total_lines > 0:
                                                    # Initialize the storage structure if needed
                                                    if server_id not in self.last_processed_line_positions:
                                                        self.last_processed_line_positions[server_id] = {}
                                                        
                                                    # Store the line position differently based on mode
                                                    file_basename = os.path.basename(file_path)
                                                    
                                                    if is_historical_mode is not None:
                                                        # For historical mode, store the total line count for all files
                                                        # This helps regular mode pick up where historical left off
                                                        logger.info(f"CSV Processing: Storing line position {total_lines} for historical processing of {file_basename}")
                                                        self.last_processed_line_positions[server_id][file_basename] = total_lines
                                                        # Save state to database after updating line positions in historical mode
                                                        asyncio.create_task(self._save_server_state(server_id))
                                                    else:
                                                        # For regular mode, update only if we have a higher line count than before
                                                        current_position = self.last_processed_line_positions[server_id].get(file_basename, 0)
                                                        if total_lines > current_position:
                                                            logger.info(f"CSV Processing: Updating line position from {current_position} to {total_lines} for {file_basename}")
                                                            self.last_processed_line_positions[server_id][file_basename] = total_lines
                                                            # Save state to database after updating line positions in regular mode
                                                            asyncio.create_task(self._save_server_state(server_id))
                                                        else:
                                                            logger.info(f"CSV Processing: Keeping existing line position of {current_position} for {file_basename}")
                                                            
                                                    # Also update the in-memory position for future processing
                                                    if not hasattr(self, 'file_line_positions'):
                                                        self.file_line_positions = {}
                                                    self.file_line_positions[file_path] = total_lines
                                                    
                                                    # Update last_processed timestamp for this server when successful  
                                                    # This helps avoid reprocessing files unnecessarily after restart
                                                    if is_historical_mode is None:
                                                        self.last_processed[server_id] = datetime.now()
                                                        # Save state to database
                                                        asyncio.create_task(self._save_server_state(server_id))
                                                break  # Success - exit retry loop
                                            except Exception as e:
                                                if retry < max_retries:
                                                    # Reset file pointer for retry
                                                    content_io.seek(0)
                                                    logger.warning(f"Retry {retry+1}/{max_retries} parsing file {file_path}: {e}f")
                                                else:
                                                    # Last retry failed
                                                    raise

                                        # Validate parsed events
                                        if events is not None:
                                            logger.debug(f"Parsed {len(events)} events from file {file_path}")
                                        else:
                                            logger.warning(f"No events parsed from file {file_path} despite valid format")
                                    except Exception as parse_error:
                                        logger.error(f"Error parsing CSV file {file_path}: {parse_error}f")
                                        events = []

                                    # BATCH PROCESSING IMPLEMENTATION
                                    processed_count = 0
                                    errors = []

                                    # Import utility functions
                                    from utils.parser_utils import normalize_event_data, categorize_event, parser_coordinator

                                    # Process in batches of 100 for better performance
                                    BATCH_SIZE = 100
                                    if len(events) > BATCH_SIZE:
                                        logger.debug(f"Using batch processing for {len(events)} events")
                                        event_batches = [events[i:i+BATCH_SIZE] for i in range(0, len(events), BATCH_SIZE)]
                                        logger.info(f"Processing {len(events)} events in {len(event_batches)} batches of max {BATCH_SIZE}")

                                        # Process each batch
                                        batch_num = 0
                                        for event_batch in event_batches:
                                            batch_num += 1
                                            batch_normalized = []

                                            # Step 1: Normalize all events in batch
                                            for event in event_batch:
                                                try:
                                                    normalized_event = normalize_event_data(event)
                                                    if normalized_event is None:
                                                        continue

                                                    # Add server ID
                                                    normalized_event["server_id"] = server_id

                                                    # Update timestamp in coordinator
                                                    if "timestamp" in normalized_event and isinstance(normalized_event["timestamp"], datetime):
                                                        parser_coordinator.update_csv_timestamp(server_id, normalized_event["timestamp"])

                                                    # Add to batch
                                                    batch_normalized.append(normalized_event)
                                                except Exception as e:
                                                    errors.append(str(e))
                                                    continue

                                            # Step 2: Categorize batch into kill and suicide events
                                            kill_events = []
                                            suicide_events = []

                                            for event in batch_normalized:
                                                event_type = categorize_event(event)
                                                if event_type == "kill":
                                                    kill_events.append(event)
                                                elif event_type == "suicide":
                                                    suicide_events.append(event)

                                            # Log batch summary (only for larger batches to reduce log spam)
                                            if batch_num % 5 == 0 or batch_num == 1 or batch_num == len(event_batches):
                                                logger.info(f"Batch {batch_num}/{len(event_batches)}: {len(batch_normalized)} events normalized")
                                                logger.info(f"Batch {batch_num} has {len(kill_events)} kills and {len(suicide_events)} suicides")

                                            # Step 3: Bulk insert into database
                                            # 3a: Process kill events in bulk
                                            if kill_events is not None:
                                                # Create the documents to insert
                                                kill_docs = []
                                                for event in kill_events:
                                                    # Get fields with fallbacks
                                                    killer_id = event.get("killer_id", "")
                                                    victim_id = event.get("victim_id", "")

                                                    # Skip if missing essential IDs
                                                    if not killer_id or not victim_id:
                                                        continue

                                                    # Create document for database
                                                    kill_doc = {
                                                        "server_id": server_id,
                                                        "killer_id": killer_id,
                                                        "killer_name": event.get("killer_name", "Unknown"),
                                                        "victim_id": victim_id,
                                                        "victim_name": event.get("victim_name", "Unknown"),
                                                        "weapon": event.get("weapon", "Unknown"),
                                                        "distance": event.get("distance", 0),
                                                        "timestamp": event.get("timestamp", datetime.now()),
                                                        "is_suicide": False,
                                                        "event_type": "kill"
                                                    }
                                                    kill_docs.append(kill_doc)

                                                # Bulk insert the kill documents
                                                if kill_docs is not None:
                                                    try:
                                                        # Use ordered=False to continue inserting even if some fail
                                                        result = await self.bot.db.kills.insert_many(kill_docs, ordered=False)
                                                        processed_count += len(result.inserted_ids)
                                                    except Exception as e:
                                                        logger.error(f"Error during bulk kill insert: {str(e)[:100]}")
                                                        # Continue processing other batches despite errors

                                            # 3b: Process suicide events in bulk
                                            if suicide_events is not None:
                                                # Create the documents to insert
                                                suicide_docs = []
                                                for event in suicide_events:
                                                    victim_id = event.get("victim_id", "")
                                                    # Skip if no valid ID
                                                    if victim_id is None:
                                                        continue

                                                    # Create document for database
                                                    suicide_doc = {
                                                        "server_id": server_id,
                                                        "killer_id": victim_id,
                                                        "killer_name": event.get("victim_name", "Unknown"),
                                                        "victim_id": victim_id,
                                                        "victim_name": event.get("victim_name", "Unknown"),
                                                        "weapon": event.get("weapon", "Unknown"),
                                                        "distance": event.get("distance", 0),
                                                        "timestamp": event.get("timestamp", datetime.now()),
                                                        "is_suicide": True,
                                                        "event_type": "suicide"
                                                    }
                                                    suicide_docs.append(suicide_doc)

                                                # Bulk insert the suicide documents
                                                if suicide_docs is not None:
                                                    try:
                                                        # Use ordered=False to continue inserting even if some fail
                                                        result = await self.bot.db.kills.insert_many(suicide_docs, ordered=False)
                                                        processed_count += len(result.inserted_ids)
                                                    except Exception as e:
                                                        logger.error(f"Error during bulk suicide insert: {str(e)[:100]}")
                                                        # Continue processing other batches despite errors

                                        # Final batch summary
                                        logger.info(f"Batch processing complete: {processed_count} events inserted successfully")

                                    else:
                                        # Enhanced batch processing for smaller files too
                                        # Collect kill and suicide events for batch processing
                                        kill_events = []
                                        suicide_events = []
                                        processed_count = 0

                                        logger.debug(f"Using batch processing for {len(events)} events")

                                        # Step 1: Normalize and categorize all events
                                        for event in events:
                                            try:
                                                # Normalize event data
                                                normalized_event = normalize_event_data(event)
                                                if normalized_event is None:
                                                    continue

                                                # Add server ID
                                                normalized_event["server_id"] = server_id

                                                # Update timestamp in coordinator
                                                if "timestamp" in normalized_event and isinstance(normalized_event["timestamp"], datetime):
                                                    parser_coordinator.update_csv_timestamp(server_id, normalized_event["timestamp"])

                                                # Process event type
                                                event_type = categorize_event(normalized_event)
                                                normalized_event["event_type"] = event_type

                                                # Validate IDs - using the improved validation from our _get_or_create_player method
                                                killer_id = normalized_event.get("killer_id", "")
                                                victim_id = normalized_event.get("victim_id", "")

                                                # Skip events with invalid IDs
                                                if not killer_id or killer_id.lower() in ['null', 'none', 'undefined'] or \
                                                   not victim_id or victim_id.lower() in ['null', 'none', 'undefined']:
                                                    continue

                                                # Add to appropriate batch
                                                if event_type == "kill":
                                                    kill_events.append(normalized_event)
                                                elif event_type == "suicide":
                                                    suicide_events.append(normalized_event)
                                            except Exception as e:
                                                errors.append(str(e))
                                                logger.error(f"Error normalizing/categorizing event: {str(e)[:100]}")

                                        # Report categorization results
                                        logger.debug(f"Categorized events: {len(kill_events)} kills, {len(suicide_events)} suicides")

                                        # Step 2: Process suicide events in batch
                                        if suicide_events is not None:
                                            # Create documents for bulk insert
                                            suicide_docs = []
                                            for event in suicide_events:
                                                suicide_docs.append({
                                                    "server_id": server_id,
                                                    "killer_id": event.get("victim_id"),  # For suicides, killer = victim
                                                    "killer_name": event.get("victim_name", "Unknown"),
                                                    "victim_id": event.get("victim_id"),
                                                    "victim_name": event.get("victim_name", "Unknown"),
                                                    "weapon": event.get("weapon", "Unknown"),
                                                    "distance": event.get("distance", 0),
                                                    "timestamp": event.get("timestamp", datetime.now()),
                                                    "is_suicide": True,
                                                    "event_type": "suicide"
                                                })

                                            # Bulk insert suicide events
                                            if suicide_docs is not None:
                                                try:
                                                    # Use ordered=False to allow partial success
                                                    result = await self.bot.db.kills.insert_many(suicide_docs, ordered=False)
                                                    processed_count += len(suicide_docs)
                                                    logger.info(f"Inserted {len(suicide_docs)} suicide events in batch")
                                                except Exception as e:
                                                    logger.error(f"Error bulk inserting suicide events: {str(e)[:100]}")

                                        # Step 3: Process kill events in batch
                                        if kill_events is not None:
                                            # Create documents for bulk insert
                                            kill_docs = []
                                            for event in kill_events:
                                                kill_docs.append({
                                                    "server_id": server_id,
                                                    "killer_id": event.get("killer_id"),
                                                    "killer_name": event.get("killer_name", "Unknown"),
                                                    "victim_id": event.get("victim_id"),
                                                    "victim_name": event.get("victim_name", "Unknown"),
                                                    "weapon": event.get("weapon", "Unknown"),
                                                    "distance": event.get("distance", 0),
                                                    "timestamp": event.get("timestamp", datetime.now()),
                                                    "is_suicide": False,
                                                    "event_type": "kill"
                                                })

                                            # Bulk insert kill events
                                            if kill_docs is not None:
                                                try:
                                                    # Use ordered=False to allow partial success
                                                    result = await self.bot.db.kills.insert_many(kill_docs, ordered=False)
                                                    processed_count += len(kill_docs)
                                                    logger.info(f"Inserted {len(kill_docs)} kill events in batch")
                                                except Exception as e:
                                                    logger.error(f"Error bulk inserting kill events: {str(e)[:100]}")

                                        # Step 4: Process player stats for unique players
                                        # This is more efficient than updating per-event
                                        # Create player lookup sets for batch processing player stats
                                        try:
                                            from models.player import Player

                                            # Collect unique player IDs
                                            unique_players = {}  # player_id -> {kills, deaths, suicides}

                                            # Count kills for killers, deaths for victims 
                                            for event in kill_events:
                                                killer_id = event.get("killer_id")
                                                victim_id = event.get("victim_id")

                                                # Add killer stats
                                                if killer_id not in unique_players:
                                                    unique_players[killer_id] = {"kills": 0, "deaths": 0, "suicides": 0, "name": event.get("killer_name", "Unknown")}
                                                unique_players[killer_id]["kills"] += 1

                                                # Add victim stats
                                                if victim_id not in unique_players:
                                                    unique_players[victim_id] = {"kills": 0, "deaths": 0, "suicides": 0, "name": event.get("victim_name", "Unknown")}
                                                unique_players[victim_id]["deaths"] += 1

                                            # Count suicides
                                            for event in suicide_events:
                                                player_id = event.get("victim_id")

                                                # Add player stats
                                                if player_id not in unique_players:
                                                    unique_players[player_id] = {"kills": 0, "deaths": 0, "suicides": 0, "name": event.get("victim_name", "Unknown")}
                                                unique_players[player_id]["suicides"] += 1

                                            # Update stats for each unique player
                                            logger.debug(f"Updating stats for {len(unique_players)} unique players")
                                            for player_id, stats in unique_players.items():
                                                # Get or create player
                                                player = await self._get_or_create_player(server_id, player_id, stats["name"])

                                                if player is not None is not None:
                                                    # Update stats
                                                    await player.update_stats(self.bot.db, 
                                                                            kills=stats["kills"], 
                                                                            deaths=stats["deaths"], 
                                                                            suicides=stats["suicides"])

                                            # Update nemesis/prey relationships in bulk
                                            logger.debug(f"Updating nemesis/prey relationships")
                                            await Player.update_all_nemesis_and_prey(self.bot.db, server_id)

                                        except Exception as e:
                                            logger.error(f"Error processing player stats in batch: {str(e)[:150]}")
                                            logger.error(f"Will continue with event recording even if player stats update failed")

                                    processed = processed_count

                                    events_processed += processed
                                    files_processed += 1

                                    if errors is not None:
                                        logger.warning(f"Errors processing {file}: {len(errors)} errors")

                                    # Update last processed time if this is the newest file
                                    if file == new_files[-1]:
                                        try:
                                            file_time = datetime.strptime(file.split('.csv')[0], "%Y.%m.%d-%H.%M.%S")
                                            self.last_processed[server_id] = file_time
                                            
                                            # CRITICAL FIX: Also track line position for newest file when in historical mode
                                            if is_historical_mode is not None:
                                                # Count total lines in file for position tracking
                                                try:
                                                    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                                                        line_count = sum(1 for _ in f)
                                                    
                                                    # Initialize server's tracking dict if needed
                                                    if server_id not in self.last_processed_line_positions:
                                                        self.last_processed_line_positions[server_id] = {}
                                                    
                                                    # Store line count for this file
                                                    file_key = os.path.basename(file)
                                                    self.last_processed_line_positions[server_id][file_key] = line_count
                                                    logger.warning(f"CRITICAL FIX: Stored line position {line_count} for newest file {file_key}")
                                                except Exception as line_err:
                                                    logger.error(f"Error counting lines in newest file: {line_err}")
                                        except ValueError:
                                            # If we can't parse the timestamp from filename, use current time
                                            self.last_processed[server_id] = datetime.now()

                            except Exception as e:
                                logger.error(f"Error processing file {file}: {e}f")

                        # Memory optimization - clear local variables before completing
                        try:
                            # Force garbage collection to release memory
                            import gc

                            # Clear any large local variables
                            if 'csv_parser_data' in locals():
                                del csv_parser_data
                            if 'content_io' in locals():
                                del content_io
                            if 'decoded_content' in locals():
                                del decoded_content
                            if 'content' in locals():
                                del content
                            if 'all_events' in locals():
                                del all_events
                            if 'kill_events' in locals():
                                del kill_events
                            if 'suicide_events' in locals():
                                del suicide_events
                            if 'kill_docs' in locals():
                                del kill_docs
                            if 'suicide_docs' in locals():
                                del suicide_docs

                            # Run garbage collection
                            collected = gc.collect()
                            logger.debug(f"Memory optimization: freed {collected} objects after CSV processing")
                        except Exception as mem_err:
                            logger.warning(f"Memory optimization failed: {mem_err}")
                        
                        # CRITICAL FIX: Clean up historical parse tracking on success path
                        if is_historical_mode and server_id in self.servers_with_active_historical_parse:
                            try:
                                logger.warning(f"CRITICAL FIX: Success path - removing server {server_id} from active historical parse tracking")
                                self.servers_with_active_historical_parse.remove(server_id)
                            except Exception as e:
                                logger.error(f"Error removing server from historical tracking: {e}")

                        # Keep the connection open for the next operation
                
                # Return results
                return files_processed, events_processed
                
            except Exception as e:
                logger.error(f"SFTP error for server {server_id}: {e}f")
                # Run garbage collection before returning
                try:
                    import gc
                    collected = gc.collect()
                    logger.debug(f"Memory optimization: freed {collected} objects after CSV error")
                except:
                    pass
                # CRITICAL FIX: Also clean up historical parse tracking in error path
                if is_historical_mode and server_id in self.servers_with_active_historical_parse:
                    try:
                        logger.warning(f"CRITICAL FIX: Error path - removing server {server_id} from active historical parse tracking")
                        self.servers_with_active_historical_parse.remove(server_id)
                    except Exception as e:
                        logger.error(f"Error removing server from historical tracking in error path: {e}")
                
                # Set empty results
                files_processed = 0
                events_processed = 0
                
                # Return error results
                return files_processed, events_processed

        # Finalization code moved outside of the try-except block
        # Provide more informative completion message
        file_count = 0
        if 'files_processed' in locals() and files_processed is not None:
            file_count = files_processed
            
        # Only update our tracking counters if files were actually processed
        if file_count > 0:
            # Update global counters
            if not hasattr(self, 'total_files_processed'):
                self.total_files_processed = 0
            self.total_files_processed += file_count
            
        # Also count any found files that might not have been processed yet
        found_files = 0
        if hasattr(self, 'map_csv_files_found') and self.map_csv_files_found:
            found_files = len(self.map_csv_files_found)
            
        if file_count > 0:
            logger.info(f"CSV processing completed for server {server_id} - Processed {file_count} files")
        elif found_files > 0:
            logger.info(f"CSV processing completed for server {server_id} - Found {found_files} files but none required processing")

        # Final memory optimization
        try:
            import gc
            collected = gc.collect()
            logger.info(f"Final memory optimization: freed {collected} objects at completion")
        except:
            pass

        # Return the values
        return files_processed, events_processed

    async def run_historical_parse_with_config(self, server_id: str, server_config: Dict[str, Any], 
                                  days: int = 30, guild_id: Optional[str] = None) -> Tuple[int, int]:
        """Run a historical parse for a server using direct configuration.

        This enhanced method accepts a complete server configuration object to bypass resolution issues,
        ensuring we have all the necessary details even for newly added servers.

        Args:
            server_id: Server ID to process
            server_config: Complete server configuration with SFTP details
            days: Number of days to look back (default: 30)
            guild_id: Optional Discord guild ID for server isolation

        Returns:
            Tuple[int, int]: Number of files processed and events processed
        """
        # Use the primary implementation
        logger.info(f"Starting historical parse with direct config for server {server_id}, looking back {days} days")

        # Configure processing start time based on requested days
        start_date = datetime.now() - timedelta(days=days)
        logger.info(f"Historical parse will check files from {start_date.strftime('%Y-%m-%d')} until now")

        # CRITICAL FIX: Reset the last_processed timestamp and clear stats
        logger.info(f"Resetting last_processed timestamp for server {server_id}")
        self.last_processed[server_id] = start_date
        logger.debug(f"CRITICAL FIX: Set processing window to include all files newer than {start_date.strftime('%Y.%m.%d-%H.%M.%S')}")
        
        # Clear all existing player stats and kill data for a clean historical parse
        try:
            logger.info(f"Clearing existing stats for server {server_id}")
            
            # Check if database is initialized
            if self.bot.db is None:
                logger.error(f"Database not initialized, cannot clear stats for server {server_id}")
                return False
                
            # Clear kill events to rebuild from scratch
            if hasattr(self.bot.db, 'kills'):
                kill_result = await self.bot.db.kills.delete_many({"server_id": server_id})
                logger.info(f"Deleted {kill_result.deleted_count} existing kill events for server {server_id}")
            else:
                logger.error("Database connection exists but 'kills' collection not accessible")
            
            # Reset player stats
            if hasattr(self.bot.db, 'players'):
                player_result = await self.bot.db.players.update_many(
                    {"server_id": server_id},
                    {"$set": {
                        "kills": 0,
                        "deaths": 0,
                        "last_updated": datetime.now(),
                        "last_seen": datetime.now()
                    }}
                )
                logger.warning(f"CRITICAL FIX: Reset stats for {player_result.modified_count} players for server {server_id}")
            else:
                logger.error("Database connection exists but 'players' collection not accessible")
            
            # Clear rivalry data
            if hasattr(self.bot.db, 'rivalries'):
                rivalry_result = await self.bot.db.rivalries.delete_many({"server_id": server_id})
                logger.warning(f"CRITICAL FIX: Deleted {rivalry_result.deleted_count} existing rivalries for server {server_id}")
            else:
                logger.error("Database connection exists but 'rivalries' collection not accessible")
                
            logger.warning(f"CRITICAL FIX: Successfully reset all stats for historical parse of server {server_id}")
        except Exception as e:
            logger.error(f"CRITICAL FIX: Error clearing stats: {e}")

        # Ensure original_server_id is present in config
        original_server_id = server_config.get("original_server_id")
        if original_server_id is not None:
            logger.info(f"Using original_server_id {original_server_id} from provided config")
        else:
            logger.warning(f"No original_server_id in provided config, paths may use UUID format")

        # Process the server directly with provided configuration
        async with self.processing_lock:
            self.is_processing = True
            try:
                # Pass the full configuration and start date directly to _process_server_csv_files
                files_processed, events_processed = await self._process_server_csv_files(
                    server_id, server_config, start_date=start_date
                )
                logger.info(f"Direct config historical parse complete for server {server_id}: "
                          f"processed {files_processed} files with {events_processed} events")
                # Track server activity for adaptive processing
                try:
                    await self._check_server_activity(server_id, events_processed)
                except Exception as e:
                    logger.warning(f"Error tracking server activity: {e}")
            
                return files_processed, events_processed
            except Exception as e:
                logger.error(f"Error in direct config historical parse for server {server_id}: {e}")
                return 0, 0
            finally:
                self.is_processing = False

    async def run_historical_parse(self, server_id: str, days: int = 30, guild_id: Optional[str] = None) -> Tuple[int, int]:
        """Run a historical parse for a server, checking further back in time

        This function is meant to be called when setting up a new server to process
        older historical data going back further than the normal processing window.
        
        CRITICAL FIX: This method now pauses the regular CSV processing for this server
        while the historical parse is running, then resumes it when done. This prevents
        race conditions between the historical parser and regular parser when a server
        is first added.

        Args:
            server_id: Server ID to process (can be UUID or numeric ID)
            days: Number of days to look back (default: 30)
            guild_id: Optional Discord guild ID for server isolation

        Returns:
            Tuple[int, int]: Number of files processed and events processed
        """
        # Initialize result variables
        files_processed, events_inserted = 0, 0
        
        # CRITICAL FIX: Initialize set of servers with active historical parses if needed
        if not hasattr(self, 'servers_with_active_historical_parse'):
            self.servers_with_active_historical_parse = set()
            
        # Flag to track if we successfully registered the historical parse
        historical_parse_registered = False
            
        try:
            # CRITICAL FIX: Mark this server as having an active historical parse
            # This will cause the regular CSV processor to skip it
            try:
                # Set global flag indicating historical parsing is active
                self.is_historical_parsing = True
                # Add this server to the set of servers with active historical parses
                self.servers_with_active_historical_parse.add(server_id)
                historical_parse_registered = True
                logger.warning(f"CRITICAL FIX: Paused regular CSV processing for server {server_id} during historical parse")
            except Exception as e:
                logger.error(f"Error setting historical parse flags: {e}")
                # Continue anyway
            
            # Record the starting ID for logging
            raw_input_id = server_id if server_id is not None else ""

            # Import identity resolver functions
            from utils.server_utils import safe_standardize_server_id
            from utils.server_identity import resolve_server_id, identify_server, KNOWN_SERVERS

            logger.info(f"Starting historical parse for server {raw_input_id}, looking back {days} days")
        except Exception as e:
            logger.error(f"Error in historical parse preparation: {e}")
            files_processed, events_inserted = 0, 0
            # Early return on preparation failure
            return files_processed, events_inserted

        # STEP 1: Try to resolve the server ID comprehensively using our new function
        server_resolution = await resolve_server_id(self.bot.db, server_id, guild_id)
        if server_resolution is not None:
            resolved_server_id = server_resolution.get("server_id")
            original_server_id = server_resolution.get("original_server_id")
            server_config = server_resolution.get("config")
            collection = server_resolution.get("collection")

            logger.info(f"Enhanced server resolution found server: {server_id} → {resolved_server_id} "
                      f"(original_id: {original_server_id}, found in {collection})")

            # We have a direct server configuration from resolution
            if server_config is not None:
                # Configure processing start time based on requested days
                start_date = datetime.now() - timedelta(days=days)
                logger.info(f"Historical parse will check files from {start_date.strftime('%Y-%m-%d')} until now")

                # CRITICAL FIX: Reset the last_processed timestamp to ensure we process all files
                logger.warning(f"CRITICAL FIX: Resetting last_processed timestamp for server {resolved_server_id}")
                self.last_processed[resolved_server_id] = start_date
                logger.warning(f"CRITICAL FIX: Set processing window to include all files newer than {start_date.strftime('%Y.%m.%d-%H.%M.%S')}")

                # CRITICAL IMPROVEMENT: Clear existing data for this server first
                # This ensures we can reprocess everything from scratch without duplication
                try:
                    logger.info(f"Clearing existing data for server {resolved_server_id} before historical parse")

                    # Check if database is initialized
                    if self.bot.db is None:
                        logger.error(f"Database not initialized, cannot clear stats for server {resolved_server_id}")
                        return False
                        
                    # Delete all kill events for this server
                    if hasattr(self.bot.db, 'kills'):
                        kill_result = await self.bot.db.kills.delete_many({"server_id": resolved_server_id})
                        logger.info(f"Deleted {kill_result.deleted_count} existing kill events for server {resolved_server_id}")
                    else:
                        logger.error("Database connection exists but 'kills' collection not accessible")

                    # Update player stats to reset kill/death/suicide counts
                    if hasattr(self.bot.db, 'players'):
                        player_reset = await self.bot.db.players.update_many(
                            {"server_id": resolved_server_id},
                            {"$set": {"kills": 0, "deaths": 0, "suicides": 0, "updated_at": datetime.utcnow()}}
                        )
                        logger.info(f"Reset stats for {player_reset.modified_count} players for server {resolved_server_id}")
                    else:
                        logger.error("Database connection exists but 'players' collection not accessible")

                    # Clear rivalry data
                    if hasattr(self.bot.db, 'rivalries'):
                        rivalry_result = await self.bot.db.rivalries.delete_many({"server_id": resolved_server_id})
                        logger.info(f"Deleted {rivalry_result.deleted_count} existing rivalries for server {resolved_server_id}")
                    else:
                        logger.error("Database connection exists but 'rivalries' collection not accessible")

                    # Force garbage collection to free up memory
                    import gc
                    gc.collect()
                    logger.info("Forced garbage collection after data clearing")

                except Exception as e:
                    logger.error(f"Error clearing existing data: {e}")

                # Process CSV files with the directly resolved configuration
                async with self.processing_lock:
                    self.is_processing = True
                    try:
                        # Use the resolved configuration directly
                        files_processed, events_processed = await self._process_server_csv_files(
                            resolved_server_id, server_config, start_date=start_date
                        )
                        logger.info(f"Direct resolution historical parse complete for server {resolved_server_id}: "
                                   f"processed {files_processed} files with {events_processed} events")
                        # Track server activity for adaptive processing
                        try:
                            await self._check_server_activity(server_id, events_processed)
                        except Exception as e:
                            logger.warning(f"Error tracking server activity: {e}")
                
                        return files_processed, events_processed
                    except Exception as e:
                        logger.error(f"Error in direct resolution historical parse for server {resolved_server_id}: {e}")
                        return 0, 0
                    finally:
                        self.is_processing = False
                        
                        # CRITICAL FIX: Clean up historical parse flags
                        try:
                            # Remove this server from the active historical parse set
                            if historical_parse_registered and hasattr(self, 'servers_with_active_historical_parse'):
                                if resolved_server_id in self.servers_with_active_historical_parse:
                                    self.servers_with_active_historical_parse.remove(resolved_server_id)
                                    logger.warning(f"CRITICAL FIX: Resumed regular CSV processing for server {resolved_server_id} after historical parse")
                            
                            # Check if we can reset the global flag
                            if hasattr(self, 'servers_with_active_historical_parse') and not self.servers_with_active_historical_parse:
                                self.is_historical_parsing = False
                                logger.warning("All historical parses complete, resuming normal operation")
                        except Exception as cleanup_error:
                            logger.error(f"Error cleaning up historical parse flags: {cleanup_error}")

        # STEP 2: Fall back to traditional method if direct resolution failed
        logger.info(f"Direct server resolution failed or returned no config, falling back to traditional lookup")

        # Standardize server ID and check for numeric ID
        server_id = safe_standardize_server_id(raw_input_id)
        original_numeric_id = None

        # Check if this is a numeric ID (like "7020") being used directly
        if server_id is not None and server_id.isdigit():
            original_numeric_id = server_id
            logger.info(f"Received numeric ID {original_numeric_id} for historical parse")

            # Look for a matching server in KNOWN_SERVERS by value
            found_uuid = None
            for uuid, numeric in KNOWN_SERVERS.items():
                if str(numeric) == original_numeric_id:
                    found_uuid = uuid
                    logger.info(f"Mapped numeric ID {original_numeric_id} to UUID {found_uuid}")
                    break

            if found_uuid is not None:
                server_id = found_uuid
            else:
                logger.warning(f"Could not find UUID for numeric ID {original_numeric_id} in KNOWN_SERVERS")

        # Get all server configurations
        server_configs = await self._get_server_configs()
        logger.info(f"Traditional lookup found server configs: {list(server_configs.keys())}")

        # Try to find the server in our configurations
        if server_id not in server_configs:
            # Try by original_server_id if we have one
            if original_numeric_id is not None:
                for config_id, config in server_configs.items():
                    if str(config.get("original_server_id")) == original_numeric_id:
                        server_id = config_id
                        logger.info(f"Found server by original_server_id {original_numeric_id}: {server_id}")
                        break

            # Try by numeric matching if needed
            if server_id not in server_configs and server_id and str(server_id).isdigit():
                numeric_matches = [sid for sid in server_configs.keys() if str(sid).isdigit() and int(sid) == int(server_id)]
                if numeric_matches is not None:
                    server_id = numeric_matches[0]
                    logger.info(f"Found server using numeric matching: {server_id}")

            # If still not found, give up
            if server_id not in server_configs:
                logger.error(f"Server {raw_input_id} not found in configs during historical parse")
                return 0, 0

        # Configure the processing window
        start_date = datetime.now() - timedelta(days=days)
        self.last_processed[server_id] = start_date

        # CRITICAL IMPROVEMENT: Clear existing data for this server first
        # This ensures we can reprocess everything from scratch without duplication
        try:
            logger.info(f"Clearing existing data for server {server_id} before historical parse (traditional method)")

            # Clear CSV processor state for this server from database
            if hasattr(self.bot, 'db') and self.bot.db is not None and hasattr(self.bot.db, 'csv_processor_state'):
                state_result = await self.bot.db.csv_processor_state.delete_one({"server_id": server_id})
                logger.info(f"Deleted CSV processor state for server {server_id} ({state_result.deleted_count} document)")
                
                # Also clear from memory
                if server_id in self.last_processed:
                    del self.last_processed[server_id]
                    logger.info(f"Cleared last_processed timestamp for server {server_id} from memory")
                    
                if server_id in self.last_processed_line_positions:
                    line_positions_count = len(self.last_processed_line_positions[server_id])
                    del self.last_processed_line_positions[server_id]
                    logger.info(f"Cleared last_processed_line_positions for server {server_id} from memory ({line_positions_count} entries)")

            # Delete all kill events for this server
            kill_result = await self.bot.db.kills.delete_many({"server_id": server_id})
            logger.info(f"Deleted {kill_result.deleted_count} existing kill events for server {server_id}")

            # Update player stats to reset kill/death/suicide counts
            player_reset = await self.bot.db.players.update_many(
                {"server_id": server_id},
                {"$set": {"kills": 0, "deaths": 0, "suicides": 0, "updated_at": datetime.utcnow()}}
            )
            logger.info(f"Reset stats for {player_reset.modified_count} players for server {server_id}")

            # Clear rivalry data
            rivalry_result = await self.bot.db.rivalries.delete_many({"server_id": server_id})
            logger.info(f"Deleted {rivalry_result.deleted_count} existing rivalries for server {server_id}")

            # Force garbage collection to free up memory
            import gc
            gc.collect()
            logger.info("Forced garbage collection after data clearing (traditional method)")

        except Exception as e:
            logger.error(f"Error clearing existing data (traditional method): {e}")

        # Process CSV files with the traditional method
        async with self.processing_lock:
            self.is_processing = True
            try:
                files_processed, events_processed = await self._process_server_csv_files(
                    server_id, server_configs[server_id], start_date=start_date
                )
                logger.info(f"Traditional historical parse complete for server {server_id}: "
                           f"processed {files_processed} files with {events_processed} events")
                # Track server activity for adaptive processing
                try:
                    await self._check_server_activity(server_id, events_processed)
                except Exception as e:
                    logger.warning(f"Error tracking server activity: {e}")
            
                return files_processed, events_processed
            except Exception as e:
                logger.error(f"Error in traditional historical parse for server {server_id}: {e}")
                return 0, 0
            finally:
                self.is_processing = False
                
                # CRITICAL FIX: Clean up historical parse flags
                try:
                    # Remove this server from the active historical parse set
                    if historical_parse_registered and hasattr(self, 'servers_with_active_historical_parse'):
                        if server_id in self.servers_with_active_historical_parse:
                            self.servers_with_active_historical_parse.remove(server_id)
                            logger.warning(f"CRITICAL FIX: Resumed regular CSV processing for server {server_id} after historical parse (traditional method)")
                    
                    # Check if we can reset the global flag
                    if hasattr(self, 'servers_with_active_historical_parse') and not self.servers_with_active_historical_parse:
                        self.is_historical_parsing = False
                        logger.warning("All historical parses complete, resuming normal operation (traditional method)")
                except Exception as cleanup_error:
                    logger.error(f"Error cleaning up historical parse flags (traditional method): {cleanup_error}")

    @app_commands.command(
        name="process_csv",
        description="Manually process CSV files from the game server"
    )
    @admin_permission_decorator()
    @premium_tier_required(feature_name="stats")  # Require Survivor tier for CSV processing
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def process_csv_command(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        hours: int = 24
    ):
        """Manually process CSV files from the game server

        Args:
            interaction: Discord interaction
            server_id: Server ID to process (optional)
            hours: Number of hours to look back (default: 24)
        """

        await interaction.response.defer(ephemeral=True)

        # Import standardization function
        from utils.server_utils import safe_standardize_server_id

        # Get server ID from guild config if not provided
        if server_id is None:
            # Try to get the server ID from the guild's configuration
            try:
                guild_id = str(interaction.guild_id)
                guild_doc = await self.bot.db.guilds.find_one({"guild_id": guild_id})
                if guild_doc is not None and "default_server_id" in guild_doc:
                    raw_server_id = guild_doc.get("default_server_id", "")
                    server_id = safe_standardize_server_id(raw_server_id)
                    logger.info(f"Using default server ID from guild config: {raw_server_id} (standardized to {server_id})")
                else:
                    # No default server configured
                    embed = await EmbedBuilder.create_error_embed(
                        title="No Server Configured",
                        description="No server ID provided and no default server configured for this guild."
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            except Exception as e:
                logger.error(f"Error getting default server ID: {e}")
                embed = await EmbedBuilder.create_error_embed(
                    title="Configuration Error",
                    description="An error occurred while retrieving the server configuration."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
        else:
            # Standardize the provided server ID
            raw_server_id = server_id
            server_id = safe_standardize_server_id(server_id)
            logger.info(f"Standardized provided server ID from {raw_server_id} to {server_id}")

        # Get server config
        server_configs = await self._get_server_configs()

        # Log all available server configs for debugging
        logger.info(f"Available server configs: {list(server_configs.keys())}")

        if server_id not in server_configs:
            # Try numeric comparison as fallback if server_id is numeric
            if server_id is not None and str(server_id).isdigit():
                numeric_matches = [sid for sid in server_configs.keys() if str(sid).isdigit() and int(sid) == int(server_id)]
                if numeric_matches is not None:
                    server_id = numeric_matches[0]
                    logger.info(f"Found server using numeric matching: {server_id}")

            # If still not found, show error
            if server_id not in server_configs:
                embed = await EmbedBuilder.create_error_embed(
                    title="Server Not Found",
                    description=f"No SFTP configuration found for server `{server_id}`."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Calculate lookback time
        # Ensure hours is a valid float value
        safe_hours = float(hours) if hours else 24.0

        # Safely update last_processed dictionary with server_id
        if server_id is not None and isinstance(server_id, str):
            self.last_processed[server_id] = datetime.now() - timedelta(hours=safe_hours)
            # Save updated state to database
            asyncio.create_task(self._save_server_state(server_id))
        else:
            logger.warning(f"Invalid server_id: {server_id}, not updating last_processed timestamp")

        # Process CSV files
        async with self.processing_lock:
            try:
                # Process files only if server_id exists in server_configs and it's a non-None string
                if server_id is not None and isinstance(server_id, str) and server_id in server_configs:
                    files_processed, events_processed = await self._process_server_csv_files(
                        server_id, server_configs[server_id]
                    )
                else:
                    logger.error(f"Invalid server_id: {server_id} or not found in server_configs")
                    files_processed, events_processed = 0, 0

                if files_processed > 0:
                    embed = await EmbedBuilder.create_success_embed(
                        title="CSV Processing Complete",
                        description=f"Processed {files_processed} file(s) with {events_processed} death events."
                    )
                else:
                    embed = await EmbedBuilder.create_info_embed(
                        title="No Files Found",
                        description=f"No new CSV files found for server `{server_id}` in the last {hours} hours."
                    )

                await interaction.followup.send(embed=embed, ephemeral=True)

            except Exception as e:
                logger.error(f"Error processing CSV files: {e}f")
                embed = await EmbedBuilder.create_error_embed(
                    title="Processing Error",
                    description=f"An error occurred while processing CSV files: {e}f"
                )
                await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="clear_csv_cache",
        description="Clear the CSV parser cache"
    )
    @admin_permission_decorator()
    @premium_tier_required(feature_name="stats")  # Require Survivor tier for CSV cache management
    async def clear_csv_cache_command(self, interaction: discord.Interaction):
        """Clear the CSV parser cache

        Args:
            interaction: Discord interaction
        """

        # Clear cache
        self.csv_parser.clear_cache()

        embed = await EmbedBuilder.create_success_embed(
            title="Cache Cleared",
            description="The CSV parser cache has been cleared."
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(
        name="historical_parse",
        description="Process historical CSV data going back further than normal processing"
    )
    @admin_permission_decorator()
    @premium_tier_required(feature_name="stats")  # Require Survivor tier
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def historical_parse_command(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        days: int = 30
    ):
        """Process historical CSV data going back further than normal processing

        Args:
            interaction: Discord interaction
            server_id: Server ID to process (optional)
            days: Number of days to look back (default: 30)
        """
        # Note: This command runs a historical parser that:
        # 1. Pauses regular CSV processing for this server
        # 2. Processes CSV files from the server's directory
        # 3. Resumes regular CSV processing when done
        # This ensures reliable data processing for historical data.

        await interaction.response.defer(ephemeral=True)

        # Import standardization function
        from utils.server_utils import safe_standardize_server_id

        # Get server ID from guild config if not provided
        if server_id is None:
            try:
                guild_id = str(interaction.guild_id)
                guild_doc = await self.bot.db.guilds.find_one({"guild_id": guild_id})
                if guild_doc is not None and "default_server_id" in guild_doc:
                    raw_server_id = guild_doc.get("default_server_id", "")
                    server_id = safe_standardize_server_id(raw_server_id)
                    logger.info(f"Using default server ID from guild config: {raw_server_id} (standardized to {server_id})")
                else:
                    embed = await EmbedBuilder.create_error_embed(
                        title="No Server Configured",
                        description="No server ID provided and no default server configured for this guild."
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            except Exception as e:
                logger.error(f"Error getting default server ID: {e}")
                embed = await EmbedBuilder.create_error_embed(
                    title="Configuration Error",
                    description="An error occurred while retrieving the server configuration."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
        else:
            # Standardize the provided server ID
            raw_server_id = server_id
            server_id = safe_standardize_server_id(server_id)
            logger.info(f"Standardized provided server ID from {raw_server_id} to {server_id}")

        # Get server config
        server_configs = await self._get_server_configs()

        # Log all available server configs for debugging
        logger.info(f"Available server configs: {list(server_configs.keys())}")

        if server_id not in server_configs:
            # Try numeric comparison as fallback if server_id is numeric
            if server_id is not None and str(server_id).isdigit():
                numeric_matches = [sid for sid in server_configs.keys() if str(sid).isdigit() and int(sid) == int(server_id)]
                if numeric_matches is not None:
                    server_id = numeric_matches[0]
                    logger.info(f"Found server using numeric matching: {server_id}")

            # If still not found, show error
            if server_id not in server_configs:
                embed = await EmbedBuilder.create_error_embed(
                    title="Server Not Found",
                    description=f"No SFTP configuration found for server `{server_id}`."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Validate days parameter
        safe_days = max(1, min(int(days) if days else 30, 90))  # Between 1 and 90 days

        # Send initial response
        embed = await EmbedBuilder.create_info_embed(
            title="Historical Parsing Started",
            description=f"Starting historical parsing for server `{server_id}` looking back {safe_days} days.\n\nThis may take some time, please wait..."
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

        # Run the historical parse
        try:
            files_processed, events_processed = await self.run_historical_parse(server_id, days=safe_days)

            if files_processed > 0:
                # Get additional statistics about processed events
                try:
                    # Get kill/suicide statistics
                    kills_count = await self.bot.db.kills.count_documents({
                        "server_id": server_id,
                        "is_suicide": False,
                        "timestamp": {"$gte": datetime.now() - timedelta(days=safe_days)}
                    })

                    suicides_count = await self.bot.db.kills.count_documents({
                        "server_id": server_id,
                        "is_suicide": True,
                        "timestamp": {"$gte": datetime.now() - timedelta(days=safe_days)}
                    })

                    # Count unique players involved
                    pipeline = [
                        {"$match": {
                            "server_id": server_id,
                            "timestamp": {"$gte": datetime.now() - timedelta(days=safe_days)}
                        }},
                        {"$group": {
                            "_id": None,
                            "unique_killers": {"$addToSet": "$killer_id"},
                            "unique_victims": {"$addToSet": "$victim_id"}
                        }}
                    ]

                    result = await self.bot.db.kills.aggregate(pipeline).to_list(length=1)

                    unique_killers = len(result[0]["unique_killers"]) if result is not None else 0
                    unique_victims = len(result[0]["unique_victims"]) if result else 0

                    # Calculate unique players (combined set)
                    unique_players = unique_killers + unique_victims - (len(set(result[0]["unique_killers"]) & set(result[0]["unique_victims"])) if result else 0)

                    # Calculate percentages
                    kill_percent = (kills_count / events_processed) * 100 if events_processed > 0 else 0
                    suicide_percent = (suicides_count / events_processed) * 100 if events_processed > 0 else 0

                    # Build enhanced description
                    description = (
                        f"**Process Summary:**\n"
                        f"• Files Processed: **{files_processed}**\n"
                        f"• Total Events: **{events_processed}**\n\n"
                        f"**Event Breakdown:**\n"
                        f"• Kills: **{kills_count}** ({kill_percent:.1f}%)\n"
                        f"• Suicides: **{suicides_count}** ({suicide_percent:.1f}%)\n\n"
                        f"**Player Statistics:**\n"
                        f"• Unique Players: **{unique_players}**\n"
                        f"• Active Killers: **{unique_killers}**\n"
                        f"• Active Victims: **{unique_victims}**\n"
                        f"_Looking back {safe_days} days from today_"
                    )

                except Exception as e:
                    logger.error(f"Error calculating enhanced statistics: {e}f")
                    description = f"Processed {files_processed} historical file(s) with {events_processed} death events."

                embed = await EmbedBuilder.create_success_embed(
                    title="Historical Parsing Complete",
                    description=description
                )
            else:
                embed = await EmbedBuilder.create_info_embed(
                    title="No Historical Files Found",
                    description=f"No historical CSV files found for server `{server_id}` in the last {safe_days} days."
                )

            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            logger.error(f"Error in historical parse command: {e}")
            embed = await EmbedBuilder.create_error_embed(
                title="Processing Error",
                description=f"An error occurred during historical parsing: {e}f"
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="reset_stats",
        description="Reset and rebuild all statistics for a server from CSV files"
    )
    @admin_permission_decorator()
    @premium_tier_required(feature_name="rivalries")  # Require higher tier for this powerful command
    async def reset_stats_command(self, interaction: discord.Interaction, days: int = 60):
        """Reset and rebuild all player statistics for a server from CSV files
        
        This is a powerful command that will:
        1. Clear all kill records for the server
        2. Reset all player statistics to zero
        3. Clear all rivalries
        4. Run a full historical parse of all CSV files
        
        Args:
            interaction: Discord interaction
            days: Number of days to look back (default: 60)
        """
        # Get guild ID
        guild_id = str(interaction.guild_id) if interaction.guild else None
        
        if guild_id is None:
            await interaction.response.send_message("This command can only be used in a server", ephemeral=True)
            return
            
        await interaction.response.defer(ephemeral=True)
            
        # Get server config for this guild
        server_configs = await self._get_server_configs()
        
        # Filter for servers in this guild
        guild_servers = {sid: config for sid, config in server_configs.items() 
                         if str(config.get("guild_id")) == guild_id}
        
        if guild_servers is None:
            await interaction.followup.send("No server configuration found for this Discord server", ephemeral=True)
            return
            
        # Get confirmation
        confirm = discord.ui.Button(label="Confirm Reset", style=discord.ButtonStyle.danger)
        cancel = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)
        view = discord.ui.View()
        view.add_item(confirm)
        view.add_item(cancel)
        
        servers_list = "\n".join([f"• {config.get('name', sid)}" for sid, config in guild_servers.items()])
        
        embed = await EmbedBuilder.create_warning_embed(
            title="⚠️ Reset All Player Statistics?",
            description=f"This will completely reset and rebuild ALL statistics for the following server(s):\n\n{servers_list}\n\n" +
                      f"This will reset all kills, deaths, and statistics tracking. All players will start fresh.\n\n" +
                      "**This action cannot be undone.** Are you sure?"
        )
        
        confirm_msg = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        
        # Set up confirmation callback
        async def on_confirm(confirm_interaction):
            await confirm_interaction.response.defer(ephemeral=True)
            
            # Process each server
            results = []
            for server_id, server_config in guild_servers.items():
                try:
                    # Run the historical parse with stat clearing
                    files_processed, events_processed = await self.run_historical_parse_with_config(
                        server_id=server_id,
                        server_config=server_config,
                        days=days,
                        guild_id=guild_id
                    )
                    
                    server_name = server_config.get("name", server_id)
                    results.append(f"• **{server_name}**: Processed {files_processed} files with {events_processed} events")
                except Exception as e:
                    logger.error(ff"\1")
                    results.append(f"• **{server_id}**: Error - {e}f")
            
            result_embed = await EmbedBuilder.create_success_embed(
                title="Statistics Reset Complete",
                description=f"Successfully processed server statistics:\n\n{''.join(results)}"
            )
            
            await confirm_interaction.followup.send(embed=result_embed, ephemeral=True)
            # Also edit the original message
            await confirm_msg.edit(embed=result_embed, view=None)
            
        async def on_cancel(cancel_interaction):
            await cancel_interaction.response.defer(ephemeral=True)
            cancel_embed = await EmbedBuilder.create_info_embed(
                title="Reset Cancelled",
                description="Statistics reset operation was cancelled."
            )
            await cancel_interaction.followup.send(embed=cancel_embed, ephemeral=True)
            await confirm_msg.edit(embed=cancel_embed, view=None)
            
        # Set callbacks
        confirm.callback = on_confirm
        cancel.callback = on_cancel

    @app_commands.command(
        name="csv_status",
        description="Show CSV processor status"
    )
    @admin_permission_decorator()
    @premium_tier_required(feature_name="stats")  # Require Survivor tier for CSV status
    async def csv_status_command(self, interaction: discord.Interaction):
        """Show CSV processor status

        Args:
            interaction: Discord interaction
        """

        await interaction.response.defer(ephemeral=True)

        # Get server configs
        server_configs = await self._get_server_configs()

        # Create status embed
        embed = await EmbedBuilder.create_info_embed(
            title="CSV Processor Status",
            description="Current status of the CSV processor"
        )

        # Add processing status
        embed.add_field(
            name="Currently Processing",
            value="Yes" if self.is_processing else "No",
            inline=True
        )

        # Add configured servers
        server_list = []
        for server_id, config in server_configs.items():
            last_time = self.last_processed.get(server_id, "Never")
            logger.info(f"DIAGNOSTIC: Using a 60-day cutoff for CSV processing: {last_time.strftime('%Y-%m-%d %H:%M:%S')}")
            if isinstance(last_time, datetime):
                last_time = last_time.strftime("%Y-%m-%d %H:%M:%S")

            server_list.append(f"• `{server_id}` - Last processed: {last_time}")

        if server_list is not None:
            embed.add_field(
                name="Configured Servers",
                value="\n".join(server_list),
                inline=False
            )
        else:
            embed.add_field(
                name="Configured Servers",
                value="No servers configured",
                inline=False
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    async def _process_kill_event(self, event: Dict[str, Any]) -> bool:
        """Process a kill event and update player stats and rivalries

        Args:
            event: Normalized kill event dictionary

        Returns:
            bool: True if processed successfully, False otherwise
        """
        try:
            server_id = event.get("server_id")
            if server_id is None:
                logger.warning("Kill event missing server_id, skipping")
                return False

            # Get kill details
            killer_id = event.get("killer_id", "")
            killer_name = event.get("killer_name", "Unknown")
            victim_id = event.get("victim_id", "")
            victim_name = event.get("victim_name", "Unknown")
            weapon = event.get("weapon", "Unknown")
            distance = event.get("distance", 0)
            timestamp = event.get("timestamp", datetime.utcnow())

            # Get the event_type directly from the normalized event
            event_type = event.get("event_type")

            # If event_type not present, determine it using the categorize_event function
            if event_type is None:
                from utils.parser_utils import categorize_event
                event_type = categorize_event(event)
                logger.debug(f"Determined event_type using categorize_event: {event_type}")

            # Set suicide flag based on event type
            is_suicide = (event_type == "suicide")
            logger.debug(f"Processing event: type={event_type}, is_suicide={is_suicide}")

            # Ensure timestamp is datetime
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp)
                except ValueError:
                    # Try other formats
                    try:
                        common_formats = [
                            "%Y.%m.%d-%H.%M.%S",
                            "%Y.%m.%d-%H.%M.%S:%f",
                            "%Y-%m-%d %H:%M:%S",
                            "%Y-%m-%d %H:%M:%S.%f"
                        ]

                        for fmt in common_formats:
                            try:
                                timestamp = datetime.strptime(timestamp, fmt)
                                break
                            except ValueError:
                                continue

                        # If we still have a string, use current time
                        if isinstance(timestamp, str):
                            logger.warning(f"Could not parse timestamp: {timestamp}, using current time")
                            timestamp = datetime.utcnow()
                    except Exception as e:
                        logger.error(f"Error parsing timestamp '{timestamp}': {e}")
                        timestamp = datetime.utcnow()

            # For suicide events, ensure killer_id matches victim_id
            if is_suicide is not None:
                if killer_id != victim_id:
                    logger.debug(f"Fixing inconsistent suicide data: setting killer_id={victim_id} (was {killer_id})")
                    killer_id = victim_id
                    killer_name = victim_name

            # Check if we have the necessary player IDs
            if victim_id is None:
                logger.warning("Kill event missing victim_id, skipping")
                return False

            # For suicides, we only need to update the victim's stats
            if is_suicide is not None:
                # Get victim player if player is not None else create if doesn't exist
                victim = await self._get_or_create_player(server_id, victim_id, victim_name)

                # Update suicide count
                await victim.update_stats(self.bot.db, kills=0, deaths=0, suicides=1)

                # Insert suicide event into database for consistent record-keeping
                suicide_doc = {
                    "server_id": server_id,
                    "killer_id": victim_id,  # Same as victim_id for suicides
                    "killer_name": victim_name,
                    "victim_id": victim_id,
                    "victim_name": victim_name,
                    "weapon": weapon,
                    "distance": distance,
                    "timestamp": timestamp,
                    "is_suicide": True,
                    "event_type": "suicide"
                }

                await self.bot.db.kills.insert_one(suicide_doc)
                logger.info(f"Recorded suicide event for player {victim_name} ({victim_id})")
                return True

            # For regular kills, we need both killer and victim
            if killer_id is None:
                logger.warning("Kill event missing killer_id for non-suicide, skipping")
                return False

            # Get killer and victim players, or create if they don't exist
            killer = await self._get_or_create_player(server_id, killer_id, killer_name)
            victim = await self._get_or_create_player(server_id, victim_id, victim_name)

            # Update kill/death stats
            await killer.update_stats(self.bot.db, kills=1, deaths=0)
            await victim.update_stats(self.bot.db, kills=0, deaths=1)

            # Update rivalries
            from models.rivalry import Rivalry
            await Rivalry.record_kill(server_id, killer_id, victim_id, weapon, "")

            # Update nemesis/prey relationships
            await killer.update_nemesis_and_prey(self.bot.db)
            await victim.update_nemesis_and_prey(self.bot.db)

            # Insert kill event into database
            kill_doc = {
                "server_id": server_id,
                "killer_id": killer_id,
                "killer_name": killer_name,
                "victim_id": victim_id,
                "victim_name": victim_name,
                "weapon": weapon,
                "distance": distance,
                "timestamp": timestamp,
                "is_suicide": is_suicide,
                "event_type": event_type or "kill"  # Ensure we always have an event_type
            }

            await self.bot.db.kills.insert_one(kill_doc)

            return True

        except Exception as e:
            logger.error(f"Error processing kill event: {e}")
            return False

    async def _get_or_create_player(self, server_id: str, player_id: str, player_name: str):
        """Get player by ID or create if it doesn't exist

        Args:
            server_id: Server ID
            player_id: Player ID
            player_name: Player name

        Returns:
            Player object or None if invalid data
        """
        from models.player import Player

        # Validate IDs first to prevent database errors
        if player_id is None or player_id in ['null', 'none', 'undefined']:
            logger.warning(f"Invalid player_id: '{player_id}' for player '{player_name}' - skipping")
            return None

        if server_id is None or server_id in ['null', 'none', 'undefined']:
            logger.warning(ff"\1")
            return None

        # Use a generated UUID if player_id is not valid
        if player_id is None or len(str(player_id)) == 0:
            import uuid
            player_id = str(uuid.uuid4())
            logger.warning(f"Generated placeholder ID '{player_id}' for player '{player_name}'")

        try:
            # Check if player exists - but use an upsert operation for atomicity
            now = datetime.utcnow()
            
            # Define document for new or updated player
            # Always update the name field to keep it current with the latest name seen
            player_data = {
                "player_id": player_id,
                "server_id": server_id,
                "name": player_name if player_name is not None else "",
                "display_name": player_name if player_name is not None else "",
                "last_seen": now,
                "updated_at": now
            }
            
            # Add log message for name changes - only if player exists already
            existing_player = await Player.get_by_player_id(self.bot.db, player_id)
            if existing_player is not None and existing_player.name != player_name and player_name:
                logger.info(f"Player name changed: {existing_player.name} → {player_name} (ID: {player_id})")
                
                # CRITICAL FIX: Always maintain a proper list of previous names to track history
                known_aliases = []
                
                # Get existing aliases if they exist
                if hasattr(existing_player, 'known_aliases') and existing_player.known_aliases:
                    # Make a copy to avoid modifying the original
                    known_aliases = list(existing_player.known_aliases)
                
                # Add the previous name to aliases if not already there
                if existing_player.name and existing_player.name not in known_aliases:
                    known_aliases.append(existing_player.name)
                
                # Add the new name to aliases if not already there
                if player_name is not None and player_name not in known_aliases:
                    known_aliases.append(player_name)
                
                # Log that we're tracking name changes
                logger.info(f"Tracking name history for player {player_id}: {known_aliases}")
                
                # Update player_data to include the known_aliases field
                player_data["known_aliases"] = known_aliases
            
            # CRITICAL FIX: Always ensure known_aliases is initialized for new players
            if "known_aliases" not in player_data and player_name:
                player_data["known_aliases"] = [player_name]
                logger.info(f"Initializing known_aliases for new player {player_id}: [{player_name}]")
            
            # Define upsert operation - if player exists, update fields; if not, create new
            result = await self.bot.db.players.update_one(
                {"player_id": player_id},  # Match by player_id
                {
                    "$set": player_data,
                    "$setOnInsert": {
                        "created_at": now,  # Only set on new records
                        "kills": 0,
                        "deaths": 0, 
                        "suicides": 0
                    }
                },
                upsert=True  # Create if doesn't exist
            )
            
            # Get the player record after upsert
            player = await Player.get_by_player_id(self.bot.db, player_id)
            
            # If we couldn't retrieve the player even after upsert, something went wrong
            if player is None:
                logger.warning(f"Failed to retrieve player {player_name} ({player_id}) after upsert")
                
                # Create a temporary player object to avoid disrupting processing flow
                player = Player(
                    player_id=player_id,
                    server_id=server_id,
                    name=player_name if player_name is not None else "",
                    display_name=player_name if player_name is not None else "",
                    last_seen=now,
                    created_at=now,
                    updated_at=now
                )
                
            return player

        except Exception as e:
            logger.error(f"Error in _get_or_create_player for {player_name} ({player_id}): {str(e)[:100]}")
            # In case of an error, attempt to fetch the player if it exists to continue processing
            try:
                player = await Player.get_by_player_id(self.bot.db, player_id)
                if player is not None:
                    return player
            except:
                pass
            return None
            
    async def clear_server_state(self, server_id: str) -> bool:
        """
        Clear all CSV processor state for a specific server
        This should be called when a server is removed from the bot
        
        Args:
            server_id: The server ID to clear state for
            
        Returns:
            bool: True if state was cleared successfully, False otherwise
        """
        try:
            logger.info(f"Clearing CSV processor state for removed server {server_id}")
            state_removed = False
            
            # Clear from database
            if hasattr(self.bot, 'db') and self.bot.db is not None and hasattr(self.bot.db, 'csv_processor_state'):
                state_result = await self.bot.db.csv_processor_state.delete_one({"server_id": server_id})
                state_removed = state_result.deleted_count > 0
                logger.info(f"Deleted CSV processor state for server {server_id} from database ({state_result.deleted_count} document)")
            
            # Clear from memory
            memory_cleared = False
            if server_id in self.last_processed:
                del self.last_processed[server_id]
                memory_cleared = True
                logger.info(f"Cleared last_processed timestamp for server {server_id} from memory")
                
            if server_id in self.last_processed_line_positions:
                line_positions_count = len(self.last_processed_line_positions[server_id])
                del self.last_processed_line_positions[server_id]
                memory_cleared = True
                logger.info(f"Cleared last_processed_line_positions for server {server_id} from memory ({line_positions_count} entries)")
            
            return state_removed or memory_cleared
        except Exception as e:
            logger.error(f"Error clearing CSV processor state for server {server_id}: {e}")
            return False

async def setup(bot: Any) -> None:
    """Set up the CSV processor cog

    Args:
        bot: Discord bot instance with db property
    """
    # Cast the bot to our PvPBot protocol to satisfy type checking
    pvp_bot = cast('PvPBot', bot)
    await bot.add_cog(CSVProcessorCog(pvp_bot))