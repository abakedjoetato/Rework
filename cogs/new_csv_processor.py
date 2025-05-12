"""
# module: csv_processor_cog
CSV Processor Cog

This cog provides commands and background tasks for processing CSV files
from game servers. It uses the stable CSV processor components to ensure
reliable operation.
"""
import asyncio
import logging
import os
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Protocol, Union, cast

import discord
from discord import app_commands
from discord.ext import commands, tasks

# Import our stable components
from utils.csv_processor_coordinator import CSVProcessorCoordinator
from utils.file_discovery import FileDiscovery
from utils.stable_csv_parser import StableCSVParser
from utils.sftp import SFTPManager

# Setup logging
logger = logging.getLogger(__name__)

class MotorDatabase(Protocol):
    """Protocol defining the motor database interface"""
    def __getattr__(self, name: str) -> Any: ...
    async def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]: ...
    async def find(self, query: Dict[str, Any]) -> Any: ...
    async def find_many(self, query: Dict[str, Any]) -> List[Dict[str, Any]]: ...

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
    @property
    def rivalries(self) -> Any: ...

class PvPBot(Protocol):
    """Protocol defining the PvPBot interface with required properties"""
    @property
    def db(self) -> Optional[MotorDatabase]: ...
    async def wait_until_ready(self) -> None: ...
    @property
    def user(self) -> Optional[Union[discord.User, discord.ClientUser]]: ...

class CSVProcessorCog(commands.Cog):
    """Commands and background tasks for processing CSV files"""

    def __init__(self, bot: 'PvPBot'):
        """Initialize the CSV processor cog

        Args:
            bot: PvPBot instance with db property
        """
        self.bot = bot

        # Create our stable components
        self.coordinator = CSVProcessorCoordinator()

        # Set the event processor
        self.coordinator.set_events_processor(self._process_kill_event)

        # Initialize state tracking
        self.is_processing = False
        self.sftp_connections = {}

        # Start the background task
        self.process_csv_files_task.start()

    def cog_unload(self):
        """Stop background tasks and close connections when cog is unloaded"""
        self.process_csv_files_task.cancel()

        # Close SFTP connections
        for connection in self.sftp_connections.values():
            asyncio.create_task(connection.close())

    @tasks.loop(minutes=5)
    async def process_csv_files_task(self):
        """Background task for processing CSV files

        This task runs every 5 minutes to check for new CSV files and process them promptly.
        """
        try:
            if self.is_processing is not None:
                logger.info("CSV processor task already running, skipping this run")
                return

            self.is_processing = True
            start_time = datetime.now()

            logger.info("Starting CSV processor task")

            # Track processing statistics
            total_files_processed = 0
            total_events_processed = 0

            # Get server configurations
            server_configs = await self._get_server_configs()

            if server_configs is None:
                logger.warning("No servers with SFTP enabled found, skipping CSV processing")
                return

            logger.info(f"Found {len(server_configs)} server(s) with SFTP enabled")

            # Check for stale timestamps and fix them
            await self.check_and_fix_stale_timestamps()

            # Process each server
            for server_id, config in server_configs.items():
                logger.info(f"New CSV Processor starting: {len(self.servers)} servers configured")

                files_processed, events_processed = await self._process_server_csv_files(server_id, config)

                total_files_processed += files_processed
                total_events_processed += events_processed

            # Calculate duration
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Log final status
            self._log_final_status(total_files_processed, total_events_processed, duration)
        except Exception as e:
            logger.error(f"Error in CSV processor task: {e}")
            traceback.print_exc()
        finally:
            self.is_processing = False

    @process_csv_files_task.before_loop
    async def before_process_csv_files_task(self):
        """Wait for bot to be ready before starting task"""
        await self.bot.wait_until_ready()
        logger.info("Bot ready, starting CSV processor task")

    async def direct_csv_processing(self, server_id: str, days: int = 30) -> Tuple[int, int]:
        """
        Process CSV files using the direct processor.

        Args:
            server_id: Server ID
            days: Number of days to look back

        Returns:
            Tuple[int, int]: Number of files processed and events imported
        """
        logger.info(f"Direct CSV processing requested for server {server_id}, days={days}")

        # Get server configuration
        server_configs = await self._get_server_configs()
        config = server_configs.get(server_id)

        if config is not None is None:
            logger.error(f"No configuration found for server {server_id}")
            return 0, 0

        # Create SFTP manager
        sftp = await self._get_sftp_manager(server_id, config)

        if sftp is None:
            logger.error(f"Failed to create SFTP manager for server {server_id}")
            return 0, 0

        try:
            # Use the historical processor for direct processing
            files_processed, events_processed = await self.coordinator.process_historical(
                sftp=sftp,
                server_id=server_id,
                days=days
            )

            return files_processed, events_processed
        except Exception as e:
            logger.error(f"Error in direct CSV processing: {e}")
            traceback.print_exc()
            return 0, 0

    async def _get_server_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get configurations for all servers with SFTP enabled

        This method searches through various collections to find server configurations,
        including the standalone 'servers' collection, the 'game_servers' collection,
        and embedded server configurations within guild documents.

        Returns:
            Dict: Dictionary of server IDs to server configurations
        """
        if self.bot.db is None:
            logger.error("Database not available")
            return {}

        server_configs = {}

        try:
            # Check the 'servers' collection first
            if hasattr(self.bot.db, 'servers'):
                async for server in self.bot.db.servers.find({"sftp_enabled": True}):
                    server_id = str(server.get("server_id", ""))
                    raw_server_id = str(server.get("original_server_id", server_id))

                    if server_id is None:
                        continue

                    await self._process_server_config(server, server_id, raw_server_id, server_configs)

            # Check the 'game_servers' collection next
            if hasattr(self.bot.db, 'game_servers'):
                async for server in self.bot.db.game_servers.find({"sftp_enabled": True}):
                    server_id = str(server.get("server_id", ""))
                    raw_server_id = str(server.get("original_server_id", server_id))

                    if server_id is None:
                        continue

                    await self._process_server_config(server, server_id, raw_server_id, server_configs)

            # Finally, check guild documents for embedded server configurations
            if hasattr(self.bot.db, 'guilds'):
                async for guild in self.bot.db.guilds.find({}):
                    if "servers" in guild:
                        for server in guild.get("servers", []):
                            if server.get("sftp_enabled", False):
                                server_id = str(server.get("server_id", ""))
                                raw_server_id = str(server.get("original_server_id", server_id))

                                if server_id is None:
                                    continue

                                # Add guild ID to the server config
                                server["guild_id"] = str(guild.get("guild_id", ""))

                                await self._process_server_config(server, server_id, raw_server_id, server_configs)

            logger.info(f"Found {len(server_configs)} servers with SFTP enabled")
            return server_configs
        except Exception as e:
            logger.error(f"Error getting server configs: {e}")
            return {}

    async def _process_server_config(self, server: Dict[str, Any], server_id: str, 
                                   raw_server_id: Optional[str], server_configs: Dict[str, Dict[str, Any]]) -> None:
        """Process a server configuration and add it to the server_configs dictionary

        Args:
            server: Server document from database
            server_id: Standardized server ID
            raw_server_id: Original server ID from database
            server_configs: Dictionary to add the processed config to
        """
        if server_id is None:
            return

        # Extract SFTP details
        sftp_config = {
            "hostname": server.get("sftp_host", ""),
            "port": server.get("sftp_port", 22),
            "username": server.get("sftp_username", ""),
            "password": server.get("sftp_password", ""),
            "server_id": server_id,
            "original_server_id": raw_server_id,
            "guild_id": server.get("guild_id", ""),
            "name": server.get("name", f"Server {server_id}")
        }

        # Only add if we have required fields
        if isinstance(sftp_config, dict) and sftp_config["hostname"] and sftp_config["username"] and sftp_config["password"]:
            server_configs[server_id] = sftp_config

    async def check_and_fix_stale_timestamps(self):
        """
        Check for and fix any stale timestamps in the last_processed dictionary.
        A timestamp is considered stale if it's more than 7 days old, which could
        lead to missing recent data due to excessive filtering.

        This is a safety mechanism to prevent the issue where CSV files aren't processed
        due to the last_processed value being too far in the past.
        """
        now = datetime.now()
        stale_threshold = now - timedelta(days=7)

        for server_id, last_time in self.coordinator.last_processed.items():
            if last_time < stale_threshold:
                logger.warning(f"Detected stale timestamp for server {server_id}: {last_time}")
                logger.warning(f"Resetting to 30 days ago for safety")

                # Reset to 30 days ago rather than now to ensure some historical data is still processed
                self.coordinator.last_processed[server_id] = now - timedelta(days=30)

                # Log the action for audit trail
                logger.info(f"Reset stale timestamp for server {server_id} to {self.coordinator.last_processed[server_id]}")

    async def _process_server_csv_files(self, server_id: str, config: Dict[str, Any]) -> Tuple[int, int]:
        """Process CSV files for a specific server

        Args:
            server_id: Server ID
            config: Server configuration

        Returns:
            Tuple[int, int]: Number of files processed and total death events processed
        """
        logger.info(f"Processing CSV files for server {server_id} with configuration: {config}")

        # Enforce string server ID
        server_id = str(server_id)

        # Create SFTP manager
        sftp = await self._get_sftp_manager(server_id, config)

        if sftp is None:
            logger.error(f"Failed to create SFTP manager for server {server_id}")
            return 0, 0

        try:
            # Use the killfeed processor for regular incremental updates
            files_processed, events_processed = await self.coordinator.process_killfeed(
                sftp=sftp,
                server_id=server_id
            )

            return files_processed, events_processed
        except Exception as e:
            logger.error(f"Error processing CSV files for server {server_id}: {e}")
            return 0, 0

    async def _get_sftp_manager(self, server_id: str, config: Dict[str, Any]) -> Optional[SFTPManager]:
        """Get or create an SFTP manager for a server

        Args:
            server_id: Server ID
            config: Server configuration with SFTP details

        Returns:
            SFTPManager: SFTP manager for this server, or None if creation fails
        """
        # Check if we already have a connection
        if server_id in self.sftp_connections:
            sftp = self.sftp_connections[server_id]

            # Check if the connection is still valid
            if sftp.is_connected is not None:
                return sftp

            # Connection is invalid, close it
            await sftp.close()
            del self.sftp_connections[server_id]

        # Create a new connection
        try:
            sftp = SFTPManager(
                hostname=config["hostname"],
                port=config["port"],
                username=config["username"],
                password=config["password"],
                server_id=server_id,
                original_server_id=config.get("original_server_id")
            )

            # Connect to the server
            await sftp.connect()

            # Store the connection for reuse
            self.sftp_connections[server_id] = sftp

            return sftp
        except Exception as e:
            logger.error(f"Failed to create SFTP manager for server {server_id}: {e}")
            return None

    def _log_final_status(self, files_processed: int, events_processed: int, duration: float):
        """Log the final status of the CSV processing

        Args:
            files_processed: Number of files processed
            events_processed: Number of events processed
            duration: Duration of processing in seconds
        """
        logger.info(f"CSV processing completed in {duration:.2f} seconds")
        logger.info(f"Processed {files_processed} files with {events_processed} events")

        # Add detailed statistics for all servers
        for server_id, stats in self.coordinator.processing_stats.items():
            logger.info(f"Server {server_id} statistics:")

            # Log historical stats if available
            if "historical_last_run" in stats:
                historical_time = stats["historical_last_run"]
                historical_files = stats.get("historical_files_processed", 0)
                historical_events = stats.get("historical_events_processed", 0)

                logger.info(f"  Historical last run: {historical_time}")
                logger.info(f"  Historical files processed: {historical_files}")
                logger.info(f"  Historical events processed: {historical_events}")

            # Log killfeed stats if available
            if "killfeed_last_run" in stats:
                killfeed_time = stats["killfeed_last_run"]
                killfeed_files = stats.get("killfeed_files_processed", 0)
                killfeed_events = stats.get("killfeed_events_processed", 0)

                logger.info(f"  Killfeed last run: {killfeed_time}")
                logger.info(f"  Killfeed files processed: {killfeed_files}")
                logger.info(f"  Killfeed events processed: {killfeed_events}")

            # Log discovery stats
            files_found = stats.get("files_found", 0)
            map_files = stats.get("map_files_found", 0)

            logger.info(f"  Total files found: {files_found}")
            logger.info(f"  Map files found: {map_files}")

    async def run_historical_parse(self, server_id: str, days: int = 30, guild_id: Optional[str] = None) -> Tuple[int, int]:
        """Run a historical parse for a server, checking further back in time

        This function is meant to be called when setting up a new server to process
        older historical data going back further than the normal processing window.

        Args:
            server_id: Server ID to process (can be UUID or numeric ID)
            days: Number of days to look back (default: 30)
            guild_id: Optional Discord guild ID for server isolation

        Returns:
            Tuple[int, int]: Number of files processed and events processed
        """
        logger.info(f"Running historical parse for server {server_id}, days={days}")

        # Get server configuration
        server_configs = await self._get_server_configs()
        config = server_configs.get(server_id)

        if config is None:
            logger.error(f"No configuration found for server {server_id}")
            return 0, 0

        # Create SFTP manager
        sftp = await self._get_sftp_manager(server_id, config)

        if sftp is None:
            logger.error(f"Failed to create SFTP manager for server {server_id}")
            return 0, 0

        try:
            # Use the historical processor
            files_processed, events_processed = await self.coordinator.process_historical(
                sftp=sftp,
                server_id=server_id,
                days=days
            )

            return files_processed, events_processed
        except Exception as e:
            logger.error(f"Error in historical parse: {e}")
            traceback.print_exc()
            return 0, 0

    @app_commands.command(name="process_csv", description="Manually process CSV files from the game server")
    @app_commands.describe(
        server_id="Server ID to process (optional)",
        hours="Number of hours to look back (default: 24)"
    )
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
        await interaction.response.defer(thinking=True)

        try:
            # Get server configurations
            server_configs = await self._get_server_configs()

            if server_configs is None:
                await interaction.followup.send("No servers with SFTP enabled found")
                return

            # If server_id is not provided, get the one for this guild
            guild_id = str(interaction.guild_id) if interaction.guild else None

            if not server_id and guild_id:
                for sid, config in server_configs.items():
                    if config.get("guild_id") == guild_id:
                        server_id = sid
                        break

            if server_id is None:
                # List available servers
                servers_list = "\n".join([f"• {config.get('name', sid)}" for sid, config in server_configs.items()])
                await interaction.followup.send(f"Please specify a server ID. Available servers:\n{servers_list}")
                return

            # Check if the server exists
            if server_id not in server_configs:
                await interaction.followup.send(f"Server {server_id} not found or SFTP not enabled")
                return

            # Set the start date
            start_date = datetime.now() - timedelta(hours=hours)

            # Create SFTP manager
            config = server_configs[server_id]
            sftp = await self._get_sftp_manager(server_id, config)

            if sftp is None:
                await interaction.followup.send(f"Failed to connect to SFTP server for {server_id}")
                return

            # Send initial response
            await interaction.followup.send(f"Processing CSV files for server {server_id}, looking back {hours} hours...")

            # Run the killfeed processor manually
            files_processed, events_processed = await self.coordinator.process_killfeed(
                sftp=sftp,
                server_id=server_id
            )

            # Send results
            if files_processed > 0:
                await interaction.followup.send(f"Processed {files_processed} CSV files with {events_processed} events")
            else:
                stats = self.coordinator.get_processing_stats(server_id)
                files_found = stats.get("files_found", 0)

                if files_found > 0:
                    await interaction.followup.send(f"Found {files_found} CSV files but none required processing (already processed)")
                else:
                    await interaction.followup.send("No CSV files found")
        except Exception as e:
            logger.error(f"Error in process_csv_command: {e}")
            await interaction.followup.send(f"Error processing CSV files: {e}f")

    @app_commands.command(name="historical_parse", description="Process historical CSV data")
    @app_commands.describe(
        server_id="Server ID to process (optional)",
        days="Number of days to look back (default: 30)"
    )
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
        await interaction.response.defer(thinking=True)

        try:
            # Get server configurations
            server_configs = await self._get_server_configs()

            if server_configs is None:
                await interaction.followup.send("No servers with SFTP enabled found")
                return

            # If server_id is not provided, get the one for this guild
            guild_id = str(interaction.guild_id) if interaction.guild else None

            if not server_id and guild_id:
                for sid, config in server_configs.items():
                    if config.get("guild_id") == guild_id:
                        server_id = sid
                        break

            if server_id is None:
                # List available servers
                servers_list = "\n".join([f"{sid}: {config.get('name', 'Unknown')}" for sid, config in server_configs.items()])
                await interaction.followup.send(f"Please specify a server ID. Available servers:\n{servers_list}")
                return

            # Check if the server exists
            if server_id not in server_configs:
                await interaction.followup.send(f"Server {server_id} not found or SFTP not enabled")
                return

            # Send initial response
            await interaction.followup.send(f"Starting historical parse for server {server_id}, looking back {days} days. This may take a while...")

            # Run the historical parse
            files_processed, events_processed = await self.run_historical_parse(
                server_id=server_id,
                days=days,
                guild_id=guild_id
            )

            # Send results
            if files_processed > 0:
                await interaction.followup.send(f"Historical parse complete. Processed {files_processed} CSV files with {events_processed} events")
            else:
                await interaction.followup.send("No CSV files found or processed during historical parse")
        except Exception as e:
            logger.error(f"Error in historical_parse_command: {e}")
            await interaction.followup.send(f"Error processing historical data: {e}f")

    @app_commands.command(name="csv_status", description="Show CSV processor status")
    async def csv_status_command(self, interaction: discord.Interaction):
        """Show CSV processor status

        Args:
            interaction: Discord interaction
        """
        await interaction.response.defer(thinking=True)

        try:
            # Get server configurations
            server_configs = await self._get_server_configs()

            if server_configs is None:
                await interaction.followup.send("No servers with SFTP enabled found")
                return

            # Build status message
            status_lines = ["**CSV Processor Status**"]

            for server_id, config in server_configs.items():
                server_name = config.get("name", f"Server {server_id}")
                stats = self.coordinator.get_processing_stats(server_id)

                status_lines.append(f"\n**{server_name}** (ID: {server_id})")

                # Add historical stats if available
                if "historical_last_run" in stats:
                    historical_time = stats["historical_last_run"]
                    historical_files = stats.get("historical_files_processed", 0)
                    historical_events = stats.get("historical_events_processed", 0)

                    status_lines.append(f"• Historical last run: {historical_time}")
                    status_lines.append(f"• Historical files: {historical_files}")
                    status_lines.append(f"• Historical events: {historical_events}")

                # Add killfeed stats if available
                if "killfeed_last_run" in stats:
                    killfeed_time = stats["killfeed_last_run"]
                    killfeed_files = stats.get("killfeed_files_processed", 0)
                    killfeed_events = stats.get("killfeed_events_processed", 0)

                    status_lines.append(f"• Killfeed last run: {killfeed_time}")
                    status_lines.append(f"• Killfeed files: {killfeed_files}")
                    status_lines.append(f"• Killfeed events: {killfeed_events}")

                # Add discovery stats
                files_found = stats.get("files_found", 0)
                map_files = stats.get("map_files_found", 0)

                status_lines.append(f"• Total files found: {files_found}")
                status_lines.append(f"• Map files found: {map_files}")

                # Add last processed time
                last_processed = self.coordinator.last_processed.get(server_id)
                if last_processed is not None:
                    status_lines.append(f"• Last processed: {last_processed}")

            # Send status message
            await interaction.followup.send("\n".join(status_lines))
        except Exception as e:
            logger.error(f"Error in csv_status_command: {e}")
            await interaction.followup.send(f"Error retrieving CSV processor status: {e}f")

    async def _process_kill_event(self, events: List[Dict[str, Any]], server_id: str) -> bool:
        """Process kill events and update player stats and rivalries

        Args:
            events: List of normalized kill events
            server_id: Server ID associated with these events

        Returns:
            bool: True if processed successfully, False otherwise
        """
        if self.bot.db is None:
            logger.error("Database not available")
            return False

        try:
            # Process each event
            for event in events:
                # Skip events without necessary fields
                if not event.get("killer_id") or not event.get("victim_id"):
                    continue

                # Add server_id if not present
                if "server_id" not in event:
                    event["server_id"] = server_id

                # Check if this is a suicide
                is_suicide = event.get("is_suicide", False)

                # Get player references
                killer = await self._get_or_create_player(
                    server_id=server_id,
                    player_id=event["killer_id"],
                    player_name=event["killer_name"]
                )

                victim = await self._get_or_create_player(
                    server_id=server_id,
                    player_id=event["victim_id"],
                    player_name=event["victim_name"]
                )

                if not killer or not victim:
                    continue

                # Create kill record
                kill_record = {
                    "server_id": server_id,
                    "killer_id": event["killer_id"],
                    "victim_id": event["victim_id"],
                    "weapon": event.get("weapon", ""),
                    "distance": event.get("distance", 0.0),
                    "timestamp": event.get("timestamp", datetime.now()),
                    "is_suicide": is_suicide,
                    "killer_system": event.get("killer_system", ""),
                    "victim_system": event.get("victim_system", "")
                }

                # Save kill record
                await self.bot.db.kills.insert_one(kill_record)

                # Skip further processing for suicides
                if is_suicide is not None:
                    continue

                # Update player stats
                await self.bot.db.players.update_stats(killer["_id"], "kills")
                await self.bot.db.players.update_stats(victim["_id"], "deaths")

                # Update rivalries
                await self.bot.db.rivalries.update_nemesis_and_prey(
                    killer_id=killer["_id"],
                    victim_id=victim["_id"],
                    server_id=server_id
                )

            return True
        except Exception as e:
            logger.error(f"Error processing kill events: {e}")
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
        if self.bot.db is None:
            return None

        if player_id is None or not player_name:
            return None

        try:
            # Try to find existing player
            player = await self.bot.db.players.find_one({
                "player_id": player_id,
                "server_id": server_id
            })

            if player is not None is not None:
                # Check if we need to update the name
                if player_name is not None and player_name != player.get("name"):
                    # Add to known aliases if different from current name
                    known_aliases = player.get("known_aliases", [])
                    if player_name not in known_aliases:
                        known_aliases.append(player_name)

                    # Update player record with new name and aliases
                    await self.bot.db.players.update_one(
                        {"_id": player["_id"]},
                        {"$set": {"name": player_name, "known_aliases": known_aliases}}
                    )

                return player

            # Player not found, create new one
            new_player = {
                "player_id": player_id,
                "server_id": server_id,
                "name": player_name,
                "kills": 0,
                "deaths": 0,
                "known_aliases": [player_name],
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }

            result = await self.bot.db.players.insert_one(new_player)

            # Get the created player with its ID
            return await self.bot.db.players.find_one({"_id": result.inserted_id})
        except Exception as e:
            logger.error(f"Error getting or creating player: {e}")
            return None


async def setup(bot: Any) -> None:
    """Set up the CSV processor cog

    Args:
        bot: Discord bot instance with db property
    """
    await bot.add_cog(CSVProcessorCog(bot))