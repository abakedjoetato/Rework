import os
import sys
import asyncio
import logging
from utils.logging_setup import setup_logging
import discord
from discord.ext import commands
import motor.motor_asyncio
from typing import Optional, List, Dict, Any, Union, cast
import traceback
from datetime import datetime

# Set up custom logging configuration
setup_logging()

# Configure logger
logger = logging.getLogger("bot")

class Bot(commands.Bot):
    """Main bot class with enhanced error handling and initialization"""

    def __init__(self, *, production: bool = False, debug_guilds: Optional[List[int]] = None):
        """Initialize the bot with proper intents and configuration

        Args:
            production: Whether the bot is running in production mode
            debug_guilds: List of guild IDs for debug commands
        """
        # Set up proper intents for the required functionality
        intents = discord.Intents.default()
        intents.members = True
        intents.message_content = True

        # Initialize the base bot with command prefix and intents
        super().__init__(
            command_prefix=commands.when_mentioned_or("!"),
            intents=intents,
            case_insensitive=True,
            auto_sync_commands=True
        )

        # Bot configuration
        self.production = production
        self.debug_guilds = debug_guilds
        self._db = None
        self.ready = False

        # Additional bot-specific attributes
        self.home_guild_id = os.environ.get("HOME_GUILD_ID", "")
        
        # Set owner ID (hard-coded per user request)
        self.owner_id = int(462961235382763520)

        # Extension loading state tracking
        self.loaded_extensions = []
        self.failed_extensions = []

        # Background task tracking
        self.background_tasks = {}

        # Add bot_status attribute for health monitoring
        self._bot_status = {
            "startup_time": datetime.now().isoformat(),
            "is_ready": False,
            "connected_guilds": 0,
            "loaded_extensions": [],
            "failed_extensions": [],
            "last_error": None
        }

        # Register error handlers
        self.setup_error_handlers()

    def setup_error_handlers(self):
        """Set up global error handlers"""
        @self.event
        async def on_error(event, *args, **kwargs):
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_msg = f"Error in event {event}: {str(exc_value)}"

            # Log detailed error information
            logger.error(error_msg)
            logger.error("".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))

            # Update bot_status
            self._bot_status["last_error"] = {
                "time": datetime.now().isoformat(),
                "event": event,
                "error": str(exc_value)
            }

    @property
    def db(self):
        """Database property with error handling

        Returns:
            MongoDB database instance

        Raises:
            RuntimeError: If database is not initialized
        """
        if self._db is None:
            raise RuntimeError("Database has not been initialized. Call init_db() first.")
        return self._db

    async def init_db(self, max_retries=3, retry_delay=2):
        """Initialize database connection with error handling and retries

        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Seconds to wait between retries

        Returns:
            bool: True if connection successful, False otherwise
        """
        # Check if database is already initialized
        if self._db is not None:
            logger.info("Database already initialized")
            return True
            
        # Check environment variable
        mongodb_uri = os.environ.get("MONGODB_URI")
        if not mongodb_uri:
            logger.critical("MONGODB_URI environment variable not set")
            return False
            
        # Get database name from environment or use default
        db_name = os.environ.get("DB_NAME", "mukti_bot")
        logger.info(f"Using database: {db_name}")

        # Initialize attempt counter
        attempts = 0
        last_error = None

        # Try to connect with retry logic
        while attempts < max_retries:
            attempts += 1
            try:
                # Import database connection utilities
                try:
                    from utils.db_connection import get_database, test_database_connection, DatabaseConnectionError
                except ImportError as import_error:
                    logger.critical(f"Failed to import database utilities: {import_error}")
                    logger.critical(traceback.format_exc())
                    return False
                
                logger.info(f"Testing MongoDB connection (attempt {attempts}/{max_retries})...")
                
                # First test the connection
                success, message = await test_database_connection()
                if not success:
                    logger.error(f"Connection test failed: {message}")
                    last_error = f"Connection test failed: {message}"
                    
                    # Wait before retrying
                    if attempts < max_retries:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                    continue
                
                logger.info(f"Connection test successful: {message}")
                
                # Get the database instance
                try:
                    db = await get_database()
                except DatabaseConnectionError as db_conn_error:
                    logger.error(f"Database connection error: {db_conn_error}")
                    last_error = f"Database connection error: {db_conn_error}"
                    
                    # Wait before retrying
                    if attempts < max_retries:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                    continue
                
                if db is None:
                    logger.error("Failed to get database instance (returned None)")
                    last_error = "Failed to get database instance (returned None)"
                    
                    # Wait before retrying
                    if attempts < max_retries:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                    continue
                
                # Test if we can perform basic operations
                try:
                    # Try to list collections as an additional test
                    collection_names = await db.list_collection_names(limit=5)
                    logger.info(f"Found {len(collection_names)} collections")
                    
                    # Log the collections we found for debugging
                    if collection_names:
                        logger.info(f"Collections: {', '.join(collection_names)}")
                    
                    # Try to create a test collection if database is empty
                    if not collection_names:
                        logger.info("No collections found, creating test collection")
                        test_collection = db.get_collection("connection_test")
                        await test_collection.insert_one({"test": True, "timestamp": datetime.now().isoformat()})
                        await test_collection.delete_many({"test": True})
                        logger.info("Test write/delete operation successful")
                    
                    # Try to access a document from guilds collection if it exists
                    if "guilds" in collection_names:
                        try:
                            count = await db.guilds.count_documents({})
                            logger.info(f"Found {count} documents in guilds collection")
                        except Exception as guilds_error:
                            logger.warning(f"Unable to count guilds documents: {guilds_error}")
                            # Non-fatal error, continue
                except Exception as db_op_error:
                    logger.error(f"Database operations test failed: {db_op_error}")
                    logger.error(traceback.format_exc())
                    last_error = f"Database operations test failed: {db_op_error}"
                    
                    # Wait before retrying
                    if attempts < max_retries:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                    continue
                
                # If we got here, connection is successful
                # Store the database reference
                self._db = db
                
                logger.info("Successfully connected to MongoDB")
                return True

            except Exception as e:
                last_error = str(e)
                logger.critical(f"Database connection failed (attempt {attempts}/{max_retries}): {e}")
                logger.critical(traceback.format_exc())
                
                # Wait before retrying
                if attempts < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)

        # If we got here, all attempts failed
        logger.critical(f"All {max_retries} database connection attempts failed. Last error: {last_error}")
        return False

    async def on_ready(self):
        """Handle bot ready event with additional setup"""
        # Check if already ready (for reconnection events)
        if self.ready:
            logger.info("Bot reconnected")
            return

        # Set ready state
        self.ready = True
        self._bot_status["is_ready"] = True

        # Log successful login with safeguards against None values
        user_name = ""
        if self.user is not None:
            user_name = self.user.name if hasattr(self.user, 'name') else str(self.user.id)
        
        logger.info(f"Bot logged in as {user_name}")
        
        # Get connected guilds count safely
        guilds_count = 0
        if hasattr(self, 'guilds'):
            guilds_count = len(self.guilds)
        
        logger.info(f"Connected to {guilds_count} guilds")
        self._bot_status["connected_guilds"] = guilds_count

        # Sync commands with error handling
        try:
            logger.info("Syncing application commands...")
            await self.sync_commands()
            logger.info("Application commands synced!")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")
            logger.error(traceback.format_exc())

        # Start background task monitor
        try:
            self.start_background_task_monitor()
            logger.info("Background task monitor started")
        except Exception as e:
            logger.error(f"Failed to start background task monitor: {e}")
            logger.error(traceback.format_exc())

        # Log successful startup
        logger.info("Bot is ready!")

    async def sync_commands(self):
        """Sync application commands with proper error handling"""
        if self.debug_guilds is not None:
            # Sync to specific debug guilds only
            for guild_id in self.debug_guilds:
                try:
                    # Get the guild object
                    guild = self.get_guild(guild_id)
                    
                    if guild is not None:
                        # Sync commands to this guild
                        await self.tree.sync(guild=guild)
                        
                        # Get guild name safely
                        guild_name = ""
                        try:
                            if hasattr(guild, 'name') and guild.name is not None:
                                guild_name = guild.name
                        except AttributeError:
                            pass
                        
                        logger.info(f"Synced commands to guild {guild_name} ({guild_id})")
                    else:
                        logger.warning(f"Could not find guild with ID {guild_id}")
                except Exception as e:
                    logger.error(f"Error syncing commands to guild {guild_id}: {e}")
                    logger.error(traceback.format_exc())
        else:
            try:
                # Global sync
                await self.tree.sync()
                logger.info("Synced commands globally")
            except Exception as e:
                logger.error(f"Error syncing commands globally: {e}")
                logger.error(traceback.format_exc())

    async def load_extension(self, name: str, *, package: Optional[str] = None) -> None:
        """Load a bot extension with enhanced error handling

        Args:
            name: Name of the extension to load

        Raises:
            commands.ExtensionError: If loading fails
        """
        try:
            await super().load_extension(name, package=package)
            self.loaded_extensions.append(name)
            self._bot_status["loaded_extensions"].append(name)
            logger.info(f"Loaded extension: {name}")
        except commands.ExtensionError as e:
            logger.error(f"Failed to load extension {name}: {e}")
            logger.error(traceback.format_exc())
            self.failed_extensions.append(name)
            self._bot_status["failed_extensions"].append({
                "name": name,
                "error": str(e)
            })
            # Don't return False as it's incompatible with the parent's return type (None)
        except Exception as e:
            logger.error(f"Unexpected error loading extension {name}: {e}")
            logger.error(traceback.format_exc())
            self.failed_extensions.append(name)
            self._bot_status["failed_extensions"].append({
                "name": name,
                "error": str(e)
            })
            # Don't return False as it's incompatible with the parent's return type (None)

    def start_background_task_monitor(self):
        """Start a background task to monitor other background tasks"""
        async def monitor_background_tasks():
            while True:
                try:
                    # Check each background task
                    for task_name, task in list(self.background_tasks.items()):
                        if task is None:
                            logger.warning(f"Task {task_name} is None, removing from tracking")
                            if task_name in self.background_tasks:
                                del self.background_tasks[task_name]
                            continue

                        if task.done():
                            try:
                                # Get result to handle any exceptions
                                task.result()
                                logger.warning(f"Background task {task_name} completed unexpectedly")
                                
                                # Remove the completed task from tracking
                                if task_name in self.background_tasks:
                                    del self.background_tasks[task_name]
                                    
                            except asyncio.CancelledError:
                                # Task was cancelled, which is expected in some cases
                                logger.info(f"Background task {task_name} was cancelled")
                                # Clean up the cancelled task
                                if task_name in self.background_tasks:
                                    del self.background_tasks[task_name]
                            except Exception as task_error:
                                logger.error(f"Error in background task {task_name}: {task_error}")
                                logger.error(traceback.format_exc())

                                # Clean up the failed task
                                if task_name in self.background_tasks:
                                    del self.background_tasks[task_name]

                                # Restart critical tasks with retry logic
                                if task_name.startswith("critical_"):
                                    logger.info(f"Attempting to restart critical task: {task_name}")
                                    # Extract the base name without the "critical_" prefix
                                    base_name = task_name[9:] if task_name.startswith("critical_") else task_name
                                    
                                    # Special handling for known critical tasks
                                    if base_name == "csv_processor":
                                        try:
                                            # Dynamically import the module to avoid import errors
                                            import importlib
                                            try:
                                                csv_processor_module = importlib.import_module("cogs.csv_processor")
                                                if hasattr(csv_processor_module, "start_csv_processor"):
                                                    start_func = getattr(csv_processor_module, "start_csv_processor")
                                                    # Check if it's callable
                                                    if callable(start_func):
                                                        new_task = self.create_background_task(
                                                            start_func(self), 
                                                            base_name,
                                                            critical=True
                                                        )
                                                        logger.info(f"Restarted critical task: {task_name}")
                                                    else:
                                                        logger.error(f"start_csv_processor is not callable in csv_processor module")
                                                else:
                                                    logger.error(f"start_csv_processor function not found in csv_processor module")
                                            except Exception as import_error:
                                                logger.error(f"Error importing csv_processor module: {import_error}")
                                                logger.error(traceback.format_exc())
                                        except Exception as restart_error:
                                            logger.error(f"Failed to restart {task_name}: {restart_error}")
                                            logger.error(traceback.format_exc())
                except Exception as e:
                    logger.error(f"Error in background task monitor: {e}")
                    logger.error(traceback.format_exc())

                # Check every 30 seconds
                await asyncio.sleep(30)

        # Start the monitor task
        monitor_task = asyncio.create_task(monitor_background_tasks(), name="task_monitor")
        self.background_tasks["task_monitor"] = monitor_task
        logger.info("Started background task monitor")

    def create_background_task(self, coro, name, critical=False):
        """Create and track a background task with proper naming

        Args:
            coro: Coroutine to run as a background task
            name: Name of the task for tracking
            critical: Whether the task is critical and should be auto-restarted
        """
        task_name = f"critical_{name}" if critical else name
        task = asyncio.create_task(coro, name=task_name)
        self.background_tasks[task_name] = task
        return task

    async def on_command_error(self, ctx, error):
        """Global command error handler"""
        if isinstance(error, commands.CommandNotFound):
            return

        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing required argument: {error.param.name if error.param is not None else ''}")
            return

        if isinstance(error, commands.BadArgument):
            await ctx.send(f"Bad argument: {error}")
            return

        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You don't have the required permissions to use this command.")
            return

        # Log unexpected errors
        logger.error(f"Command error in {ctx.command}: {error}", exc_info=error)

        # Notify user
        await ctx.send("An error occurred while processing this command. The error has been logged.")

    async def on_application_command_error(self, interaction: discord.Interaction, error: Exception):
        """Global application command error handler"""
        # Handle known error types
        if isinstance(error, commands.MissingPermissions):
            try:
                # Safely respond to the interaction
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "You don't have the required permissions to use this command.", 
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "You don't have the required permissions to use this command.", 
                        ephemeral=True
                    )
            except Exception as response_error:
                logger.error(f"Failed to send permission error message: {response_error}")
            return

        # Get command name safely
        command_name = "Unknown"
        try:
            if interaction.command is not None:
                if hasattr(interaction.command, 'name'):
                    command_name = str(interaction.command.name)
                elif hasattr(interaction.command, 'qualified_name'):
                    command_name = str(interaction.command.qualified_name)
        except Exception as name_error:
            logger.error(f"Error getting command name: {name_error}")
        
        # Get guild and channel info for better debugging
        guild_info = "DM"
        channel_info = "Unknown"
        user_info = "Unknown"
        
        try:
            # Try to get guild info
            if interaction.guild:
                guild_info = f"{interaction.guild.name} ({interaction.guild.id})" if hasattr(interaction.guild, 'name') else f"Guild ID: {interaction.guild.id}"
            
            # Try to get channel info
            if interaction.channel:
                try:
                    # Get channel ID safely
                    channel_id = str(getattr(interaction.channel, 'id', 'Unknown'))
                    
                    # Try to determine what type of channel it is
                    # Use isinstance checks which are safer than checking attributes
                    if isinstance(interaction.channel, discord.DMChannel):
                        # Handle DM channels
                        try:
                            if hasattr(interaction.channel, 'recipient') and interaction.channel.recipient:
                                recipient_name = str(getattr(interaction.channel.recipient, 'name', 'Unknown User'))
                                channel_info = f"DM with {recipient_name} ({channel_id})"
                            else:
                                channel_info = f"DM Channel ({channel_id})"
                        except Exception:
                            channel_info = f"DM Channel ({channel_id})"
                    elif isinstance(interaction.channel, discord.TextChannel):
                        # Handle text channels in guilds
                        try:
                            if hasattr(interaction.channel, 'name'):
                                channel_name = str(interaction.channel.name)
                                channel_info = f"#{channel_name} ({channel_id})"
                            else:
                                channel_info = f"Text Channel ({channel_id})"
                        except Exception:
                            channel_info = f"Text Channel ({channel_id})"
                    elif isinstance(interaction.channel, discord.VoiceChannel):
                        # Handle voice channels
                        try:
                            if hasattr(interaction.channel, 'name'):
                                channel_name = str(interaction.channel.name)
                                channel_info = f"Voice: {channel_name} ({channel_id})"
                            else:
                                channel_info = f"Voice Channel ({channel_id})"
                        except Exception:
                            channel_info = f"Voice Channel ({channel_id})"
                    elif isinstance(interaction.channel, discord.Thread):
                        # Handle threads
                        try:
                            if hasattr(interaction.channel, 'name'):
                                channel_name = str(interaction.channel.name)
                                parent_id = str(getattr(interaction.channel, 'parent_id', 'Unknown'))
                                channel_info = f"Thread: {channel_name} (ID: {channel_id}, Parent: {parent_id})"
                            else:
                                channel_info = f"Thread ({channel_id})"
                        except Exception:
                            channel_info = f"Thread ({channel_id})"
                    else:
                        # Handle any other channel type
                        channel_info = f"Channel ({channel_id})"
                except Exception as channel_error:
                    logger.warning(f"Error getting channel info: {channel_error}")
                    channel_info = f"Unknown Channel Type"
            
            # Try to get user info
            if interaction.user:
                user_info = f"{interaction.user.name}#{interaction.user.discriminator if hasattr(interaction.user, 'discriminator') else ''} ({interaction.user.id})"
        except Exception as context_error:
            logger.error(f"Error getting interaction context: {context_error}")
        
        # Log the error with context
        logger.error(
            f"Application command error in {command_name}\n"
            f"Guild: {guild_info}\n"
            f"Channel: {channel_info}\n"
            f"User: {user_info}\n"
            f"Error: {error}"
        )
        logger.error(traceback.format_exc())

        # Update bot status with error information
        try:
            self._bot_status["last_error"] = {
                "time": datetime.now().isoformat(),
                "command": command_name,
                "guild": guild_info,
                "error": str(error)
            }
        except Exception:
            pass

        # Notify user - with enhanced error handling
        try:
            error_message = "An error occurred while processing this command. The error has been logged."
            
            # Add specific error messages for common issues
            if isinstance(error, commands.CommandOnCooldown):
                retry_after = getattr(error, 'retry_after', 0)
                if retry_after > 0:
                    error_message = f"This command is on cooldown. Please try again in {int(retry_after)} seconds."
            
            # Send the error message
            if hasattr(interaction, 'response') and callable(getattr(interaction.response, 'is_done', None)):
                if interaction.response.is_done():
                    if hasattr(interaction, 'followup') and callable(getattr(interaction.followup, 'send', None)):
                        await interaction.followup.send(error_message, ephemeral=True)
                else:
                    await interaction.response.send_message(error_message, ephemeral=True)
        except Exception as notification_error:
            logger.error(f"Failed to send error message to user: {notification_error}")
            logger.error(traceback.format_exc())