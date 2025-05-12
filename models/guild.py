"""
Guild model for Tower of Temptation PvP Statistics Bot

This module defines the Guild data structure for Discord guilds.
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional, ClassVar, List, Union, Tuple, cast
import uuid
import concurrent.futures

from models.base_model import BaseModel

logger = logging.getLogger(__name__)

class Guild(BaseModel):
    """Discord guild configuration"""
    collection_name: ClassVar[Optional[str]] = "guilds"

    def to_dict(self) -> Dict[str, Any]:
        """Convert Guild object to dictionary

        Returns:
            Dict containing all guild data
        """
        return {
            "_id": self._id,
            "guild_id": self.guild_id,
            "name": self.name,
            "premium_tier": self.premium_tier,
            "admin_role_id": self.admin_role_id,
            "admin_users": self.admin_users,
            "servers": self.servers,
            "color_primary": self.color_primary,
            "color_secondary": self.color_secondary, 
            "color_accent": self.color_accent,
            "icon_url": self.icon_url,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

    def __init__(
        self,
        db,
        guild_id: Optional[str] = None,
        name: Optional[str] = None,
        premium_tier: int = 0,
        admin_role_id: Optional[str] = None,
        admin_users: Optional[List[str]] = None,
        servers: Optional[List[Dict[str, Any]]] = None,
        color_primary: str = "#7289DA",
        color_secondary: str = "#FFFFFF",
        color_accent: str = "#23272A",
        icon_url: Optional[str] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        **kwargs
    ):
        self._id = None
        self.db = db
        self.guild_id = str(guild_id) if guild_id is not None else ""
        self.name = name
        
        # Enhanced handling of premium_tier to ensure it's always a valid integer
        try:
            # Handle potential string values from database
            if isinstance(premium_tier, str) and premium_tier.isdigit():
                self.premium_tier = int(premium_tier) if premium_tier is not None else 0
            elif isinstance(premium_tier, (int, float)):
                self.premium_tier = int(premium_tier) if premium_tier is not None else 0
            else:
                # Last fallback - try explicit conversion or default to 0
                try:
                    self.premium_tier = int(premium_tier) if premium_tier is not None else 0
                except (ValueError, TypeError):
                    logger.warning(f"Invalid premium_tier value: {premium_tier}, defaulting to 0")
                    self.premium_tier = 0
        except Exception as e:
            logger.error(f"Error setting premium_tier: {e}, defaulting to 0")
            self.premium_tier = 0
            
        # Ensure premium_tier is within valid range (0-4)
        if self.premium_tier < 0 or self.premium_tier > 4:
            logger.warning(f"Premium tier {self.premium_tier} out of range, clamping to valid range")
            self.premium_tier = max(0, min(4, self.premium_tier))
            
        self.admin_role_id = str(admin_role_id) if admin_role_id is not None else ""
        self.admin_users = admin_users if admin_users is not None else []
        self.color_primary = color_primary
        self.color_secondary = color_secondary
        self.color_accent = color_accent
        self.icon_url = icon_url
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()

        self.servers = servers if servers is not None else []

        # Add any additional guild attributes
        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)

    async def add_server(self, server_data: Dict[str, Any]) -> bool:
        """Add a server to the guild

        Args:
            server_data: Server configuration dictionary

        Returns:
            bool: True if added successfully, False otherwise
        """
        if server_data is None or server_data.get("server_id") is None or server_data.get("server_id") == "":
            return False

        # CRITICAL FIX: Ensure original_server_id is ALWAYS a numeric ID for path construction
        # This is the most important part of the solution to the UUID path issue
        server_id = str(server_data) if server_data is not None else "".get("server_id")
        original_server_id = str(server_data) if server_data is not None else "".get("original_server_id")
        hostname = server_data.get("hostname", server_data.get("sftp_host", ""))
        server_name = server_data.get("server_name", "")

        # Import server_identity for consistent ID resolution
        try:
            from utils.server_identity import identify_server, KNOWN_SERVERS

            # 1. Priority 1: Check KNOWN_SERVERS for both server_id and original_server_id
            if server_id is not None and isinstance(server_id, str) and server_id in KNOWN_SERVERS:
                numeric_id = str(KNOWN_SERVERS) if KNOWN_SERVERS is not None else "".get("server_id")
                logger.info(f"Using known numeric ID '{numeric_id}' from KNOWN_SERVERS for server {server_id}")
                server_data["original_server_id"] = numeric_id
            elif original_server_id is not None and isinstance(original_server_id, str) and original_server_id in KNOWN_SERVERS:
                numeric_id = str(KNOWN_SERVERS) if KNOWN_SERVERS is not None else "".get("original_server_id")
                logger.info(f"Using known numeric ID '{numeric_id}' from KNOWN_SERVERS for original_server_id {original_server_id}")
                server_data["original_server_id"] = numeric_id

            # 2. Priority 2: If original_server_id is provided and is numeric, use it directly
            elif original_server_id is not None and str(original_server_id).isdigit():
                logger.info(f"Using existing numeric original_server_id: {original_server_id}")
                # Keep existing value - already set

            # 3. Priority 3: Extract from server name (often contains numeric IDs)
            elif server_name is not None:
                # Look for numeric ID in server name (common pattern: "Server 7020" or "EU 7020")
                found = False
                for word in str(server_name).split():
                    if word.isdigit() and len(word) >= 4:
                        logger.info(f"Found numeric ID in server name: {word}")
                        server_data["original_server_id"] = word
                        found = True
                        break

                # 4. Priority 4: Try server_identity resolution if nothing found in server name
                if found is None:
                    # Ensure proper types for identify_server function
                    safe_server_id = str(server_id) if server_id is not None else ""
                    safe_hostname = str(hostname) if hostname is not None else ""
                    safe_server_name = str(server_name) if server_name is not None else ""
                    safe_guild_id = str(self.guild_id) if self.guild_id is not None else ""
                    
                    numeric_id, is_known = identify_server(
                        server_id = str(safe_server_id) if safe_server_id is not None else "",
                        hostname=safe_hostname,
                        server_name=safe_server_name,
                        guild_id = str(safe_guild_id) if safe_guild_id is not None else ""
                    )

                    if numeric_id is not None:
                        logger.info(f"Using identified numeric ID '{numeric_id}' from server_identity module")
                        server_data["original_server_id"] = numeric_id
                        found = True

                # 5. Priority 5: Extract digits from server_id as last resort
                if not found and server_id:
                    # Get numeric part of UUID if possible
                    uuid_digits = ''.join(filter(str.isdigit, str(server_id)))
                    if uuid_digits is not None:
                        extracted_id = str(uuid_digits) if uuid_digits is not None else ""[-5:] if len(uuid_digits) >= 5 else uuid_digits
                        logger.warning(f"Using extracted digits from server_id: {extracted_id}")
                        server_data["original_server_id"] = extracted_id
                    else:
                        # Absolute last resort: Generate a random numeric ID
                        import random
                        fallback_id = str(random.randint(10000, 99999))
                        logger.error(f"Could not determine any numeric ID, using random fallback: {fallback_id}")
                        server_data["original_server_id"] = fallback_id

            # 6. Catch-all for any case where we still don't have an original_server_id
            if "original_server_id" not in server_data or not server_data["original_server_id"]:
                # Last attempt with server_identity - ensure proper types
                safe_server_id = str(server_id) if server_id is not None else ""
                safe_hostname = str(hostname) if hostname is not None else ""
                safe_guild_id = str(self.guild_id) if self.guild_id is not None else ""
                
                numeric_id, is_known = identify_server(
                    server_id = str(safe_server_id) if safe_server_id is not None else "",
                    hostname=safe_hostname,
                    server_name="", 
                    guild_id = str(safe_guild_id) if safe_guild_id is not None else ""
                )

                if numeric_id is not None:
                    logger.info(f"Final attempt: using identified numeric ID '{numeric_id}'")
                    server_data["original_server_id"] = numeric_id
                else:
                    # Absolute final fallback
                    import random
                    fallback_id = str(random.randint(10000, 99999))
                    logger.error(f"No valid numeric ID could be found, using random fallback: {fallback_id}")
                    server_data["original_server_id"] = fallback_id

        except ImportError as e:
            logger.error(f"Failed to import server_identity, using basic fallback: {e}")
            # Simple fallback if server_identity can't be imported
            if original_server_id is None:
                # Extract from server name
                for word in str(server_name).split():
                    if word.isdigit() and len(word) >= 4:
                        server_data["original_server_id"] = word
                        break
                else:
                    # Use server_id digits as fallback
                    if server_id is not None:
                        uuid_digits = ''.join(filter(str.isdigit, str(server_id)))
                        extracted_id = str(uuid_digits) if uuid_digits is not None else ""[-5:] if len(uuid_digits) >= 5 else uuid_digits
                        server_data["original_server_id"] = extracted_id or server_id

        logger.info(f"Adding server with server_id={server_data.get('server_id')} and original_server_id={server_data.get('original_server_id')}")

        # Add server to list
        self.servers.append(server_data)
        self.updated_at = datetime.utcnow()

        # Update in database
        result = await self.db.guilds.update_one(
            {"guild_id": self.guild_id},
            {
                "$set": {
                    "servers": self.servers,
                    "updated_at": self.updated_at
                }
            }
        )

        # IMPORTANT: Also save to servers collection for CSV processor
        # This ensures the server is found by the historical parser
        try:
            # Make sure sftp_enabled is set for servers with SFTP credentials
            if all(key in server_data for key in ["sftp_host", "sftp_username", "sftp_password"]):
                server_data["sftp_enabled"] = True

            # Save to servers collection (used by CSV processor)
            server_result = await self.db.servers.update_one(
                {"server_id": server_data["server_id"]},
                {"$set": server_data},
                upsert=True  # Create if doesn't exist
            )
            logger.info(f"Added server to 'servers' collection: {server_data['server_id']}, upsert={server_result.upserted_id is not None}")

            # Also save to game_servers collection for better compatibility
            game_server_result = await self.db.game_servers.update_one(
                {"server_id": server_data["server_id"]},
                {"$set": server_data},
                upsert=True
            )
            logger.info(f"Added server to 'game_servers' collection: {server_data['server_id']}, upsert={game_server_result.upserted_id is not None}")
        except Exception as e:
            logger.error(f"Error saving server to collections: {e}")

        return result.modified_count > 0

    async def remove_server(self, server_id: Union[str, int, None]) -> bool:
        """Remove a server from the guild and standalone collection

        Args:
            server_id: Server ID to remove (string, int, or other convertible type)

        Returns:
            bool: True if removed successfully, False otherwise
        """
        # Ensure we have database instance
        if not hasattr(self, 'db') or not self.db:
            # If no database connection, use application-level removal only
            logger.warning("No database connection available for Guild.remove_server")

            # Import standardize_server_id here to avoid circular imports
            from utils.server_utils import standardize_server_id

            # Standardize the server_id to ensure consistent formatting
            standardized_server_id = standardize_server_id(server_id)

            # Memory-only removal from servers array
            if hasattr(self, 'servers') and self.servers:
                before_count = len(self.servers)
                self.servers = [s for s in self.servers if standardize_server_id(s.get("server_id")) != standardized_server_id]
                after_count = len(self.servers)
                return before_count > after_count

            return False

        # Import standardize_server_id here to avoid circular imports
        from utils.server_utils import standardize_server_id

        # Standardize the server_id to ensure consistent formatting
        standardized_server_id = standardize_server_id(server_id)
        str_server_id = str(server_id) if server_id is not None else ""

        if standardized_server_id is None:
            logger.warning(f"Invalid server_id format for removal: {server_id}")
            return False

        # Log server information before removal for debugging
        logger.info(f"Removing server with ID from guild {self.guild_id}:")
        logger.info(f"  - Raw server_id: {server_id}, Type: {type(server_id)}")
        logger.info(f"  - String repr: {str_server_id}")
        logger.info(f"  - Standardized: {standardized_server_id}")

        # Make sure servers is initialized
        if not hasattr(self, 'servers') or self.servers is None:
            self.servers = []

        # Log all existing servers for debugging
        logger.info(f"Current servers in guild {self.guild_id}:")
        for i, s in enumerate(self.servers):
            s_id = str(s) if s is not None else "".get("server_id")
            s_name = s.get("server_name", "Unknown")
            std_id = standardize_server_id(s_id)
            logger.info(f"  - Server {i}: ID={s_id}, StdID={std_id}, Name={s_name}, Type={type(s_id)}")

        # First try exact match
        found_exact = False
        for s in self.servers:
            if s.get("server_id") == standardized_server_id:
                found_exact = True
                logger.info(f"Found exact match for server ID {standardized_server_id}")
                break

        # Find and remove server from guild.servers, using standardized comparison
        original_server_count = len(self.servers)

        # Try different matching approaches if needed
        if found_exact is not None:
            # Use exact matching first
            self.servers = [s for s in self.servers if s.get("server_id") != standardized_server_id]
        else:
            # Try case-insensitive standardized matching
            logger.info("No exact match found, using standardized comparison")
            self.servers = [s for s in self.servers if standardize_server_id(s.get("server_id")) != standardized_server_id]

            # Also try direct string comparison with raw server_id
            if len(self.servers) == original_server_count:
                logger.info("No matches with standardized comparison, trying raw string comparison")
                self.servers = [s for s in self.servers if str(s.get("server_id")) != str_server_id]

            # Last resort: try numeric comparison if server_id is numeric
            if standardized_server_id.isdigit() and len(self.servers) == original_server_count:
                logger.info("No matches with string comparison, trying numeric comparison")
                numeric_id = str(standardized_server_id)
                self.servers = [s for s in self.servers if (
                    not str(s.get("server_id")).isdigit() or int(s.get("server_id")) != numeric_id
                )]

        servers_removed = original_server_count - len(self.servers)

        if servers_removed > 0:
            logger.info(f"Removed {servers_removed} server entries from guild.servers array")
        else:
            logger.warning(f"No servers matched {standardized_server_id} in guild.servers array")

        self.updated_at = datetime.utcnow()

        # Update guild in database
        guild_result = await self.db.guilds.update_one(
            {"guild_id": self.guild_id},
            {
                "$set": {
                    "servers": self.servers,
                    "updated_at": self.updated_at
                }
            }
        )

        # Try multiple approaches to remove from standalone servers collection
        # 1. First try exact match
        standalone_exact = await self.db.servers.delete_many({"server_id": standardized_server_id})

        # 2. Try case-insensitive regex match
        standalone_query = {"server_id": {"$regex": f"^{standardized_server_id}$", "$options": "i"}}
        standalone_result = await self.db.servers.delete_many(standalone_query)

        # 3. If server ID is numeric, try numeric match
        standalone_numeric = 0
        if standardized_server_id.isdigit():
            try:
                numeric_id = str(standardized_server_id)
                numeric_result = await self.db.servers.delete_many({"server_id": numeric_id})
                standalone_numeric = numeric_result.deleted_count
            except:
                pass

        # Similar approaches for game_servers collection
        # 1. Exact match
        game_exact = await self.db.game_servers.delete_many({"server_id": standardized_server_id})

        # 2. Case-insensitive match
        game_servers_query = {"server_id": {"$regex": f"^{standardized_server_id}$", "$options": "i"}}
        game_regex = await self.db.game_servers.delete_many(game_servers_query)

        # 3. Numeric match if applicable
        game_numeric = 0
        if standardized_server_id.isdigit():
            try:
                numeric_id = str(standardized_server_id)
                numeric_result = await self.db.game_servers.delete_many({"server_id": numeric_id})
                game_numeric = numeric_result.deleted_count
            except:
                pass

        # Combine all deletion counts
        standalone_count = standalone_exact.deleted_count + standalone_result.deleted_count + standalone_numeric
        game_count = game_exact.deleted_count + game_regex.deleted_count + game_numeric

        # Log detailed deletion results
        logger.info(f"Server removal results - Guild: {guild_result.modified_count}")
        logger.info(f"Servers collection - Exact: {standalone_exact.deleted_count}, Regex: {standalone_result.deleted_count}, Numeric: {standalone_numeric}")
        logger.info(f"Game servers - Exact: {game_exact.deleted_count}, Regex: {game_regex.deleted_count}, Numeric: {game_numeric}")

        return guild_result.modified_count > 0 or standalone_count > 0 or game_count > 0

    async def get_server(self, server_id: Union[str, int, None]) -> Optional[Dict[str, Any]]:
        """Get a server by ID

        This method ensures server_id is always treated as a string
        for consistent comparisons, following the standardized approach.

        Args:
            server_id: Server ID to find (will be converted to string)

        Returns:
            Optional[Dict]: Server data if found, None otherwise
        """
        # Use the standardize_server_id function from server_utils
        # for consistent handling across the codebase
        from utils.server_utils import standardize_server_id

        # Standardize server ID
        str_server_id = standardize_server_id(server_id)
        if str_server_id is None or str_server_id == "":
            logger.warning(f"Invalid server_id provided to get_server: {server_id}")
            return None

        # Check servers list exists and is actually a list
        if not hasattr(self, 'servers') or not isinstance(self.servers, list):
            logger.warning(f"Guild {self.guild_id} has no servers attribute or it is not a list")
            return None

        # Check each server with consistent string conversion
        for server in self.servers:
            if not isinstance(server, dict):
                logger.warning(f"Non-dict server entry found in guild {self.guild_id}: {type(server)}")
                continue

            # Get server_id with fallback to empty string if missing
            server_id_value = server.get("server_id", "")
            if server_id_value is None:
                continue

            # Always compare as strings with consistent handling
            if standardize_server_id(server_id_value) == str_server_id:
                return server

        return None

    def get_max_servers(self) -> int:
        """Get maximum number of servers allowed for guild's tier"""
        from config import PREMIUM_TIERS
        tier_info = PREMIUM_TIERS.get(self.premium_tier, {})
        return tier_info.get("max_servers", 1)
        
    async def check_feature_access(self, feature_name: str) -> bool:
        """Check if guild is not None has access to a specific premium feature
        
        This method implements tier inheritance, ensuring that
        higher tiers have access to all features from lower tiers.
        
        Args:
            feature_name: Name of the feature to check
            
        Returns:
            bool: True if guild has access to the feature, False otherwise
        """
        # Import central premium utilities
        from utils.premium_utils import verify_premium_for_feature
        from utils.safe_mongodb import SafeDocument
        
        # Add diagnostic logging for feature check tracing
        import logging
        logger = logging.getLogger(__name__)
        
        # Log the beginning of feature check
        logger.info(f"Guild {self.guild_id}: Checking access to feature '{feature_name}'")
        
        try:
            # Ensure we have a database connection
            if not hasattr(self, 'db') or self.db is None:
                logger.warning(f"No database connection for guild {self.guild_id}")
                # Try to use class database if instance db is not available
                if hasattr(self.__class__, 'db') and self.__class__.db is not None:
                    self.db = self.__class__.db
                else:
                    logger.error(f"No database available for guild {self.guild_id}")
                    # Default to allowing access if we can't check
                    return True
            
            # Convert guild ID to string for consistency
            str_guild_id = str(self.guild_id)
            
            # Use the centralized premium verification function
            # This handles all the error cases, type conversions, and feature normalization
            has_access = await verify_premium_for_feature(self.db, str_guild_id, feature_name)
            
            # Log the result
            logger.info(f"Guild {str_guild_id} access to {feature_name}: {has_access}")
            
            return has_access
            
        except Exception as e:
            logger.error(f"Error in feature access check: {e}", exc_info=True)
            
            # Default to allowing access if we encounter errors
            # This prevents locking users out due to technical issues
            return True

    @classmethod
    async def get_by_guild_id(cls, db, guild_id: str) -> Optional['Guild']:
        """Get a guild by guild_id

        Args:
            db: Database connection
            guild_id: Discord guild ID (will be converted to string)

        Returns:
            Guild object or None if not found
        """
        # CRITICAL FIX: Enhanced logging and more robust guild ID handling
        logger.info(f" Looking up guild by ID: {guild_id}, type: {type(guild_id).__name__}")

        # Import SafeMongoDBOperations for enhanced error handling
        from utils.safe_mongodb import SafeMongoDBOperations, SafeDocument
        from utils.mongodb_migrator import ensure_safe_document, document_exists

        # Create safe operations wrapper
        safe_db = SafeMongoDBOperations(db)

        # CRITICAL FIX: Handle guild_id conversion more robustly
        try:
            if guild_id is None:
                logger.warning("Attempted to get guild with None guild_id")
                return None

            # Standardize guild_id to string format
            if isinstance(guild_id, int):
                string_guild_id = str(guild_id)
                logger.info(f"Converted integer guild_id {guild_id} to string: {string_guild_id}")
            elif isinstance(guild_id, str):
                string_guild_id = str(guild_id) if guild_id is not None else ""
                logger.info(f"Using string guild_id directly: {string_guild_id}")
            else:
                # Handle other types safely
                string_guild_id = str(guild_id)
                logger.warning(f"Converted unexpected guild_id type {type(guild_id).__name__} to string: {string_guild_id}")

            if string_guild_id is None:
                logger.warning("Empty guild_id after conversion")
                return None
        except Exception as e:
            logger.error(f"Error converting guild_id: {e}")
            return None

        try:
            # CRITICAL FIX: Try multiple query approaches simultaneously for improved robustness
            safe_document = None
            
            # Build query to try both string and numeric ID formats at once
            # Using a list with explicit typing to avoid type errors
            from typing import Dict, Union, List, Any
            
            # Define properly typed structures
            or_conditions: List[Dict[str, Any]] = [{"guild_id": string_guild_id}]
            
            # Add numeric version to query if possible
            if string_guild_id.isdigit():
                numeric_id = str(string_guild_id)
                numeric_condition: Dict[str, Any] = {"guild_id": numeric_id}
                or_conditions.append(numeric_condition)
                logger.info(f" Adding numeric guild_id to query: {numeric_id}")
                
            # Create the final query
            query = {"$or": or_conditions}
            
            # Execute combined query using safe operations
            logger.info(f" Executing combined query: {query}")
            safe_document = await safe_db.find_one("guilds", query)
            
            # Fallback to regex match as last resort
            if not document_exists(safe_document):
                logger.info(f" Trying case-insensitive lookup for guild_id: {string_guild_id}")
                regex_query = {"guild_id": {"$regex": f"^{string_guild_id}$", "$options": "i"}}
                safe_document = await safe_db.find_one("guilds", regex_query)

            # Check if we found a document
            if document_exists(safe_document):
                logger.info(f" Found guild document for guild_id: {string_guild_id}")
                # Get the dictionary from the document
                from utils.mongodb_migrator import get_document_dict
                document = get_document_dict(safe_document)
                guild = cls.create_from_db_document(document, db)

                # CRITICAL FIX: Verify the returned guild has the expected premium_tier
                if guild and hasattr(guild, 'premium_tier'):
                    logger.info(f" Guild {string_guild_id} premium_tier: {guild.premium_tier}, type: {type(guild.premium_tier).__name__}")
                return guild
            else:
                logger.warning(f" No guild found for guild_id: {string_guild_id}")
                return None
        except Exception as e:
            logger.error(f" Error retrieving guild: {e}")
            return None

    async def set_premium_tier(self, db, tier: int) -> bool:
        """Set premium tier for guild

        Args:
            db: Database connection
            tier: Premium tier (0-4)

        Returns:
            True if updated successfully, False otherwise
        """
        # Import SafeMongoDBOperations for enhanced error handling
        from utils.safe_mongodb import SafeMongoDBOperations

        # Create safe operations wrapper
        safe_db = SafeMongoDBOperations(db)

        # Validate tier range
        if tier < 0 or tier > 4:
            logger.warning(f"Attempt to set invalid premium tier: {tier}")
            return False

        # Ensure tier is an integer
        try:
            tier_int = int(tier) if tier is not None else 0 if tier is not None else 0
        except (ValueError, TypeError):
            logger.error(f"Invalid tier type: {type(tier).__name__}, value: {tier}")
            return False

        # Log the tier change
        logger.info(f"Setting premium tier for guild {self.guild_id}: {self.premium_tier} -> {tier_int}")

        # Set tier in model
        self.premium_tier = tier_int
        self.updated_at = datetime.utcnow()

        # Update in database
        try:
            # Use safe MongoDB operations for update
            update_result = await safe_db.update_one(
                "guilds",
                {"guild_id": self.guild_id},
                {"$set": {
                    "premium_tier": tier_int,  # Explicitly use tier_int for db storage
                    "updated_at": self.updated_at
                }}
            )

            success = update_result.success and update_result.modified_count > 0
            if success is not None:
                logger.info(f"Successfully updated premium tier for guild {self.guild_id} to {tier_int}")
            else:
                logger.warning(f"Failed to update premium tier for guild {self.guild_id}, no documents modified")

            return success
        except Exception as e:
            logger.error(f"Error updating premium tier: {e}")
            return False

    async def set_admin_role(self, db, role_id: str) -> bool:
        """Set admin role for guild

        Args:
            db: Database connection
            role_id: Discord role ID

        Returns:
            True if updated successfully, False otherwise
        """
        self.admin_role_id = str(role_id) if role_id is not None else ""
        self.updated_at = datetime.utcnow()

        # Update in database
        result = await db.guilds.update_one(
            {"guild_id": self.guild_id},
            {"$set": {
                "admin_role_id": self.admin_role_id,
                "updated_at": self.updated_at
            }}
        )

        return result.modified_count > 0

    async def add_admin_user(self, db, user_id: str) -> bool:
        """Add admin user for guild

        Args:
            db: Database connection
            user_id: Discord user ID

        Returns:
            True if updated successfully, False otherwise
        """
        if not hasattr(self, "admin_users"):
            self.admin_users = []

        if user_id in self.admin_users:
            return True

        self.admin_users.append(user_id)
        self.updated_at = datetime.utcnow()

        # Update in database
        result = await db.guilds.update_one(
            {"guild_id": self.guild_id},
            {"$set": {
                "admin_users": self.admin_users,
                "updated_at": self.updated_at
            }}
        )

        return result.modified_count > 0

    async def remove_admin_user(self, db, user_id: str) -> bool:
        """Remove admin user for guild

        Args:
            db: Database connection
            user_id: Discord user ID

        Returns:
            True if updated successfully, False otherwise
        """
        if not hasattr(self, "admin_users") or user_id not in self.admin_users:
            return True

        self.admin_users.remove(user_id)
        self.updated_at = datetime.utcnow()

        # Update in database
        result = await db.guilds.update_one(
            {"guild_id": self.guild_id},
            {"$set": {
                "admin_users": self.admin_users,
                "updated_at": self.updated_at
            }}
        )

        return result.modified_count > 0

    async def update_theme(self, db, color_primary: Optional[str] = None, color_secondary: Optional[str] = None, color_accent: Optional[str] = None, icon_url: Optional[str] = None) -> bool:
        """Update theme colors for guild

        Args:
            db: Database connection
            color_primary: Primary color (hex)
            color_secondary: Secondary color (hex)
            color_accent: Accent color (hex)
            icon_url: Icon URL

        Returns:
            True if updated successfully, False otherwise
        """
        # Timestamp for the update
        update_timestamp = datetime.utcnow()

        # Create the update dictionary with only the fields to update
        # This avoids including color fields that might have non-datetime values
        update_dict = {}
        update_dict["updated_at"] = update_timestamp

        # Only add fields that are explicitly being updated
        if color_primary is not None:
            self.color_primary = color_primary
            update_dict["color_primary"] = color_primary

        if color_secondary is not None:
            self.color_secondary = color_secondary
            update_dict["color_secondary"] = color_secondary

        if color_accent is not None:
            self.color_accent = color_accent

            update_dict["color_accent"] = color_accent

        if icon_url is not None:
            self.icon_url = icon_url
            update_dict["icon_url"] = icon_url

        # Update the instance timestamp
        self.updated_at = update_timestamp

        # Update in database
        result = await db.guilds.update_one(
            {"guild_id": self.guild_id},
            {"$set": update_dict}
        )

        return result.modified_count > 0

    @classmethod
    async def get_by_id(cls, db, guild_id) -> Optional['Guild']:
        """Get a guild by its Discord ID (alias for get_by_guild_id)

        Args:
            db: Database connection
            guild_id: Discord guild ID (will be converted to string)

        Returns:
            Guild object or None if not found
        """
        # This method is an alias that ensures backward compatibility
        # While ensuring the same type safety as get_by_guild_id
        return await cls.get_by_guild_id(db, guild_id)

    @classmethod
    async def get_guild(cls, db, guild_id) -> Optional['Guild']:
        """Get a guild by its Discord ID (alias for get_by_id)

        This method is used throughout the codebase for consistency.

        Args:
            db: Database connection
            guild_id: Discord guild ID (will be converted to string)

        Returns:
            Guild object or None if not found
        """
        return await cls.get_by_id(db, guild_id)

    @classmethod
    async def get_or_create(cls, db, guild_id, guild_name: Optional[str] = None) -> Optional['Guild']:
        """Get an existing guild if guild is not None else create a new one if it doesn\'t exist

        This is particularly useful for premium validation where we might need
        a guild model without requiring complete setup.

        Args:
            db: Database connection 
            guild_id: Discord guild ID (will be converted to string)
            guild_name: Optional guild name to use if creating a new guild (defaults to "Guild {guild_id}")

        Returns:
            Guild object or None if retrieval/creation failed
        """
        # Import utilities for safe MongoDB operations
        from utils.safe_mongodb import SafeMongoDBOperations, SafeDocument
        from utils.mongodb_migrator import document_exists, get_value

        # Ensure guild_id is a string for consistent handling
        string_guild_id = str(guild_id)
        logger.info(f" get_or_create for guild_id: {string_guild_id}")
        
        # First try to get the existing guild using safe operations
        try:
            # Use get_by_guild_id which provides consistent guild ID handling
            guild = await cls.get_by_guild_id(db, string_guild_id)
            
            # Check guild using proper None check instead of truthiness
            if guild is not None:
                logger.info(f" Found existing guild for ID {string_guild_id}, premium_tier: {getattr(guild, 'premium_tier', 0)}")
                return guild
            else:
                logger.info(f" No existing guild found for ID {string_guild_id}, will create new one")
        except Exception as e:
            logger.error(f" Error in get_or_create lookup: {e}")
            # Continue to creation logic

        # Otherwise create a new guild with basic information
        if guild_name is None:
            guild_name = f"Guild {string_guild_id}"

        # Create the guild with minimal setup using SafeMongoDBOperations
        try:
            logger.info(f" Auto-creating guild during feature access: {string_guild_id}")
            
            # Use SafeMongoDBOperations for insertion to ensure proper error handling
            safe_ops = SafeMongoDBOperations(db)
            
            # Import datetime for timestamps
            import datetime
            
            # Create minimal guild document
            new_guild_data = {
                "guild_id": string_guild_id,
                "name": guild_name,
                "premium_tier": 0,
                "setup_complete": False,
                "created_at": datetime.datetime.utcnow(),
                "updated_at": datetime.datetime.utcnow()
            }
            
            # Insert with safe operations using the instance method
            insert_result = await safe_ops.insert_one_to_collection("guilds", new_guild_data)
            
            if insert_result.success and insert_result.inserted_id:
                logger.info(f" Successfully created guild {string_guild_id}")
                # Now get the full Guild object
                return await cls.get_by_guild_id(db, string_guild_id)
            else:
                logger.error(f" Failed to create guild {string_guild_id}: {insert_result.message}")
                return None
                
        except Exception as e:
            logger.error(f" Error auto-creating guild during feature access: {e}", exc_info=True)
            return None

    @classmethod
    async def create(cls, db, guild_id: str, name: str) -> Optional['Guild']:
        """Create a new guild

        Args:
            db: Database connection
            guild_id: Discord guild ID
            name: Guild name

        Returns:
            Created Guild object or None if creation failed
        """
        # Import SafeMongoDBOperations for enhanced error handling
        from utils.safe_mongodb import SafeMongoDBOperations
        from utils.mongodb_migrator import document_exists

        # Create safe operations wrapper
        safe_db = SafeMongoDBOperations(db)

        # CRITICAL FIX: Ensure guild_id is always stored as string
        string_guild_id = str(guild_id)
        logger.info(f" Creating new guild with ID: {string_guild_id}, name: {name}")
        
        # Create document
        document = {
            "guild_id": string_guild_id,  # Always store as string for consistent handling
            "name": name,
            "premium_tier": 0,  # Default tier
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        # Insert into database using safe operations
        try:
            result = await safe_db.insert_one("guilds", document)
            
            if result.success is not None:
                # Add the _id to the document
                document["_id"] = result.inserted_id
                return cls.create_from_db_document(document, db)
            else:
                error = result.error()
                logger.error(f"Failed to create guild: {error}")
                return None
        except Exception as e:
            logger.error(f"Error creating guild: {e}")
            return None

    @classmethod
    def create_from_db_document(cls, document: Optional[Dict[str, Any]], db=None) -> Optional['Guild']:
        """Create a Guild instance from a database document with db connection

        This is a custom method specifically for Guild to handle db parameter.
        Unlike BaseModel.from_document, this accepts a db connection parameter.

        Args:
            document: MongoDB document, can be None
            db: Database connection

        Returns:
            Guild instance or None if document is not None is None
        """
        if document is None:
            return None

        # CRITICAL FIX: Enhance logging for document inspection
        guild_id = str(document) if document is not None else "".get('guild_id', 'unknown')
        logger.info(f" Creating Guild instance for guild_id={guild_id}, document keys: {list(document.keys())}")

        # Extract and properly convert premium_tier to int before initializing
        # This ensures premium tier is always stored as an integer
        # Deep debug inspect the document
        guild_id = str(document) if document is not None else "".get("guild_id", "unknown")
        try:
            # Detailed log of the document to help diagnose
            doc_keys = list(document.keys())
            logger.info(f" Guild {guild_id}: Document from DB has keys: {doc_keys}")
            # Dump the document's main keys (except servers which is too large)
            safe_keys = [k for k in doc_keys if k != 'servers']
            doc_sample = {k: document.get(k) for k in safe_keys}
            logger.info(f" Guild {guild_id}: Document contents: {doc_sample}")
        except Exception as e:
            logger.error(f" Error inspecting document: {e}")

        document_copy = document.copy()

        # Handle premium_tier conversion specifically with enhanced logging and validation
        if 'premium_tier' in document_copy:
            try:
                # Convert premium_tier to integer with more detailed logging
                premium_tier_value = document_copy['premium_tier']
                logger.info(f" Guild {guild_id}: Raw premium_tier from DB: {premium_tier_value}, type: {type(premium_tier_value).__name__}")

                if premium_tier_value is not None:
                    # CRITICAL FIX: More robust type conversion with validation
                    if isinstance(premium_tier_value, int):
                        # Already an integer, validate range
                        premium_tier_int = premium_tier_value
                        logger.info(f" Guild {guild_id}: premium_tier is already an integer: {premium_tier_int}")
                    elif isinstance(premium_tier_value, str):
                        # Handle string conversion with additional checks
                        if premium_tier_value.strip().isdigit():
                            # Clean digit string (most common case)
                            premium_tier_int = int(premium_tier_value.strip())
                            logger.info(f" Guild {guild_id}: Converted clean string premium_tier '{premium_tier_value}' to integer: {premium_tier_int}")
                        else:
                            # Try to extract first digit sequence or use default
                            import re
                            digit_match = re.search(r'\d+', premium_tier_value)
                            if digit_match is not None:
                                premium_tier_int = int(digit_match.group(0))
                                logger.info(f" Guild {guild_id}: Extracted digits from premium_tier string: {premium_tier_int}")
                            else:
                                premium_tier_int = 0
                                logger.warning(f" Guild {guild_id}: String premium_tier '{premium_tier_value}' contains no digits, defaulting to 0")
                    else:
                        # Fallback conversion with strong error handling for any other type
                        try:
                            premium_tier_int = int(float(premium_tier_value))
                            logger.info(f" Guild {guild_id}: Converted {type(premium_tier_value).__name__} premium_tier to integer: {premium_tier_int}")
                        except (ValueError, TypeError) as e:
                            logger.error(f" Guild {guild_id}: Failed to convert premium_tier '{premium_tier_value}' to integer: {e}")
                            premium_tier_int = 0

                    # CRITICAL FIX: Validate tier range (0-5) for additional safety
                    if premium_tier_int < 0 or premium_tier_int > 5:
                        logger.warning(f" Guild {guild_id}: premium_tier {premium_tier_int} out of valid range (0-5), clamping")
                        premium_tier_int = max(0, min(5, premium_tier_int))

                    # Set the validated integer value
                    document_copy['premium_tier'] = premium_tier_int
                    logger.info(f" Guild {guild_id}: Final premium_tier set to {premium_tier_int}")
                else:
                    # Default to 0 if None
                    document_copy['premium_tier'] = 0
                    logger.warning(f" Guild {guild_id}: Premium tier is None in database, defaulting to 0")
            except Exception as e:
                logger.error(f" Guild {guild_id}: Unexpected error converting premium_tier: {e}")
                document_copy['premium_tier'] = 0
        else:
            # If no premium_tier in document, default to 0
            document_copy['premium_tier'] = 0
            logger.warning(f" Guild {guild_id}: No premium_tier found in database document, defaulting to 0")

        # CRITICAL FIX: Verify premium_tier value after conversion
        logger.info(f" Guild {guild_id}: Final premium_tier in document_copy: {document_copy['premium_tier']}, type: {type(document_copy['premium_tier']).__name__}")

        # Create Guild instance with the properly converted document
        instance = cls(db, **document_copy)

        # CRITICAL FIX: Perform final validation on the created instance
        if hasattr(instance, 'premium_tier'):
            logger.info(f" Guild {guild_id}: Instance premium_tier after creation: {instance.premium_tier}, type: {type(instance.premium_tier).__name__}")

            # Force int conversion one more time to ensure consistency
            try:
                # Make sure premium_tier is always an integer by this point
                if not isinstance(instance.premium_tier, int):
                    instance.premium_tier = int(float(instance.premium_tier))
                
                # Ensure it's in valid range
                instance.premium_tier = max(0, min(5, instance.premium_tier))
                logger.info(f" Guild {guild_id}: Final instance premium_tier: {instance.premium_tier}")
            except (ValueError, TypeError) as e:
                logger.error(f" Guild {guild_id}: Error in final premium_tier validation: {e}")
                # Set failsafe default
                instance.premium_tier = 0

        # Ensure all IDs are strings for consistent handling
        if hasattr(instance, 'guild_id'):
            instance.guild_id = str(instance.guild_id)
        if hasattr(instance, 'admin_role_id') and instance.admin_role_id is not None:
            instance.admin_role_id = str(instance.admin_role_id)

        return instance