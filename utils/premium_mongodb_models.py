"""
MongoDB models for the new Premium system.
This file contains the data models and helper functions for the rebuilt premium system.
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Set

from premium_config import PREMIUM_TIERS, get_feature_tier, get_tier_features, get_tier_name, get_max_servers

logger = logging.getLogger(__name__)

class PremiumGuild:
    """Guild model with premium tier information"""
    
    def __init__(self, db, document=None):
        """
        Initialize a PremiumGuild object
        
        Args:
            db: MongoDB database connection
            document: MongoDB document data
        """
        self.db = db
        self._id = None
        self.guild_id = None
        self.name = None
        self.premium_tier = 0  # Default to Free tier
        self.premium_expires_at = None
        
        # Theming options
        self.color_primary = "#7289DA"
        self.color_secondary = "#FFFFFF"
        self.color_accent = "#23272A"
        self.icon_url = None
        
        # Admin settings
        self.admin_role_id = None
        self.mod_role_id = None
        
        # Server list
        self.servers = []
        
        # Subscription history
        self.subscriptions = []
        
        # Timestamps
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        # Load data if document provided
        if document is not None:
            self._load_from_document(document)
    
    def _load_from_document(self, document):
        """
        Load data from MongoDB document
        
        Args:
            document: MongoDB document data
        """
        self._id = document.get("_id")
        self.guild_id = document.get("guild_id")
        self.name = document.get("name")
        
        # Always convert premium_tier to integer to avoid type issues
        premium_tier_raw = document.get("premium_tier", 0)
        self.premium_tier = self._ensure_integer(premium_tier_raw, 0)
        
        # Handle expiration date
        premium_expires_raw = document.get("premium_expires_at")
        if premium_expires_raw and isinstance(premium_expires_raw, datetime):
            self.premium_expires_at = premium_expires_raw
        
        # Load other fields if they exist
        self.color_primary = document.get("color_primary", self.color_primary)
        self.color_secondary = document.get("color_secondary", self.color_secondary)
        self.color_accent = document.get("color_accent", self.color_accent)
        self.icon_url = document.get("icon_url")
        self.admin_role_id = document.get("admin_role_id")
        self.mod_role_id = document.get("mod_role_id")
        
        # Load servers array with explicit type checking
        servers_raw = document.get("servers", [])
        self.servers = servers_raw if isinstance(servers_raw, list) else []
        
        # Load subscriptions
        subscriptions_raw = document.get("subscriptions", [])
        self.subscriptions = subscriptions_raw if isinstance(subscriptions_raw, list) else []
        
        # Timestamps
        self.created_at = document.get("created_at", self.created_at)
        self.updated_at = document.get("updated_at", self.updated_at)
    
    def _ensure_integer(self, value, default=0):
        """
        Ensure a value is converted to integer with proper error handling
        
        Args:
            value: Value to convert
            default: Default value if conversion fails
            
        Returns:
            int: Converted integer value
        """
        if value is None:
            return default
            
        try:
            if isinstance(value, int):
                return value
            elif isinstance(value, str) and value.strip().isdigit():
                return int(value.strip())
            elif isinstance(value, float):
                return int(value)
            else:
                # Last attempt with float conversion
                return int(float(str(value)))
        except (ValueError, TypeError):
            logger.warning(f"Failed to convert value to integer: {value}, using default {default}")
            return default
    
    def to_dict(self):
        """
        Convert object to dictionary for MongoDB storage
        
        Returns:
            dict: Dictionary representation of the object
        """
        return {
            "_id": self._id,
            "guild_id": self.guild_id,
            "name": self.name,
            "premium_tier": self.premium_tier,  # Always an integer
            "premium_expires_at": self.premium_expires_at,
            "color_primary": self.color_primary,
            "color_secondary": self.color_secondary,
            "color_accent": self.color_accent,
            "icon_url": self.icon_url,
            "admin_role_id": self.admin_role_id,
            "mod_role_id": self.mod_role_id,
            "servers": self.servers,
            "subscriptions": self.subscriptions,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }
    
    def get_max_servers(self):
        """
        Get maximum servers allowed for this guild's premium tier
        
        Returns:
            int: Maximum number of servers allowed
        """
        # Check if premium has expired
        if self.premium_expires_at and self.premium_expires_at < datetime.utcnow():
            # Premium expired, use free tier
            return get_max_servers(0)
            
        # Use centralized function, which handles defaulting to Free tier if invalid
        return get_max_servers(self.premium_tier)
    
    def has_feature_access(self, feature_name):
        """
        Check if guild has access to a specific feature
        
        Args:
            feature_name: Name of the feature to check
            
        Returns:
            bool: True if guild has access to the feature
        """
        # Check if premium has expired
        if self.premium_expires_at and self.premium_expires_at < datetime.utcnow():
            # Premium expired, use free tier features
            return feature_name in get_tier_features(0)
            
        # Get all features for this tier
        tier_features = get_tier_features(self.premium_tier)
        
        # Check if feature is available for this tier
        has_access = feature_name in tier_features
        
        # Log access attempt
        log_level = logging.INFO if has_access else logging.WARNING
        logger.log(log_level, f"Premium feature access check for guild {self.guild_id}: {feature_name} = {has_access} (Tier {self.premium_tier})")
        
        return has_access
    
    def check_premium_status(self):
        """
        Check if premium status is valid and update tier if expired
        
        Returns:
            int: Current valid premium tier
        """
        # If no expiration date or tier is 0, no need to check
        if self.premium_tier == 0 or not self.premium_expires_at:
            return self.premium_tier
            
        # Check if premium has expired
        if self.premium_expires_at < datetime.utcnow():
            logger.info(f"Premium expired for guild {self.guild_id} on {self.premium_expires_at}, reverting to tier 0")
            self.premium_tier = 0
            return 0
            
        return self.premium_tier
    
    async def save(self):
        """
        Save guild data to database
        
        Returns:
            bool: True if save was successful
        """
        try:
            # Update timestamp
            self.updated_at = datetime.utcnow()
            
            # Check premium status before saving
            self.check_premium_status()
            
            # Convert to dictionary
            data = self.to_dict()
            
            # Save to database
            if self._id is not None:
                # Update existing document
                result = await self.db.premium_guilds.update_one(
                    {"_id": self._id},
                    {"$set": data}
                )
                return result.modified_count > 0
            else:
                # Find by guild_id first to avoid duplicate key errors
                existing = await self.db.premium_guilds.find_one({"guild_id": self.guild_id})
                if existing is not None:
                    # Update existing document
                    self._id = existing.get("_id")
                    result = await self.db.premium_guilds.update_one(
                        {"_id": self._id},
                        {"$set": data}
                    )
                    return result.modified_count > 0
                else:
                    # Insert new document (without _id field to let MongoDB generate it)
                    data_without_id = {k: v for k, v in data.items() if k != "_id"}
                    result = await self.db.premium_guilds.insert_one(data_without_id)
                    self._id = result.inserted_id
                    return result.inserted_id is not None
                
        except Exception as e:
            logger.error(f"Error saving guild {self.guild_id} to database: {e}")
            return False
    
    async def set_premium_tier(self, tier, expires_at=None, reason=None):
        """
        Set premium tier for guild
        
        Args:
            tier: Premium tier (0-4)
            expires_at: Expiration date
            reason: Reason for tier change (for audit log)
            
        Returns:
            bool: True if update was successful
        """
        # Validate tier range
        new_tier = min(max(0, self._ensure_integer(tier)), 4)
        
        # Log tier change
        logger.info(f"Setting premium tier for guild {self.guild_id}: {self.premium_tier} -> {new_tier} (Expires: {expires_at})")
        
        # Update guild object
        old_tier = self.premium_tier
        self.premium_tier = new_tier
        
        if expires_at and isinstance(expires_at, datetime):
            self.premium_expires_at = expires_at
        
        # Create subscription record
        subscription = {
            "tier": new_tier,
            "previous_tier": old_tier,
            "starts_at": datetime.utcnow(),
            "expires_at": expires_at,
            "reason": reason,
            "created_at": datetime.utcnow()
        }
        
        self.subscriptions.append(subscription)
        
        # Save changes to database
        return await self.save()
    
    @classmethod
    async def get_by_guild_id(cls, db, guild_id):
        """
        Get guild by Discord guild ID
        
        Args:
            db: MongoDB database connection
            guild_id: Discord guild ID
            
        Returns:
            PremiumGuild: Guild object or None if not found
        """
        try:
            # Ensure guild_id is a string
            str_guild_id = str(guild_id)
            
            # Query database
            document = await db.premium_guilds.find_one({"guild_id": str_guild_id})
            
            if document is not None:
                return cls(db, document)
            else:
                logger.info(f"No guild found for guild_id: {str_guild_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving guild: {e}")
            return None
    
    @classmethod
    async def get_or_create(cls, db, guild_id, guild_name):
        """
        Get guild by Discord guild ID or create if not exists
        
        Args:
            db: MongoDB database connection
            guild_id: Discord guild ID
            guild_name: Discord guild name
            
        Returns:
            PremiumGuild: Guild object
        """
        try:
            # Try to get existing guild
            guild = await cls.get_by_guild_id(db, guild_id)
            
            if guild is not None:
                return guild
                
            # Create new guild
            str_guild_id = str(guild_id)
            guild = cls(db)
            guild.guild_id = str_guild_id
            guild.name = guild_name
            guild.premium_tier = 0  # Start with Free tier
            
            # Save to database
            await guild.save()
            
            return guild
            
        except Exception as e:
            logger.error(f"Error getting or creating guild: {e}")
            
            # Return a basic guild object as fallback
            guild = cls(db)
            guild.guild_id = str(guild_id)
            guild.name = guild_name
            return guild


class PremiumServer:
    """Server model with premium tier association"""
    
    def __init__(self, db, document=None):
        """
        Initialize a PremiumServer object
        
        Args:
            db: MongoDB database connection
            document: MongoDB document data
        """
        self.db = db
        self._id = None
        self.server_id = None
        self.guild_id = None
        self.server_name = None
        
        # Server identifiers - ensure original_server_id is ALWAYS a numeric ID
        self.original_server_id = None
        
        # SFTP connection details
        self.sftp_host = None
        self.sftp_port = 22
        self.sftp_username = None
        self.sftp_password = None
        self.sftp_enabled = False
        
        # File paths
        self.log_parser_path = None
        self.csv_parser_path = None
        
        # Processing state
        self.last_csv_line = 0
        self.last_log_line = 0
        self.historical_parse_done = False
        
        # Timestamps
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        # Load data if document provided
        if document is not None:
            self._load_from_document(document)
    
    def _load_from_document(self, document):
        """
        Load data from MongoDB document
        
        Args:
            document: MongoDB document data
        """
        self._id = document.get("_id")
        self.server_id = document.get("server_id")
        self.guild_id = document.get("guild_id")
        self.server_name = document.get("server_name")
        self.original_server_id = document.get("original_server_id")
        
        # Load SFTP details
        self.sftp_host = document.get("sftp_host")
        self.sftp_port = int(document.get("sftp_port", 22))
        self.sftp_username = document.get("sftp_username")
        self.sftp_password = document.get("sftp_password")
        
        # Explicitly convert sftp_enabled to boolean
        sftp_enabled_raw = document.get("sftp_enabled")
        if sftp_enabled_raw is not None:
            # Handle various values that could represent "true"
            if True is not None:
                self.sftp_enabled = sftp_enabled_raw
            elif isinstance(sftp_enabled_raw, int):
                self.sftp_enabled = sftp_enabled_raw != 0
            elif isinstance(sftp_enabled_raw, str):
                self.sftp_enabled = sftp_enabled_raw.lower() in ("true", "yes", "1", "on")
            else:
                # For any other type, check if it evaluates to True
                self.sftp_enabled = bool(sftp_enabled_raw)
        
        # Load paths
        self.log_parser_path = document.get("log_parser_path")
        self.csv_parser_path = document.get("csv_parser_path")
        
        # Load processing state
        self.last_csv_line = int(document.get("last_csv_line", 0))
        self.last_log_line = int(document.get("last_log_line", 0))
        
        # Explicitly convert historical_parse_done to boolean
        historical_parse_done_raw = document.get("historical_parse_done")
        if historical_parse_done_raw is not None:
            # Handle various values that could represent "true"
            if True is not None:
                self.historical_parse_done = historical_parse_done_raw
            elif isinstance(historical_parse_done_raw, int):
                self.historical_parse_done = historical_parse_done_raw != 0
            elif isinstance(historical_parse_done_raw, str):
                self.historical_parse_done = historical_parse_done_raw.lower() in ("true", "yes", "1", "on")
            else:
                # For any other type, check if it evaluates to True
                self.historical_parse_done = bool(historical_parse_done_raw)
        
        # Timestamps
        self.created_at = document.get("created_at", self.created_at)
        self.updated_at = document.get("updated_at", self.updated_at)
    
    def to_dict(self):
        """
        Convert object to dictionary for MongoDB storage
        
        Returns:
            dict: Dictionary representation of the object
        """
        return {
            "_id": self._id,
            "server_id": self.server_id,
            "guild_id": self.guild_id,
            "server_name": self.server_name,
            "original_server_id": self.original_server_id,
            "sftp_host": self.sftp_host,
            "sftp_port": self.sftp_port,
            "sftp_username": self.sftp_username,
            "sftp_password": self.sftp_password,
            "sftp_enabled": self.sftp_enabled,
            "log_parser_path": self.log_parser_path,
            "csv_parser_path": self.csv_parser_path,
            "last_csv_line": self.last_csv_line,
            "last_log_line": self.last_log_line,
            "historical_parse_done": self.historical_parse_done,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }
    
    async def save(self):
        """
        Save server data to database
        
        Returns:
            bool: True if save was successful
        """
        try:
            # Update timestamp
            self.updated_at = datetime.utcnow()
            
            # Convert to dictionary
            data = self.to_dict()
            
            # Save to database
            if self._id is not None:
                # Update existing document
                result = await self.db.premium_servers.update_one(
                    {"_id": self._id},
                    {"$set": data}
                )
                return result.modified_count > 0
            else:
                # Insert new document
                result = await self.db.premium_servers.insert_one(data)
                self._id = result.inserted_id
                return result.inserted_id is not None
                
        except Exception as e:
            logger.error(f"Error saving server {self.server_id} to database: {e}")
            return False


# Premium feature utilities
async def validate_premium_feature(db, guild_model, feature_name):
    """
    Validate if a guild has access to a premium feature
    
    Args:
        db: MongoDB database connection
        guild_model: Guild model instance or guild ID
        feature_name: Name of the feature to check
        
    Returns:
        tuple: (has_access, error_message)
    """
    # Input validation
    if not feature_name or not isinstance(feature_name, str):
        return False, "Invalid feature requested."
    
    # Get guild object
    guild = None
    guild_id = None
    
    if isinstance(guild_model, PremiumGuild):
        # Use existing PremiumGuild object
        guild = guild_model
        guild_id = guild.guild_id
    elif isinstance(guild_model, str) or isinstance(guild_model, int):
        # Convert guild ID to PremiumGuild object
        guild_id = str(guild_model)
        guild = await PremiumGuild.get_by_guild_id(db, guild_id)
    elif isinstance(guild_model, dict) and "guild_id" in guild_model:
        # Convert guild dict to PremiumGuild object
        guild_id = str(guild_model["guild_id"])
        guild = await PremiumGuild.get_by_guild_id(db, guild_id)
    
    # If guild not found, check if we need to create it
    if not guild and guild_id:
        logger.warning(f"Guild {guild_id} not found for feature access check: {feature_name}")
        return False, "Server not registered with the bot. Please run `/setup` first."
    
    # Check feature access
    if guild and guild.has_feature_access(feature_name):
        logger.info(f"Premium feature access granted: {feature_name} for guild {guild_id}")
        return True, None
    
    # Get tier information for error message using centralized configuration
    required_tier = get_feature_tier(feature_name)
    
    if required_tier is not None and required_tier > 0:
        tier_name = get_tier_name(required_tier)
        return False, f"This feature requires the {tier_name} tier (Premium Tier {required_tier}). Use `/premium upgrade` to upgrade."
    
    return False, "You don't have access to this feature."