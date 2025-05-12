# Ensure proper boolean conversion in player model by replacing the old code with the new code.
"""
Player model represents a player in the game with statistics and relationships
"""
import logging
import asyncio
from typing import Dict, Any, List, Optional, Union, Set, cast
from datetime import datetime, timedelta
import math
import re
import uuid
from pymongo import ReturnDocument

from models.base_model import BaseModel

logger = logging.getLogger("player_model")

class Player(BaseModel):
    """
    Player model with improved type handling and validation
    """

    collection_name = "players"

    def __init__(
        self,
        player_id: Optional[str] = None,
        server_id: Optional[str] = None,
        name: Optional[str] = None,
        kills: int = 0,
        deaths: int = 0,
        suicides: int = 0,
        display_name: Optional[str] = None,
        last_seen: Optional[datetime] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        known_aliases: Optional[List[str]] = None,
        db=None,
        data=None,
        **kwargs
    ):
        # Initialize the base class
        super().__init__(db=db, data=data or {})
        
        # Ensure player_id and server_id are properly sanitized to prevent errors
        # CRITICAL FIX: Defensive handling of player_id and server_id
        if player_id is None or player_id == "":
            logger.error("Attempted to create Player with empty player_id")
            # Generate a fallback ID for emergency recovery
            import uuid

            player_id = f"recovered_{uuid.uuid4()}"
            logger.warning(f"Using emergency fallback player_id: {player_id}")

        if server_id is None or server_id == "":
            logger.error("Attempted to create Player with empty server_id")
            server_id = "default_server"
            logger.warning(f"Using emergency fallback server_id: {server_id}")

        # Convert to string with robust error handling
        try:
            self.player_id = str(player_id).strip()
            self.server_id = str(server_id).strip()
        except Exception as e:
            logger.error(f"Error converting player_id or server_id to string: {e}")
            # Ensure we have valid strings even if conversion fails
            self.player_id = (
                str(player_id) if player_id is not None else "unknown_player"
            )
            self.server_id = (
                str(server_id) if server_id is not None else "unknown_server"
            )

        # Safely handle name
        if name is None:
            # Create a name based on player_id if not provided
            self.name = (
                f"Player_{self.player_id[:8]}"
                if len(self.player_id) >= 8
                else f"Player_{self.player_id}"
            )
        else:
            # Ensure name is a string
            try:
                self.name = str(name).strip()
            except Exception as e:
                logger.error(f"Error converting name to string: {e}")
                self.name = (
                    f"Player_{self.player_id[:8]}"
                    if len(self.player_id) >= 8
                    else f"Player_{self.player_id}"
                )

        # Handle display name and other fields safely
        try:
            self.display_name = str(display_name).strip() if display_name else self.name
        except Exception:
            self.display_name = self.name

        # Safely convert numeric fields to integers with defaults
        try:
            self.kills = int(kills) if kills is not None else 0
        except (ValueError, TypeError):
            self.kills = 0

        try:
            self.deaths = int(deaths) if deaths is not None else 0
        except (ValueError, TypeError):
            self.deaths = 0

        try:
            self.suicides = int(suicides) if suicides is not None else 0
        except (ValueError, TypeError):
            self.suicides = 0

        # Time fields with defaults
        self.last_seen = last_seen or datetime.utcnow()

        # Handle aliases list safely
        if known_aliases and isinstance(known_aliases, list):
            self.known_aliases = known_aliases
        elif known_aliases and isinstance(known_aliases, str):
            self.known_aliases = [known_aliases]
        else:
            self.known_aliases = [self.name] if self.name else []

        # Time tracking
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()

        # Initialize additional fields from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

        # Log player creation at debug level to reduce log spam
        logger.debug(
            f"Player object initialized: {self.player_id} ({self.name}) for server {self.server_id}"
        )

    @staticmethod
    def _validate_player_id(player_id: Optional[str]) -> bool:
        """Validate player ID with enhanced safety checks"""
        # Strengthen validation to catch various edge cases
        if player_id is None:
            return False
        if not isinstance(player_id, str):
            try:
                player_id = str(player_id).strip()
                return len(player_id) > 0
            except (ValueError, TypeError, AttributeError):
                return False
        return len(player_id.strip()) > 0

    @staticmethod
    def _validate_server_id(server_id: Optional[str]) -> bool:
        """Validate server ID with enhanced safety checks"""
        if server_id is None:
            return False
        if not isinstance(server_id, str):
            try:
                server_id = str(server_id).strip()
                return len(server_id) > 0
            except (ValueError, TypeError, AttributeError):
                return False
        return len(server_id.strip()) > 0

    @staticmethod
    def _validate_name(name: Optional[str]) -> bool:
        """Validate player name with enhanced safety checks"""
        if name is None:
            return False
        if not isinstance(name, str):
            try:
                name = str(name).strip()
                return len(name) > 0
            except (ValueError, TypeError, AttributeError):
                return False
        return len(name.strip()) > 0

    @staticmethod
    def _validate_optional_field(field_name: str, value: Any) -> Any:
        """Validate optional fields with improved robustness"""
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @classmethod
    async def get_by_player_id(
        cls, db, player_id: str, server_id: Optional[str] = None
    ) -> Optional["Player"]:
        """Get a player by player_id with enhanced validation and error handling

        Args:
            db: Database connection
            player_id: Player ID
            server_id: Optional server ID for additional filtering

        Returns:
            Player object or None if not found
        """
        try:
            # Additional safety check before database query
            if not cls._validate_player_id(player_id):
                logger.error(
                    f"Invalid player_id passed to get_by_player_id: {player_id}"
                )
                return None

            # Ensure player_id is properly formatted
            if not isinstance(player_id, str):
                try:
                    player_id = str(player_id).strip()
                except (ValueError, TypeError, AttributeError):
                    logger.error(f"Could not convert player_id to string: {player_id}")
                    return None

            query = {"player_id": player_id}
            if server_id is not None:
                if not cls._validate_server_id(server_id):
                    logger.error(
                        f"Invalid server_id passed to get_by_player_id: {server_id}"
                    )
                    return None

                # Ensure server_id is properly formatted
                if not isinstance(server_id, str):
                    try:
                        server_id = str(server_id).strip()
                    except (ValueError, TypeError, AttributeError):
                        logger.error(f"Could not convert server_id to string: {server_id}")
                        return None

                query["server_id"] = server_id

            document = await db.players.find_one(query)
            if document is not None:
                return cls.from_document(document, db=db)
            return None
        except Exception as e:
            logger.error(f"Error in get_by_player_id: {e}", exc_info=True)
            return None

    @classmethod
    async def update_all_nemesis_and_prey(cls, db, server_id: str) -> bool:
        """Update all nemesis and prey relationships for a server

        Args:
            db: Database connection
            server_id: Server ID

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Updating nemesis and prey relationships for server {server_id}")

            # Get all players for this server
            players_cursor = db.players.find({"server_id": server_id, "active": True})
            player_ids = []
            async for player in players_cursor:
                player_ids.append(player["player_id"])

            # For each player, calculate their nemesis and prey
            for player_id in player_ids:
                # Find who killed this player the most (nemesis)
                nemesis_pipeline = [
                    {"$match": {"victim_id": player_id, "server_id": server_id}},
                    {"$group": {"_id": "$killer_id", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 1}
                ]

                # Find who this player killed the most (prey)
                prey_pipeline = [
                    {"$match": {"killer_id": player_id, "server_id": server_id}},
                    {"$group": {"_id": "$victim_id", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 1}
                ]

                # Execute the aggregation
                nemesis_result = await db.kills.aggregate(nemesis_pipeline).to_list(1)
                prey_result = await db.kills.aggregate(prey_pipeline).to_list(1)

                # Extract the results
                nemesis_id = nemesis_result[0]["_id"] if nemesis_result else None
                nemesis_count = nemesis_result[0]["count"] if nemesis_result else 0
                prey_id = prey_result[0]["_id"] if prey_result else None
                prey_count = prey_result[0]["count"] if prey_result else 0

                # Update the rivalry record
                await db.rivalries.update_one(
                    {"player_id": player_id, "server_id": server_id},
                    {"$set": {
                        "nemesis_id": nemesis_id,
                        "nemesis_count": nemesis_count,
                        "prey_id": prey_id,
                        "prey_count": prey_count,
                        "updated_at": datetime.utcnow()
                    }},
                    upsert=True
                )

            return True
        except Exception as e:
            logger.error(f"Error in update_all_nemesis_and_prey: {e}")
            return False

    async def update_nemesis_and_prey(self, db) -> bool:
        """Update nemesis and prey relationships for a single player

        Args:
            db: Database connection

        Returns:
            True if updated successfully, False otherwise
        """
        try:
            logger.debug(f"Updating nemesis and prey for player {self.player_id}")

            # This method delegates to the Rivalry collection to handle the relationship updates
            if hasattr(db, 'rivalries') and hasattr(db.rivalries, 'update_nemesis_and_prey'):
                # Update player-specific nemesis and prey
                await db.rivalries.update_nemesis_and_prey(
                    self.server_id, player_id=self.player_id
                )
                return True
            else:
                logger.warning("Cannot update nemesis/prey - db.rivalries.update_nemesis_and_prey not available")
                # Return success to allow processing to continue
                return True

        except Exception as e:
            logger.error(f"Error updating nemesis and prey for {self.player_id}: {e}", exc_info=True)
            # Return success even on error to allow processing to continue
            return True

    async def update_stats(
        self,
        db,
        kills: Optional[int] = None,
        deaths: Optional[int] = None,
        suicides: Optional[int] = None,
    ) -> bool:
        """Update player statistics atomically with enhanced error handling

        Args:
            db: Database connection
            kills: Number of kills to add
            deaths: Number of deaths to add
            suicides: Number of suicides to add

        Returns:
            True if updated successfully, False otherwise
        """
        try:
            # Validate player_id for this operation with additional safety
            if not self._validate_player_id(self.player_id):
                logger.error(f"Invalid player_id in update_stats: {self.player_id}")
                return False

            if not self._validate_server_id(self.server_id):
                logger.error(f"Invalid server_id in update_stats: {self.server_id}")
                return False

            # Ensure we have valid values to increment (more defensive)
            try:
                kills_inc = max(0, int(kills if kills is not None else 0))
                deaths_inc = max(0, int(deaths if deaths is not None else 0))
                suicides_inc = max(0, int(suicides if suicides is not None else 0))
            except (ValueError, TypeError):
                logger.error(
                    f"Invalid increment values: kills={kills}, deaths={deaths}, suicides={suicides}"
                )
                # Use safe defaults
                kills_inc = 0 if kills is None else max(0, 1)
                deaths_inc = 0 if deaths is None else max(0, 1)
                suicides_inc = 0 if suicides is None else max(0, 1)

            # If all values are 0, we don't need to update anything except timestamp
            if kills_inc == 0 and deaths_inc == 0 and suicides_inc == 0:
                # Just update the timestamp
                update_dict = {"updated_at": datetime.utcnow()}
                result = await db.players.find_one_and_update(
                    {"player_id": self.player_id, "server_id": self.server_id},
                    {"$set": update_dict},
                    return_document=ReturnDocument.AFTER,
                )

                if result is not None is not None:
                    self.updated_at = result.get("updated_at", self.updated_at)
                    return True

                logger.error(f"Failed to update timestamp for {self.player_id}")
                return False

            # Get current player to ensure it exists before updating stats
            current_player = await db.players.find_one(
                {"player_id": self.player_id, "server_id": self.server_id}
            )

            if current_player is None:
                logger.error(
                    f"Player not found for stats update: {self.player_id} (server: {self.server_id})"
                )
                return False

            # Prepare update operations
            update_dict = {"updated_at": datetime.utcnow()}
            inc_dict = {}

            # Add increments for non-zero values
            if kills_inc > 0:
                inc_dict["kills"] = kills_inc
                logger.debug(f"Incrementing kills for {self.player_id} by {kills_inc}")

            if deaths_inc > 0:
                inc_dict["deaths"] = deaths_inc
                logger.debug(f"Incrementing deaths for {self.player_id} by {deaths_inc}")

            if suicides_inc > 0:
                inc_dict["suicides"] = suicides_inc
                logger.debug(
                    f"Incrementing suicides for {self.player_id} by {suicides_inc}"
                )

            # Construct the update operation
            operations = {}

            if inc_dict is not None:
                operations["$inc"] = inc_dict

            if update_dict is not None:
                operations["$set"] = update_dict

            # Execute the atomic update
            result = await db.players.find_one_and_update(
                {"player_id": self.player_id, "server_id": self.server_id},
                operations,
                return_document=ReturnDocument.AFTER,
            )

            if result is not None:
                # Update local object with the new values
                self.kills = result.get("kills", self.kills)
                self.deaths = result.get("deaths", self.deaths)
                self.suicides = result.get("suicides", self.suicides)
                self.updated_at = result.get("updated_at", self.updated_at)
                logger.info(
                    f"Updated stats for {self.player_id}: K={self.kills}, D={self.deaths}, S={self.suicides}"
                )
                return True

            logger.error(
                f"Failed to update stats for {self.player_id} - database update returned no result"
            )
            return False
        except Exception as e:
            logger.error(f"Error updating player stats: {e}", exc_info=True)
            return False

    @classmethod
    def from_document(
        cls, document: Optional[Dict[str, Any]], db=None
    ) -> Optional["Player"]:
        """Create a Player instance from a document safely with enhanced validation

        Args:
            document: Database document to convert
            db: Optional database connection

        Returns:
            Player instance or None if document is invalid
        """
        if document is None:
            logger.error("Cannot create Player from None document")
            return None

        # Create a working copy to avoid modifying the original
        doc = document.copy()

        # Ensure critical fields exist with additional safety
        # First, validate player_id explicitly to catch issues early
        if "player_id" not in doc if doc is not None else not doc["player_id"]:
            logger.error(f"Missing player_id in document: {doc}")
            # Instead of returning None, attempt a last-chance recovery
            doc["player_id"] = (
                str(doc.get("_id", "")) or f"recovered_{uuid.uuid4()}"
            )
            logger.warning(f"Created recovery player_id: {doc['player_id']}")

        # Validate server_id explicitly
        if "server_id" not in doc or not doc["server_id"]:
            logger.error(f"Missing server_id in document: {doc}")
            # Use a placeholder server ID for recovery
            doc["server_id"] = "recovery_server"
            logger.warning(f"Using recovery server_id: {doc['server_id']}")

        # Validate name explicitly - critical for player identity
        if "name" not in doc or not doc["name"]:
            logger.error(f"Missing name in document: {doc}")
            # Try to use display_name as fallback if available
            if "display_name" in doc and doc["display_name"]:
                doc["name"] = doc["display_name"]
                logger.info(
                    f"Using display_name as fallback for missing name: {doc['display_name']}"
                )
            else:
                # Create a placeholder name for recovery
                doc["name"] = f"Unknown_{doc['player_id']}"
                logger.warning(f"Using generated name: {doc['name']}")

        try:
            # Ensure numeric fields are valid
            kills = doc.get("kills", 0)
            deaths = doc.get("deaths", 0)
            suicides = doc.get("suicides", 0)

            try:
                kills = max(0, int(kills))
                deaths = max(0, int(deaths))
                suicides = max(0, int(suicides))
            except (ValueError, TypeError):
                logger.warning(f"Invalid numeric stats in player document: {doc}")
                kills = 0
                deaths = 0
                suicides = 0

            # Prepare a safe version of known_aliases
            known_aliases = doc.get("known_aliases", [])
            if not isinstance(known_aliases, list):
                known_aliases = [str(known_aliases)] if known_aliases else []

            # Filter out any None values in known_aliases
            known_aliases = [alias for alias in known_aliases if alias]

            # Ensure known_aliases includes name
            if isinstance(doc, dict) and doc["name"] not in known_aliases:
                known_aliases.append(doc["name"])

            # Create Player instance with validated data
            player = cls(
                db=db,
                player_id=doc["player_id"],
                server_id=doc["server_id"],
                name=doc["name"],
                kills=kills,
                deaths=deaths,
                suicides=suicides,
                display_name=doc.get("display_name") or doc["name"],
                last_seen=doc.get("last_seen") or datetime.utcnow(),
                created_at=doc.get("created_at") or datetime.utcnow(),
                updated_at=doc.get("updated_at") or datetime.utcnow(),
                known_aliases=known_aliases,
                **{
                    k: v
                    for k, v in doc.items()
                    if k
                    not in [
                        "_id",
                        "player_id",
                        "server_id",
                        "name",
                        "kills",
                        "deaths",
                        "suicides",
                        "display_name",
                        "last_seen",
                        "created_at",
                        "updated_at",
                        "known_aliases",
                    ]
                }
            )

            # Set document ID if available
            if "_id" in doc:
                player._id = doc["_id"]

            return player
        except Exception as e:
            logger.error(f"Error creating Player from document: {e}", exc_info=True)

            # Last resort emergency recovery
            try:
                emergency_player = cls(
                    db=db,
                    player_id=doc["player_id"],
                    server_id=doc["server_id"],
                    name=doc["name"],
                    kills=0,
                    deaths=0,
                    suicides=0,
                )

                if "_id" in doc:
                    emergency_player._id = doc["_id"]

                logger.warning(f"Created emergency player instance: {emergency_player}")
                return emergency_player
            except Exception as recovery_error:
                logger.error(
                    f"Emergency player creation also failed: {recovery_error}"
                )
                return None

    @classmethod
    async def create_or_update(
        cls, db, player_id: str, server_id: str, name: str, **kwargs
    ) -> Optional["Player"]:
        """Create a new player if player is not None else update an existing one.

        Args:
            db: Database connection
            player_id: Player ID (required)
            server_id: Server ID (required)
            name: Player name
            **kwargs: Additional fields to update

        Returns:
            Player object if created/updated, None otherwise
        """
        # CRITICAL FIX: Robust parameter validation and error handling
        if db is None:
            logger.error("Database not available for player creation/update")
            return None

        # Ensure we have valid string values for critical fields
        try:
            # Defensive sanitization of inputs
            safe_player_id = (
                str(player_id).strip() if player_id is not None else None
            )
            safe_server_id = (
                str(server_id).strip() if server_id is not None else None
            )
            safe_name = str(name).strip() if name is not None else None

            # Emergency fallbacks if values are still empty after sanitization
            if safe_player_id is None:
                logger.error(f"Invalid player_id for create_or_update: {player_id}")
                import uuid

                safe_player_id = f"recovered_{uuid.uuid4()}"
                logger.warning(f"Using generated player_id: {safe_player_id}")

            if safe_server_id is None:
                logger.error(f"Invalid server_id for create_or_update: {server_id}")
                safe_server_id = "default_server"
                logger.warning(f"Using default server_id: {safe_server_id}")

            if safe_name is None:
                safe_name = (
                    f"Player_{safe_player_id[:8]}"
                    if len(safe_player_id) >= 8
                    else f"Player_{safe_player_id}"
                )
                logger.warning(f"Using generated name: {safe_name}")

        except Exception as e:
            logger.error(f"Critical error sanitizing player data: {e}")
            # Last resort emergency values
            import uuid

            safe_player_id = f"emergency_{uuid.uuid4()}"
            safe_server_id = "emergency_server"
            safe_name = f"Emergency_{safe_player_id[-8:]}"
            logger.warning(
                f"Using emergency values: id={safe_player_id}, server={safe_server_id}, name={safe_name}"
            )

        # Log at debug level to reduce log spam
        logger.debug(
            f"Creating/updating player: {safe_player_id} ({safe_name}) for server {safe_server_id}"
        )

        try:
            # Implementation with improved error handling and atomic operations
            now = datetime.utcnow()

            # Check if player is not None exists using a properly constructed query
            query = {"player_id": safe_player_id, "server_id": safe_server_id}
            existing = await db.players.find_one(query)

            if existing is not None:
                # Update existing player with improved atomicity
                logger.debug(f"Updating existing player: {safe_player_id}")

                # Build update data with safely converted values
                # Use explicitly typed Dict[str, Any] to allow mixed types (strings, datetime, etc.)
                update_data: Dict[str, Any] = {
                    "updated_at": now,
                    "last_seen": now
                }

                # Add name if available
                if safe_name is not None:
                    update_data["name"] = safe_name  # String type

                # Process kwargs with validation
                for key, value in kwargs.items():
                    # Skip None values to avoid overwriting with None
                    if value is not None:
                        # Handle date fields that might come as strings
                        if key in ["created_at", "updated_at", "last_seen"]:
                            if isinstance(value, str):
                                try:
                                    # Try to parse the string as a datetime
                                    update_data[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert string to datetime for {key}: {value}")
                                    # Skip this field rather than setting an invalid value
                                    continue
                            elif isinstance(value, datetime):
                                update_data[key] = value
                            else:
                                logger.warning(f"Invalid type for datetime field {key}: {type(value)}")
                                continue
                        else:
                            update_data[key] = value

                # Single atomic update with addToSet for aliases
                update_ops: Dict[str, Any] = {
                    "$set": update_data,
                }
                
                # Only add the $addToSet operation if we have a valid name to add
                if safe_name:
                    update_ops["$addToSet"] = {"known_aliases": safe_name}

                # Execute update with retry logic
                retry_count = 0
                max_retries = 3
                while retry_count < max_retries:
                    try:
                        result = await db.players.update_one(query, update_ops)
                        if result.modified_count > 0 or result.matched_count > 0:
                            break
                        retry_count += 1
                        await asyncio.sleep(0.1)  # Small delay between retries
                    except Exception as update_error:
                        logger.warning(
                            f"Retry {retry_count+1}/{max_retries} for player update failed: {update_error}"
                        )
                        retry_count += 1
                        await asyncio.sleep(0.2)  # Longer delay after error

                # Get the updated document
                updated_doc = await db.players.find_one(query)

                if updated_doc is not None:
                    # Use the static method to create player from document
                    return cls.from_document(updated_doc)
                else:
                    # Fallback to creating an instance directly
                    logger.warning(
                        f"Failed to retrieve updated player {safe_player_id}, creating instance directly"
                    )
                    existing.update(update_data)  # Merge updates with existing data
                    return cls.from_document(existing)
            else:
                # Create new player with improved atomicity
                logger.debug(f"Creating new player: {safe_player_id}")

                # Build comprehensive player data
                player_data = {
                    "player_id": safe_player_id,
                    "server_id": safe_server_id,
                    "name": safe_name,
                    "display_name": kwargs.get("display_name", safe_name),
                    "kills": kwargs.get("kills", 0),
                    "deaths": kwargs.get("deaths", 0),
                    "suicides": kwargs.get("suicides", 0),
                    "created_at": now,
                    "updated_at": now,
                    "last_seen": now,
                    "known_aliases": [safe_name] if safe_name else [],
                }

                # Add remaining kwargs with validation
                for key, value in kwargs.items():
                    if (
                        key not in player_data and value is not None
                    ):  # Skip None and already set values
                        player_data[key] = value

                # Insert with retry logic
                retry_count = 0
                max_retries = 3
                inserted_id = None

                while retry_count < max_retries:
                    try:
                        result = await db.players.insert_one(player_data)
                        if result is not None and result.inserted_id:
                            inserted_id = result.inserted_id
                            logger.debug(f"Successfully created player: {safe_player_id}")
                            break
                        retry_count += 1
                        await asyncio.sleep(0.1)
                    except Exception as insert_error:
                        # Check for duplicate key error (possible race condition)
                        if "duplicate key" in str(insert_error).lower():
                            logger.warning(
                                f"Race condition detected for player {safe_player_id}, attempting update instead"
                            )
                            # If duplicate, try an upsert as fallback
                            try:
                                update_result = await db.players.update_one(
                                    query,
                                    {"$setOnInsert": player_data},
                                    upsert=True,
                                )
                                if (
                                    update_result.upserted_id
                                    or update_result.modified_count > 0
                                ):
                                    logger.debug(
                                        f"Successfully upserted player: {safe_player_id}"
                                    )
                                    break
                            except Exception as upsert_error:
                                logger.warning(f"Upsert fallback also failed: {upsert_error}")

                        logger.warning(
                            f"Retry {retry_count+1}/{max_retries} for player creation failed: {insert_error}"
                        )
                        retry_count += 1
                        await asyncio.sleep(0.2)

                # If we successfully inserted, return a player instance
                if inserted_id is not None:
                    logger.info(f"Successfully created new player: {safe_player_id}")
                    return cls(**player_data)

                # Last retrieval attempt for race condition cases
                final_doc = await db.players.find_one(query)
                if final_doc is not None:
                    logger.debug(f"Retrieved player after race condition: {safe_player_id}")
                    return cls.from_document(final_doc)

                # Final fallback - create an instance directly even if DB operation failed
                logger.warning(
                    f"DB operations failed for player {safe_player_id}, returning direct instance"
                )
                return cls(**player_data)

        except Exception as e:
            logger.error(f"Unhandled error in create_or_update: {e}", exc_info=True)
            # Last resort fallback - return a minimal player instance
            try:
                return cls(
                    player_id=safe_player_id,
                    server_id=safe_server_id,
                    name=safe_name,
                )
            except Exception as final_error:
                logger.error(f"Critical failure creating fallback player: {final_error}")
                return None

    @property
    def kd_ratio(self) -> float:
        """Calculate K/D ratio safely

        Returns:
            K/D ratio (kills / deaths, with deaths=1 if deaths=0)
        """
        try:
            if self.deaths == 0:
                return float(self.kills)
            return float(self.kills) / float(self.deaths)
        except (TypeError, ZeroDivisionError):
            return 0.0

    def __str__(self) -> str:
        """String representation of player"""
        return f"Player(id={self.player_id}, name={self.name}, server={self.server_id})"

    def __repr__(self) -> str:
        """Detailed string representation"""
        return f"Player(id={self.player_id}, name={self.name}, server={self.server_id}, k={self.kills}, d={self.deaths})"
        
    async def get_detailed_stats(self, db=None) -> Dict[str, Any]:
        """Get detailed player statistics, including nemesis and prey relationships
        
        Args:
            db: Optional database connection (if not provided, will use self.db if available)
            
        Returns:
            Dict containing player statistics
        """
        # Start with basic stats from the model
        stats = {
            "player_id": self.player_id,
            "name": self.name,
            "server_id": self.server_id,
            "kills": self.kills,
            "deaths": self.deaths,
            "suicides": self.suicides,
            "headshots": getattr(self, "headshots", 0),
            "kdr": self.kd_ratio,
            "longest_shot": getattr(self, "longest_shot", 0),
            "highest_killstreak": getattr(self, "highest_killstreak", 0),
            "current_killstreak": getattr(self, "current_killstreak", 0),
            "last_seen": self.last_seen,
            "active": getattr(self, "active", False),
            "ranks": getattr(self, "ranks", {}),
            "average_lifetime": getattr(self, "average_lifetime", 0),
            "weapons": getattr(self, "weapons", {})
        }
        
        # Get the database connection
        database = db
        if database is None:
            database = getattr(self, "db", None)
            
        # Skip rivalry lookups if no database connection is available
        if database is None:
            logger.warning("No database connection available for getting rivalries")
            stats["nemesis"] = None
            stats["prey"] = None
            return stats
            
        # Get nemesis and prey relationships if available
        try:
            # Import here to avoid circular imports
            from models.rivalry import Rivalry
            
            # Get player's rivalries
            rivalries = await Rivalry.get_for_player(database, self.player_id, None, self.server_id)
            
            # Initialize nemesis and prey data
            nemesis_data = None
            prey_data = None
            
            # Find nemesis (player who kills this player the most)
            max_deaths_to = 0
            max_deaths_to_player = None
            
            # Find prey (player who this player kills the most)
            max_kills_of = 0
            max_kills_of_player = None
            
            for rivalry in rivalries:
                # Get stats from this rivalry
                rivalry_stats = await rivalry.get_stats_for_player(self.player_id)
                kills = rivalry_stats.get("kills", 0)
                deaths = rivalry_stats.get("deaths", 0)
                opponent_id = rivalry_stats.get("opponent_id")
                opponent_name = rivalry_stats.get("opponent_name", "Unknown")
                
                # Check if this is a potential nemesis
                if deaths > max_deaths_to:
                    max_deaths_to = deaths
                    max_deaths_to_player = {
                        "player_id": opponent_id,
                        "name": opponent_name,
                        "kills": deaths  # From opponent's perspective
                    }
                
                # Check if this is a potential prey
                if kills > max_kills_of:
                    max_kills_of = kills
                    max_kills_of_player = {
                        "player_id": opponent_id,
                        "name": opponent_name,
                        "deaths": kills  # From opponent's perspective
                    }
            
            # Only set nemesis if they've killed the player at least 3 times
            if max_deaths_to >= 3:
                stats["nemesis"] = max_deaths_to_player
            
            # Only set prey if the player has killed them at least 3 times
            if max_kills_of >= 3:
                stats["prey"] = max_kills_of_player
                
        except Exception as e:
            logger.error(f"Error getting nemesis/prey relationships for player {self.player_id}: {e}")
            # Continue without the nemesis/prey data
        
        return stats

    @classmethod
    async def get_by_name(
        cls, db, name: str, server_id: Optional[str] = None
    ) -> Optional["Player"]:
        """Get a player by name

        Args:
            db: Database connection
            name: Player name
            server_id: Optional server ID to filter by

        Returns:
            Player object or None if found is None
        """
        query = {"name": name}
        if server_id is not None:
            if not cls._validate_server_id(server_id):
                logger.error(f"Invalid server_id passed to get_by_name: {server_id}")
                return None
            query["server_id"] = server_id

        document = await db.players.find_one(query)
        return cls.from_document(document) if document else None

    @classmethod
    async def get_players_for_server(cls, db, server_id: str) -> List["Player"]:
        """Get all players for a server

        Args:
            db: Database connection
            server_id: Server ID

        Returns:
            List of Player objects
        """
        players = []
        cursor = db.players.find({"server_id": server_id})
        async for document in cursor:
            player = cls.from_document(document)
            if player is not None:
                players.append(player)
        return players
        
    @classmethod
    async def get_leaderboard(cls, db, server_id: str, stat_type: str = "kills", limit: int = 10) -> List[Dict[str, Any]]:
        """Get leaderboard for a specific statistic
        
        Args:
            db: Database connection
            server_id: Server ID
            stat_type: Type of statistic to rank by (kills, deaths, kdr, etc.)
            limit: Maximum number of players to return
            
        Returns:
            List of player data dictionaries
        """
        # Define valid stat types and their sort direction
        valid_stats = {
            "kills": -1,
            "deaths": -1,
            "suicides": -1,
            "kdr": -1,  # We'll handle this specially
            "headshots": -1,
            "longest_shot": -1,
            "highest_killstreak": -1
        }
        
        if stat_type not in valid_stats:
            stat_type = "kills"  # Default to kills
            
        sort_direction = valid_stats[stat_type]
        
        # KDR needs special handling
        if stat_type == "kdr":
            # For KDR, we need to aggregate and calculate it
            pipeline = [
                {"$match": {"server_id": server_id}},
                {"$addFields": {
                    "kdr": {
                        "$cond": [
                            {"$eq": ["$deaths", 0]},
                            "$kills",
                            {"$divide": ["$kills", "$deaths"]}
                        ]
                    }
                }},
                {"$sort": {"kdr": sort_direction, "kills": -1}},
                {"$limit": limit}
            ]
            
            cursor = db[cls.collection_name].aggregate(pipeline)
        else:
            # For other stats, we can sort directly
            query = {"server_id": server_id}
            cursor = db[cls.collection_name].find(query).sort(stat_type, sort_direction).limit(limit)
        
        # Collect results
        results = []
        async for doc in cursor:
            player = cls.from_document(doc)
            if player is not None:
                # Build a simplified player data dict
                player_data = {
                    "player_id": player.player_id,
                    "name": player.name,
                    "display_name": getattr(player, "display_name", player.name),
                    "kills": player.kills,
                    "deaths": player.deaths,
                    "suicides": player.suicides,
                    "kdr": player.kd_ratio,
                    "headshots": getattr(player, "headshots", 0),
                    "longest_shot": getattr(player, "longest_shot", 0),
                    "highest_killstreak": getattr(player, "highest_killstreak", 0),
                    "last_seen": player.last_seen
                }
                results.append(player_data)
                
        return results

    def _sanitize_player_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitizes player data, ensuring correct types and handling defaults.
        """
        if not isinstance(data, dict):
            logger.warning(f"Invalid data format for sanitization: {data}")
            return {}

        # Handle the 'active' status safely.
        active = data.get('active', True)  # Default to True if not provided.
        if not isinstance(active, bool):
            try:
                active = bool(active)  # Attempt conversion.
            except ValueError:
                logger.error(f"Invalid 'active' value: {active}. Defaulting to True.")
                active = True

        # Handle the 'hidden' status safely, defaulting to False if not provided.
        hidden = data.get('hidden', False)
        if not isinstance(hidden, bool):
            try:
                hidden = bool(hidden)
            except ValueError:
                logger.error(f"Invalid 'hidden' value: {hidden}. Defaulting to False.")
                hidden = False

        # Ensure 'times_map_loaded' is an integer.
        times_map_loaded = data.get('times_map_loaded', 0)
        if not isinstance(times_map_loaded, int):
            try:
                times_map_loaded = int(times_map_loaded)
            except ValueError:
                logger.error(f"Invalid 'times_map_loaded' value: {times_map_loaded}. Defaulting to 0.")
                times_map_loaded = 0

        # Ensure 'found_map_files' is a boolean, default to False
        found_map_files = data.get('found_map_files', False)

        if not isinstance(found_map_files, bool):
            try:
                found_map_files = bool(found_map_files)
            except ValueError:
                logger.error(f"Invalid 'found_map_files' value: {found_map_files}. Defaulting to False.")
                found_map_files = False

        sanitized_data = {
            'active': active,
            'hidden': hidden,
            'times_map_loaded': times_map_loaded,
            'found_map_files': found_map_files
        }

        return sanitized_data

    async def load_map_and_update(self, db, map_name: str) -> bool:
        """
        Simulates loading a map and updates player stats.
        """
        try:
            if not self._validate_player_id(self.player_id):
                logger.error(f"Invalid player_id in load_map_and_update: {self.player_id}")
                return False

            if not self._validate_server_id(self.server_id):
                logger.error(f"Invalid server_id in load_map_and_update: {self.server_id}")
                return False

            # Simulate the loading process.
            await asyncio.sleep(0.1)  # Simulate I/O delay.

            # Prepare the update operation.
            update_ops = {
                '$inc': {'times_map_loaded': 1},
                '$set': {'updated_at': datetime.utcnow()}
            }

            # Perform the update.
            result = await db.players.find_one_and_update(
                {'player_id': self.player_id, 'server_id': self.server_id},
                update_ops,
                return_document=ReturnDocument.AFTER
            )

            if result is not None:
                # Update the local object.
                self.times_map_loaded = result.get('times_map_loaded', self.times_map_loaded)
                self.updated_at = result.get('updated_at', self.updated_at)

                logger.info(f"Player {self.player_id} loaded map {map_name} successfully.")
                return True
            else:
                logger.error(f"Failed to update player {self.player_id} after loading map {map_name}.")
                return False
        except Exception as e:
            logger.error(f"Error in load_map_and_update: {e}", exc_info=True)
            return False