"""
Player link model for associating players across servers
"""
import logging
from typing import Dict, Any, List, Optional, Union, Set, cast
from datetime import datetime

from models.base_model import BaseModel

logger = logging.getLogger("player_link_model")

class PlayerLink(BaseModel):
    """Player link model with improved type handling and validation"""

    collection_name = "player_links"

    @classmethod
    async def create(cls, db, data: Dict[str, Any]) -> 'PlayerLink':
        """
        Create a new player link with proper validation

        Args:
            db: Database connection
            data: Player link data

        Returns:
            New PlayerLink instance

        Raises:
            ValueError: If required fields are missing
        """
        # Validate required fields
        required_fields = ["player_id", "guild_id", "link_type"]
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field '{field}' in player link data")

        # Ensure guild_id is stored as a string
        if "guild_id" in data:
            data["guild_id"] = str(data["guild_id"])

        # Initialize default values
        now = datetime.utcnow()

        defaults = {
            "created_at": now,
            "updated_at": now,
            "status": "active",
            "metadata": {}
        }

        # Merge defaults with provided data
        for key, value in defaults.items():
            if key not in data:
                data[key] = value

        # Create the player link in the database
        try:
            result = await db[cls.collection_name].insert_one(data)
            data["_id"] = result.inserted_id
            logger.info(f"Created player link for player {data['player_id']} (guild={data['guild_id']})")
            return cls(db, data)
        except Exception as e:
            logger.error(f"Failed to create player link: {e}f")
            raise

    @classmethod
    async def find_by_player(cls, db, player_id: str, guild_id: Union[str, int], link_type: Optional[str] = None) -> List['PlayerLink']:
        """
        Find all links for a player in a guild

        Args:
            db: Database connection
            player_id: Player ID
            guild_id: Discord guild ID
            link_type: Optional link type to filter by

        Returns:
            List of PlayerLink instances
        """
        # Convert guild_id to string for consistency
        guild_id_str = str(guild_id)

        # Build query
        query = {
            "player_id": player_id,
            "guild_id": guild_id_str
        }

        # Add link type if provided
        if link_type is not None:
            query["link_type"] = link_type

        # Find all matching links
        cursor = db[cls.collection_name].find(query)

        links = []
        async for doc in cursor:
            links.append(cls(db, doc))

        return links

    @classmethod
    async def find_by_linked_id(cls, db, linked_id: str, guild_id: Union[str, int], link_type: str) -> List['PlayerLink']:
        """
        Find all links with a specific linked ID in a guild

        Args:
            db: Database connection
            linked_id: Linked ID (e.g., Discord user ID, Steam ID)
            guild_id: Discord guild ID
            link_type: Link type

        Returns:
            List of PlayerLink instances
        """
        # Convert guild_id to string for consistency
        guild_id_str = str(guild_id)

        # Find all matching links
        cursor = db[cls.collection_name].find({
            "linked_id": linked_id,
            "guild_id": guild_id_str,
            "link_type": link_type
        })

        links = []
        async for doc in cursor:
            links.append(cls(db, doc))

        return links

    @classmethod
    async def find_or_create(cls, db, query: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> tuple['PlayerLink', bool]:
        """
        Find a player link or create if not found

        Args:
            db: Database connection
            query: Query to find the link
            defaults: Default values to use if creating a new link

        Returns:
            Tuple of (PlayerLink, created) where created is True if a new link was created
        """
        # Ensure guild_id is stored as a string
        if "guild_id" in query:
            query["guild_id"] = str(query["guild_id"])

        # Try to find the link
        link_data = await db[cls.collection_name].find_one(query)

        if link_data is not None:
            # Link found, return it
            return cls(db, link_data), False
        else:
            # Link not found, create a new one
            data = query.copy()
            if defaults is not None:
                data.update(defaults)

            # Create the link
            link = await cls.create(db, data)
            return link, True

    async def update(self, data: Dict[str, Any]) -> 'PlayerLink':
        """
        Update player link

        Args:
            data: Data to update

        Returns:
            Updated PlayerLink instance
        """
        # Don't allow updating critical fields
        for field in ["_id", "guild_id", "player_id", "link_type"]:
            if field in data:
                del data[field]

        # Set updated_at timestamp
        data["updated_at"] = datetime.utcnow()

        # Update in database
        try:
            await self.db[self.collection_name].update_one(
                {"_id": self.data["_id"]},
                {"$set": data}
            )

            # Update local data
            for key, value in data.items():
                self.data[key] = value

            return self
        except Exception as e:
            logger.error(f"Failed to update player link: {e}f")
            raise

    async def deactivate(self) -> 'PlayerLink':
        """
        Deactivate this player link

        Returns:
            Updated PlayerLink instance
        """
        return await self.update({
            "status": "inactive",
            "deactivated_at": datetime.utcnow()
        })