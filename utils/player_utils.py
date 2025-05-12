
"""
Player utility functions for consistent instantiation and operations
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from models.player import Player

logger = logging.getLogger("player_utils")

async def create_player(db, player_id: str, server_id: str, name: str, **kwargs) -> Optional[Player]:
    """
    Create a new player using the correct instantiation pattern
    
    Args:
        db: Database connection
        player_id: Player ID
        server_id: Server ID
        name: Player name
        **kwargs: Additional player attributes
        
    Returns:
        Player instance or None if creation failed
    """
    try:
        # Use the Player.create_or_update method which follows the first instantiation pattern
        player = await Player.create_or_update(
            db=db,
            player_id=player_id,
            server_id=server_id,
            name=name,
            **kwargs
        )
        
        if player is not None:
            logger.info(f"Created/updated player: {player.name} (id={player.player_id}, server={player.server_id})")
        else:
            logger.error(f"Failed to create player: {name} (id={player_id}, server={server_id})")
            
        return player
    except Exception as e:
        logger.error(f"Error creating player: {e}", exc_info=True)
        return None

async def get_player(db, player_id: str, server_id: Optional[str] = None) -> Optional[Player]:
    """
    Get a player by ID using the correct instantiation pattern
    
    Args:
        db: Database connection
        player_id: Player ID
        server_id: Optional server ID
        
    Returns:
        Player instance or None if not found
    """
    try:
        # Use the Player.get_by_player_id method
        player = await Player.get_by_player_id(db, player_id, server_id)
        return player
    except Exception as e:
        logger.error(f"Error getting player: {e}", exc_info=True)
        return None

async def get_top_players(db, server_id: str, sort_by: str = "kills", limit: int = 10) -> List[Player]:
    """
    Get top players using the correct instantiation pattern
    
    Args:
        db: Database connection
        server_id: Server ID
        sort_by: Field to sort by
        limit: Number of players to return
        
    Returns:
        List of Player instances
    """
    try:
        # Use the Player.get_top_players method
        players = await Player.get_top_players(db, server_id, sort_by, limit)
        return players
    except Exception as e:
        logger.error(f"Error getting top players: {e}", exc_info=True)
        return []

async def update_player_stats(db, player: Player, kills: int = 0, deaths: int = 0, suicides: int = 0) -> bool:
    """
    Update player statistics using the correct method
    
    Args:
        db: Database connection
        player: Player instance
        kills: Number of kills to add
        deaths: Number of deaths to add
        suicides: Number of suicides to add
        
    Returns:
        True if update succeeded, False otherwise
    """
    try:
        # Use the Player.update_stats method
        success = await player.update_stats(db, kills, deaths, suicides)
        return success
    except Exception as e:
        logger.error(f"Error updating player stats: {e}", exc_info=True)
        return False
