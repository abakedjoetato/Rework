"""
Command Handlers and Metrics Tracking

This module provides utilities for command metrics tracking, performance monitoring,
and error handling for bot commands.
"""
from typing import Dict, List, Any, Optional, Union
from collections import defaultdict
import time
import traceback
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Global command metrics tracking
COMMAND_METRICS = defaultdict(lambda: {
    "invocations": 0,
    "errors": 0,
    "success_rate": 1.0, 
    "avg_runtime": 0.0,
    "last_success": None,
    "last_error": None,
    "error_messages": []
})

# Command history tracking
COMMAND_HISTORY = defaultdict(list)  # guild_id -> list of recent commands

# Error tracking
ERROR_COUNT_THRESHOLD = 5  # Min number of invocations before considering error rate
HIGH_ERROR_THRESHOLD = 0.3  # Error rate that's considered problematic (30%)

# Command cooldowns
COMMAND_COOLDOWNS = {}

def track_command_invocation(command_name: str, guild_id: Optional[str] = None, user_id: Optional[str] = None):
    """Track a command invocation
    
    Args:
        command_name: Name of the command
        guild_id: Guild ID (optional)
        user_id: User ID (optional)
    """
    if command_name not in COMMAND_METRICS:
        COMMAND_METRICS[command_name] = {
            "invocations": 0,
            "errors": 0,
            "success_rate": 1.0,
            "avg_runtime": 0.0,
            "last_success": None,
            "last_error": None,
            "error_messages": []
        }
    
    COMMAND_METRICS[command_name]["invocations"] += 1
    
    # Track in history if guild_id provided
    if guild_id is not None:
        COMMAND_HISTORY[guild_id].append({
            "command": command_name,
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat(),
        })
        
        # Keep history manageable
        if len(COMMAND_HISTORY[guild_id]) > 100:
            COMMAND_HISTORY[guild_id] = COMMAND_HISTORY[guild_id][-100:]

def track_command_error(command_name: str, error_msg: str):
    """Track a command error
    
    Args:
        command_name: Name of the command
        error_msg: Error message
    """
    if command_name not in COMMAND_METRICS:
        track_command_invocation(command_name)
        
    COMMAND_METRICS[command_name]["errors"] += 1
    COMMAND_METRICS[command_name]["last_error"] = datetime.utcnow().isoformat()
    COMMAND_METRICS[command_name]["error_messages"].append(error_msg)
    
    # Keep error list manageable
    if len(COMMAND_METRICS[command_name]["error_messages"]) > 10:
        COMMAND_METRICS[command_name]["error_messages"] = COMMAND_METRICS[command_name]["error_messages"][-10:]
    
    # Recalculate success rate
    invocations = COMMAND_METRICS[command_name]["invocations"]
    errors = COMMAND_METRICS[command_name]["errors"]
    
    if invocations > 0:
        COMMAND_METRICS[command_name]["success_rate"] = (invocations - errors) / invocations

def track_command_success(command_name: str, runtime: float):
    """Track a command success
    
    Args:
        command_name: Name of the command
        runtime: Execution time in seconds
    """
    if command_name not in COMMAND_METRICS:
        track_command_invocation(command_name)
        
    COMMAND_METRICS[command_name]["last_success"] = datetime.utcnow().isoformat()
    
    # Update average runtime using exponential moving average
    # Give 20% weight to new value, 80% to historical average
    prev_avg = COMMAND_METRICS[command_name]["avg_runtime"]
    COMMAND_METRICS[command_name]["avg_runtime"] = (0.8 * prev_avg) + (0.2 * runtime)
    
    # Recalculate success rate
    invocations = COMMAND_METRICS[command_name]["invocations"]
    errors = COMMAND_METRICS[command_name]["errors"]
    
    if invocations > 0:
        COMMAND_METRICS[command_name]["success_rate"] = (invocations - errors) / invocations