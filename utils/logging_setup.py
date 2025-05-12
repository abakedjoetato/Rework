#!/usr/bin/env python3
"""
Logging setup utility for the Discord bot.

This module configures the logging format and levels for different loggers.
It helps reduce console noise by adjusting log levels appropriately.
"""
import logging

def setup_logging():
    """Set up logging configuration for the bot
    
    This configures:
    - Default INFO level for main application
    - Reduced levels for noisy external libraries
    - Custom format with timestamps and logger name
    """
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Configure console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    
    # Set format with timestamp, level, and logger name
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s', 
                                 datefmt='%Y-%m-%d %H:%M:%S,%f')
    console.setFormatter(formatter)
    
    # Add handler to root logger
    root_logger.addHandler(console)
    
    # Reduce noise from external libraries
    logging.getLogger('discord').setLevel(logging.WARNING)
    logging.getLogger('websockets').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    
    # NEW: Reduce spammy connection logs from AsyncSSH
    logging.getLogger('asyncssh').setLevel(logging.WARNING)
    # Only show warnings and errors from the SFTP client
    logging.getLogger('asyncssh.sftp').setLevel(logging.WARNING)
