#!/usr/bin/env python3
"""
Replit Run Button Launcher for Discord Bot
This script starts the Discord bot using the proper configuration
and entry point (main.py) when the Replit Run button is pressed.
"""
import os
import sys
import subprocess
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('replit_run.log')
    ]
)
logger = logging.getLogger('replit_runner')

def main():
    """Main entry point for the Replit Run Button"""
    logger.info("Starting Discord bot from Replit Run button")
    
    # Check if .replit file exists and contains proper run configuration
    try:
        with open('.replit', 'r') as f:
            replit_content = f.read()
            if 'run = "python main.py"' not in replit_content:
                logger.warning(".replit file doesn't contain proper run configuration")
                # We created .replit.new as a template but can't modify .replit directly
                logger.info("Using alternative launch method")
    except Exception as e:
        logger.warning(f"Could not check .replit file: {e}")
    
    # Print a banner
    print("=" * 60)
    print("  Emeralds Killfeed PvP Statistics Discord Bot")
    print("  Starting from Replit Run Button")
    print("=" * 60)
    
    # Launch the bot using the main.py script
    try:
        logger.info("Executing main.py")
        # Execute main.py directly
        sys.exit(subprocess.call([sys.executable, "main.py"]))
    except Exception as e:
        logger.error(f"Error starting bot: {e}")
        print(f"Error starting bot: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()