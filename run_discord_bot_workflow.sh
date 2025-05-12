#!/bin/bash
# Workflow script for running the Discord bot
# This file is used by Replit's workflow system

# Set up Python environment
export PYTHONPATH="."
export PYTHONUNBUFFERED="1"

# Clear Python cache to ensure latest code is used
echo "Clearing Python cache..."
find . -type d -name "__pycache__" -exec rm -rf {} +

# Log start time
echo "Starting Discord bot at $(date)" >> workflow_bot_startup.log

# Print banner
echo "====================================================="
echo "  Tower of Temptation PvP Statistics Discord Bot"
echo "  Starting from Replit Workflow"
echo "====================================================="

# Execute the bot with proper error handling
python bot.py

# Check exit code
if [ $? -ne 0 ]; then
    echo "Bot exited with error code $?" >> workflow_bot_error.log
    echo "Error occurred at $(date)" >> workflow_bot_error.log
    echo "See logs for details" >> workflow_bot_error.log
fi

echo "Bot process ended at $(date)" >> workflow_bot_startup.log