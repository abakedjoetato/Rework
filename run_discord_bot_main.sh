#!/bin/bash

# Discord Bot Workflow Script
# This script runs the Discord bot using main.py, which is the proper entry point

echo "Starting Discord Bot Workflow..."
echo "Timestamp: $(date)"
echo "-----------------------------------"

# Check Python version
python --version

# Make the main script executable if needed
chmod +x main.py

# Run the main bot script
python main.py

# This point should only be reached if the bot exits
echo "-----------------------------------"
echo "Discord Bot has exited."
echo "Timestamp: $(date)"