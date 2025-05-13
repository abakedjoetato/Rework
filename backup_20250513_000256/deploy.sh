#!/bin/bash

# Tower of Temptation PvP Statistics Bot Deployment Script
# This script applies all fixes and starts the bot

echo "Tower of Temptation PvP Statistics Bot Deployment"
echo "================================================="

# Check for Python 3.9+
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Using Python version: $python_version"

# Make sure we have required packages
echo "Checking required packages..."
python3 -m pip install -r requirements.txt

# Apply all fixes
echo "Applying MongoDB truthiness and string fixes..."
python3 apply_all_fixes.py

# Check for MongoDB URI and Discord token
if [ -z "$MONGODB_URI" ]; then
    echo "ERROR: MONGODB_URI environment variable is not set"
    echo "Please set this variable in your environment or .env file"
    exit 1
fi

if [ -z "$DISCORD_TOKEN" ]; then
    echo "ERROR: DISCORD_TOKEN environment variable is not set"
    echo "Please set this variable in your environment or .env file"
    exit 1
fi

# Start the bot
echo "Starting the bot..."
python3 run.py