#!/bin/bash

echo "Tower of Temptation PvP Statistics Bot Startup"
echo "=============================================="

# Check environment
echo "Checking environment variables..."
python check_environment.py
if [ $? -ne 0 ]; then
    echo "ERROR: Environment check failed"
    exit 1
fi

# Apply fixes
echo "Applying MongoDB and string formatting fixes..."
python apply_all_fixes.py

# Start the bot
echo "Starting the Discord bot..."
exec python run.py