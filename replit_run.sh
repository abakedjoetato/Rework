#!/bin/bash
# This script is the main entry point for the Replit Run button

# Determine if we should launch the web interface or the bot directly
# Default to web interface
LAUNCH_MODE=${LAUNCH_MODE:-"web"}

echo "Tower of Temptation PvP Statistics Bot"
echo "====================================="

if [ "$LAUNCH_MODE" = "web" ]; then
    echo "Starting web launcher interface on port 5000..."
    python web_launcher.py
else
    echo "Starting bot directly..."
    ./launch.sh production
fi