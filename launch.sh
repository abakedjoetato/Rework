#!/bin/bash
# This script serves as the entry point for the Replit run button

echo "Tower of Temptation Discord Bot Launcher"
echo "========================================"

# Check if a specific mode was provided
MODE=${1:-"production"}

case "$MODE" in
  "production")
    echo "Starting in PRODUCTION mode..."
    ./start.sh
    ;;
  "fix-only")
    echo "Running fixes only..."
    python apply_all_fixes.py
    ;;
  "check-env")
    echo "Checking environment variables..."
    python check_environment.py
    ;;
  *)
    echo "Unknown mode: $MODE"
    echo "Usage: ./launch.sh [production|fix-only|check-env]"
    exit 1
    ;;
esac