# Tower of Temptation PvP Statistics Bot - Startup Guide

This guide explains how to use the Replit run button to launch the bot with the fixes applied.

## Quick Start

1. Click the **Run** button in Replit to start the bot in production mode
   - This will automatically check environment variables, apply all fixes, and launch the bot

## Alternative Launch Methods

### Using launch.sh Script

The `launch.sh` script provides different modes for running the bot:

```bash
# Start in production mode (default) - applies fixes and runs the bot
./launch.sh 

# Only apply fixes without starting the bot
./launch.sh fix-only

# Only check environment variables
./launch.sh check-env
```

### Using Individual Scripts

You can also use the individual scripts directly:

```bash
# Apply all MongoDB and string formatting fixes
python apply_all_fixes.py

# Check environment variables
python check_environment.py

# Run the bot after fixes have been applied
python run.py
```

## Required Environment Variables

The bot requires these environment variables to be set:

- `MONGODB_URI`: MongoDB connection URI
- `DISCORD_TOKEN`: Discord Bot Token

These can be set in Replit's Secrets tab.

## Troubleshooting

If the bot fails to start:

1. Check environment variables using `./launch.sh check-env`
2. Verify MongoDB connection is working
3. Check that Discord token is valid
4. Look for error logs in the console output