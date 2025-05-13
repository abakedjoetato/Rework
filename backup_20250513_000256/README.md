# Tower of Temptation PvP Statistics Bot

This Discord bot tracks PvP statistics for the Tower of Temptation game with MongoDB integration.

## Quick Start

1. Make sure these environment variables are set in Replit Secrets:
   - `MONGODB_URI`: Your MongoDB connection string
   - `DISCORD_TOKEN`: Your Discord bot token

2. Click the **Run** button in Replit to start the bot automatically

## What Happens When You Click Run

The bot startup process follows these steps:

1. **Environment Check**: Verifies that all required environment variables are set
2. **Bot Initialization**: Creates the bot instance with production settings
3. **Database Connection**: Establishes a connection to MongoDB with robust error handling
4. **Load Extensions**: Loads all bot extensions (cogs) 
5. **Start Bot**: Connects to Discord and starts responding to events

All MongoDB truthiness issues and string formatting errors have been directly fixed in the codebase rather than using runtime patches or monkey patching.

## Troubleshooting

If the bot fails to start:

1. Check the console output for error messages
2. Verify that your MongoDB URI is correct and accessible
3. Make sure your Discord token is valid
4. Look for specific error messages in the logs

## Key Scripts

- `main.py`: The main entry point for the Replit run button
- `apply_all_fixes.py`: Automatically applies fixes for MongoDB truthiness issues and string formatting
- `check_environment.py`: Verifies that all required environment variables are present
- `bot.py`: Contains the core Bot class implementation