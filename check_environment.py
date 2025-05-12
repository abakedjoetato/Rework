#!/usr/bin/env python3
"""
Environment Variable Check Script

This script checks that all required environment variables are set
for the Tower of Temptation PvP Statistics Bot to function properly.
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Define required environment variables
REQUIRED_VARS = {
    "MONGODB_URI": "MongoDB connection URI for database storage",
    "DISCORD_TOKEN": "Discord bot token for authentication with Discord API"
}

# No optional variables - critical services must work or the bot fails
OPTIONAL_VARS = {}

def check_environment():
    """
    Check environment variables and print status report.
    
    Returns:
        int: 0 if all required variables are set, 1 otherwise
    """
    print("Environment Variable Check")
    print("==========================")
    
    # Check required variables
    missing_required = []
    for var, description in REQUIRED_VARS.items():
        value = os.environ.get(var)
        if not value:
            print(f"ERROR: {var} is not set in the environment")
            missing_required.append(var)
        else:
            masked_value = value[:5] + "..." + value[-5:] if len(value) > 12 else "***"
            print(f"✅ {var}: {masked_value}")
    
    # Check optional variables
    print("\nOptional Variables:")
    for var, description in OPTIONAL_VARS.items():
        value = os.environ.get(var)
        if not value:
            print(f"⚠️ Not set: {var} - {description}")
        else:
            masked_value = value[:5] + "..." + value[-5:] if len(value) > 12 else "***"
            print(f"✅ {var}: {masked_value}")
    
    # Provide summary and instructions
    if missing_required:
        print("\n❌ ERROR: Missing required environment variables.")
        print("\nPlease set the following variables in your environment or .env file:")
        for var in missing_required:
            print(f"  - {var}: {REQUIRED_VARS[var]}")
        return 1
    else:
        print("\n✅ All required environment variables are set.")
        return 0

if __name__ == "__main__":
    sys.exit(check_environment())