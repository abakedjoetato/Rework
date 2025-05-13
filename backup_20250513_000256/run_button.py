#!/usr/bin/env python3
"""
Run Button Entry Point for Replit

This script serves as the main entry point for the Replit run button.
It starts the web interface for launching the bot with different options.
"""

import os
import sys
import subprocess

def main():
    """Main entry point."""
    print("Tower of Temptation PvP Statistics Bot Launcher")
    print("==============================================")
    
    # Determine launch mode from environment variable
    launch_mode = os.environ.get("LAUNCH_MODE", "web")
    
    if launch_mode == "web":
        print("Starting web interface on port 5000...")
        try:
            from flask import Flask
            subprocess.run(["python", "web_launcher.py"], check=True)
        except ImportError:
            print("Flask not found. Installing...")
            subprocess.run(["pip", "install", "flask"], check=True)
            print("Flask installed. Starting web interface...")
            subprocess.run(["python", "web_launcher.py"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error starting web interface: {e}")
            return 1
    else:
        print("Starting bot directly...")
        try:
            subprocess.run(["./launch.sh", "production"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error starting bot: {e}")
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())