"""
# module: restart_with_cache_clear
Restart Bot with Python Cache Clearing

This script ensures changes to Python modules take effect by:
1. Clearing all Python __pycache__ directories
2. Killing any running bot processes
3. Restarting the bot with a clean environment
"""
import os
import shutil
import subprocess
import sys
import time

def clear_python_cache():
    """Clear all Python __pycache__ directories to ensure module changes take effect"""
    print("Clearing Python cache...")
    
    # Count of cache directories removed
    count = 0
    
    # Walk the directory tree looking for __pycache__ directories
    for root, dirs, files in os.walk("."):
        for dir_name in dirs:
            if dir_name == "__pycache__":
                cache_dir = os.path.join(root, dir_name)
                print(f"Removing cache directory: {cache_dir}")
                try:
                    shutil.rmtree(cache_dir)
                    count += 1
                except Exception as e:
                    print(f"Error removing {cache_dir}: {e}")
                    
    print(f"Removed {count} cache directories")
    return count > 0

def kill_bot_processes():
    """Kill any running bot processes"""
    print("Killing bot processes...")
    
    # Try to kill using pkill
    try:
        # Kill various bot-related processes
        subprocess.run(["pkill", "-", "python.*bot.py"], check=False)
        subprocess.run(["pkill", "-", "python.*run_discord_bot"], check=False)
        subprocess.run(["pkill", "-", "python.*bot_wrapper.py"], check=False)
        
        # Wait for processes to terminate
        time.sleep(2)
        
        # Check if any bot processes are still running
        result = subprocess.run(
            ["pgrep", "-", "python.*bot"],
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.stdout.strip():
            print(f"Some bot processes are still running: {result.stdout.strip()}")
            print("Attempting to force kill...")
            subprocess.run(["pkill", "-9", "-", "python.*bot"], check=False)
            time.sleep(1)
        else:
            print("All bot processes successfully terminated")
            
        return True
    except Exception as e:
        print(f"Error killing bot processes: {e}")
        return False

def restart_bot():
    """Restart the Discord bot"""
    print("Starting the bot...")
    
    try:
        # Start the bot using the appropriate script
        if os.path.exists("run_discord_bot.sh"):
            subprocess.Popen(
                ["bash", "run_discord_bot.sh"],
                stdout=open("bot_startup.log", "w"),
                stderr=subprocess.STDOUT
            )
            print("Started bot using run_discord_bot.sh")
        elif os.path.exists("bot_wrapper.py"):
            subprocess.Popen(
                ["python", "bot_wrapper.py"],
                stdout=open("bot_startup.log", "w"),
                stderr=subprocess.STDOUT
            )
            print("Started bot using bot_wrapper.py")
        else:
            subprocess.Popen(
                ["python", "bot.py"],
                stdout=open("bot_startup.log", "w"),
                stderr=subprocess.STDOUT
            )
            print("Started bot directly using bot.py")
            
        # Sleep briefly to allow startup to begin
        time.sleep(5)
        
        # Check if bot is running
        result = subprocess.run(
            ["pgrep", "-", "python.*bot"],
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.stdout.strip():
            print(f"Bot started successfully, pid: {result.stdout.strip()}")
            return True
        else:
            print("Bot failed to start")
            return False
    except Exception as e:
        print(f"Error starting bot: {e}")
        return False

def main():
    """Main function"""
    print("="*60)
    print("RESTARTING BOT WITH CACHE CLEARING")
    print("="*60)
    
    # Clear Python cache
    if not clear_python_cache():
        print("Warning: No cache directories found to clear")
    
    # Kill bot processes
    if not kill_bot_processes():
        print("Warning: Could not properly kill bot processes")
    
    # Restart bot
    if not restart_bot():
        print("Error: Failed to restart bot")
        return False
        
    print("\nBot restart completed successfully!")
    print("Check bot_startup.log for startup messages")
    print("="*60)
    return True

if __name__ == "__main__":
    main()