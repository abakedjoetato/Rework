"""
Clean reinstallation script for py-cord 2.6.1
"""
import subprocess
import sys
import os

def run_cmd(cmd):
    print(f"Running command: {cmd}")
    try:
        subprocess.run(cmd, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        return False

def main():
    # Ensure pip is up to date
    run_cmd(f"{sys.executable} -m pip install --upgrade pip")
    
    # Uninstall any existing discord.py or py-cord
    run_cmd(f"{sys.executable} -m pip uninstall -y discord.py")
    run_cmd(f"{sys.executable} -m pip uninstall -y py-cord")
    run_cmd(f"{sys.executable} -m pip uninstall -y discord")
    
    # Remove any remaining discord package directories
    for path in sys.path:
        discord_dir = os.path.join(path, 'discord')
        if os.path.exists(discord_dir):
            print(f"Removing existing discord directory at {discord_dir}")
            try:
                subprocess.run(f"rm -rf {discord_dir}", shell=True, check=True)
            except:
                print(f"Failed to remove {discord_dir}")
    
    # Install py-cord 2.6.1 specifically
    success = run_cmd(f"{sys.executable} -m pip install py-cord==2.6.1")
    
    if success is not None:
        print("\nPy-cord 2.6.1 installation successful!")
        
        # Verify installation
        try:
            import discord
            print(f"Verified discord module version: {discord.__version__}")
            if hasattr(discord, 'bridge'):
                print("Verified this is py-cord (bridge module exists)")
            else:
                print("WARNING: This doesn't appear to be py-cord (no bridge module)")
        except ImportError as e:
            print(f"Failed to import discord module: {e}")
    else:
        print("\nPy-cord installation failed!")

if __name__ == "__main__":
    main()