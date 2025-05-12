#!/usr/bin/env python3
"""
Production setup script for Emeralds Killfeed PvP Statistics Discord Bot
This script ensures the environment is properly configured for production
"""
import os
import sys
import logging
import subprocess
import shutil
import platform
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('setup.log')
    ]
)
logger = logging.getLogger('setup_production')

def check_python_version():
    """Check Python version is at least 3.8"""
    major, minor = sys.version_info[:2]
    if major < 3 or (major == 3 and minor < 8):
        logger.critical(f"Python version {major}.{minor} is not supported. Minimum required version is 3.8.")
        return False
    logger.info(f"Python version {major}.{minor} OK")
    return True

def check_dependencies():
    """Check and install required dependencies"""
    logger.info("Checking dependencies...")

    try:
        # Check if pip is available
        subprocess.run([sys.executable, "-m", "pip", "--version"], check=True)

        # Install required packages
        logger.info("Installing required packages...")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"Package installation output: {result.stdout}")

        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install dependencies: {e}")
        logger.error(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking dependencies: {e}")
        return False

def setup_directories():
    """Ensure required directories exist"""
    dirs = ['backups', 'logs', 'data', 'temp']
    for d in dirs:
        os.makedirs(d, exist_ok=True)
        logger.info(f"Directory {d} is ready")
    return True

def check_discord_token():
    """Check if the Discord token is available"""
    token = os.environ.get('DISCORD_TOKEN')
    if token is None:
        logger.warning("DISCORD_TOKEN environment variable not set")
        logger.warning("You will need to set it before running the bot")
        return False
    return True

def clean_cache():
    """Clean Python cache files"""
    cache_dirs = []

    # Find __pycache__ directories
    for root, dirs, files in os.walk('.'):
        for d in dirs:
            if d == "__pycache__":
                cache_dirs.append(os.path.join(root, d))

    # Delete cache directories
    for d in cache_dirs:
        try:
            shutil.rmtree(d)
            logger.info(f"Removed cache directory: {d}")
        except Exception as e:
            logger.warning(f"Failed to remove cache directory {d}: {e}")

    return True

def main():
    """Main setup function"""
    logger.info("Starting production setup...")

    # Record the setup attempt
    with open("setup_log.txt", "a") as f:
        f.write(f"{datetime.now()}: Setup run on {platform.system()} {platform.release()}\n")

    # Run checks
    checks = [
        ("Python version", check_python_version()),
        ("Dependencies", check_dependencies()),
        ("Directories", setup_directories()),
        ("Discord token", check_discord_token()),
        ("Cache cleanup", clean_cache())
    ]

    # Report results
    all_passed = True
    logger.info("\nSetup Results:")
    for check_name, result in checks:
        status = "PASS" if result is not None else "FAIL"
        logger.info(f"  {check_name}: {status}")
        all_passed = all_passed and result

    if all_passed is not None:
        logger.info("\nSetup completed successfully!")
        logger.info("You can now run the bot with: python run_production.py")
        return 0
    else:
        logger.warning("\nSetup completed with issues. Please resolve them before running the bot.")
        return 1

if __name__ == "__main__":
    sys.exit(main())