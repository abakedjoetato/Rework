#!/usr/bin/env python3
"""
Cleanup script to remove development-only files and prepare for production
"""
import os
import re
import sys
import shutil
from typing import List, Set
from datetime import datetime

# Essential production files that should never be deleted
ESSENTIAL_FILES = {
    'main.py',
    'bot.py',
    'config.py',
    'requirements.txt',
    'pyproject.toml',
    '.replit',
    'Procfile',
    'README.md'
}

# Essential directories that shouldn't be removed
ESSENTIAL_DIRS = {
    'cogs',
    'models',
    'utils',
    'templates'
}

# Patterns for development-only files
DEV_FILE_PATTERNS = [
    r'^test_.*\.py$',              # Test scripts
    r'^debug_.*\.py$',             # Debug scripts
    r'^verify_.*\.py$',            # Verification scripts
    r'^check_.*\.py$',             # Check scripts
    r'^fix_.*\.py$',               # Fix scripts
    r'^setup_.*\.py$',             # Setup scripts
    r'^deploy_.*\.py$',            # Deployment scripts
    r'^ensure_.*\.py$',            # Verification scripts
    r'^remove_.*\.py$',            # Cleanup scripts
    r'^.*_test\.py$',              # Test files
    r'^.*\.py\.bak$',              # Backup files
    r'.*\.py\.\d+$',               # Numbered backups
    r'.*_debug\.py$',              # Debug variants
    r'.*_old\.py$',                # Old versions
]

def should_remove_file(filename: str) -> bool:
    """Check if a file should be removed"""
    if filename in ESSENTIAL_FILES:
        return False

    for pattern in DEV_FILE_PATTERNS:
        if re.match(pattern, filename):
            return True

    return False

def cleanup_directory(directory: str) -> None:
    """Clean up development files in a directory"""
    for root, dirs, files in os.walk(directory, topdown=True):
        # Skip essential directories
        dirs[:] = [d for d in dirs if d not in ESSENTIAL_DIRS and not d.startswith('.')]

        for file in files:
            if should_remove_file(file):
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                    print(f"Removed: {file_path}")
                except Exception as e:
                    print(f"Error removing {file_path}: {e}")

def main():
    """Main cleanup function"""
    print("Starting cleanup...")

    # Create backup directory
    backup_dir = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(backup_dir, exist_ok=True)

    # Backup current state
    for item in os.listdir('.'):
        if item != backup_dir and not item.startswith('.'):
            try:
                shutil.copy2(item, backup_dir)
            except Exception as e:
                print(f"Error backing up {item}: {e}")

    # Clean up the main directory
    cleanup_directory('.')

    print("Cleanup completed!")
    print(f"Backup created in: {backup_dir}")

if __name__ == "__main__":
    main()