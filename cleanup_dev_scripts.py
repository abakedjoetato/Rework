
#!/usr/bin/env python3
"""
# module: cleanup_dev_scripts
Cleanup Development Scripts

This script safely removes development-only scripts, test scripts, verification scripts,
backup files, and other non-production files from the project.

It performs safety checks to ensure we don't delete any files that might be needed
for production.
"""
import os
import re
import sys
import shutil
from typing import List, Set, Dict, Tuple
from datetime import datetime

# Configure essential production files that should never be deleted
ESSENTIAL_FILES = {
    'main.py',
    'bot.py',
    'app.py',
    'config.py',
    'database.py',
    'commands.py',
    'Procfile',
    'README.md',
    '.replit',
    'replit.nix',
    'requirements.txt',
}

# Configure essential directories that shouldn't have their files removed
ESSENTIAL_DIRS = {
    'cogs',
    'models',
    'utils',
    'static',
    'templates',
}

# Define patterns for development-only files
DEV_FILE_PATTERNS = [
    r'^debug_.*\.py$',              # Debug scripts
    r'^test_.*\.py$',               # Test scripts
    r'^verify_.*\.py$',             # Verification scripts
    r'^fix_.*\.py$',                # Fix scripts
    r'^apply_.*\.py$',              # Apply fix scripts
    r'^check_.*\.py$',              # Check scripts
    r'^clean_.*\.py$',              # Cleanup scripts
    r'^enable_.*\.py$',             # Enable scripts
    r'^deploy_.*\.py$',             # Deployment scripts
    r'^.*\.py\.bak$',               # Python backup files
    r'^.*\.py\.backup$',            # Python backup files
    r'^.*\.py\.\d+\.backup$',       # Python numbered backup files
    r'^.*\.py\.mongodb\.bak$',      # MongoDB fix backup files
    r'^.*\.py\.bak\.\d+$',          # Python timestamped backup files
    r'^.*\.py\.bak_\d+$',           # Python dated backup files
    r'^.*\.py\.\d+_\d+\.bak$',      # Python dated backup files
    r'^.*\.py\.new$',               # New version files
    r'^.*\.py\.original$',          # Original version files
    r'^.*\.py\.conflict\.bak$',     # Conflict backup files
]

# Explicit named scripts that are development-only
EXPLICIT_DEV_SCRIPTS = {
    'add_test_server.py',
    'remove_test_server.py',
    'find_mongodb_bool_issues.py',
    'final_test_command.py',
    'ensure_correct_timestamp_parsing.py',
    'additional_log_fixes.py',
    'bot_adapter_runner.py',
    'bot_wrapper.py',
    'direct_csv_fixes.py',
    'discord_bot_workflow.sh',
    'deploy_to_production.sh',
    'final_csv_fix.py',
    'final_map_files_fix.py',
}

def should_preserve(file_path: str) -> bool:
    """
    Check if a file should be preserved (not deleted)
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if the file should be preserved, False if it can be deleted
    """
    # Get file name and directory
    file_name = os.path.basename(file_path)
    dir_name = os.path.dirname(file_path)
    
    # Always preserve files in essential directories
    for essential_dir in ESSENTIAL_DIRS:
        if dir_name == essential_dir or dir_name.startswith("."):
            return True
    
    # Always preserve essential files
    if file_name in ESSENTIAL_FILES:
        return True
    
    # Check if it's referenced by main.py
    with open('main.py', 'r', encoding='utf-8', errors='ignore') as main_file:
        main_content = main_file.read()
        # Extract file name without extension
        base_name = os.path.splitext(file_name)[0]
        # Check for imports or references
        if f"import {base_name}" in main_content or f"from {base_name}" in main_content:
            return True
    
    # Check if it's referenced by bot.py
    try:
        with open('bot.py', 'r', encoding='utf-8', errors='ignore') as bot_file:
            bot_content = bot_file.read()
            # Extract file name without extension
            base_name = os.path.splitext(file_name)[0]
            # Check for imports or references
            if f"import {base_name}" in bot_content or f"from {base_name}" in bot_content:
                return True
    except FileNotFoundError:
        pass
    
    return False

def is_dev_script(file_name: str) -> bool:
    """
    Check if a file is a development script
    
    Args:
        file_name: Name of the file
        
    Returns:
        True if it's a development script, False otherwise
    """
    # Check explicit list
    if file_name in EXPLICIT_DEV_SCRIPTS:
        return True
    
    # Check patterns
    for pattern in DEV_FILE_PATTERNS:
        if re.match(pattern, file_name):
            return True
    
    return False

def create_backup_dir() -> str:
    """
    Create a backup directory for deleted files
    
    Returns:
        Path to the backup directory
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"dev_scripts_backup_{timestamp}"
    
    os.makedirs(backup_dir, exist_ok=True)
    return backup_dir

def cleanup_dev_scripts(dry_run: bool = False) -> Tuple[List[str], List[str]]:
    """
    Clean up development scripts
    
    Args:
        dry_run: If True, just list files that would be deleted without actually deleting
        
    Returns:
        Tuple of (deleted_files, preserved_files)
    """
    deleted_files = []
    preserved_files = []
    backup_dir = create_backup_dir() if not dry_run else None
    
    # Walk through all files in the project
    for root, _, files in os.walk('.'):
        # Skip the backup directory itself
        if backup_dir and root.startswith(f"./{backup_dir}"):
            continue
            
        # Skip .git directory
        if '.git' in root:
            continue
            
        for file in files:
            # Only process Python files or explicit dev scripts
            if file.endswith('.py') or file in EXPLICIT_DEV_SCRIPTS:
                file_path = os.path.join(root, file)
                
                # Check if it's a dev script
                if is_dev_script(file):
                    # Check if it should be preserved
                    if should_preserve(file_path):
                        preserved_files.append(file_path)
                        print(f"Preserving essential file: {file_path}")
                    else:
                        deleted_files.append(file_path)
                        print(f"  Preserved: {file_path}")
                        
                        # Move to backup instead of deleting
                        if dry_run is None:
                            rel_path = os.path.relpath(file_path, '.')
                            backup_path = os.path.join(backup_dir, rel_path)
                            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                            shutil.copy2(file_path, backup_path)
                            try:
                                os.remove(file_path)
                                print(f"Successfully deleted: {file_path}")
                            except Exception as e:
                                print(f"Failed to delete {file_path}: {e}")
                                # Try force delete
                                try:
                                    os.unlink(file_path)
                                    print(f"Force deleted using unlink: {file_path}")
                                except Exception as unlink_error:
                                    print(f"Even force delete failed for {file_path}: {unlink_error}")
    
    return deleted_files, preserved_files

def main():
    """Main entry point"""
    print("Development Script Cleanup Utility")
    print("=================================")
    
    # Default to dry run for safety
    dry_run = True
    if len(sys.argv) > 1 and sys.argv[1] == '--execute':
        dry_run = False
        print("WARNING: Running in EXECUTE mode. Files will be deleted!")
    else:
        print("Running in DRY RUN mode. No files will be deleted.")
        print("To execute the cleanup, run with: python cleanup_dev_scripts.py --execute")
    
    print("\nScanning for development scripts...")
    deleted, preserved = cleanup_dev_scripts(dry_run)
    
    print("\nSummary:")
    print(f"  Files to be deleted: {len(deleted)}")
    print(f"  Files preserved: {len(preserved)}")
    
    if dry_run is None:
        print("\nBackup created in case of accidental deletion.")
    
    print("\nComplete!")

if __name__ == "__main__":
    main()
