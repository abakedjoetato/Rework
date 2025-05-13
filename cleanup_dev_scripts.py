
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
    'main.py',           # Main entry point
    'bot.py',           # Core bot class
    'config.py',        # Configuration
    'requirements.txt', # Dependencies
    'pyproject.toml',   # Project metadata
    '.replit',          # Replit config
    'Procfile',         # Process file
    'README.md'         # Documentation
}

# Essential directories that shouldn't be removed
ESSENTIAL_DIRS = {
    'cogs',       # Bot commands/features
    'models',     # Data models
    'utils',      # Utility functions
    'templates'   # Template files
}

# Non-essential directories that can be removed
REMOVABLE_DIRS = {
    'backup_20250513_000256',
    'diagnostics', 
    'docs',
    'examples',
    'fixes',
    'tests'
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
    r'^run_.*\.py$',               # Various run scripts
    r'^.*_test\.py$',              # Test files
    r'^.*\.py\.bak$',              # Backup files
    r'.*\.py\.\d+$',               # Numbered backups
    r'.*_debug\.py$',              # Debug variants
    r'.*_old\.py$',                # Old versions
    r'^restart_.*\.py$',           # Restart scripts
    r'^premium_.*\.py$',           # Premium setup scripts
    r'^.*_trace.*\.py$',           # Trace files
    r'^generate_.*\.py$',          # Asset generators
    r'^initialize_.*\.py$',        # Init scripts
    r'^install_.*\.py$',           # Install scripts
    r'^.*_migration\.py$',         # Migration scripts
    r'^trigger_.*\.py$',           # Trigger scripts
    r'^sync_.*\.py$',              # Sync scripts
    r'^reduce_.*\.py$',            # Reduction scripts
    r'^quick_.*\.py$',             # Quick check scripts
    r'^.*_verification\.py$',      # Verification scripts
    r'^.*_compatibility\.py$',     # Compatibility scripts
    r'^implement_.*\.py$',         # Implementation scripts
    r'^.*_analyzer\.py$',          # Analysis tools
]

def is_dev_file(filename: str) -> bool:
    """Check if a file matches development patterns"""
    return any(re.match(pattern, filename) for pattern in DEV_FILE_PATTERNS)

def backup_files(files: List[str], backup_dir: str):
    """Create backup of files before removal"""
    os.makedirs(backup_dir, exist_ok=True)
    for file in files:
        try:
            if os.path.isfile(file):
                shutil.copy2(file, os.path.join(backup_dir, os.path.basename(file)))
            elif os.path.isdir(file):
                print(f"Error backing up {file}: Is a directory")
        except Exception as e:
            print(f"Error backing up {file}: {e}")

def main():
    """Main cleanup function"""
    print("Starting cleanup...")
    
    # Create backup directory with timestamp
    backup_dir = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Backup files before removal
    all_files = [f for f in os.listdir('.') if f not in ESSENTIAL_FILES]
    backup_files(all_files, backup_dir)
    
    # Remove non-essential directories
    for dir_name in REMOVABLE_DIRS:
        if os.path.exists(dir_name):
            try:
                shutil.rmtree(dir_name)
                print(f"Removed directory: {dir_name}")
            except Exception as e:
                print(f"Error removing directory {dir_name}: {e}")
    
    # Remove development files
    for file in os.listdir('.'):
        if file in ESSENTIAL_FILES or file in ESSENTIAL_DIRS:
            continue
            
        if os.path.isfile(file) and is_dev_file(file):
            try:
                os.remove(file)
                print(f"Removed: {os.path.join('.', file)}")
            except Exception as e:
                print(f"Error removing {file}: {e}")
    
    print("Cleanup completed!")
    print(f"Backup created in: {backup_dir}")

if __name__ == "__main__":
    main()
