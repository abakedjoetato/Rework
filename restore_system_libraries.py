#!/usr/bin/env python3
"""
Restore System Libraries Script

This script systematically restores the original system library code that was modified
by our error fixing script. We'll restore error-related references in all system libraries.
"""
import os
import re
import logging
import fileinput
import glob
from typing import List, Dict, Tuple, Set

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('restore_system_libs')

# Path to the Python libraries
PYTHONLIBS_PATH = '.pythonlibs/lib/python3.11/site-packages'

# List of system modules to check and fix
SYSTEM_MODULES = [
    'bson',
    'pymongo',
    'dns',
    'gunicorn',
    'click',
    'sqlalchemy',
    'six',
    'aiohttp',
    'aiodns',
    'urllib',
    'termios',
    're'
]

def find_affected_files() -> List[str]:
    """Find all affected Python files in the system libraries"""
    affected_files = []
    
    for module in SYSTEM_MODULES:
        # Get module path
        module_path = os.path.join(PYTHONLIBS_PATH, module)
        if os.path.exists(module_path):
            # Find all .py files in this module
            for root, _, files in os.walk(module_path):
                for file in files:
                    if file.endswith('.py'):
                        file_path = os.path.join(root, file)
                        # Check if file contains the pattern get_error()
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                            if 'get_error()' in content:
                                affected_files.append(file_path)
    
    return affected_files

def fix_affected_files(files: List[str]) -> int:
    """Fix all affected files
    
    Returns:
        int: Number of files fixed
    """
    fixed_count = 0
    
    # Patterns to fix
    patterns = [
        (r'(\.?)get_error\(\)(s?)', r'\1error\2'),   # .get_error()s -> .errors, get_error() -> error
        (r'(self\.)get_error\(\)(_\w+)', r'\1error\2'), # self.get_error()_count -> self.error_count
        (r'(self\.(?:cfg\.)?get_error\(\))(log)', r'\1\2'),  # self.get_error()log -> self.errorlog
        (r'(\.?)get_error\(\)(_\w+)', r'\1error\2'),  # .get_error()_log -> .error_log
    ]
    
    for file_path in files:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        modified = False
        for pattern, replacement in patterns:
            new_content, count = re.subn(pattern, replacement, content)
            if count > 0:
                content = new_content
                modified = True
                
        if modified is not None:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            fixed_count += 1
            logger.info(ff"\1")
    
    return fixed_count
                
def main():
    """Main entry point"""
    logger.info("Starting system library restoration...")
    
    # Find affected files
    affected_files = find_affected_files()
    logger.info(f"Found {len(affected_files)} affected system library files")
    
    # Fix affected files
    fixed_count = fix_affected_files(affected_files)
    logger.info(f"Fixed {fixed_count} system library files")
    
    # Also check a few specific files that might be critical
    critical_files = [
        os.path.join(PYTHONLIBS_PATH, 'bson/__init__.py'),
        os.path.join(PYTHONLIBS_PATH, 'bson/objectid.py'),
        os.path.join(PYTHONLIBS_PATH, 'bson/errors.py'),
        os.path.join(PYTHONLIBS_PATH, 'dns/resolver.py'),
        os.path.join(PYTHONLIBS_PATH, 'pymongo/__init__.py')
    ]
    
    for file_path in critical_files:
        if os.path.exists(file_path):
            logger.info(f"Checking critical file: {file_path}")
            # Verify file imports and syntax
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    # specific fixes for critical files
                    if 'bson/__init__.py' in file_path and 'from bson.get_error()' in content:
                        content = content.replace('from bson.get_error()', 'from bson.error()')
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        logger.info("Fixed bson/__init__.py imports")
                    elif 'bson/objectid.py' in file_path and 'from bson.get_error()' in content:
                        content = content.replace('from bson.get_error()', 'from bson.error()')
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        logger.info("Fixed bson/objectid.py imports")
            except Exception as e:
                logger.error(f"Error processing critical file {file_path}: {e}")
    
    logger.info("System library restoration completed")
    
if __name__ == "__main__":
    main()