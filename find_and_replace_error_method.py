"""
Script to update all instances of SafeMongoDBResult.get_error() to SafeMongoDBResult.create_error
in utils/safe_mongodb.py

This script performs a search and replace operation on the safe_mongodb.py file
to standardize error creation methods.
"""

import re
import os
import logging
import sys
from typing import Tuple

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_file(file_path: str) -> Tuple[bool, str]:
    """
    Read the contents of a file with error handling
    
    Args:
        file_path: Path to the file to read
        
    Returns:
        Tuple of (success, content)
    """
    if not os.path.exists(file_path):
        logger.error(f"File does not exist: {file_path}")
        return False, ""
        
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        return True, content
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return False, ""

def write_file(file_path: str, content: str) -> bool:
    """
    Write content to a file with error handling
    
    Args:
        file_path: Path to the file to write
        content: Content to write to the file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        with open(file_path, 'w') as f:
            f.write(content)
        return True
    except Exception as e:
        logger.error(f"Error writing to file {file_path}: {e}")
        return False

def replace_error_with_create_error(file_path='utils/safe_mongodb.py'):
    """
    Replace all instances of SafeMongoDBResult.error() with SafeMongoDBResult.create_error()
    
    Args:
        file_path: Path to the file to modify
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Starting to process file: {file_path}")
    
    # Read the file content
    success, content = read_file(file_path)
    if not success:
        return False
    
    # Check if the file contains any instances of the pattern
    pattern = r'SafeMongoDBResult\.error\('
    matches = re.findall(pattern, content)
    if not matches:
        logger.info("No instances of SafeMongoDBResult.error() found in the file")
        return True
    
    logger.info(f"Found {len(matches)} instances of SafeMongoDBResult.error()")
    
    # Replace all instances of SafeMongoDBResult.error() with SafeMongoDBResult.create_error()
    replacement = r'SafeMongoDBResult.create_error('
    updated_content = re.sub(pattern, replacement, content)
    
    # Write the updated content back to the file
    if write_file(file_path, updated_content):
        logger.info(f"Successfully replaced {len(matches)} instances in {file_path}")
        return True
    
    return False
    
def main():
    """Main entry point"""
    # Allow specifying the file path as a command line argument
    file_path = sys.argv[1] if len(sys.argv) > 1 else 'utils/safe_mongodb.py'
    
    if replace_error_with_create_error(file_path):
        logger.info("Replacement completed successfully")
        return 0
    else:
        logger.error("Replacement failed")
        return 1
    
if __name__ == "__main__":
    sys.exit(main())