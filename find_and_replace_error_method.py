"""
Script to update all instances of SafeMongoDBResult.get_error() to SafeMongoDBResult.create_error
in utils/safe_mongodb.py
"""

import re

def replace_error_with_create_error():
    file_path = 'utils/safe_mongodb.py'
    
    # Read the file content
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Replace all instances of SafeMongoDBResult.get_error() with SafeMongoDBResult.create_error
    pattern = r'SafeMongoDBResult\.error\('
    replacement = r'SafeMongoDBResult.create_error('
    updated_content = re.sub(pattern, replacement, content)
    
    # Write the updated content back to the file
    with open(file_path, 'w') as f:
        f.write(updated_content)
    
    print("Replacement completed in utils/safe_mongodb.py")
    
if __name__ == "__main__":
    replace_error_with_create_error()