#!/usr/bin/env python3
"""
Fix for the invalid hex escape in dns/ipv6.py file in dnspython library.
This script fixes the formatting error in IPV6 mapping prefix by updating the problematic line.
"""
import os
import sys

def fix_ipv6_library():
    """Fix the invalid \x escape sequence in the ipv6.py file."""
    ipv6_path = os.path.expanduser('~/.pythonlibs/lib/python3.11/site-packages/dns/ipv6.py')
    
    if not os.path.exists(ipv6_path):
        print(f"Could not find ipv6.py at {ipv6_path}")
        return False
    
    # Read the content of the file
    with open(ipv6_path, 'r') as f:
        content = f.read()
    
    # Replace the problematic line
    if '_mapped_prefix = b"\\x00" * 10 + b"\\xff\\xf"' in content:
        content = content.replace(
            '_mapped_prefix = b"\\x00" * 10 + b"\\xff\\xf"',
            '_mapped_prefix = b"\\x00" * 10 + b"\\xff\\xff"'
        )
        
        # Write the fixed content back
        with open(ipv6_path, 'w') as f:
            f.write(content)
        
        print(f"Fixed invalid hex escape in {ipv6_path}")
        return True
    else:
        print("Could not find the problematic line to fix")
        return False

if __name__ == "__main__":
    success = fix_ipv6_library()
    sys.exit(0 if success else 1)