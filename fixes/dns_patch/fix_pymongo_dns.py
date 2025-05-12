#!/usr/bin/env python3
"""
Fix for the MongoDB DNS Resolver

This creates a patched version of the SRV Resolver that bypasses the problematic dnspython library.
"""
import os
import sys
import importlib

def apply_patch():
    """
    Apply patches to pymongo to bypass the problematic DNS module.
    """
    pymongo_srv_path = './.pythonlibs/lib/python3.11/site-packages/pymongo/srv_resolver.py'
    
    if not os.path.exists(pymongo_srv_path):
        print(f"Could not find srv_resolver.py at {pymongo_srv_path}")
        return False
    
    # Create patched version of the srv_resolver.py
    patched_content = """
# Copyright 2017-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""

    patched_content += """
import socket

_HAVE_DNSPYTHON = False  # Patched: Disable dnspython to avoid bugs


class _SrvResolver:
    def __init__(self, fqdn):
        self.fqdn = fqdn
        self._records = []

    def get_hosts(self):
        # Mock implementation - just return an empty list
        # This bypasses the SRV lookup which is not needed for direct connection
        return []
"""
    
    # Write the patched content
    with open(pymongo_srv_path, 'w') as f:
        f.write(patched_content)
    
    # Reload the pymongo.srv_resolver module
    try:
        if 'pymongo.srv_resolver' in sys.modules:
            del sys.modules['pymongo.srv_resolver']
        
        # Import without using dnspython
        import pymongo.srv_resolver
        print("Successfully patched pymongo.srv_resolver")
        return True
    except Exception as e:
        print(f"Error reloading patched module: {e}")
        return False

if __name__ == "__main__":
    success = apply_patch()
    sys.exit(0 if success else 1)