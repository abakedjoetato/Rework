
"""
Coroutine diagnostic utility

This module provides functions to check for and diagnose coroutine
handling issues in Discord bot commands.
"""
import inspect
import logging
import os
import re
import asyncio
from typing import List, Dict, Any, Set, Tuple

logger = logging.getLogger(__name__)

# Common patterns that might indicate coroutine issues
COROUTINE_PATTERNS = [
    r'(\w+)\s*=\s*EmbedBuilder\.create_\w+\(',  # EmbedBuilder creation without await
    r'ctx\.send\(\s*embed\s*=\s*(\w+\.create_\w+\()',  # Direct embedding of creator methods
    r'embed\s*=\s*(\w+\.create_\w+\()',  # Assigning creator methods to embed
    r'await\s+ctx\.send\(\s*embed\s*=\s*await\s+',  # Double awaiting (not an issue but worth checking)
    r'interaction\.response\.send_message\(\s*embed\s*=\s*(\w+\.create_\w+\()',  # Direct embedding with interactions
]

async def check_file_for_coroutine_issues(file_path: str) -> List[Dict[str, Any]]:
    """
    Check a file for potential coroutine handling issues
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        List of potential issues with line numbers and descriptions
    """
    issues = []
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            
        lines = content.split('\n')
        for i, line in enumerate(lines, 1):
            for pattern in COROUTINE_PATTERNS:
                match = re.search(pattern, line)
                if match:
                    # Check if this line has an await before the matched pattern
                    has_await = 'await' in line[:match.start()]
                    
                    if not has_await and 'await' not in line:
                        issues.append({
                            'file': file_path,
                            'line': i,
                            'text': line.strip(),
                            'issue': f"Potential missing await for coroutine {match.group(1) if match.groups() else ''}",
                            'pattern': pattern
                        })
    except Exception as e:
        logger.error(f"Coroutine check failed: {func_name}")
        
    return issues

async def scan_directory_for_coroutine_issues(directory: str = '.', 
                                             extensions: Set[str] = {'.py'}, 
                                             exclude_dirs: Set[str] = {'venv', 'env', '__pycache__', '.git'}) -> List[Dict[str, Any]]:
    """
    Scan a directory recursively for potential coroutine handling issues
    
    Args:
        directory: Root directory to start scanning
        extensions: File extensions to check
        exclude_dirs: Directories to exclude from scanning
        
    Returns:
        List of potential issues with file paths, line numbers and descriptions
    """
    all_issues = []
    
    for root, dirs, files in os.walk(directory):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        
        for file in files:
            if any(file.endswith(ext) for ext in extensions):
                file_path = os.path.join(root, file)
                issues = await check_file_for_coroutine_issues(file_path)
                all_issues.extend(issues)
    
    return all_issues

async def fix_simple_coroutine_issues(file_path: str) -> Tuple[bool, str]:
    """
    Attempt to automatically fix simple coroutine issues in a file
    
    Args:
        file_path: Path to the file to fix
        
    Returns:
        Tuple of (success, message)
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            
        # Simple fix: add await to EmbedBuilder.create_* calls
        pattern = r'(\s+)(\w+)\s*=\s*(EmbedBuilder\.create_\w+\(.*?\))(?!\s*await)'
        fixed_content = re.sub(pattern, r'\1\2 = await \3', content)
        
        # Fix direct embedding in send calls
        pattern = r'(await\s+ctx\.send\(\s*embed\s*=\s*)(EmbedBuilder\.create_\w+\(.*?\))(?!\s*await)'
        fixed_content = re.sub(pattern, r'\1await \2', fixed_content)
        
        # Check if we made any changes
        if fixed_content != content:
            # Backup the original file
            backup_path = f"{file_path}.coroutine_fix.bak"
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
            # Write the fixed content
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
                
            return True, f"Fixed coroutine issues in {file_path}. Original backed up to {backup_path}"
        else:
            return False, f"No simple coroutine issues found in {file_path}"
            
    except Exception as e:
        logger.error(f"Error fixing file {file_path}: {e}")
        return False, f"Error fixing {file_path}: {e}f"

async def main():
    """Command-line entry point for diagnostics"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Check for coroutine handling issues in Python files')
    parser.add_argument('--directory', '-d', default='.', help='Directory to scan')
    parser.add_argument('--fix', '-f', action='store_true', help='Attempt to fix simple issues')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show verbose output')
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    
    # Scan for issues
    issues = await scan_directory_for_coroutine_issues(args.directory)
    
    if not issues:
        print("No potential coroutine issues found.")
        return
        
    # Group issues by file
    issues_by_file = {}
    for issue in issues:
        file_path = issue['file']
        if file_path not in issues_by_file:
            issues_by_file[file_path] = []
        issues_by_file[file_path].append(issue)
    
    # Print summary
    print(f"Found {len(issues)} potential coroutine issues in {len(issues_by_file)} files:")
    
    for file_path, file_issues in issues_by_file.items():
        print(f"\n{file_path}: {len(file_issues)} issues")
        for issue in file_issues:
            print(f"  Line {issue['line']}: {issue['issue']}")
            if args.verbose:
                print(f"    {issue['text']}")
    
    # Fix issues if requested
    if args.fix:
        print("\nAttempting to fix simple issues...")
        for file_path in issues_by_file.keys():
            success, message = await fix_simple_coroutine_issues(file_path)
            print(message)

if __name__ == "__main__":
    asyncio.run(main())
