#!/usr/bin/env python3
"""
Utility script to reduce logging levels from INFO to DEBUG
across multiple modules in the Discord bot.

This helps minimize console output for non-critical messages.
"""
import re
import os
import sys
import glob

def reduce_log_levels(file_path):
    """
    Reduce logging levels from INFO to DEBUG in the specified file.
    
    Args:
        file_path: Path to the file to modify
    """
    print(ff"\1")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        print(f"  Skipping binary or non-UTF-8 file: {file_path}")
        return
    except Exception as e:
        print(f"  Error reading file {file_path}: {e}")
        return
    
    # Count original logger.info calls
    info_count = len(re.findall(r'logger\.info\(', content))
    if info_count == 0:
        print("  No INFO logs found, skipping file")
        return
    
    print(f"  Found {info_count} INFO log statements")
    
    # Common replacements for all modules
    common_replacements = [
        # Debug and status information
        (r'logger\.info\(f"Memory optimization: freed (\{.*?\}) objects after (.*?)"\)', r'logger.debug("Memory optimization: freed \1 objects after \2")'),
        (r'logger\.info\(f"Processing (.*?) for (\{.*?\}) servers"\)', r'logger.debug("Processing \1 for \2 servers")'),
        (r'logger\.info\(f"Found (\{.*?\}) (.*?) in (.*?) collection"\)', r'logger.debug("Found \1 \2 in \3 collection")'),
        
        # File processing details
        (r'logger\.info\(f"Downloaded content type: (\{.*?\}), length: (\{.*?\})"\)', r'logger.debug("Downloaded content type: \1, length: \2")'),
        (r'logger\.info\(f"Found (\{.*?\}) (.*?) files in (.*?)"\)', r'logger.debug("Found \1 \2 files in \3")'),
        (r'logger\.info\("Using detected delimiter: \'(.*?)\' for file (.*?)"\)', r'logger.debug("Using detected delimiter: \'\1\' for file \2")'),
        
        # SFTP and file operations
        (r'logger\.info\("Using original server ID \'(.*?)\' for path construction"\)', r'logger.debug("Using original server ID \'\1\' for path construction")'),
        (r'logger\.info\("Using numeric original_server_id \'(.*?)\' for path construction"\)', r'logger.debug("Using numeric original_server_id \'\1\' for path construction")'),
        (r'logger\.info\("Detected AsyncSSH SFTP client, using optimized methods"\)', r'logger.debug("Detected AsyncSSH SFTP client, using optimized methods")'),
        (r'logger\.info\(f"Downloaded (.*?) using AsyncSSH open\+read \((\{.*?\}) bytes\)"\)', r'logger.debug("Downloaded \1 using AsyncSSH open+read (\2 bytes)")'),
        (r'logger\.info\(f"Downloaded (\{.*?\}) bytes from file (.*?)"\)', r'logger.debug("Downloaded \1 bytes from file \2")'),
        
        # Server and configuration details
        (r'logger\.info\("Server in \'(.*?)\': ID=(.*?), sftp_enabled=(.*?), name=(.*?)"\)', r'logger.debug("Server in \'\1\': ID=\2, sftp_enabled=\3, name=\4")'),
        (r'logger\.info\("Looking for (.*?) in path: (.*?)"\)', r'logger.debug("Looking for \1 in path: \2")'),
        (r'logger\.info\("Found (.*?) at: (.*?)"\)', r'logger.debug("Found \1 at: \2")'),
    ]
    
    # Module-specific replacements
    module_specific_replacements = {
        "csv_processor.py": [
            # CSV processor specific logs
            (r'logger\.info\(f"Using batch processing for (\{.*?\}) events"\)', r'logger.debug("Using batch processing for \1 events")'),
            (r'logger\.info\(f"Categorized events: (\{.*?\}) kills, (\{.*?\}) suicides"\)', r'logger.debug("Categorized events: \1 kills, \2 suicides")'),
            (r'logger\.info\(f"Updating stats for (\{.*?\}) unique players"\)', r'logger.debug("Updating stats for \1 unique players")'),
            (r'logger\.info\("Updating nemesis/prey relationships"\)', r'logger.debug("Updating nemesis/prey relationships")'),
            (r'logger\.info\("CSV content sample: (.*?)"\)', r'logger.debug("CSV content sample: \1")'),
            (r'logger\.info\(f"Added (\{.*?\}) CSV files from (.*?) to tracking lists"\)', r'logger.debug("Added \1 CSV files from \2 to tracking lists")'),
            (r'logger\.info\(f"Total tracked (.*?) files now: (\{.*?\})"\)', r'logger.debug("Total tracked \1 files now: \2")'),
        ],
        "log_processor.py": [
            # Log processor specific logs
            (r'logger\.info\("Final path_server_id: (.*?)"\)', r'logger.debug("Final path_server_id: \1")'),
            (r'logger\.info\("Building server directory with resolved server ID: (.*?)"\)', r'logger.debug("Building server directory with resolved server ID: \1")'),
            (r'logger\.info\("Using default directory structure with ID (.*?): (.*?)"\)', r'logger.debug("Using default directory structure with ID \1: \2")'),
            (r'logger\.info\("Getting stats for log file: (.*?)"\)', r'logger.debug("Getting stats for log file: \1")'),
        ],
        "sftp.py": [
            # SFTP manager specific logs
            (r'logger\.info\("SFTPClient using known numeric ID \'(.*?)\' for path construction instead of \'(.*?)\'"\)', r'logger.debug("SFTPClient using known numeric ID \'\1\' for path construction instead of \'\2\'")'),
            (r'logger\.info\("Using original server ID \'(.*?)\' for path construction instead of standardized ID \'(.*?)\'"\)', r'logger.debug("Using original server ID \'\1\' for path construction instead of standardized ID \'\2\'")'),
            (r'logger\.info\("Found (.*?) at: (.*?)"\)', r'logger.debug("Found \1 at: \2")'),
            (r'logger\.info\(f"Total (.*?) files found after deduplication: (\{.*?\}) \(from (\{.*?\}) total\)"\)', r'logger.debug("Total \1 files found after deduplication: \2 (from \3 total)")'),
        ],
        "direct_csv_handler.py": [
            # Direct CSV handler specific logs
            (r'logger\.info\("Direct parsing CSV content from file: (.*?)"\)', r'logger.debug("Direct parsing CSV content from file: \1")'),
            (r'logger\.info\("Using delimiter \'(.*?)\' for content parsing \((.*?)\)"\)', r'logger.debug("Using delimiter \'\1\' for content parsing (\2)")'),
            (r'logger\.info\(f"Directly parsed (\{.*?\}) events from (\{.*?\}) rows in CSV content"\)', r'logger.debug("Directly parsed \1 events from \2 rows in CSV content")'),
        ],
        "csv_parser.py": [
            # CSV parser specific logs
            (r'logger\.info\("Parsing CSV file: (.*?)"\)', r'logger.debug("Parsing CSV file: \1")'),
            (r'logger\.info\("Detected delimiter: \'(.*?)\' \((.*?)\)"\)', r'logger.debug("Detected delimiter: \'\1\' (\2)")'),
            (r'logger\.info\(f"Parsed (\{.*?\}) events from (\{.*?\}) rows in (.*?)"\)', r'logger.debug("Parsed \1 events from \2 rows in \3")'),
        ],
    }
    
    # Apply common replacements
    for pattern, replacement in common_replacements:
        content = re.sub(pattern, replacement, content)
    
    # Apply module-specific replacements
    filename = os.path.basename(file_path)
    if filename in module_specific_replacements:
        for pattern, replacement in module_specific_replacements[filename]:
            content = re.sub(pattern, replacement, content)
    
    # Count new logger.info calls
    new_info_count = len(re.findall(r'logger\.info\(', content))
    changes = info_count - new_info_count
    
    if changes > 0:
        print(f"  Reduced INFO logs from {info_count} to {new_info_count} ({changes} changed)")
        
        # Save the modified content
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"  Saved changes to {file_path}")
    else:
        print(f"  No changes made to {file_path}")

def find_python_files(directory):
    """Find all Python files in the directory and its subdirectories"""
    return glob.glob(f"{directory}/**/*.py", recursive=True)

if __name__ == "__main__":
    # Define base directories to search for Python files
    base_directories = [
        "cogs",
        "utils",
        "models"
    ]
    
    # If specific files are provided as arguments, process only those
    if len(sys.argv) > 1:
        files_to_process = sys.argv[1:]
    else:
        # Otherwise, find all Python files in the specified directories
        files_to_process = []
        for directory in base_directories:
            if os.path.exists(directory):
                files_to_process.extend(find_python_files(directory))
    
    # Filter out non-existent files
    files_to_process = [f for f in files_to_process if os.path.exists(f)]
    
    print(f"Found {len(files_to_process)} Python files to process")
    
    # Process each file
    for file_path in files_to_process:
        reduce_log_levels(file_path)