"""
Direct CSV Handler

This module provides a completely separate, simplified CSV parsing implementation
that bypasses all the complex infrastructure of the main application.

It's designed to be used as a fallback when the main parsing logic fails.
"""
import os
import csv
import io
import logging
import traceback
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union, cast

# Set up logging
logger = logging.getLogger(__name__)

# Define constants - NO LONGER USING attached_assets
# The direct CSV handler should only process files from SFTP now
# Set the assets directory to a real path to ensure it works
ASSETS_DIR = os.path.join(os.getcwd(), "attached_assets")

def direct_parse_csv_content(content_str: str, file_path: str = "", server_id: str = "", 
                    track_line_numbers: bool = False, start_line: int = 0) -> Tuple[List[Dict[str, Any]], int]:
    """
    Direct, simplified CSV parsing of string content, bypassing all complex infrastructure.
    
    Args:
        content_str: CSV content as a string
        file_path: Original file path (for logging)
        server_id: Server ID to associate with the events
        track_line_numbers: Whether to track and return the total number of lines
        start_line: Line number to start processing from (0-based, default: 0)
        
    Returns:
        Tuple of (parsed event dictionaries, total line count)
    """
    logger.info(f"Direct parsing CSV content from {'file: ' + file_path if file_path else 'string content'}")
    if start_line > 0:
        logger.debug(f"Incremental processing starting from line position {start_line}")
    
    try:
        if content_str is None:
            logger.error("Empty content provided")
            return [], 0
            
        # Detect delimiter (semicolons or commas)
        # CRITICAL FIX: Based on the screenshot, these logs explicitly use semicolon delimiter
        # However, we'll still perform auto-detection for robustness
        semicolons = content_str.count(';')
        commas = content_str.count(',')
        
        # Default to semicolon (as seen in the screenshot) unless there's very strong evidence of commas
        delimiter = ';'
        if commas > semicolons * 3:  # Very high threshold to override
            delimiter = ','
            logger.warning(f"Unusual delimiter detected: using comma instead of semicolon")
        
        logger.debug(f"Using delimiter \'{delimiter}\' for content parsing (semicolons: {semicolons}, commas: {commas})")
        
        # Create CSV reader
        csv_reader = csv.reader(io.StringIO(content_str), delimiter=delimiter)
        
        # Parse events
        events = []
        row_count = 0
        
        for row in csv_reader:
            row_count += 1
            
            # CRITICAL FIX: Skip lines up to the start_line position
            if start_line > 0 and row_count <= start_line:
                continue
            
            # Skip empty rows or those without enough fields
            if not row or len(row) < 5:
                continue
                
            # Skip header rows
            if any(keyword in row[0].lower() for keyword in ['time', 'date', 'timestamp']):
                continue
                
            # Extract data from row
            try:
                event = {
                    'timestamp': row[0] if len(row) > 0 else "",
                    'killer_name': row[1] if len(row) > 1 else "",
                    'killer_id': row[2] if len(row) > 2 else "",
                    'victim_name': row[3] if len(row) > 3 else "",
                    'victim_id': row[4] if len(row) > 4 else "",
                    'weapon': row[5] if len(row) > 5 else "",
                    'distance': float(row[6]) if len(row) > 6 and row[6].strip() else 0.0,
                    'server_id': server_id,
                    'event_type': 'kill'
                }
            except Exception as e:
                logger.error(f"Error processing row: {e}")
                continue
            
            # Check for suicide (killer == victim)
            if event['killer_name'] == event['victim_name'] or event['killer_id'] == event['victim_id']:
                event['event_type'] = 'suicide'
                event['is_suicide'] = True
            else:
                event['is_suicide'] = False
                
            # Parse timestamp
            try:
                ts_str = event['timestamp']
                
                # Try various timestamp formats
                timestamp_formats = [
                    '%Y.%m.%d-%H.%M.%S',
                    '%Y.%m.%d-%H:%M:%S',
                    '%Y.%m.%d %H.%M.%S',
                    '%Y.%m.%d %H:%M:%S',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H.%M.%S',
                    '%m/%d/%Y %H:%M:%S',
                    '%d/%m/%Y %H:%M:%S'
                ]
                
                for fmt in timestamp_formats:
                    try:
                        dt = datetime.strptime(ts_str, fmt)
                        event['timestamp'] = dt
                        break
                    except ValueError:
                        continue
                        
                # If still a string, use current time
                if isinstance(event['timestamp'], str):
                    event['timestamp'] = datetime.now()
                    
            except Exception as e:
                logger.error(f"Error parsing timestamp: {e}")
                event['timestamp'] = datetime.now()
                
            # Add to events list
            events.append(event)
            
        logger.debug(f"Directly parsed {len(events)} events from {row_count} rows in CSV content")
        return events, row_count
        
    except Exception as e:
        logger.error(f"Error in direct CSV content parsing: {e}")
        logger.error(traceback.format_exc())
        return [], 0

def direct_parse_csv_file(file_path: str, server_id: str, start_line: int = 0) -> Tuple[List[Dict[str, Any]], int]:
    """
    Direct, simplified CSV parsing implementation that bypasses all complex infrastructure.
    
    Args:
        file_path: Path to the CSV file
        server_id: Server ID to associate with the events
        start_line: Line number to start processing from (0-based, default: 0)
        
    Returns:
        Tuple of (parsed event dictionaries, total line count)
    """
    logger.info(f"Direct parsing CSV file: {file_path}")
    
    try:
        # FIXED: Enhanced file reading and content detection
        logger.info(f"Processing file: {os.path.basename(file_path)}")
        
        
        # FIXED: Enhanced file reading and content detection
        logger.info(f"Processing file: {os.path.basename(file_path)}")
        
        # Read file as binary for maximum compatibility
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
                
            if content is None:
                logger.error(f"Empty file: {file_path}")
                return [], 0
        except Exception as read_error:
            logger.error(f"Error reading file {file_path}: {read_error}")
            return [], 0
            
        # FIXED: Try multiple encodings with better error handling
        content_str = None
        successful_encoding = None
        
        for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
            try:
                content_str = content.decode(encoding, errors='replace')
                successful_encoding = encoding
                # Only break if we didn't get too many replacement characters
                if content_str.count('\ufffd') < len(content_str) / 10:  # Less than 10% replacements
                    break
            except Exception as decode_error:
                logger.warning(f"Failed to decode with {encoding}: {decode_error}")
                continue
                
        if content_str is None:
            logger.error(f"Failed to decode file content with any encoding: {file_path}")
            return [], 0
            
        logger.info(f"Successfully decoded file using {successful_encoding} encoding")
        
        # FIXED: Better delimiter detection with multiple passes if needed
        # First check for standard delimiters
        semicolons = content_str.count(';')
        commas = content_str.count(',')
        tabs = content_str.count('\t')
        
        # Calculate which delimiter is most likely based on relative frequency
        # and priority for different formats
        delimiter = ';'  # Default for most game logs
        
        if tabs > max(semicolons, commas) * 0.8:  # Tab is at least 80% as common as the most common delimiter
            delimiter = '\t'
            logger.info(f"Selected tab as delimiter based on frequency: {tabs} tabs")
        elif commas > semicolons * 1.5:  # Significantly more commas than semicolons
            delimiter = ','
            logger.info(f"Selected comma as delimiter based on frequency: {commas} commas vs {semicolons} semicolons")
        else:
            # Default to semicolon delimiter
            logger.info(f"Selected semicolon as delimiter based on frequency or default: {semicolons} semicolons")
            
        logger.info(f"Using delimiter '{delimiter}' for {file_path}")
        
        # Create CSV reader
        csv_reader = csv.reader(io.StringIO(content_str), delimiter=delimiter)
        
        # Parse events
        events = []
        row_count = 0
        
        for row in csv_reader:
            row_count += 1
            
            # CRITICAL FIX: Skip lines up to the start_line position
            if start_line > 0 and row_count <= start_line:
                continue
                
            # Skip empty rows or those without enough fields
            if not row or len(row) < 5:
                continue
                
            # Skip header rows
            if any(keyword in row[0].lower() for keyword in ['time', 'date', 'timestamp']):
                continue
                
            # Extract data from row
            try:
                event = {
                    'timestamp': row[0] if len(row) > 0 else "",
                    'killer_name': row[1] if len(row) > 1 else "",
                    'killer_id': row[2] if len(row) > 2 else "",
                    'victim_name': row[3] if len(row) > 3 else "",
                    'victim_id': row[4] if len(row) > 4 else "",
                    'weapon': row[5] if len(row) > 5 else "",
                    'distance': float(row[6]) if len(row) > 6 and row[6].strip() else 0.0,
                    'server_id': server_id,
                    'event_type': 'kill'
                }
            except Exception as e:
                logger.error(f"Error processing row: {e}")
                continue
            
            # Check for suicide (killer == victim)
            if event['killer_name'] == event['victim_name'] or event['killer_id'] == event['victim_id']:
                event['event_type'] = 'suicide'
                event['is_suicide'] = True
            else:
                event['is_suicide'] = False
                
            # Parse timestamp
            try:
                ts_str = event['timestamp']
                
                # Try various timestamp formats
                timestamp_formats = [
                    '%Y.%m.%d-%H.%M.%S',
                    '%Y.%m.%d-%H:%M:%S',
                    '%Y.%m.%d %H.%M.%S',
                    '%Y.%m.%d %H:%M:%S',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H.%M.%S',
                    '%m/%d/%Y %H:%M:%S',
                    '%d/%m/%Y %H:%M:%S'
                ]
                
                for fmt in timestamp_formats:
                    try:
                        dt = datetime.strptime(ts_str, fmt)
                        event['timestamp'] = dt
                        break
                    except ValueError:
                        continue
                        
                # If still a string, use current time
                if isinstance(event['timestamp'], str):
                    event['timestamp'] = datetime.now()
                    
            except Exception as e:
                logger.error(f"Error parsing timestamp: {e}")
                event['timestamp'] = datetime.now()
                
            # Add to events list
            events.append(event)
            
        logger.info(f"Directly parsed {len(events)} events from {row_count} rows in {file_path}")
        return events, row_count
        
    except Exception as e:
        logger.error(f"Error in direct CSV parsing of {file_path}: {e}")
        logger.error(traceback.format_exc())
        return [], 0

async def direct_import_events(db, events: List[Dict[str, Any]]) -> int:
    """
    Import events directly into the database.
    
    Args:
        db: Database connection
        events: List of events to import
        
    Returns:
        Number of events imported
    """
    if events is None:
        return 0
        
    logger.info(f"Directly importing {len(events)} events")
    
    try:
        result = await db.kills.insert_many(events)
        imported = len(result.inserted_ids)
        logger.info(f"Successfully imported {imported} events directly")
        return imported
    except Exception as e:
        logger.error(f"Error importing events: {e}")
        logger.error(traceback.format_exc())
        return 0

async def process_directory(db, server_id: str, days: int = 30) -> Tuple[int, int]:
    """
    Process all CSV files in the attached_assets directory recursively.
    
    Args:
        db: Database connection
        server_id: Server ID to use for the events
        days: Number of days to look back
        
    Returns:
        Tuple of (files_processed, events_imported)
    """
    logger.info(f"Processing CSV files from all directories for server {server_id}, looking back {days} days")
    
    if server_id is None:
        logger.error("No server ID provided")
        return 0, 0
    
    # Get the original server ID for directory patterns
    import motor.motor_asyncio  # Import here to avoid circular imports
    
    # First try to get the original_server_id from the server configuration
    original_server_id = None
    server_config = None
    
    try:
        # Look up the server in the database to get the original_server_id
        server_doc = await db.game_servers.find_one({"server_id": server_id})
        if server_doc is not None and "original_server_id" in server_doc:
            original_server_id = server_doc["original_server_id"]
            logger.info(f"Found original_server_id {original_server_id} from database for server {server_id}")
            server_config = server_doc
    except Exception as e:
        logger.error(f"Error finding original_server_id: {e}")
    
    # CRITICAL FIX: Search multiple locations for CSV files for maximum reliability
    base_dirs = [
        os.path.join(os.getcwd(), "attached_assets"),  # Local assets
        os.path.join(os.getcwd(), "zipbot_temp"),      # Temp directory for unzipped files
        os.path.join(os.getcwd(), f"zipbot_temp/{server_id}"),  # Server-specific temp directory
    ]
    
    # Add server-specific directories
    if original_server_id is not None:
        # Common format is hostname_serverid
        # Get hostname from config
        hostname = server_config.get("hostname") if server_config else None
        
        if hostname is not None:
            server_dir = f"{hostname}_{original_server_id}"
            logger.info(f"Using server directory: {server_dir}")
            
            # Add standard path patterns
            base_dirs.extend([
                os.path.join("/", server_dir),  # /hostname_serverid
                os.path.join("/", server_dir, "actual1", "deathlogs"),  # Most common path
                os.path.join("/", server_dir, "actual", "deathlogs"),
                os.path.join("/", server_dir, "Logs"),
                os.path.join("/", server_dir, "deathlogs"),
            ])
    
    # Look for directories with the server ID pattern
    for root_dir in [os.getcwd(), "/", "/home"]:
        if os.path.exists(root_dir):
            # Try with both UUID and original server ID
            for id_pattern in [server_id, original_server_id]:
                if id_pattern is None:
                    continue
                    
                # Pattern like hostname_serverid or *_serverid
                patterns = [f"*_{id_pattern}", f"*/{id_pattern}", f"*/{id_pattern}/*"]
                
                import glob
                for pattern in patterns:
                    matching_dirs = glob.glob(os.path.join(root_dir, pattern))
                    logger.info(f"Looking for directories matching pattern {pattern}, found {len(matching_dirs)} matches")
                    
                    for match in matching_dirs:
                        if os.path.isdir(match):
                            base_dirs.append(match)
                            logger.info(f"Adding directory: {match}")
                            
                            # Also check for standard game subdirectories
                            for subdir in ["actual1/deathlogs", "actual/deathlogs", "logs", "deathlogs", "world_0", "world_1"]:
                                path = os.path.join(match, subdir)
                                if os.path.exists(path) and os.path.isdir(path):
                                    base_dirs.append(path)
                                    logger.info(f"Adding subdirectory: {path}")
    
    # FIXED: Improved file discovery to handle more directory structures
    csv_files = []
    cutoff_date = datetime.now() - timedelta(days=days)
    
    # Remove duplicates from base_dirs while preserving order
    unique_base_dirs = []
    for directory in base_dirs:
        if directory not in unique_base_dirs:
            # Check if directory exists
            if os.path.exists(directory):
                unique_base_dirs.append(directory)
                logger.info(f"Adding valid directory: {directory}")
            else:
                logger.warning(f"Skipping non-existent directory: {directory}")
    
    logger.info(f"Searching for CSV files in {len(unique_base_dirs)} base directories")
    
    # If we don't have any valid directories, try to search common locations
    if len(unique_base_dirs) == 0:
        logger.warning("No valid directories found, trying common locations")
        common_locations = [
            os.path.join(os.getcwd()),  # Current directory
            os.path.join(os.getcwd(), "attached_assets"),  # Local assets
            os.path.join(os.getcwd(), "log"),  # Common log directory
            os.path.join(os.getcwd(), "logs"),  # Common logs directory
            os.path.join(os.getcwd(), "data"),  # Common data directory
            "/var/log",  # System logs
            "/var/log/game",  # Game logs
            "/srv/game/logs",  # Game server logs
        ]
        
        for location in common_locations:
            if os.path.exists(location) and os.path.isdir(location):
                unique_base_dirs.append(location)
                logger.info(f"Added common location: {location}")
    
    # Add the server_id as a possible subdirectory name to check
    if server_id is not None and not server_id.startswith('/'):
        for root_dir in unique_base_dirs.copy():
            potential_server_dir = os.path.join(root_dir, server_id)
            if os.path.exists(potential_server_dir) and os.path.isdir(potential_server_dir):
                unique_base_dirs.append(potential_server_dir)
                logger.info(f"Added server-specific directory: {potential_server_dir}")
    
    # Debug: List all directories we're going to search
    for idx, dir in enumerate(unique_base_dirs):
        logger.info(f"Search directory #{idx+1}: {dir}")
    
    # Function to check if a file is already in our list (avoid duplicates)
    def is_duplicate_file(file_path):
        for existing_file in csv_files:
            if os.path.samefile(file_path, existing_file):
                return True
        return False
    
    for base_dir in unique_base_dirs:
        if not os.path.exists(base_dir):
            logger.warning(f"Directory {base_dir} does not exist, skipping")
            continue
            
        logger.info(f"Searching directory: {base_dir}")
        try:
            # Walk through the directory and all subdirectories
            for root, dirs, files in os.walk(base_dir):
                # Log only if we find files or this is one of known important subdirectories
                important_subdir = any(keyword in root.lower() for keyword in ['world_', 'deathlogs', 'actual'])
                csv_in_dir = any(f.endswith('.csv') for f in files)
                
                if csv_in_dir or important_subdir:
                    logger.info(f"Searching in directory: {root} - contains {len(files)} files, {sum(1 for f in files if f.endswith('.csv'))} CSV files")
                
                # Always look for subdirectories that match our patterns
                for subdir in dirs:
                    # Skip hidden subdirectories
                    if subdir.startswith('.'):
                        continue
                        
                    # Check for important game subdirectories
                    if any(pattern in subdir.lower() for pattern in ['world_', 'map_', 'deathlogs', 'logs', 'actual']):
                        subdir_path = os.path.join(root, subdir)
                        logger.info(f"Found important subdirectory: {subdir_path}")
                        
                    # Check specifically for numbered world directories (world_0, world_1, etc.)
                    elif re.match(r'world_\d+', subdir.lower()):
                        subdir_path = os.path.join(root, subdir)
                        logger.info(f"Found numbered world directory: {subdir_path}")
                
                # FIXED: Enhanced file discovery with better format support
                for filename in files:
                    # Check for various file formats that might contain CSV data
                    is_csv_file = (
                        # Standard formats
                        filename.lower().endswith('.csv') or
                        
                        # Alternative formats that might contain CSV data
                        (filename.lower().endswith('.log') and "death" in filename.lower()) or
                        (filename.lower().endswith('.txt') and "kill" in filename.lower()) or
                        
                        # Check for date patterns common in game logs
                        (re.search(r'\d{4}[.-]\d{2}[.-]\d{2}', filename) and not filename.lower().endswith('.zip'))
                    )
                    
                    if is_csv_file is None:
                        continue
                    
                    full_path = os.path.join(root, filename)
                    
                    # Skip if we've already found this file
                    try:
                        if is_duplicate_file(full_path):
                            logger.debug(f"Skipping duplicate file: {full_path}")
                            continue
                    except:
                        # If the comparison fails (e.g., cross-device), compare paths
                        if full_path in csv_files:
                            continue
                    
                    # First try to extract date using the full timestamp pattern (YYYY.MM.DD-HH.MM.SS)
                    date_match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})-(\d{2})\.(\d{2})\.(\d{2})', filename)
                    
                    if date_match is not None:
                        # Full timestamp pattern found
                        year, month, day, hour, minute, second = map(int, date_match.groups())
                        try:
                            file_date = datetime(year, month, day, hour, minute, second)
                            logger.info(f"Found CSV file: {full_path} (datetime: {file_date.strftime('%Y-%m-%d %H:%M:%S')})")
                        except ValueError:
                            # If parsing fails, try just the date part
                            try:
                                file_date = datetime(year, month, day)
                                logger.info(f"Found CSV file: {full_path} (date only: {file_date.strftime('%Y-%m-%d')})")
                            except ValueError:
                                # Still failed, include file anyway
                                logger.info(f"Found CSV file: {full_path} (datetime parsing failed)")
                                file_date = None
                    else:
                        # Try date-only pattern (YYYY.MM.DD)
                        date_only_match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', filename)
                        if date_only_match is not None:
                            year, month, day = map(int, date_only_match.groups())
                            try:
                                file_date = datetime(year, month, day)
                                logger.info(f"Found CSV file: {full_path} (date: {file_date.strftime('%Y-%m-%d')})")
                            except ValueError:
                                # If date parsing fails, still include file
                                logger.info(f"Found CSV file: {full_path} (date parsing failed)")
                                file_date = None
                        else:
                            # No recognizable date pattern
                            logger.info(f"Found CSV file: {full_path} (no date in filename)")
                            file_date = None
                    
                    # FIXED: Safer date filtering to avoid missing important files
                    # Instead of strict date filtering, we'll include all files by default
                    # but use dates for sorting/prioritization
                    include_file = True
                    
                    # Log date information for debugging but don't use it to exclude files
                    if file_date is not None and cutoff_date is not None:
                        if file_date < cutoff_date:
                            logger.info(f"File date {file_date} is older than cutoff {cutoff_date}, but including it anyway: {os.path.basename(full_path)}")
                        else:
                            logger.info(f"File date {file_date} is newer than cutoff {cutoff_date}: {os.path.basename(full_path)}")
                    
                    # Always add file to processing list regardless of date
                    if include_file is not None:
                        csv_files.append(full_path)
                    
                    # Line removed - file is already added in the conditional block above
        except Exception as e:
            logger.error(f"Error searching directory {base_dir}: {e}")
            logger.error(traceback.format_exc())
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Process each file
    files_processed = 0
    events_imported = 0
    
    for file_path in csv_files:
        # CRITICAL FIX: Check if file actually exists before processing
        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist, skipping")
            continue
            
        # Check file size
        try:
            if os.path.getsize(file_path) == 0:
                logger.error(f"Empty file: {file_path}")
                continue
        except Exception as e:
            logger.error(f"Error checking file size: {e}")
            continue
        
        # FIXED: Improved parsing with better error handling and logging
        try:
            # Parse events - unpack tuple return value (events, line_count)
            events, line_count = direct_parse_csv_file(file_path, server_id)
            
            if events is not None:
                logger.info(f"Successfully parsed {len(events)} events from {os.path.basename(file_path)} ({line_count} total lines)")
                
                # Import events with better error handling
                try:
                    imported = await direct_import_events(db, events)
                    if imported > 0:
                        files_processed += 1
                        events_imported += imported
                        logger.info(f"Successfully imported {imported} events from {os.path.basename(file_path)}")
                    else:
                        logger.warning(f"No events were imported from {os.path.basename(file_path)} despite successful parsing")
                        # Count the file as processed even if no events were imported
                        files_processed += 1
                except Exception as import_error:
                    logger.error(f"Error importing events from {os.path.basename(file_path)}: {import_error}")
                    # Try to continue with other files
            else:
                if line_count > 0:
                    logger.warning(f"File {os.path.basename(file_path)} has {line_count} lines but no valid events were parsed")
                else:
                    logger.warning(f"No valid content found in {os.path.basename(file_path)}")
                    
                # Count this as processed to avoid reprocessing the same empty file
                files_processed += 1
        except Exception as parse_error:
            logger.error(f"Error parsing file {os.path.basename(file_path)}: {parse_error}")
            # Continue with other files
    
    logger.info(f"Direct processing complete: processed {files_processed} files, imported {events_imported} events")
    return files_processed, events_imported

async def update_player_stats(db, server_id: str) -> int:
    """
    Update player statistics based on kill events.
    
    Args:
        db: Database connection
        server_id: Server ID to update stats for
        
    Returns:
        Number of players updated
    """
    logger.info(f"Updating player statistics for server {server_id}")
    
    try:
        # Get all kill events for this server
        kill_cursor = db.kills.find({"server_id": server_id})
        
        # Group by player
        player_stats = {}
        
        async for event in kill_cursor:
            killer_id = event.get('killer_id')
            killer_name = event.get('killer_name')
            victim_id = event.get('victim_id')
            victim_name = event.get('victim_name')
            is_suicide = event.get('is_suicide', False)
            
            if not killer_id or not victim_id:
                continue
                
            # Update killer stats
            if killer_id not in player_stats:
                player_stats[killer_id] = {
                    'player_id': killer_id,
                    'name': killer_name,
                    'server_id': server_id,
                    'kills': 0,
                    'deaths': 0,
                    'suicides': 0
                }
                
            # Update victim stats
            if victim_id not in player_stats:
                player_stats[victim_id] = {
                    'player_id': victim_id,
                    'name': victim_name,
                    'server_id': server_id,
                    'kills': 0,
                    'deaths': 0,
                    'suicides': 0
                }
                
            if is_suicide is not None:
                player_stats[killer_id]['suicides'] += 1
                player_stats[killer_id]['deaths'] += 1
            else:
                player_stats[killer_id]['kills'] += 1
                player_stats[victim_id]['deaths'] += 1
        
        # Update player documents
        updated_count = 0
        
        for player_id, stats in player_stats.items():
            # Try to find existing player
            player = await db.players.find_one({
                'server_id': server_id,
                'player_id': player_id
            })
            
            if player is not None:
                # Update existing player
                result = await db.players.update_one(
                    {'_id': player['_id']},
                    {
                        '$set': {
                            'name': stats['name'],
                            'kills': stats['kills'],
                            'deaths': stats['deaths'],
                            'suicides': stats['suicides'],
                            'updated_at': datetime.now()
                        }
                    }
                )
                
                if result.modified_count > 0:
                    updated_count += 1
            else:
                # Create new player
                stats['created_at'] = datetime.now()
                stats['updated_at'] = datetime.now()
                
                result = await db.players.insert_one(stats)
                if result.inserted_id is not None:
                    updated_count += 1
        
        logger.info(f"Updated {updated_count} players for server {server_id}")
        return updated_count
        
    except Exception as e:
        logger.error(f"Error updating player statistics: {e}")
        logger.error(traceback.format_exc())
        return 0

async def update_rivalries(db, server_id: str) -> int:
    """
    Update rivalries based on kill events.
    
    Args:
        db: Database connection
        server_id: Server ID to update rivalries for
        
    Returns:
        Number of rivalries updated
    """
    logger.info(f"Updating rivalries for server {server_id}")
    
    try:
        # Get non-suicide kill events
        kill_cursor = db.kills.find({
            "server_id": server_id,
            "is_suicide": {"$ne": True}
        })
        
        # Count kills between players
        rivalry_counts = {}
        
        async for event in kill_cursor:
            killer_id = event.get('killer_id')
            killer_name = event.get('killer_name')
            victim_id = event.get('victim_id')
            victim_name = event.get('victim_name')
            
            if not killer_id or not victim_id:
                continue
                
            rivalry_key = f"{killer_id}:{victim_id}"
            
            if rivalry_key not in rivalry_counts:
                rivalry_counts[rivalry_key] = {
                    'killer_id': killer_id,
                    'killer_name': killer_name,
                    'victim_id': victim_id,
                    'victim_name': victim_name,
                    'server_id': server_id,
                    'kills': 0
                }
                
            rivalry_counts[rivalry_key]['kills'] += 1
        
        # Update rivalry documents
        updated_count = 0
        
        for key, data in rivalry_counts.items():
            # Skip if no kills
            if data['kills'] == 0:
                continue
                
            # Try to find existing rivalry
            rivalry = await db.rivalries.find_one({
                'server_id': server_id,
                'killer_id': data['killer_id'],
                'victim_id': data['victim_id']
            })
            
            if rivalry is not None:
                # Update existing rivalry
                result = await db.rivalries.update_one(
                    {'_id': rivalry['_id']},
                    {
                        '$set': {
                            'killer_name': data['killer_name'],
                            'victim_name': data['victim_name'],
                            'kills': data['kills'],
                            'updated_at': datetime.now()
                        }
                    }
                )
                
                if result.modified_count > 0:
                    updated_count += 1
            else:
                # Create new rivalry
                data['created_at'] = datetime.now()
                data['updated_at'] = datetime.now()
                
                result = await db.rivalries.insert_one(data)
                if result.inserted_id is not None:
                    updated_count += 1
        
        logger.info(f"Updated {updated_count} rivalries for server {server_id}")
        return updated_count
        
    except Exception as e:
        logger.error(f"Error updating rivalries: {e}")
        logger.error(traceback.format_exc())
        return 0