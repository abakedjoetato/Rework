"""
CSV Parser for the Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. CSV file parsing with robust error handling
2. Streaming event extraction for large files
3. Resilient log processing with format detection
4. Fault-tolerant statistics aggregation
5. Cross-platform statistics for post-April format
"""
import csv
import io
import re
import logging
import traceback
import os # Added import for os.path.join
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Set, Tuple, BinaryIO, TextIO, Iterator, Generator

logger = logging.getLogger(__name__)

class CSVParser:
    """Enhanced CSV file parser for game log files with robust error handling"""

    # Define standard log formats with fallbacks for format variations
    LOG_FORMATS = {
        "deadside": {
            "separator": ";",
            "columns": ["timestamp", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance", "platform"],
            "datetime_format": "%Y.%m.%d-%H.%M.%S",  # Format matching 2025.03.27-10.42.18
            "datetime_column": "timestamp",
            "fallback_formats": [
                # Pre-April format (7-field CSV)
                {
                    "separator": ";",
                    "columns": ["timestamp", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance"]
                },
                # Post-April format (9-field CSV with console information)
                {
                    "separator": ";",
                    "columns": ["timestamp", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance", "killer_console", "victim_console"]
                },
                # Alternative separators and columns
                {
                    "separator": ",",
                    "columns": ["timestamp", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance", "platform"]
                },
                {
                    "separator": ";",
                    "columns": ["date", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance", "platform"],
                    "datetime_column": "date"
                },
                # Alternative datetime formats
                {
                    "separator": ";",
                    "columns": ["timestamp", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance", "platform"],
                    "datetime_format": "%Y-%m-%d %H:%M:%S"
                }
            ],
            "required_columns": ["killer_name", "victim_name", "weapon"]  # Absolute minimum required
        },
        "custom": {
            "separator": ",",
            "columns": ["timestamp", "event_type", "player1_name", "player1_id", "player2_name", "player2_id", "details", "location"],
            "datetime_format": "%Y-%m-%d %H:%M:%S",
            "datetime_column": "timestamp"
        }
    }

    def __init__(self, format_name: str = "deadside", hostname: Optional[str] = None, server_id: Optional[str] = None):
        """Initialize CSV parser with specified format and server info

        Args:
            format_name: Log format name (default: "deadside")
            hostname: Server hostname
            server_id: Original server ID (not UUID)
        """
        self.format_name = format_name
        if hostname and server_id:
            # Use standardized CSV path structure
            clean_hostname = hostname.split(':')[0] if hostname else "server"
            self.base_path = os.path.join("/", f"{clean_hostname}_{server_id}", "actual1", "deathlogs")
        else:
            self.base_path = None

        # Get format configuration
        if format_name in self.LOG_FORMATS:
            self.format_config = self.LOG_FORMATS[format_name]
        else:
            # Default to deadside format
            logger.warning(f"Unknown log format: {format_name}, using deadside format")
            self.format_name = "deadside"
            self.format_config = self.LOG_FORMATS["deadside"]

        # Extract configuration
        self.separator = self.format_config["separator"]
        self.columns = self.format_config["columns"]
        self.datetime_format = self.format_config["datetime_format"]
        self.datetime_column = self.format_config["datetime_column"]
        
        # Added attribute to track last detected delimiter for testing and diagnostics
        self.last_detected_delimiter = None

        # Initialize format detection state
        self.detected_format_info = {"april_update": False}  # Default to pre-April format

        # Initialize caches
        self._event_cache = {}
        self._player_stats_cache = {}
        
        # Track last processed line for each file
        self._last_processed_line_count = {}  # {file_path: line_count}

    def clear_cache(self):
        """Clear all parser caches"""
        self._event_cache = {}
        self._player_stats_cache = {}
        self._last_processed_line_count = {}  # Reset line tracking
        logger.info("CSV parser cache cleared")

    def parse_csv_data(self, data: Union[str, bytes, memoryview], delimiter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Parse CSV data and return list of events

        Args:
            data: CSV data string, bytes, or memoryview
            delimiter: Optional delimiter to use for CSV parsing (overrides auto-detection)

        Returns:
            List[Dict]: List of parsed event dictionaries
        """
        # Check for empty data
        if not data or (isinstance(data, str) and not data.strip()):
            logger.warning(f"Empty or blank CSV data provided")
            return []
            
        # Convert bytes, memoryview, or other types to string for processing
        if not isinstance(data, str):
            try:
                if isinstance(data, memoryview):
                    try:
                        data = bytes(data).decode("utf-8", errors="replace")
                    except UnicodeDecodeError:
                        data = bytes(data).decode("latin-1", errors="replace")
                    logger.debug(f"Converted memoryview to string")
                elif isinstance(data, bytes):
                    try:
                        data = data.decode("utf-8", errors="replace")
                    except UnicodeDecodeError:
                        # Fallback to latin-1 encoding if UTF-8 fails
                        data = data.decode("latin-1", errors="replace")
                    logger.debug("Converted bytes to string")
                else:
                    data = str(data)
                    logger.warning(f"Converted non-string data of type {type(data).__name__} to string")
            except Exception as e:
                logger.error(f"Cannot convert data to string: {e}")
                return []
            
        # Log a small sample of the data for debugging
        sample_text = data[:200] + "..." if len(data) > 200 else data
        logger.info(f"CSV data sample (first 200 chars): {sample_text}")
        
        # Quick check to see if this looks like CSV data
        sample_chunk = data[:500] if len(data) > 500 else data
        if ";" in sample_chunk or "," in sample_chunk:
            logger.info(f"Data appears to be CSV format (found delimiters)")
        else:
            logger.warning(f"Data might not be CSV format (no common delimiters found in first 500 chars)")

        # Create CSV reader
        csv_file = io.StringIO(data)

        # Parse CSV data
        try:
            logger.info(f"Starting CSV parsing with file size: {len(data)} characters")
            events = self._parse_csv_file(csv_file, delimiter=delimiter)
            logger.info(f"Successfully parsed {len(events)} events from CSV data")
            
            # Log a sample of the parsed events
            if events is not None:
                sample_event = str(events[0])[:200] + "..." if len(str(events[0])) > 200 else str(events[0])
                logger.info(f"Sample parsed event: {sample_event}")
                
            return events
        except Exception as e:
            logger.error(f"Error parsing CSV data: {e}f")
            return []
        finally:
            csv_file.close()

    def parse_csv_file(self, file_path: str, only_new_lines: bool = False, delimiter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Parse CSV file and return list of events

        Args:
            file_path: Path to CSV file
            only_new_lines: If True, only parse lines that haven't been processed before
            delimiter: Optional delimiter to use for CSV parsing (overrides auto-detection)

        Returns:
            List[Dict]: List of parsed event dictionaries
        """
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                return self._parse_csv_file(file, file_path=file_path, only_new_lines=only_new_lines, delimiter=delimiter)
        except UnicodeDecodeError:
            # Try with different encoding
            with open(file_path, "r", encoding="latin-1") as file:
                return self._parse_csv_file(file, file_path=file_path, only_new_lines=only_new_lines, delimiter=delimiter)

    def _parse_csv_file(self, file: Union[TextIO, BinaryIO], file_path: Optional[str] = None, only_new_lines: bool = False, delimiter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Parse CSV file and return list of events

        Args:
            file: File-like object (can be text or binary)
            file_path: Path to the file (used for tracking processed lines)
            only_new_lines: If True, only parse lines that haven't been processed before
            delimiter: Optional delimiter to use for CSV parsing (overrides auto-detection)

        Returns:
            List[Dict]: List of parsed event dictionaries
        """
        # Use provided delimiter if specified
        if delimiter is not None:
            self.delimiter = delimiter
            logger.info(f"Using provided delimiter: '{delimiter}'")
        # Handle possible None input
        if file is None:
            logger.error(f"Null file object provided to CSV parser for {file_path or 'unknown path'}")
            return []
            
        # Check if file object has read method
        if not hasattr(file, 'read') or not callable(getattr(file, 'read')):
            logger.error(f"Invalid file object provided (no read method) for {file_path or 'unknown path'}")
            return []
            
        # Check for empty file by trying to read a sample
        try:
            file_content = file.read(4096)  # Read a larger sample to detect content
            
            # Reset file position for subsequent reads
            if hasattr(file, 'seek') and callable(getattr(file, 'seek')):
                file.seek(0)
            else:
                logger.error(f"File object does not support seek operation, cannot process {file_path or 'unknown path'}")
                return []
                
            # Enhanced binary content handling with robust fallbacks
            # First check if it's binary and convert accordingly
            if isinstance(file_content, bytes):
                try:
                    # Try UTF-8 first with replacing invalid chars
                    file_content = file_content.decode('utf-8', errors='replace')
                    logger.info(f"Converted binary content to UTF-8 text for {file_path or 'unknown path'}")
                except Exception as e:
                    logger.warning(f"UTF-8 decode failed: {e}, trying Latin-1 as fallback")
                    try:
                        # Latin-1 will always succeed as it maps all 256 byte values
                        file_content = file_content.decode('latin-1')
                        logger.info(f"Converted binary content using Latin-1 fallback for {file_path or 'unknown path'}")
                    except Exception as e2:
                        logger.error(f"All decoding attempts failed: {e2}")
                        return []
            
            # Handle memoryview objects (common in some I/O operations)
            elif isinstance(file_content, memoryview):
                try:
                    # Convert memoryview to bytes first
                    bytes_content = file_content.tobytes()
                    # Then decode to string using UTF-8
                    file_content = bytes_content.decode('utf-8', errors='replace')
                    logger.info(f"Converted memoryview to text for {file_path or 'unknown path'}")
                except Exception as e:
                    # Try Latin-1 as fallback
                    try:
                        bytes_content = file_content.tobytes()
                        file_content = bytes_content.decode('latin-1')
                        logger.info(f"Converted memoryview using Latin-1 fallback for {file_path or 'unknown path'}")
                    except Exception as e2:
                        logger.error(f"Failed to convert memoryview: {e2}")
                        return []
                    
            # Handle other non-string types with better error reporting
            elif not isinstance(file_content, str):
                try:
                    logger.warning(f"Unexpected content type: {type(file_content)} for {file_path or 'unknown path'}")
                    file_content = str(file_content)
                    logger.info(f"Successfully converted {type(file_content)} to string")
                except Exception as e:
                    logger.error(f"Cannot convert content of type {type(file_content)} to string: {e}")
                    return []
        except Exception as e:
            logger.error(f"Error reading from file: {e}f")
            return []
        
        # Enhanced empty file detection
        # Check multiple conditions to properly detect empty files
        is_empty = False
        
        # Check if content is None or empty string
        if file_content is None or file_content == '':
            is_empty = True
            logger.warning(f"Empty CSV file detected (no content): {file_path or 'unknown'}")
        # Check if content contains only whitespace
        elif isinstance(file_content, str) and not file_content.strip():
            is_empty = True
            logger.warning(f"Empty CSV file detected (only whitespace): {file_path or 'unknown'}")
        # Check if content is too short to be meaningful (less than 10 chars)
        elif isinstance(file_content, str) and len(file_content.strip()) < 10:
            logger.warning(f"Nearly empty CSV file detected (less than 10 chars): {file_path or 'unknown'}")
            # Log the actual content for debugging
            logger.debug(f"File content: '{file_content}'")
            
            # Check if short content is actually a valid row
            if (',' in file_content or ';' in file_content) and len(file_content.strip().split(',')) >= 3:
                logger.info(f"Short content appears to be a valid CSV row, will attempt to parse")
            else:
                is_empty = True
        # For binary content, check length
        elif isinstance(file_content, bytes) and len(file_content) < 10:
            is_empty = True
            logger.warning(f"Empty binary CSV file detected (less than 10 bytes): {file_path or 'unknown'}")
            
        if is_empty:
            logger.info(f"Skipping empty file: {file_path or 'unknown'}")
            return []
            
        # Count occurrences of potential delimiters
        delimiters = {';': 0, ',': 0, '\t': 0, '|': 0}  # Added pipe as another possible delimiter
        
        # Ensure file_content is a string for delimiter counting
        if isinstance(file_content, bytes):
            try:
                file_content_for_count = file_content.decode('utf-8', errors='replace')
            except Exception:
                file_content_for_count = file_content.decode('latin-1', errors='replace')
        else:
            file_content_for_count = file_content
            
        for d in delimiters:
            delimiters[d] = file_content_for_count.count(d)
        
        # Add extra weight to semicolons to handle mixed format files better
        # Game logs commonly use semicolons and we want to prioritize them
        if delimiters.get(';', 0) > 0:
            original_count = delimiters[';']
            # Calculate boost factor using integer math to avoid float conversion
            delimiters[';'] = int(original_count * 3)  # Boost semicolons by 200% in detection (increased from 100%)
            logger.debug(f"Boosting semicolon count from {original_count} to {delimiters[';']}")
            
        # If the file has a .csv filename but uses semicolons, boost semicolon weight further
        if file_path and file_path.lower().endswith('.csv') and delimiters.get(';', 0) > 0:
            logger.debug("File has .csv extension but contains semicolons, boosting semicolon detection")
            delimiters[';'] += 20
        
        # Check for patterns that strongly indicate semicolon delimiter
        # Look for multiple sequential semicolons which often indicate empty fields in semicolon-delimited files
        if ';;' in file_content_for_count or ';;;' in file_content_for_count:
            logger.debug("Found multiple sequential semicolons, strongly indicates semicolon delimiter")
            delimiters[';'] += 50  # Add a strong bonus for this pattern
        
        # Check for patterns that indicate comma delimiter
        # In comma-delimited files, text fields are often quoted
        if file_content_for_count.count('","') > 5:
            logger.debug("Found quoted comma patterns, indicates comma delimiter")
            delimiters[','] += 20
        
        # Sample line analysis for delimiter determination
        try:
            # Use the decoded content for line analysis
            sample_lines = file_content_for_count.split('\n')[:10]  # Analyze more lines
            line_scores = {';': 0, ',': 0, '\t': 0, '|': 0}
            
            for line in sample_lines:
                if not line.strip():
                    continue
                
                # Check which delimiter produces the most consistent field counts
                for d in delimiters.keys():
                    if d in line:
                        fields = line.split(d)
                        # More fields generally means this is the right delimiter
                        if len(fields) > 1:
                            line_scores[d] += len(fields)
            
            # Add these scores to our delimiter counts
            for d, score in line_scores.items():
                if score > 0:
                    logger.debug(f"Adding line analysis score for '{d}': {score}")
                    delimiters[d] += score
        except Exception as e:
            logger.warning(f"Error during line analysis for delimiter detection: {e}")
        
        # Use the most frequent delimiter if it appears significantly
        best_delimiter = self.separator  # Default
        max_count = delimiters.get(self.separator, 0)
        
        for d, count in delimiters.items():
            if count > max_count:
                max_count = count
                best_delimiter = d
        
        # If no delimiters found at all, this might not be a valid CSV
        if max_count == 0:
            logger.warning(f"No valid delimiters found in file: {file_path or 'unknown'} - content might not be CSV")
            # We'll still try to parse with default separator, but log the warning
            
        logger.debug(f"Detected delimiter: \'{best_delimiter}\' (counts: {delimiters})")
        
        # Store the detected delimiter for testing and diagnostics
        self.last_detected_delimiter = best_delimiter
        
        # Also update the active separator for this parsing session
        # This ensures consistent delimiter usage throughout the processing
        self.separator = best_delimiter
        
        # Log first few lines for debugging
        sample_lines = file_content_for_count.split('\n')[:5]
        logger.info(f"Sample content from {file_path or 'unknown'} (first 5 lines):")
        for idx, line in enumerate(sample_lines):
            logger.info(f"Line {idx+1}: {line[:100]}{'...' if len(line) > 100 else ''}")
        
        # Reset file position
        file.seek(0)
        
        # Convert file content to string if it's not already
        if not isinstance(file_content, str):
            try:
                file_content_str = file_content.decode('utf-8', errors='replace')
            except Exception as e:
                logger.error(f"Error converting binary content to string: {e}")
                file_content_str = str(file_content)
        else:
            file_content_str = file_content
            
        # Always use StringIO with our already prepared file_content_str to ensure consistent behavior
        # This avoids issues with BinaryIO vs TextIO in the csv module
        try:
            text_file = io.StringIO(file_content_str)
            csv_reader = csv.reader(text_file, delimiter=best_delimiter)
        except Exception as e:
            logger.error(f"Error creating CSV reader: {e}")
            return []
        
        # Skip header row if present
        first_row = next(csv_reader, None)
        
        # Check if first row is header
        is_header = False
        if first_row is not None:
            # Check if first row might be column names (contains strings, not timestamps)
            first_cell = first_row[0] if first_row and len(first_row) > 0 else ""
            
            # If first field doesn't look like a date/timestamp, it might be a header
            if not re.match(r'\d{4}[-./]\d{2}[-./]\d{2}', first_cell):
                logger.info(f"First row appears to be a header: {first_row}")
                is_header = True
        
        # Reset reader if first row is not a header
        if first_row is not None and not is_header:
            # Create a new reader from the start of the content
            text_file = io.StringIO(file_content_str)
            csv_reader = csv.reader(text_file, delimiter=best_delimiter)

        # Get or initialize line counter for this file
        last_processed_line = 0
        if file_path and only_new_lines and file_path in self._last_processed_line_count:
            last_processed_line = self._last_processed_line_count[file_path]
            logger.info(f"Starting processing from line {last_processed_line} in file {file_path}")
        else:
            logger.info(f"Processing entire file {file_path or 'unknown'} (only_new_lines={only_new_lines})")
        
        # Parse rows
        events = []
        current_line = 0
        
        try:
            for row in csv_reader:
                current_line += 1
                
                # Skip already processed lines if only_new_lines is True
                if only_new_lines and current_line <= last_processed_line:
                    continue
                    
                # Skip empty rows
                if row is None:
                    logger.debug(f"Skipping empty row at line {current_line}")
                    continue
                    
                # More permissive handling of rows with insufficient fields
                if len(row) < 7:  # Standard kill event has 7-9 fields (7 for pre-April, 9 for post-April)
                    logger.warning(f"Row {current_line} has fewer fields than expected ({len(row)} < 7): {row}")
                    
                    # Try to extract whatever data we can anyway
                    if len(row) >= 3:
                        logger.debug(f"Attempting partial extraction from incomplete row: {row}")
                        
                        # Make best guess at field assignment for incomplete rows
                        # For very short rows (3-5 fields), try to intelligently map the fields
                        if len(row) == 3:
                            # Assume killer, victim, weapon format
                            row = [datetime.now().strftime(self.datetime_format), row[0], "", row[1], "", row[2], "0", ""]
                            logger.debug(f"Expanded 3-field row to: {row}")
                        elif len(row) == 4:
                            # Assume timestamp, killer, victim, weapon format
                            row = [row[0], row[1], "", row[2], "", row[3], "0", ""]
                            logger.debug(f"Expanded 4-field row to: {row}")
                        elif len(row) == 5:
                            # Assume timestamp, killer, killer_id, victim, weapon
                            row = [row[0], row[1], row[2], row[3], "", row[4], "0", ""]
                            logger.debug(f"Expanded 5-field row to: {row}")
                        elif len(row) == 6:
                            # Assume timestamp, killer, killer_id, victim, victim_id, weapon
                            row = [row[0], row[1], row[2], row[3], row[4], row[5], "0", ""]
                            logger.debug(f"Expanded 6-field row to: {row}")
                    else:
                        # Too few fields to reasonably process
                        logger.debug(f"Skipping row with too few fields: {row}")
                        continue
                    
                logger.debug(f"Processing row {current_line} with {len(row)} fields: {row[:3]}...")
                
                # Process the row data
                
                # Check if this might be a pre-April (7 columns) or post-April (9 columns) format
                row_format = None
                if len(row) >= 9:  # Has console fields - post-April format
                    row_format = "post_april"
                elif len(row) >= 7:  # Basic kill event - pre-April format
                    row_format = "pre_april"
                else:
                    # Unrecognized format, but we'll still try to parse with defaults
                    row_format = "unknown"
                
                # Create event dictionary
                event = {}
                for i, column in enumerate(self.columns):
                    if i < len(row):
                        event[column] = row[i].strip()
                    else:
                        # For missing fields, use appropriate defaults based on the field
                        if column in ("killer_console", "victim_console") and row_format == "pre_april":
                            event[column] = "Unknown"  # Pre-April format doesn't include console info
                        else:
                            event[column] = ""
                
                # Log format detection for debugging - uncomment if needed
                # logger.debug(f"Parsed row with format {row_format}, fields: {len(row)}, event keys: {list(event.keys())}")

                # Convert datetime column with multiple format support
                if self.datetime_column in event:
                    try:
                        # Try primary format first
                        event[self.datetime_column] = datetime.strptime(
                            event[self.datetime_column], 
                            self.datetime_format
                        )
                    except (ValueError, TypeError):
                        # Try alternative formats if primary fails
                        timestamp_str = event[self.datetime_column]
                        parsed = False

                        # Enhanced set of common timestamp formats with comprehensive coverage
                        alternative_formats = [
                            # Primary formats with dots (Deadside standard format)
                            "%Y.%m.%d-%H.%M.%S",      # 2025.03.27-10.42.18 (primary format)
                            "%Y.%m.%d-%H:%M:%S",      # 2025.05.09-11:58:37 (variant with colons)
                            "%Y.%m.%d-%H.%M.%S.%f",   # 2025.03.27-10.42.18.123 (with milliseconds)
                            
                            # Formats with space instead of dash
                            "%Y.%m.%d %H.%M.%S",      # 2025.05.09 11.58.37 (space instead of dash)
                            "%Y.%m.%d %H:%M:%S",      # 2025.05.09 11:58:37
                            "%Y.%m.%d %H.%M.%S.%f",   # 2025.05.09 11.58.37.123 (with milliseconds)
                            
                            # Hyphen formats (ISO 8601 style)
                            "%Y-%m-%d-%H.%M.%S",      # 2025-05-09-11.58.37
                            "%Y-%m-%d %H:%M:%S",      # 2025-05-09 11:58:37
                            "%Y-%m-%d-%H:%M:%S",      # 2025-05-09-11:58:37
                            "%Y-%m-%d %H.%M.%S",      # 2025-05-09 11.58.37
                            "%Y-%m-%dT%H:%M:%S",      # 2025-05-09T11:58:37 (ISO format)
                            "%Y-%m-%dT%H:%M:%S.%f",   # 2025-05-09T11:58:37.123 (ISO with ms)
                            "%Y-%m-%dT%H:%M:%SZ",     # 2025-05-09T11:58:37Z (ISO with UTC)
                            "%Y-%m-%dT%H:%M:%S.%fZ",  # 2025-05-09T11:58:37.123Z (ISO with ms and UTC)
                            
                            # Slash formats (US style)
                            "%Y/%m/%d %H:%M:%S",      # 2025/05/09 11:58:37
                            "%Y/%m/%d-%H:%M:%S",      # 2025/05/09-11:58:37
                            "%Y/%m/%d %H.%M.%S",      # 2025/05/09 11.58.37
                            "%m/%d/%Y %H:%M:%S",      # 05/09/2025 11:58:37 (US format)
                            
                            # Day first formats (European style)
                            "%d.%m.%Y-%H.%M.%S",      # 09.05.2025-11.58.37
                            "%d.%m.%Y %H:%M:%S",      # 09.05.2025 11:58:37
                            "%d-%m-%Y %H:%M:%S",      # 09-05-2025 11:58:37
                            "%d/%m/%Y %H:%M:%S",      # 09/05/2025 11:58:37
                            
                            # Compact formats (no separators)
                            "%Y%m%d-%H%M%S",          # 20250509-115837 (compact format)
                            "%Y%m%d_%H%M%S",          # 20250509_115837 (underscore format)
                            "%Y%m%d%H%M%S",           # 20250509115837 (fully compact)
                            
                            # Month name formats
                            "%b %d %Y %H:%M:%S",      # May 09 2025 11:58:37
                            "%d %b %Y %H:%M:%S",      # 09 May 2025 11:58:37
                            "%B %d %Y %H:%M:%S",      # May 09 2025 11:58:37 (full month name)
                            "%d %B %Y %H:%M:%S",      # 09 May 2025 11:58:37 (full month name)
                            
                            # 12-hour clock formats
                            "%Y-%m-%d %I:%M:%S %p",   # 2025-05-09 11:58:37 AM
                            "%Y.%m.%d %I:%M:%S %p",   # 2025.05.09 11:58:37 AM
                            "%d/%m/%Y %I:%M:%S %p",   # 09/05/2025 11:58:37 AM
                            
                            # Unix timestamp formats
                            "%s"                      # Unix timestamp in seconds
                        ]
                        
                        # Additional attempt to convert numeric timestamps
                        timestamp_str = event[self.datetime_column]
                        if isinstance(timestamp_str, str) and timestamp_str.isdigit():
                            # Try to interpret as unix timestamp if it's all digits
                            try:
                                timestamp_int = int(timestamp_str)
                                # Unix timestamps are typically >1000000000 (September 2001)
                                # and <2000000000 (May 2033) for current dates
                                if 1000000000 <= timestamp_int <= 2000000000:
                                    event[self.datetime_column] = datetime.fromtimestamp(timestamp_int)
                                    parsed = True
                                    logger.debug(f"Parsed numeric timestamp {timestamp_str} as Unix timestamp")
                            except (ValueError, TypeError, OverflowError):
                                pass

                        for fmt in alternative_formats:
                            try:
                                event[self.datetime_column] = datetime.strptime(timestamp_str, fmt)
                                parsed = True
                                logger.debug(f"Parsed timestamp {timestamp_str} with format {fmt}")
                                break
                            except (ValueError, TypeError):
                                continue

                        if not parsed:
                            logger.warning(f"Failed to parse timestamp: '{timestamp_str}' - keeping as string")
                            # Keep the original string value instead of failing completely
                            event[self.datetime_column] = timestamp_str
                            # Add a flag indicating this event has an unparsed timestamp
                            event['_timestamp_parse_failed'] = True
                            # Continue processing - don't skip the whole row

                # Convert numeric columns
                if self.format_name == "deadside":
                    # Convert distance to float
                    if "distance" in event:
                        try:
                            event["distance"] = float(event["distance"])
                        except (ValueError, TypeError):
                            event["distance"] = 0.0

                # Add event to list
                events.append(event)
        except Exception as e:
            logger.error(f"Error in CSV processing at line {current_line}: {e}f")
            # Return the events that were parsed successfully so far
            return events
        
        # Update last processed line count if we're tracking this file
        if file_path and current_line > 0:
            self._last_processed_line_count[file_path] = current_line
            logger.info(f"Updated last processed line for {file_path} to {current_line}")
            
        # Log summary of what we processed
        if only_new_lines:
            logger.info(f"Processed {len(events)} new events from {current_line - last_processed_line} new lines in {file_path or 'unknown'}")
        else:
            logger.info(f"Processed {len(events)} events from entire file {file_path or 'unknown'}")

        return events

    def filter_events(self, events: List[Dict[str, Any]], 
                     start_time: Optional[datetime] = None,
                     end_time: Optional[datetime] = None,
                     player_id: Optional[str] = None,
                     min_distance: Optional[float] = None,
                     max_distance: Optional[float] = None,
                     weapon: Optional[str] = None) -> List[Dict[str, Any]]:
        """Filter events by criteria

        Args:
            events: List of events to filter
            start_time: Start time for filtering (default: None)
            end_time: End time for filtering (default: None)
            player_id: Player ID for filtering (default: None)
            min_distance: Minimum distance for filtering (default: None)
            max_distance: Maximum distance for filtering (default: None)
            weapon: Weapon name for filtering (default: None)

        Returns:
            List[Dict]: Filtered events
        """
        # Start with all events
        filtered_events = events

        # Filter by time range
        if start_time is not None or end_time:
            filtered_events = [
                event for event in filtered_events
                if (not start_time or event.get(self.datetime_column, datetime.min) >= start_time) and
                   (not end_time or event.get(self.datetime_column, datetime.max) <= end_time)
            ]

        # Filter by player ID
        if player_id is not None:
            if self.format_name == "deadside":
                filtered_events = [
                    event for event in filtered_events
                    if event.get("killer_id") == player_id
                ]
            elif self.format_name == "custom":
                filtered_events = [
                    event for event in filtered_events
                    if event.get("player1_id") == player_id
                ]

        # Filter by distance range
        if (min_distance is None or max_distance is None) and "distance" in self.columns:
            filtered_events = [
                event for event in filtered_events
                if ((min_distance is None or event.get("distance", 0) >= min_distance) and
                    (max_distance is None or event.get("distance", float("inf")) <= max_distance))
            ]

        # Filter by weapon
        if weapon is not None and "weapon" in self.columns:
            filtered_events = [
                event for event in filtered_events
                if event.get("weapon", "").lower() == weapon.lower()
            ]

        return filtered_events

    def aggregate_player_stats(self, events: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Aggregate player statistics from events

        Args:
            events: List of events

        Returns:
            Dict[str, Dict]: Dictionary of player statistics by player ID
        """
        # Initialize player stats
        player_stats = {}

        if self.format_name == "deadside":
            # Process deadside format
            for event in events:
                killer_id = event.get("killer_id")
                victim_id = event.get("victim_id")

                # Skip invalid events
                if killer_id is None or not victim_id:
                    continue

                # Extract event details
                killer_name = event.get("killer_name", "Unknown")
                victim_name = event.get("victim_name", "Unknown")
                weapon = event.get("weapon", "Unknown")
                distance = event.get("distance", 0)
                timestamp = event.get(self.datetime_column, datetime.now())

                # Extract console info if available (post-April format)
                killer_console = event.get("killer_console", "Unknown")
                victim_console = event.get("victim_console", "Unknown")

                # Update killer stats
                if killer_id is not None and killer_id not in player_stats:
                    player_stats[killer_id] = {
                        "player_id": killer_id,
                        "player_name": killer_name,
                        "kills": 0,
                        "deaths": 0,
                        "weapons": {},
                        "victims": {},
                        "killers": {},
                        "longest_kill": 0,
                        "total_distance": 0,
                        "first_seen": timestamp,
                        "last_seen": timestamp,
                        "platform": killer_console,  # Add platform/console info
                        "kills_by_platform": {}  # Track kills by victim platform
                    }

                killer_stats = player_stats[killer_id]
                killer_stats["kills"] += 1
                killer_stats["weapons"][weapon] = killer_stats["weapons"].get(weapon, 0) + 1
                killer_stats["victims"][victim_id] = killer_stats["victims"].get(victim_id, 0) + 1
                killer_stats["total_distance"] += distance
                killer_stats["longest_kill"] = max(killer_stats["longest_kill"], distance)
                killer_stats["last_seen"] = max(killer_stats["last_seen"], timestamp)

                # Track kills by victim platform (for cross-platform stats)
                if victim_console and victim_console != "Unknown":
                    killer_stats["kills_by_platform"][victim_console] = killer_stats["kills_by_platform"].get(victim_console, 0) + 1

                # Update victim stats
                if victim_id is not None and victim_id not in player_stats:
                    player_stats[victim_id] = {
                        "player_id": victim_id,
                        "player_name": victim_name,
                        "kills": 0,
                        "deaths": 0,
                        "weapons": {},
                        "victims": {},
                        "killers": {},
                        "longest_kill": 0,
                        "total_distance": 0,
                        "first_seen": timestamp,
                        "last_seen": timestamp,
                        "platform": victim_console,  # Add platform/console info
                        "deaths_by_platform": {}  # Track deaths by killer platform
                    }

                victim_stats = player_stats[victim_id]
                victim_stats["deaths"] += 1
                victim_stats["killers"][killer_id] = victim_stats["killers"].get(killer_id, 0) + 1
                victim_stats["last_seen"] = max(victim_stats["last_seen"], timestamp)

                # Track deaths by killer platform (for cross-platform stats)
                if killer_console and killer_console != "Unknown":
                    victim_stats["deaths_by_platform"][killer_console] = victim_stats["deaths_by_platform"].get(killer_console, 0) + 1

        elif self.format_name == "custom":
            # Process custom format
            pass

        # Calculate additional statistics
        for player_id, stats in player_stats.items():
            # Calculate K/D ratio
            stats["kd_ratio"] = stats["kills"] / max(stats["deaths"], 1)

            # Calculate average kill distance
            if isinstance(stats, dict) and stats["kills"] > 0:
                stats["avg_kill_distance"] = stats["total_distance"] / stats["kills"]
            else:
                stats["avg_kill_distance"] = 0

            # Calculate playtime estimate
            stats["playtime"] = (stats["last_seen"] - stats["first_seen"]).total_seconds() / 3600

            # Get favorite weapon
            if isinstance(stats, dict) and stats["weapons"]:
                stats["favorite_weapon"] = max(stats["weapons"].items(), key=lambda x: x[1])[0]
            else:
                stats["favorite_weapon"] = "None"

            # Get most killed player
            if isinstance(stats, dict) and stats["victims"]:
                most_killed_id = max(stats["victims"].items(), key=lambda x: x[1])[0]
                stats["most_killed"] = {
                    "player_id": most_killed_id,
                    "player_name": player_stats.get(most_killed_id, {}).get("player_name", "Unknown"),
                    "count": stats["victims"][most_killed_id]
                }
            else:
                stats["most_killed"] = None

            # Get nemesis (player killed by the most)
            if isinstance(stats, dict) and stats["killers"]:
                nemesis_id = max(stats["killers"].items(), key=lambda x: x[1])[0]
                stats["nemesis"] = {
                    "player_id": nemesis_id,
                    "player_name": player_stats.get(nemesis_id, {}).get("player_name", "Unknown"),
                    "count": stats["killers"][nemesis_id]
                }
            else:
                stats["nemesis"] = None

            # Calculate cross-platform statistics if platform data is available
            if "platform" in stats and stats["platform"] != "Unknown":
                # Calculate platform-specific K/D ratio
                if "kills_by_platform" in stats:
                    # Total kills by player platform
                    platform_kills = sum(stats["kills_by_platform"].values())
                    # Cross-platform K/D ratio
                    if "deaths_by_platform" in stats:
                        platform_deaths = sum(stats["deaths_by_platform"].values())
                        stats["platform_kd_ratio"] = platform_kills / max(platform_deaths, 1)

                    # Get dominant platform (platform with most kills against)
                    if isinstance(stats, dict) and stats["kills_by_platform"]:
                        stats["dominant_platform"] = max(stats["kills_by_platform"].items(), key=lambda x: x[1])[0]
                        stats["dominant_platform_kills"] = stats["kills_by_platform"][stats["dominant_platform"]]
                    else:
                        stats["dominant_platform"] = "None"
                        stats["dominant_platform_kills"] = 0

        return player_stats

    def stream_parse_csv(self, file_obj: BinaryIO, chunk_size: int = 8192) -> Generator[Dict[str, Any], None, None]:
        """Parse CSV data in streaming mode for memory-efficient processing of large files

        This method is designed to handle very large CSV files by processing them in chunks
        rather than loading the entire file into memory.

        Args:
            file_obj: Binary file-like object
            chunk_size: Size of chunks to read (default: 8KB)

        Yields:
            Dict[str, Any]: Individual parsed event records
        """
        # Initialize variables
        buffer = ""
        line_buffer = []
        detected_format = None
        tried_formats = []

        # Create CSV reader with auto-detection of dialect
        try:
            # Attempt to detect format from a sample
            sample = file_obj.read(min(chunk_size * 2, 16384))  # Read a sample (up to 16KB)
            file_obj.seek(0)  # Reset file position

            if isinstance(sample, memoryview):
                try:
                    sample_str = bytes(sample).decode('utf-8', errors='replace')
                except UnicodeDecodeError:
                    sample_str = bytes(sample).decode('latin-1', errors='replace')
            elif isinstance(sample, bytes):
                try:
                    sample_str = sample.decode('utf-8', errors='replace')
                except UnicodeDecodeError:
                    sample_str = sample.decode('latin-1', errors='replace')
            else:
                sample_str = sample

            # Try to detect CSV dialect
            try:
                # Ensure sample_str is a string for the sniff function
                if not isinstance(sample_str, str):
                    logger.warning(f"Converting sample of type {type(sample_str)} to string")
                    sample_str = str(sample_str)
                    
                dialect = csv.Sniffer().sniff(sample_str, delimiters=";,\t|")
                logger.info(f"Detected CSV dialect with delimiter: {dialect.delimiter}")
            except (csv.Error, TypeError) as e:
                # Fallback to default format
                logger.warning(f"Failed to detect CSV dialect: {e}f, using default format")
                dialect = None

            # If we detected a dialect different from our configured separator, 
            # create a special format config
            if dialect is not None and dialect.delimiter != self.separator:
                logger.info(f"Adapting to detected delimiter: {dialect.delimiter}")
                detected_format = dict(self.format_config)
                detected_format["separator"] = dialect.delimiter
                tried_formats.append(detected_format)

            # Add the standard format to the list of formats to try
            tried_formats.append(self.format_config)

            # Add fallback formats if available
            if "fallback_formats" in self.format_config:
                tried_formats.extend(self.format_config["fallback_formats"])

        except Exception as e:
            logger.warning(f"Error during CSV format detection: {e}")
            # Fall back to the default format
            tried_formats = [self.format_config]
            if "fallback_formats" in self.format_config:
                tried_formats.extend(self.format_config["fallback_formats"])

        # Process the file in chunks
        while True:
            # Read a chunk
            chunk = file_obj.read(chunk_size)
            if not chunk:  # Empty string means EOF, None would be an error
                break  # End of file

            # Decode chunk
            if isinstance(chunk, memoryview):
                try:
                    chunk_str = bytes(chunk).decode('utf-8', errors='replace')
                except UnicodeDecodeError:
                    chunk_str = bytes(chunk).decode('latin-1', errors='replace')
            elif isinstance(chunk, bytes):
                try:
                    chunk_str = chunk.decode('utf-8', errors='replace')
                except UnicodeDecodeError:
                    chunk_str = chunk.decode('latin-1', errors='replace')
            else:
                chunk_str = chunk

            # Append to buffer
            # At this point chunk_str should already be a string from the previous conversion
            # but we'll handle other types just to be safe
            if isinstance(chunk_str, str):
                buffer += chunk_str
            elif isinstance(chunk_str, (bytes, bytearray)):
                try:
                    buffer += chunk_str.decode('utf-8', errors='replace')
                except UnicodeDecodeError:
                    buffer += chunk_str.decode('latin-1', errors='replace')
                except Exception:
                    # Last resort fallback
                    buffer += str(chunk_str)
            elif isinstance(chunk_str, memoryview):
                try:
                    buffer += bytes(chunk_str).decode('utf-8', errors='replace')
                except UnicodeDecodeError:
                    buffer += bytes(chunk_str).decode('latin-1', errors='replace')
                except Exception:
                    # Last resort fallback
                    buffer += str(chunk_str)
            else:
                # Fallback for any other type
                buffer += str(chunk_str)

            # Split buffer by newlines
            lines = buffer.split('\n')
            buffer = lines.pop()  # Keep last partial line in buffer

            # Add complete lines to line buffer
            line_buffer.extend(lines)

            # Process lines when we have enough data
            while line_buffer:
                line = line_buffer.pop(0)
                if not line or not line.strip():
                    continue  # Skip empty lines

                # Try all formats until one works
                parsed_record = None
                for format_config in tried_formats:
                    try:
                        separator = format_config.get("separator", self.separator)
                        columns = format_config.get("columns", self.columns)
                        datetime_column = format_config.get("datetime_column", self.datetime_column)
                        datetime_format = format_config.get("datetime_format", self.datetime_format)

                        # Parse this line
                        fields = line.split(separator)
                        if len(fields) < 3:  # Minimum required fields
                            continue

                        # Create event record
                        record = {}
                        for i, column in enumerate(columns):
                            if i < len(fields):
                                record[column] = fields[i].strip()
                            else:
                                record[column] = ""

                        # Check required columns (if specified)
                        if "required_columns" in format_config:
                            required_ok = True
                            for req_col in format_config["required_columns"]:
                                if record is None or req_col not in record:
                                    required_ok = False
                                    break

                            if not required_ok:
                                continue  # Skip to next format

                        # Convert datetime with multiple format support
                        if datetime_column in record and record[datetime_column]:
                            timestamp_str = record[datetime_column]
                            parsed = False
                            
                            # Try primary format first
                            try:
                                record[datetime_column] = datetime.strptime(
                                    timestamp_str, 
                                    datetime_format
                                )
                                parsed = True
                            except (ValueError, TypeError):
                                # Now try alternative formats
                                alternative_formats = [
                                    # Primary formats with dots
                                    "%Y.%m.%d-%H.%M.%S",      # 2025.03.27-10.42.18
                                    "%Y.%m.%d-%H:%M:%S",      # 2025.05.09-11:58:37
                                    "%Y.%m.%d %H.%M.%S",      # 2025.05.09 11.58.37
                                    "%Y.%m.%d %H:%M:%S",      # 2025.05.09 11:58:37
                                    
                                    # ISO-style formats
                                    "%Y-%m-%d %H:%M:%S",      # 2025-05-09 11:58:37
                                    "%Y-%m-%dT%H:%M:%S",      # 2025-05-09T11:58:37
                                    
                                    # Additional common formats
                                    "%m/%d/%Y %H:%M:%S",      # 05/09/2025 11:58:37 (US)
                                    "%d.%m.%Y %H:%M:%S",      # 09.05.2025 11:58:37 (EU)
                                ]
                                
                                # Try each alternative format
                                for fmt in alternative_formats:
                                    try:
                                        record[datetime_column] = datetime.strptime(timestamp_str, fmt)
                                        parsed = True
                                        break
                                    except (ValueError, TypeError):
                                        continue
                                
                                # If all parsing attempts failed but it's numeric, try timestamp
                                if not parsed and isinstance(timestamp_str, str) and timestamp_str.isdigit():
                                    try:
                                        timestamp_int = int(timestamp_str)
                                        if 1000000000 <= timestamp_int <= 2000000000:  # Valid timestamp range
                                            record[datetime_column] = datetime.fromtimestamp(timestamp_int)
                                            parsed = True
                                    except (ValueError, OverflowError, TypeError):
                                        pass
                            
                            # If all parsing failed, keep original string
                            if not parsed:
                                pass  # Keep the original string

                        # Parse numeric fields
                        if "distance" in record:
                            try:
                                record["distance"] = float(record["distance"])
                            except (ValueError, TypeError):
                                record["distance"] = 0.0

                        # Successful parse
                        parsed_record = record

                        # If we found a working format, stick with it for optimization
                        if tried_formats[0] != format_config:
                            logger.info(f"Switching to working format with separator: {separator}")
                            tried_formats.insert(0, format_config)

                        break  # Exit the format loop

                    except Exception as e:
                        # Try next format
                        continue

                # Yield the parsed record if successful
                if parsed_record is not None:
                    yield parsed_record

        # Process any remaining line in the buffer
        if buffer.strip():
            for format_config in tried_formats:
                try:
                    separator = format_config.get("separator", self.separator)
                    columns = format_config.get("columns", self.columns)
                    datetime_column = format_config.get("datetime_column", self.datetime_column)
                    datetime_format = format_config.get("datetime_format", self.datetime_format)

                    # Parse this line
                    fields = buffer.split(separator)
                    if len(fields) < 3:  # Minimum required fields
                        continue

                    # Create event record
                    record = {}
                    for i, column in enumerate(columns):
                        if i < len(fields):
                            record[column] = fields[i].strip()
                        else:
                            record[column] = ""

                    # Check required columns (if specified)
                    if "required_columns" in format_config:
                        required_ok = True
                        for req_col in format_config["required_columns"]:
                            if record is None or req_col not in record:
                                required_ok = False
                                break

                        if not required_ok:
                            continue  # Skip to next format

                    # Convert datetime with multiple format support (for final buffer line)
                    if datetime_column in record and record[datetime_column]:
                        timestamp_str = record[datetime_column]
                        parsed = False
                        
                        # Try primary format first
                        try:
                            record[datetime_column] = datetime.strptime(
                                timestamp_str, 
                                datetime_format
                            )
                            parsed = True
                        except (ValueError, TypeError):
                            # Now try alternative formats
                            alternative_formats = [
                                # Primary formats with dots
                                "%Y.%m.%d-%H.%M.%S",      # 2025.03.27-10.42.18
                                "%Y.%m.%d-%H:%M:%S",      # 2025.05.09-11:58:37
                                "%Y.%m.%d %H.%M.%S",      # 2025.05.09 11.58.37
                                "%Y.%m.%d %H:%M:%S",      # 2025.05.09 11:58:37
                                
                                # ISO-style formats
                                "%Y-%m-%d %H:%M:%S",      # 2025-05-09 11:58:37
                                "%Y-%m-%dT%H:%M:%S",      # 2025-05-09T11:58:37
                                
                                # Additional common formats
                                "%m/%d/%Y %H:%M:%S",      # 05/09/2025 11:58:37 (US)
                                "%d.%m.%Y %H:%M:%S",      # 09.05.2025 11:58:37 (EU)
                            ]
                            
                            # Try each alternative format
                            for fmt in alternative_formats:
                                try:
                                    record[datetime_column] = datetime.strptime(timestamp_str, fmt)
                                    parsed = True
                                    break
                                except (ValueError, TypeError):
                                    continue
                            
                            # If all parsing attempts failed but it's numeric, try timestamp
                            if not parsed and isinstance(timestamp_str, str) and timestamp_str.isdigit():
                                try:
                                    timestamp_int = int(timestamp_str)
                                    if 1000000000 <= timestamp_int <= 2000000000:  # Valid timestamp range
                                        record[datetime_column] = datetime.fromtimestamp(timestamp_int)
                                        parsed = True
                                except (ValueError, OverflowError, TypeError):
                                    pass
                        
                        # If all parsing failed, keep original string
                        if not parsed:
                            pass  # Keep the original string

                    # Parse numeric fields
                    if "distance" in record:
                        try:
                            record["distance"] = float(record["distance"])
                        except (ValueError, TypeError):
                            record["distance"] = 0.0

                    # Yield the parsed record
                    yield record
                    break  # Exit the format loop

                except Exception:
                    # Try next format
                    continue

    def detect_format(self, file_obj: BinaryIO) -> Dict[str, Any]:
        """Detect CSV format from file content, including pre-April vs post-April formats

        Args:
            file_obj: Binary file-like object

        Returns:
            Dict[str, Any]: Detected format configuration
        """
        # Remember current position
        current_pos = file_obj.tell()

        try:
            # Read a sample
            sample = file_obj.read(8192)  # 8KB sample
            
            # Enhanced empty file detection
            if not sample or (isinstance(sample, (str, bytes)) and len(sample.strip()) == 0):
                logger.info("Empty file or EOF reached, using default format configuration")
                return self.format_config  # Empty file, use default

            # Decode sample
            if isinstance(sample, bytes):
                try:
                    sample_str = sample.decode('utf-8', errors='replace')
                except UnicodeDecodeError:
                    sample_str = sample.decode('latin-1', errors='replace')
            else:
                sample_str = sample

            # Try to detect dialect
            try:
                # Ensure sample_str is a string for the sniff function
                if not isinstance(sample_str, str):
                    logger.warning(f"Converting sample of type {type(sample_str)} to string")
                    sample_str = str(sample_str)
                
                dialect = csv.Sniffer().sniff(sample_str, delimiters=";,\t|")
                logger.info(f"Detected delimiter: {dialect.delimiter}")

                # Store format detection for later use in the object
                if not hasattr(self, "detected_format_info"):
                    self.detected_format_info = {}

                # Create format config with detected delimiter
                format_config = dict(self.format_config)
                format_config["separator"] = dialect.delimiter

                # Determine pre-April vs post-April format
                # Check the first line to determine the number of fields
                # Handle both string and binary data types
                first_line = ""
                
                if isinstance(sample_str, str):
                    lines = sample_str.split('\n')
                    if lines:
                        first_line = lines[0].strip()
                elif isinstance(sample_str, (bytes, bytearray)):
                    lines = sample_str.split(b'\n')
                    if lines:
                        try:
                            first_line = lines[0].decode('utf-8', errors='replace').strip()
                        except Exception:
                            first_line = str(lines[0]).strip()
                elif isinstance(sample_str, memoryview):
                    # Convert memoryview to bytes first, then to string
                    sample_bytes = bytes(sample_str)
                    lines = sample_bytes.split(b'\n')
                    if lines:
                        try:
                            first_line = lines[0].decode('utf-8', errors='replace').strip()
                        except Exception:
                            first_line = str(lines[0]).strip()
                
                if dialect.delimiter in first_line:
                    parts = first_line.split(dialect.delimiter)
                    num_fields = len(parts)

                    # Check for post-April format (with console fields)
                    if num_fields >= 9:
                        logger.info("Detected post-April CSV format (9 columns with console info)")
                        self.detected_format_info["april_update"] = True

                        # Update columns if needed
                        if "killer_console" not in format_config["columns"] and len(format_config["columns"]) < 9:
                            format_config["columns"] = format_config["columns"][:7] + ["killer_console", "victim_console"]

                    # Check for pre-April format (7 fields)
                    elif num_fields >= 7:
                        logger.info("Detected pre-April CSV format (7 columns)")
                        self.detected_format_info["april_update"] = False

                return format_config

            except csv.Error:
                logger.warning("Could not detect CSV dialect, using default format")
                return self.format_config

        except Exception as e:
            logger.error(f"Error detecting CSV format: {e}")
            return self.format_config

        finally:
            # Reset file position
            file_obj.seek(current_pos)

    def get_leaderboard(self, player_stats: Dict[str, Dict[str, Any]], stat_name: str, limit: int = 10, 
                      platform: Optional[str] = None) -> List[Dict[str, Any]]:
        """Generate leaderboard from player statistics, optionally filtered by platform

        Args:
            player_stats: Dictionary of player statistics by player ID
            stat_name: Statistic name to rank by
            limit: Maximum number of entries (default: 10)
            platform: Optional platform to filter by (e.g., "PS5", "Xbox", "PC")

        Returns:
            List[Dict]: Leaderboard entries
        """
        # Filter players by platform if specified
        if platform:
            filtered_players = [
                stats for _, stats in player_stats.items()
                if stats.get("platform") == platform
            ]
        else:
            filtered_players = [stats for _, stats in player_stats.items()]

        # Sort players by statistic
        sorted_players = sorted(
            filtered_players,
            key=lambda x: x.get(stat_name, 0),
            reverse=True
        )

        # Create leaderboard entries
        leaderboard = []
        for i, player in enumerate(sorted_players[:limit]):
            entry = {
                "rank": i + 1,
                "player_id": player["player_id"],
                "player_name": player["player_name"],
                "value": player.get(stat_name, 0)
            }

            # Include platform info if available
            if "platform" in player:
                entry["platform"] = player["platform"]

            leaderboard.append(entry)

        return leaderboard

    def get_platform_comparison(self, player_stats: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Generate cross-platform comparison statistics

        Args:
            player_stats: Dictionary of player statistics by player ID

        Returns:
            Dict[str, Any]: Platform comparison statistics
        """
        # Initialize platform stats
        platforms = {}
        platform_kills = {}
        platform_deaths = {}

        # Aggregate platform statistics
        for player_id, stats in player_stats.items():
            # Skip players with unknown platform
            if "platform" not in stats or stats["platform"] == "Unknown":
                continue

            platform = stats["platform"]

            # Initialize platform entry
            if platform not in platforms:
                platforms[platform] = {
                    "total_players": 0,
                    "total_kills": 0,
                    "total_deaths": 0,
                    "avg_kd": 0.0,
                    "kills_by_victim_platform": {},
                    "deaths_by_killer_platform": {}
                }

            # Update platform stats
            platforms[platform]["total_players"] += 1
            platforms[platform]["total_kills"] += stats.get("kills", 0)
            platforms[platform]["total_deaths"] += stats.get("deaths", 0)

            # Process cross-platform kills
            if "kills_by_platform" in stats:
                for victim_platform, kill_count in stats["kills_by_platform"].items():
                    if victim_platform not in platforms[platform]["kills_by_victim_platform"]:
                        platforms[platform]["kills_by_victim_platform"][victim_platform] = 0
                    platforms[platform]["kills_by_victim_platform"][victim_platform] += kill_count

            # Process cross-platform deaths
            if "deaths_by_platform" in stats:
                for killer_platform, death_count in stats["deaths_by_platform"].items():
                    if killer_platform not in platforms[platform]["deaths_by_killer_platform"]:
                        platforms[platform]["deaths_by_killer_platform"][killer_platform] = 0
                    platforms[platform]["deaths_by_killer_platform"][killer_platform] += death_count

        # Calculate averages and additional statistics
        for platform, data in platforms.items():
            # Calculate average K/D ratio for platform
            if isinstance(data, dict) and data["total_deaths"] > 0:
                data["avg_kd"] = data["total_kills"] / data["total_deaths"]
            else:
                data["avg_kd"] = data["total_kills"]  # Avoid division by zero

            # Calculate dominant victim platform
            if isinstance(data, dict) and data["kills_by_victim_platform"]:
                data["dominant_victim_platform"] = max(
                    data["kills_by_victim_platform"].items(),
                    key=lambda x: x[1]
                )[0]
            else:
                data["dominant_victim_platform"] = "None"

            # Calculate dominant killer platform
            if isinstance(data, dict) and data["deaths_by_killer_platform"]:
                data["dominant_killer_platform"] = max(
                    data["deaths_by_killer_platform"].items(),
                    key=lambda x: x[1]
                )[0]
            else:
                data["dominant_killer_platform"] = "None"

        return platforms

    def detect_format_from_string(self, data: Union[str, bytes]) -> str:
        """Detect log format from string data, identifying pre-April vs post-April format

        Args:
            data: CSV data string or bytes

        Returns:
            str: Detected format name
        """
        # Convert bytes, memoryview, or other types to string for StringIO
        if not isinstance(data, str):
            try:
                if isinstance(data, memoryview):
                    data = bytes(data).decode("utf-8", errors="replace")
                elif isinstance(data, bytes):
                    try:
                        data = data.decode("utf-8", errors="replace")
                    except UnicodeDecodeError:
                        # Fallback to latin-1 encoding if UTF-8 fails
                        data = data.decode("latin-1", errors="replace")
                else:
                    data = str(data)
                logger.debug(f"Converted {type(data).__name__} to string for processing")
            except Exception as e:
                logger.error(f"Failed to convert data to string: {e}")
                return "deadside"  # Default format
                
        csv_file = io.StringIO(data)

        try:
            # Get first line
            first_line = csv_file.readline().strip()

            # Reset file position
            csv_file.seek(0)

            # Store format detection for later use in the object
            if not hasattr(self, "detected_format_info"):
                self.detected_format_info = {}

            # Check semicolon separator (deadside)
            if ";" in first_line:
                parts = first_line.split(";")
                num_fields = len(parts)

                # Check number of fields to determine pre or post-April format
                if num_fields >= 9:
                    # Post-April format with console fields
                    logger.info("Detected post-April CSV format (9 columns with console info)")
                    self.detected_format_info["april_update"] = True
                    return "deadside"
                elif num_fields >= 7:
                    # Pre-April format
                    logger.info("Detected pre-April CSV format (7 columns)")
                    self.detected_format_info["april_update"] = False
                    return "deadside"

                # Try to detect by timestamp pattern
                if parts and (re.match(r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}", parts[0]) or
                             re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", parts[0])):
                    logger.info(f"Detected deadside format with timestamp pattern: {parts[0]}")
                    return "deadside"

            # Check comma separator (custom)
            if "," in first_line and len(first_line.split(",")) >= 6:
                separator = ","
                parts = first_line.split(",")

                # Check for timestamp format
                if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", parts[0]):
                    return "custom"

            # Default to deadside
            logger.warning(f"Could not determine format precisely, defaulting to deadside (line: {first_line[:50]}...)")
            return "deadside"

        finally:
            csv_file.close()

    def add_custom_format(self, format_name: str, format_config: Dict[str, Any]) -> None:
        """Add custom log format

        Args:
            format_name: Format name
            format_config: Format configuration
        """
        # Validate format configuration
        required_keys = ["separator", "columns", "datetime_format", "datetime_column"]
        for key in required_keys:
            if key not in format_config:
                raise ValueError(f"Missing required key in format config: {key}")

        # Add format to LOG_FORMATS
        self.LOG_FORMATS[format_name] = format_config

        # Update current format if matching
        if format_name == self.format_name:
            self.format_config = format_config
            self.separator = format_config["separator"]
            self.columns = format_config["columns"]
            self.datetime_format = format_config["datetime_format"]
            self.datetime_column = format_config["datetime_column"]