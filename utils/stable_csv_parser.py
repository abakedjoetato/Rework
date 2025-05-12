"""
# module: stable_csv_parser
Stable CSV Parser

This module provides a robust and reliable CSV parsing implementation
designed to handle the specific CSV format used by the game servers.
It focuses on stability, error recovery, and consistent behavior.
"""
import csv
import io
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional, Set, Union

# Setup logging
logger = logging.getLogger(__name__)

class StableCSVParser:
    """
    A stable and reliable CSV parser designed specifically for game log files.
    
    This class handles the parsing of CSV files with semicolon delimiters,
    provides robust error handling, and maintains state about parsed lines.
    """
    
    def __init__(self):
        """Initialize the parser with default settings."""
        # Known timestamp formats to try when parsing
        self.timestamp_formats = [
            # CRITICAL FIX: Exact format from screenshot (2025.04.27-00.00.00.csv)
            # This format puts timestamp directly in the filename
            '%Y.%m.%d-%H.%M.%S',
            
            # Other supported formats for flexibility
            '%Y.%m.%d-%H:%M:%S',
            '%Y.%m.%d %H.%M.%S',
            '%Y.%m.%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H.%M.%S',
            '%m/%d/%Y %H:%M:%S',
            '%d/%m/%Y %H:%M:%S'
        ]
        # Set of files we've already processed fully
        self.processed_files: Set[str] = set()
        # Dictionary mapping file paths to the last line processed
        self.last_processed_line: Dict[str, int] = {}
        
    def parse_file_content(self, 
                         content: str, 
                         file_path: str, 
                         server_id: str, 
                         start_line: int = 0) -> Tuple[List[Dict[str, Any]], int]:
        """
        Parse CSV content from a string, with robust error handling.
        
        Args:
            content: The CSV content to parse
            file_path: Path to the file (for tracking)
            server_id: The server ID associated with this data
            start_line: The line to start parsing from (0-based)
            
        Returns:
            Tuple containing (list of parsed events, total lines processed)
        """
        if not content:
            logger.warning(f"Empty content provided for {file_path}")
            return [], 0
            
        # Force semicolon delimiter as required by the CSV format
        delimiter = ';'
        
        # Track lines for reporting
        total_lines = 0
        processed_events = []
        
        try:
            # Create a CSV reader with the semicolon delimiter
            reader = csv.reader(io.StringIO(content), delimiter=delimiter)
            
            # Process each row
            for row_num, row in enumerate(reader):
                total_lines += 1
                
                # Skip lines before our starting point
                if row_num < start_line:
                    continue
                    
                # Skip empty rows or incorrectly formatted rows
                if not row or len(row) < 5:
                    continue
                    
                # Skip header rows (if any)
                if any(header in row[0].lower() for header in ['time', 'date', 'timestamp']):
                    continue
                
                try:
                    # Parse the row into an event
                    event = self._parse_row_to_event(row, server_id)
                    if event is not None:
                        processed_events.append(event)
                except Exception as e:
                    logger.error(f"Error parsing row {row_num} in {file_path}: {e}")
                    # Continue with next row - don't let one bad row fail everything
                    continue
            
            # Update the last processed line for this file
            self.last_processed_line[file_path] = total_lines
            
            logger.info(f"Successfully parsed {len(processed_events)} events from {file_path}")
            return processed_events, total_lines
            
        except Exception as e:
            logger.error(f"Failed to parse CSV content from {file_path}: {e}")
            return [], total_lines
    
    def _parse_row_to_event(self, row: List[str], server_id: str) -> Optional[Dict[str, Any]]:
        """
        Parse a CSV row into an event dictionary.
        
        Args:
            row: The CSV row to parse
            server_id: The server ID to associate with the event
            
        Returns:
            Dictionary containing the parsed event, or None if parsing failed
        """
        # Extract fields from the row with safe access
        try:
            # FINAL CRITICAL FIX: Handling the correct CSV format from the actual data shown in the screenshot
            # The format has semicolon-delimited fields in this EXACT order that must be followed:
            # 2025.05.09-11.58.37;TestKiller;12345;TestVictim;67890;AK47;100;PC
            # Corresponding to:
            # timestamp;killer_name;killer_id;victim_name;victim_id;weapon;distance;system
            # Where 'system' is a single field representing the platform (PC, PS4, etc.)
            
            # Fix for type checking - use Dict[str, Any] to allow different value types
            event: Dict[str, Any] = {
                'timestamp_raw': row[0] if len(row) > 0 else "",
                'killer_name': row[1] if len(row) > 1 else "",
                'killer_id': row[2] if len(row) > 2 else "",
                'victim_name': row[3] if len(row) > 3 else "",
                'victim_id': row[4] if len(row) > 4 else "",
                'weapon': row[5] if len(row) > 5 else "",
                'server_id': server_id,
                'event_type': 'kill'
            }
            
            # Special handling for distance field which might have different formats
            if len(row) > 6:
                dist_str = row[6].strip()
                try:
                    # Handle potential comma instead of period for decimal separator
                    if ',' in dist_str and '.' not in dist_str:
                        dist_str = dist_str.replace(',', '.')
                    # Remove any non-numeric characters (except decimal point)
                    clean_dist = ''.join(c for c in dist_str if c.isdigit() or c == '.')
                    if clean_dist:
                        event['distance'] = float(clean_dist)
                    else:
                        event['distance'] = 0.0
                except (ValueError, TypeError):
                    event['distance'] = 0.0
                    logger.warning(f"Could not parse distance value: {dist_str}")
            else:
                event['distance'] = 0.0
                
            # Handle system field - in the actual data format, there's only one system field
            # which we'll use for both killer and victim systems
            if len(row) > 7:
                event['system'] = row[7].strip()
                event['killer_system'] = event['system']  # For backward compatibility
                event['victim_system'] = event['system']  # For backward compatibility
            else:
                event['system'] = ""
                event['killer_system'] = ""
                event['victim_system'] = ""
                
            # Support the 9-field format too (if it ever exists)
            if len(row) > 8:
                event['victim_system'] = row[8].strip()
            
            # Parse timestamp
            event['timestamp'] = self._parse_timestamp(event['timestamp_raw'])
            
            # Detect suicides
            if event['killer_id'] == event['victim_id'] or (
               event['killer_name'] == event['victim_name'] and event['killer_name']):
                event['event_type'] = 'suicide'
                event['is_suicide'] = True
            else:
                event['is_suicide'] = False
            
            return event
        except Exception as e:
            logger.error(f"Failed to parse row to event: {e}")
            return None
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """
        Parse a timestamp string into a datetime object.
        
        Args:
            timestamp_str: The timestamp string to parse
            
        Returns:
            Parsed datetime or current time if parsing fails
        """
        if not timestamp_str:
            return datetime.now()
            
        # Try all known formats
        for fmt in self.timestamp_formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue
                
        # If all formats fail, use current time and log warning
        logger.warning(f"Could not parse timestamp: {timestamp_str}, using current time")
        return datetime.now()
    
    def get_last_processed_line(self, file_path: str) -> int:
        """
        Get the last processed line for a file.
        
        Args:
            file_path: The path to the file
            
        Returns:
            The last processed line number (0-based), or 0 if not processed
        """
        return self.last_processed_line.get(file_path, 0)
    
    def mark_file_as_processed(self, file_path: str):
        """
        Mark a file as fully processed.
        
        Args:
            file_path: The path to the file
        """
        self.processed_files.add(file_path)
    
    def is_file_processed(self, file_path: str) -> bool:
        """
        Check if a file has been fully processed.
        
        Args:
            file_path: The path to the file
            
        Returns:
            True if the file has been fully processed, False otherwise
        """
        return file_path in self.processed_files