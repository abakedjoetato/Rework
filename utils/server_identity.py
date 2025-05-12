import logging
import re
from typing import Dict, Optional, Tuple, Any

logger = logging.getLogger(__name__)

# Known server mappings (server_id -> numeric_id)
KNOWN_SERVERS = {
    "5251382d-8bce-4abd-8bcb-cdef73698a46": "7020",  # Emerald EU
    "dc1f7c09-dabb-4607-a10d-353f66f1ea20": "7021",  # Emerald US
    "681ef676-f9f6-2ab1-6462-2334000000": "7022",    # Emerald AU
    "681ef676-2c2d-2cd8-7588-2d2a000000": "7023",    # Emerald Asia
    # Add more known mappings here
}

def identify_server(server_id: str, hostname: Optional[str] = None, 
                   server_name: Optional[str] = None, guild_id: Optional[str] = None) -> Tuple[str, bool]:
    """
    Identify a server and return its numeric ID using multiple methods of identification.

    Args:
        server_id: The UUID or numeric ID of the server
        hostname: Optional hostname of the server
        server_name: Optional name of the server
        guild_id: Optional Discord guild ID associated with the server

    Returns:
        Tuple[str, bool]: (numeric_id, is_known_mapping)
            - numeric_id: The numeric ID of the server
            - is_known_mapping: True if this was a known mapping, False if derived
    """
    # First check if this is already a numeric ID
    if server_id is not None and server_id.isdigit():
        logger.debug(f"Server ID {server_id} appears to already be numeric")
        return server_id, False

    # Check if we have a known mapping
    if server_id in KNOWN_SERVERS:
        numeric_id = KNOWN_SERVERS[server_id]
        logger.debug(f"Found known mapping for {server_id} -> {numeric_id}")
        return numeric_id, True

    # Try to extract a numeric part from the UUID
    if server_id is not None:
        # Extract a numeric value from the first part of the UUID
        uuid_parts = server_id.split('-')
        if uuid_parts:
            # Convert the first hex part to a number and use the last 4 digits
            try:
                hex_value = uuid_parts[0]
                numeric_value = int(hex_value, 16)
                # Use the last 4 digits, with a minimum of 1000
                derived_id = max(1000, numeric_value % 10000)
                logger.debug(f"Derived numeric ID {derived_id} from {server_id}")
                return str(derived_id), False
            except (ValueError, IndexError):
                logger.debug(f"Could not derive numeric ID from {server_id}")

    # If hostname is provided, try to extract numeric part
    if hostname:
        # Look for numbers in the hostname
        numbers = re.findall(r'\d+', hostname)
        if numbers:
            # Use the last group of numbers found
            derived_id = numbers[-1]
            logger.debug(f"Extracted numeric ID {derived_id} from hostname {hostname}")
            return derived_id, False

    # Fall back to using the original server_id
    logger.debug(f"Using original server_id as fallback: {server_id}")
    return server_id, False

def extract_numeric_id(server_id: str, server_name: Optional[str] = None, 
                      hostname: Optional[str] = None) -> Optional[str]:
    """Extract numeric ID from server info.
    
    This function tries multiple strategies to extract a numeric ID:
    1. Check if server_id itself is numeric
    2. Look for numeric ID in server_name
    3. Extract ID from hostname (common pattern: hostname_1234)
    4. Check if it's a UUID and extract numeric part
    5. Fall back to known mappings
    
    Args:
        server_id: Server ID (possibly UUID format)
        server_name: Optional server name that might contain numeric ID
        hostname: Optional hostname that might contain numeric ID
        
    Returns:
        Extracted numeric ID or None if not found
    """
    # Prevent log spam - only log at debug level
    # logger.debug(f"Attempting to extract numeric ID from: id={server_id}, name={server_name}, host={hostname}")
    
    # Strategy 1: If server_id is numeric, use it directly
    if server_id is not None and str(server_id).isdigit():
        # logger.debug(f"Server ID is already numeric: {server_id}")
        return str(server_id)
    
    # Strategy 2: Check in server name (common pattern: "Server 1234")
    if server_name is not None:
        # Look for any numeric sequences that are at least 4 digits
        for word in str(server_name).split():
            if word.isdigit() and len(word) >= 4:
                # logger.debug(f"Found numeric ID {word} in server name: {server_name}")
                return word
    
    # Strategy 3: Extract from hostname (common pattern: hostname_1234)
    if hostname:
        # Remove port if present
        hostname_base = hostname.split(':')[0]
        
        # Check for underscore pattern
        if '_' in hostname_base:
            # Try to extract ID after underscore
            parts = hostname_base.split('_')
            if len(parts) > 1 and parts[-1].isdigit():
                # logger.debug(f"Found numeric ID {parts[-1]} in hostname")
                return parts[-1]
        
        # Alternative approach - look for any digit sequences
        numbers = re.findall(r'\d+', hostname_base)
        if numbers:
            # Use the last or longest number found
            longest_num = max(numbers, key=len)
            # logger.debug(f"Found numeric sequence {longest_num} in hostname")
            return longest_num
    
    # Strategy 4: Check if server_id is in our known mappings
    if server_id in KNOWN_SERVERS:
        num_id = KNOWN_SERVERS[server_id]
        # logger.debug(f"Found known mapping for UUID: {server_id} -> {num_id}")
        return num_id
    
    # Strategy 5: Try to extract from UUID if it looks like one
    if server_id is not None and '-' in server_id and len(server_id) > 30:
        try:
            import uuid
            # Convert to standard UUID format and extract a numeric portion
            clean_uuid = str(server_id).strip().lower()
            parsed_uuid = uuid.UUID(clean_uuid)
            
            # Use last 10 digits to create a stable numeric ID
            numeric_id = str(int(parsed_uuid.int) % 10**10)
            
            # Ensure it's at least 4 digits
            if len(numeric_id) < 4:
                numeric_id = numeric_id.zfill(4)
                
            # logger.debug(f"UUID conversion for path construction: {server_id} -> {numeric_id}")
            return numeric_id
        except Exception as e:
            # logger.debug(f"Could not convert UUID {server_id} to numeric ID: {e}")
            pass
    
    # Failed to extract numeric ID
    return None

def get_path_components(server_id: str, hostname: str, 
                       original_server_id: Optional[str] = None,
                       guild_id: Optional[str] = None) -> Tuple[str, str]:
    """Get path components for server directories.

    This builds the directory paths consistently with server identity.

    CRITICAL BUGFIX: This function now has stronger logic for handling server identity,
    giving priority to the original_server_id parameter when provided. This ensures
    that newly added servers will use the correct ID even before database mappings
    are fully established.

    Args:
        server_id: The server ID (usually UUID) from the database
        hostname: Server hostname
        original_server_id: Optional original server ID to override detection
        guild_id: Optional Discord guild ID for isolation

    Returns:
        Tuple of (server_dir, path_server_id)
        - server_dir: The server directory name (hostname_serverid)
        - path_server_id: The server ID to use in paths
    """
    # Ensure we're working with strings
    server_id = str(server_id) if server_id is not None else ""
    hostname = str(hostname) if hostname is not None else ""
    original_server_id = str(original_server_id) if original_server_id is not None else ""
    guild_id = str(guild_id) if guild_id is not None else ""

    # Cache for log reduction
    if not hasattr(get_path_components, "logged_ids"):
        get_path_components.logged_ids = set()
    
    if not hasattr(get_path_components, "logged_extractions"):
        get_path_components.logged_extractions = set()
        
    if not hasattr(get_path_components, "logged_dirs"):
        get_path_components.logged_dirs = set()
        
    if not hasattr(get_path_components, "resolved_ids"):
        get_path_components.resolved_ids = {}

    # If we've already resolved this server_id, use the cached result
    # This drastically reduces repeated calls and log spam
    cache_key = f"{server_id}:{original_server_id}:{hostname}"
    if cache_key in get_path_components.resolved_ids:
        return get_path_components.resolved_ids[cache_key]

    # Clean hostname - handle both port specifications (:22) and embedded IDs (_1234)
    clean_hostname = hostname.split(':')[0] if hostname else "server"

    # Use our new extract_numeric_id function for more robust ID extraction
    numeric_id = None
    
    # PRIORITY 1: Use explicit original_server_id if provided (most reliable)
    if original_server_id is not None and str(original_server_id).strip():
        # Only log on first use, not repeated access to avoid spamming logs
        if original_server_id not in get_path_components.logged_ids:
            logger.debug(f"Using provided original_server_id '{original_server_id}' for path construction")
            get_path_components.logged_ids.add(original_server_id)
        numeric_id = str(original_server_id)

    # PRIORITY 2: Try to extract from available data if not explicitly provided
    if not numeric_id:
        numeric_id = extract_numeric_id(server_id, None, hostname)
        if numeric_id:
            # Only log on first extraction to avoid spamming logs
            extraction_key = f"{server_id}:{numeric_id}"
            if extraction_key not in get_path_components.logged_extractions:
                logger.debug(f"Extracted numeric ID '{numeric_id}' for server {server_id}")
                get_path_components.logged_extractions.add(extraction_key)
    
    # PRIORITY 3: Fall back to server_id if we couldn't extract a numeric ID
    if not numeric_id:
        # If this is a UUID, use a consistent numeric representation
        if '-' in server_id and len(server_id) > 30:
            try:
                import uuid
                # Convert to standard UUID format and extract a numeric portion
                clean_uuid = str(server_id).strip().lower()
                parsed_uuid = uuid.UUID(clean_uuid)
                
                # Use last 10 digits to create a stable numeric ID
                numeric_id = str(int(parsed_uuid.int) % 10**10)
                
                # Ensure it's at least 4 digits
                if len(numeric_id) < 4:
                    numeric_id = numeric_id.zfill(4)
                    
                logger.info(f"UUID conversion for path construction: {server_id} -> {numeric_id}")
            except Exception as e:
                # Fallback to server ID directly
                logger.warning(f"Could not convert UUID {server_id} to numeric ID: {e}")
                numeric_id = server_id
        else:
            logger.warning(f"Could not extract numeric ID for server {server_id}, using server_id directly")
            numeric_id = server_id

    # Build server directory with cleaned hostname and numeric ID
    # For this specific case we know the pattern is hostname_id
    server_dir = f"{clean_hostname}_{numeric_id}"
    
    # Only log new directory resolutions to avoid console spam
    if server_dir not in get_path_components.logged_dirs:
        logger.debug(f"Server directory resolved: {server_dir} (from server_id={server_id}, hostname={hostname})")
        get_path_components.logged_dirs.add(server_dir)
    
    # Cache the result for future calls
    result = (server_dir, numeric_id)
    get_path_components.resolved_ids[cache_key] = result
    
    return result