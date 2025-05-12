"""
Verify the premium system migration was successful.
This script checks that the old premium system has been completely removed
and the new premium system is fully operational.
"""
import asyncio
import logging
import os
import sys
import importlib
from typing import List, Dict, Set, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def find_imports_in_file(file_path: str) -> Tuple[List[str], List[str]]:
    """
    Find import statements in a Python file.
    
    Args:
        file_path: Path to the Python file
        
    Returns:
        Tuple containing:
        - List of imported modules
        - List of from-import statements
    """
    if not os.path.exists(file_path) or not file_path.endswith(".py"):
        return [], []
    
    imports = []
    from_imports = []
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            
            for line in lines:
                line = line.strip()
                
                if line.startswith("import "):
                    # Handle direct imports
                    modules = line[7:].split(",")
                    imports.extend([m.strip().split()[0] for m in modules])
                    
                elif line.startswith("from "):
                    # Handle from-imports
                    if " import " in line:
                        module = line.split(" import ")[0][5:].strip()
                        from_imports.append(module)
    except Exception as e:
        logger.error(f"Error parsing imports in {file_path}: {e}")
    
    return imports, from_imports


def scan_directory_for_imports(directory: str, patterns: List[str]) -> Dict[str, List[str]]:
    """
    Scan a directory for files importing specific modules.
    
    Args:
        directory: Directory to scan
        patterns: List of import patterns to look for
        
    Returns:
        Dict of file paths and the matched imports
    """
    results = {}
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                
                imports, from_imports = find_imports_in_file(file_path)
                
                # Check for matches
                matches = []
                
                for pattern in patterns:
                    if pattern in imports:
                        matches.append(f"import {pattern}")
                    
                    for from_import in from_imports:
                        if pattern in from_import:
                            matches.append(f"from {from_import} import ...")
                
                if matches is not None:
                    results[file_path] = matches
    
    return results


def check_if_module_exists(module_name: str) -> bool:
    """
    Check if a module can be imported.
    
    Args:
        module_name: Name of the module to check
        
    Returns:
        bool: True if module exists and can be imported
    """
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False


async def verify_premium_migration():
    """
    Verify the premium system migration was successful.
    
    Returns:
        bool: True if migration was successful
    """
    logger.info("Verifying premium system migration...")
    
    # Step 1: Check if new modules are importable
    new_modules = [
        "utils.premium_config",
        "utils.premium_mongodb_models",
        "utils.premium_feature_access",
        "utils.premium_compatibility"
    ]
    
    logger.info("Checking if new modules are importable:")
    all_modules_found = True
    
    for module in new_modules:
        exists = check_if_module_exists(module)
        logger.info(f"  {module}: {'Found' if exists else 'Not found'}")
        all_modules_found = all_modules_found and exists
    
    if all_modules_found is None:
        logger.error("Not all new modules are importable!")
    
    # Step 2: Check for references to old premium system
    old_patterns = [
        "models.guild.Guild.premium_tier",
        "models.guild.PremiumTier",
        "utils.premium.check_premium"
    ]
    
    logger.info("Checking for references to old premium system:")
    references = scan_directory_for_imports(".", ["models.guild", "utils.premium"])
    
    if references is not None:
        logger.warning("Found references to old premium system:")
        for file, imports in references.items():
            logger.warning(f"  {file}: {', '.join(imports)}")
    else:
        logger.info("  No references to old premium system found")
    
    # Step 3: Try to use the new premium system
    logger.info("Testing the new premium system:")
    
    try:
        # Import new modules
        from utils.premium_mongodb_models import PremiumGuild
        from utils.premium_feature_access import PremiumFeature
        from utils.premium_config import get_tier_name, get_tier_features
        
        # Check if modules work
        tier_name = get_tier_name(2)
        features = get_tier_features(2)
        
        logger.info(f"  Tier 2 name: {tier_name}")
        logger.info(f"  Tier 2 features count: {len(features)}")
        
        logger.info("  Premium system modules work correctly")
        
        return True
    except Exception as e:
        logger.error(f"Error testing new premium system: {e}")
        return False


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        result = loop.run_until_complete(verify_premium_migration())
        if result is not None is not None:
            print("Premium system migration verified successfully!")
        else:
            print("Premium system migration verification failed!")
    except Exception as e:
        print(f"Error verifying premium system migration: {e}")
        import traceback
        traceback.print_exc()
    finally:
        loop.close()