import zipfile
import os
import shutil
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_zip(zip_file_path, extract_dir='temp_extract'):
    """
    Extract a zip file to a specified directory
    
    Args:
        zip_file_path: Path to the zip file
        extract_dir: Directory where contents should be extracted
    
    Returns:
        bool: True if extraction was successful, False otherwise
    """
    if not os.path.exists(zip_file_path):
        logger.error(f"Zip file not found: {zip_file_path}")
        return False

    try:
        # Create extract directory if it doesn't exist
        if not os.path.exists(extract_dir):
            os.makedirs(extract_dir)
            
        # Extract the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # Extract all contents to the temp directory
            zip_ref.extractall(extract_dir)
            
        # List files in the extracted directory
        logger.info(f"Files extracted to {extract_dir}:")
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                logger.info(os.path.join(root, file))
                
        return True
    except Exception as e:
        logger.error(f"Error extracting zip file: {e}")
        return False

# This will be defined in the main function

def move_extracted_files(source_dir='temp_extract', target_dir='.'):
    """
    Move files from the extract directory to the target directory
    
    Args:
        source_dir: Source directory with extracted files
        target_dir: Target directory where files should be moved
        
    Returns:
        bool: True if operation was successful, False otherwise
    """
    if not os.path.exists(source_dir):
        logger.error(f"Source directory not found: {source_dir}")
        return False
        
    try:
        # Get list of files in source directory
        for item in os.listdir(source_dir):
            source_item = os.path.join(source_dir, item)
            target_item = os.path.join(target_dir, item)
            
            # If it's a directory, move its contents
            if os.path.isdir(source_item):
                logger.info(f"Extracting directory contents from {source_item} to {target_dir}")
                for subitem in os.listdir(source_item):
                    source_subitem = os.path.join(source_item, subitem)
                    target_subitem = os.path.join(target_dir, subitem)
                    
                    if os.path.exists(target_subitem):
                        logger.info(f"Overwriting {target_subitem}")
                        # Remove existing file or directory
                        if os.path.isdir(target_subitem):
                            shutil.rmtree(target_subitem)
                        else:
                            os.remove(target_subitem)
                    
                    # Move file or directory
                    if os.path.isdir(source_subitem):
                        shutil.copytree(source_subitem, target_subitem)
                        logger.info(f"Copied directory {subitem}")
                    else:
                        shutil.copy2(source_subitem, target_subitem)
                        logger.info(f"Copied file {subitem}")
            else:
                # Move file
                if os.path.exists(target_item):
                    logger.info(f"Overwriting {target_item}")
                    os.remove(target_item)
                shutil.copy2(source_item, target_item)
                logger.info(f"Copied file {item}")

        logger.info("All files moved successfully")
        return True
    except Exception as e:
        logger.error(f"Error moving files: {e}")
        return False

def cleanup(extract_dir='temp_extract'):
    """Remove temporary extraction directory"""
    try:
        if os.path.exists(extract_dir):
            shutil.rmtree(extract_dir)
            logger.info(f"Removed temporary directory: {extract_dir}")
    except Exception as e:
        logger.error(f"Error removing temporary directory: {e}")

def main():
    """Main function to extract and process a zip file"""
    # Define the zip file path from command line argument or use default
    zip_file = sys.argv[1] if len(sys.argv) > 1 else 'attached_assets/BadReplit-main.zip'
    
    logger.info(f"Processing zip file: {zip_file}")
    
    # Extract the zip file
    if extract_zip(zip_file):
        # Move extracted files to target directory
        if move_extracted_files():
            # Clean up
            cleanup()
            logger.info("Zip extraction and file copying completed successfully")
            return 0
    
    logger.error("Failed to extract zip file or move files")
    return 1

# Run the main function if the script is executed directly
if __name__ == "__main__":
    sys.exit(main())