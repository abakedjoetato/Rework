import zipfile
import os
import shutil

# Extract the zip file
with zipfile.ZipFile('attached_assets/Premdebug-main.zip', 'r') as zip_ref:
    # Extract all contents to a temporary directory
    zip_ref.extractall('temp_extract')

# List files in the extracted directory
print("Files extracted to temp_extract:")
for root, dirs, files in os.walk('temp_extract'):
    for file in files:
        print(os.path.join(root, file))

# Move files from the temporary directory to the main directory
source_dir = 'temp_extract'
target_dir = '.'

# Get list of files in source directory
for item in os.listdir(source_dir):
    source_item = os.path.join(source_dir, item)
    target_item = os.path.join(target_dir, item)
    
    # If it's a directory, move its contents
    if os.path.isdir(source_item):
        print(f"Extracting files from {zip_file} to {target_dir}")
        for subitem in os.listdir(source_item):
            source_subitem = os.path.join(source_item, subitem)
            target_subitem = os.path.join(target_dir, subitem)
            
            if os.path.exists(target_subitem):
                print(f"Overwriting {target_subitem}")
                # Remove existing file or directory
                if os.path.isdir(target_subitem):
                    shutil.rmtree(target_subitem)
                else:
                    os.remove(target_subitem)
            
            # Move file or directory
            if os.path.isdir(source_subitem):
                shutil.copytree(source_subitem, target_subitem)
                print(f"Copied directory {subitem}")
            else:
                shutil.copy2(source_subitem, target_subitem)
                print(f"Copied file {subitem}")
    else:
        # Move file
        if os.path.exists(target_item):
            print(f"Overwriting {target_item}")
            os.remove(target_item)
        shutil.copy2(source_item, target_item)
        print(f"Copied file {item}")

print("All files moved to main directory")