#!/usr/bin/env python3
"""
Script to reduce logging levels for AsyncSSH connection logs.

When scaling to 1000 servers, connection logs would flood the console.
This script moves these logs to DEBUG level to reduce console noise.
"""
import os
import re

def reduce_asyncssh_logging():
    """
    Reduce logging levels for AsyncSSH/SFTP connections to DEBUG
    """
    # Create a setup file for logger configuration
    setup_file = "utils/logging_setup.py"
    
    # Check if the file already exists
    if os.path.exists(setup_file):
        print(f"Found existing logging setup file: {setup_file}")
        with open(setup_file, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        # Create a new file with logging setup
        print(f"Creating new logging setup file: {setup_file}")
        content = """#!/usr/bin/env python3
\"\"\"
Logging setup utility for the Discord bot.

This module configures the logging format and levels for different loggers.
It helps reduce console noise by adjusting log levels appropriately.
\"\"\"
import logging

def setup_logging():
    \"\"\"Set up logging configuration for the bot
    
    This configures:
    - Default INFO level for main application
    - Reduced levels for noisy external libraries
    - Custom format with timestamps and logger name
    \"\"\"
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Configure console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    
    # Set format with timestamp, level, and logger name
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s', 
                                 datefmt='%Y-%m-%d %H:%M:%S,%f')
    console.setFormatter(formatter)
    
    # Add handler to root logger
    root_logger.addHandler(console)
    
    # Reduce noise from external libraries
    logging.getLogger('discord').setLevel(logging.WARNING)
    logging.getLogger('websockets').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
"""
    
    # Add asyncssh logging configuration
    if "logging.getLogger('asyncssh')" not in content:
        # Add configuration if not already present
        insert_point = "    logging.getLogger('asyncio').setLevel(logging.WARNING)"
        insert_content = insert_point + """
    
    # NEW: Reduce spammy connection logs from AsyncSSH
    logging.getLogger('asyncssh').setLevel(logging.WARNING)
    # Only show warnings and errors from the SFTP client
    logging.getLogger('asyncssh.sftp').setLevel(logging.WARNING)"""
        
        content = content.replace(insert_point, insert_content)
    else:
        print("AsyncSSH logging configuration already exists in setup file")
    
    # Save the setup file
    with open(setup_file, 'w', encoding='utf-8') as f:
        f.write(content)
        
    print(f"Updated logging setup file: {setup_file}")
    
    # Now update the bot.py file to use our custom logging setup
    bot_file = "bot.py"
    if not os.path.exists(bot_file):
        print(f"Bot file not found: {bot_file}")
        return
        
    print(f"Updating bot file: {bot_file}")
    
    with open(bot_file, 'r', encoding='utf-8') as f:
        bot_content = f.read()
        
    # Add import statement if not present
    if "from utils.logging_setup import setup_logging" not in bot_content:
        import_pattern = "import logging\n"
        import_replacement = "import logging\nfrom utils.logging_setup import setup_logging\n"
        
        bot_content = bot_content.replace(import_pattern, import_replacement)
        
    # Add function call if not present
    if "setup_logging()" not in bot_content:
        # Find the main entry point
        if "if __name__ == '__main__':" in bot_content:
            # Add to the beginning of main
            main_pattern = "if __name__ == '__main__':"
            main_replacement = "if __name__ == '__main__':\n    # Set up custom logging configuration\n    setup_logging()"
            
            bot_content = bot_content.replace(main_pattern, main_replacement)
        else:
            # Try to insert after imports
            try:
                import_section = re.search(r"(^import .*?\n\n)|(^from .*?\n\n)", bot_content, re.MULTILINE | re.DOTALL).group()
                bot_content = bot_content.replace(import_section, import_section + "# Set up custom logging configuration\nsetup_logging()\n\n")
            except AttributeError:
                # Fall back to beginning of file
                bot_content = "# Set up custom logging configuration\nsetup_logging()\n\n" + bot_content
                
    # Save the updated bot file
    with open(bot_file, 'w', encoding='utf-8') as f:
        f.write(bot_content)
        
    print(f"Updated bot file: {bot_file}")
    
    # Finally, make sure the utils directory has an __init__.py file
    init_file = "utils/__init__.py"
    if not os.path.exists(init_file):
        with open(init_file, 'w', encoding='utf-8') as f:
            f.write("# Utils package\n")
        print(f"Created __init__.py file: {init_file}")

if __name__ == "__main__":
    reduce_asyncssh_logging()