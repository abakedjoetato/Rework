#!/usr/bin/env python3
"""
Script to implement adaptive CSV processing frequency in the Discord bot.

This adds intelligent adjustment of CSV check frequency based on server activity:
- Active servers with events are checked more frequently (5 minutes)
- Inactive servers are checked less frequently (up to 30 minutes)
"""
import os
import asyncio
import re

def implement_adaptive_processing():
    """
    Implement adaptive CSV processing in the CSVProcessorCog class
    """
    file_path = "cogs/csv_processor.py"
    if not os.path.exists(file_path):
        print(f"Error: CSV processor file not found at {file_path}")
        return
    
    print(f"Implementing adaptive CSV processing in {file_path}...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Add the necessary instance variables to the __init__ method
    init_pattern = (
        "        # NEW: For tracking which files have been processed to avoid processing previous day's file repeatedly\n"
        "        self.processed_files_history = {}  # server_id -> set of filenames"
    )
    
    init_replacement = (
        "        # NEW: For tracking which files have been processed to avoid processing previous day's file repeatedly\n"
        "        self.processed_files_history = {}  # server_id -> set of filenames\n"
        "\n"
        "        # NEW: For adaptive processing frequency\n"
        "        self.server_activity = {}  # server_id -> {\"last_active\": datetime, \"empty_checks\": int}\n"
        "        self.default_check_interval = 5  # Default: check every 5 minutes\n"
        "        self.max_check_interval = 30  # Maximum: check every 30 minutes\n"
        "        self.inactive_threshold = 3  # After 3 empty checks, consider inactive"
    )
    
    content = content.replace(init_pattern, init_replacement)
    
    # 2. Create the adaptive processing method
    save_state_pattern = (
        "    async def _save_state(self):\n"
        "        \"\"\"Save current CSV processing state to database for all servers\"\"\"\n"
    )
    
    save_state_replacement = (
        "    async def _save_state(self):\n"
        "        \"\"\"Save current CSV processing state to database for all servers\"\"\"\n"
        "\n"
        "    async def _check_server_activity(self, server_id, events_found):\n"
        "        \"\"\"Track server activity for adaptive processing\n"
        "\n"
        "        Args:\n"
        "            server_id: The server ID to check\n"
        "            events_found: Number of events found in current check\n"
        "            \n"
        "        Returns:\n"
        "            int: Recommended minutes to wait until next check\n"
        "        \"\"\"\n"
        "        from datetime import datetime\n"
        "        now = datetime.utcnow()\n"
        "        \n"
        "        # Initialize server activity tracking if needed\n"
        "        if server_id not in self.server_activity:\n"
        "            self.server_activity[server_id] = {\n"
        "                \"last_active\": now,\n"
        "                \"empty_checks\": 0\n"
        "            }\n"
        "        \n"
        "        # Update activity metrics\n"
        "        if events_found > 0:\n"
        "            # Server is active, reset empty check counter\n"
        "            self.server_activity[server_id][\"last_active\"] = now\n"
        "            self.server_activity[server_id][\"empty_checks\"] = 0\n"
        "            return self.default_check_interval\n"
        "        else:\n"
        "            # No events found, increment empty check counter\n"
        "            self.server_activity[server_id][\"empty_checks\"] += 1\n"
        "            \n"
        "            # Calculate recommended interval based on inactivity\n"
        "            empty_checks = self.server_activity[server_id][\"empty_checks\"]\n"
        "            if empty_checks >= self.inactive_threshold:\n"
        "                # Scale the interval based on how many empty checks we've had\n"
        "                interval = min(self.default_check_interval + ((empty_checks - self.inactive_threshold + 1) * 5), \n"
        "                              self.max_check_interval)\n"
        "                logger.debug(f\"Server {server_id} has had {empty_checks} empty checks, next check in {interval} minutes\")\n"
        "                return interval\n"
        "            \n"
        "        # Default to standard interval\n"
        "        return self.default_check_interval\n"
        "\n"
    )
    
    content = content.replace(save_state_pattern, save_state_replacement)
    
    # 3. Modify the process_csv_files_task to use variable intervals
    task_pattern = (
        "    @tasks.loop(minutes=5)\n"
        "    async def process_csv_files_task(self):"
    )
    
    task_replacement = (
        "    @tasks.loop(minutes=5)\n"
        "    async def process_csv_files_task(self):\n"
        "        \"\"\"Background task for processing CSV files\n"
        "\n"
        "        This task runs regularly to check for new CSV files from game servers.\n"
        "        The interval is adaptively adjusted based on server activity:\n"
        "        - Active servers are checked more frequently (default: 5 minutes)\n"
        "        - Inactive servers are checked less frequently (up to max_check_interval)\n"
        "        \"\"\""
    )
    
    content = content.replace(task_pattern, task_replacement)
    
    # 4. Update the server processing to track events and adjust intervals
    process_server_pattern = "            return files_processed, events_processed"
    
    process_server_replacement = (
        "            # Track server activity for adaptive processing\n"
        "            recommended_interval = await self._check_server_activity(server_id, events_processed)\n"
        "            \n"
        "            return files_processed, events_processed"
    )
    
    content = content.replace(process_server_pattern, process_server_replacement)
    
    # 5. Add selective processing to avoid checking all servers every time
    process_csv_end_pattern = (
        "        # Save updated state to database\n"
        "        await self._save_state()\n"
        "        \n"
        "        logger.info(f\"CSV processing completed in {total_time:.2f} seconds. Processed {total_files} CSV files with {total_events} events.\")"
    )
    
    process_csv_end_replacement = (
        "        # Save updated state to database\n"
        "        await self._save_state()\n"
        "        \n"
        "        # Apply selective processing for next run - skip servers based on activity\n"
        "        for server_id in processed_servers:\n"
        "            # Skip scheduling for servers that had zero events and are over threshold\n"
        "            if server_id in self.server_activity and self.server_activity[server_id]['empty_checks'] >= self.inactive_threshold:\n"
        "                next_check_mins = min(self.default_check_interval + self.server_activity[server_id]['empty_checks'] * 5, \n"
        "                                     self.max_check_interval)\n"
        "                logger.debug(f\"Server {server_id} will be checked less frequently (every {next_check_mins} minutes)\")\n"
        "        \n"
        "        logger.info(f\"CSV processing completed in {total_time:.2f} seconds. Processed {total_files} CSV files with {total_events} events.\")"
    )
    
    content = content.replace(process_csv_end_pattern, process_csv_end_replacement)
    
    # 6. Initialize processed_servers tracking variable in the task
    process_csv_start_pattern = (
        "            logger.error(\"CSV processing task aborted due to lock being held\")\n"
        "            return\n"
        "        \n"
        "        self.is_processing = True"
    )
    
    process_csv_start_replacement = (
        "            logger.error(\"CSV processing task aborted due to lock being held\")\n"
        "            return\n"
        "        \n"
        "        # Dictionary to track servers processed in this run\n"
        "        processed_servers = {}\n"
        "        \n"
        "        self.is_processing = True"
    )
    
    content = content.replace(process_csv_start_pattern, process_csv_start_replacement)
    
    # 7. Update the task to track which servers were processed
    process_servers_pattern = (
        "        # Process each server configuration\n"
        "        total_files = 0\n"
        "        total_events = 0\n"
        "        \n"
        "        for server_id, config in server_configs.items():"
    )
    
    process_servers_replacement = (
        "        # Process each server configuration\n"
        "        total_files = 0\n"
        "        total_events = 0\n"
        "        \n"
        "        # Determine which servers to process based on activity\n"
        "        servers_to_process = {}\n"
        "        for server_id, config in server_configs.items():\n"
        "            # Always check servers with no activity history\n"
        "            if server_id not in self.server_activity:\n"
        "                servers_to_process[server_id] = config\n"
        "                continue\n"
        "                \n"
        "            # Skip inactive servers that were checked recently\n"
        "            empty_checks = self.server_activity[server_id]['empty_checks']\n"
        "            if empty_checks >= self.inactive_threshold:\n"
        "                from datetime import datetime, timedelta\n"
        "                now = datetime.utcnow()\n"
        "                last_processed = self.last_processed.get(server_id)\n"
        "                if last_processed is not None:\n"
        "                    # Calculate how long ago we last checked this server\n"
        "                    elapsed_mins = (now - last_processed).total_seconds() / 60\n"
        "                    # Calculate recommended interval based on inactivity\n"
        "                    recommended_interval = min(self.default_check_interval + ((empty_checks - self.inactive_threshold + 1) * 5),\n"
        "                                              self.max_check_interval)\n"
        "                    \n"
        "                    # If we haven't waited long enough, skip this server\n"
        "                    if elapsed_mins < recommended_interval:\n"
        "                        logger.debug(f\"Skipping inactive server {server_id} (last checked {elapsed_mins:.1f} mins ago, check interval {recommended_interval} mins)\")\n"
        "                        continue\n"
        "            \n"
        "            # Process this server\n"
        "            servers_to_process[server_id] = config\n"
        "            \n"
        "        logger.debug(f\"Processing {len(servers_to_process)}/{len(server_configs)} servers based on activity patterns\")\n"
        "            \n"
        "        for server_id, config in servers_to_process.items():"
    )
    
    content = content.replace(process_servers_pattern, process_servers_replacement)
    
    # 8. Update the track_server pattern
    process_single_server_pattern = (
        "            # Track files and events processed\n"
        "            total_files += files_processed\n"
        "            total_events += events_processed"
    )
    
    process_single_server_replacement = (
        "            # Track files and events processed\n"
        "            total_files += files_processed\n"
        "            total_events += events_processed\n"
        "            \n"
        "            # Record this server as processed in this run\n"
        "            processed_servers[server_id] = events_processed"
    )
    
    content = content.replace(process_single_server_pattern, process_single_server_replacement)
    
    # Write the modified content back to the file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("Successfully implemented adaptive CSV processing")


if __name__ == "__main__":
    implement_adaptive_processing()