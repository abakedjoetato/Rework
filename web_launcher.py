#!/usr/bin/env python3
"""
Web Launcher for Tower of Temptation Bot

This small Flask application serves a web interface to launch
the bot with different options.
"""

import os
import sys
import subprocess
import threading
from flask import Flask, request, render_template, redirect, url_for, send_from_directory

app = Flask(__name__)

# Global variables to store process and output
bot_process = None
bot_output = []
current_mode = None

def run_command(command):
    """Run a shell command and capture output."""
    global bot_process, bot_output, current_mode
    
    # Clear previous output
    bot_output = []
    
    # Start process
    bot_process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        shell=True
    )
    
    # Read output line by line and store it
    for line in iter(bot_process.stdout.readline, ''):
        bot_output.append(line)
        print(line, end='')
        sys.stdout.flush()
    
    bot_process.wait()
    
    # Reset process when done
    if bot_process.returncode != 0:
        bot_output.append(f"Process exited with code {bot_process.returncode}")
    
    bot_process = None

@app.route('/')
def index():
    """Serve the main page or handle run commands."""
    run_param = request.args.get('run')
    
    if run_param:
        # Start the appropriate process based on the run parameter
        if bot_process is None:  # Only start if no process is running
            global current_mode
            current_mode = run_param
            
            command = f"./launch.sh {run_param}"
            threading.Thread(target=run_command, args=(command,), daemon=True).start()
        
        # Redirect to status page
        return redirect(url_for('status'))
    
    # Serve the HTML directly if no run parameter
    try:
        with open('index.html', 'r') as f:
            return f.read()
    except FileNotFoundError:
        return "Index file not found. Please create index.html."

@app.route('/status')
def status():
    """Show the status and output of the running process."""
    global bot_output, current_mode
    
    # Determine process status
    process_status = "Running" if bot_process else "Not running"
    if current_mode:
        process_status += f" ({current_mode} mode)"
    
    # Create a simple HTML page to display status and output
    output_html = "<br>".join(bot_output[-100:])  # Show last 100 lines
    
    html = f"""<!DOCTYPE html>
    <html>
    <head>
        <title>Bot Status</title>
        <meta http-equiv="refresh" content="5"> <!-- Refresh every 5 seconds -->
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            pre {{ background-color: #f5f5f5; padding: 10px; border-radius: 4px; overflow-x: auto; }}
            .status {{ padding: 15px; background-color: #f8f9fa; border-left: 5px solid #ddd; margin: 20px 0; }}
            .running {{ border-left-color: #4CAF50; }}
            .stopped {{ border-left-color: #F44336; }}
            .buttons {{ margin: 20px 0; }}
            .buttons a {{ padding: 10px 15px; text-decoration: none; color: white; border-radius: 4px; margin-right: 10px; }}
            .home {{ background-color: #2196F3; }}
            .stop {{ background-color: #F44336; }}
        </style>
    </head>
    <body>
        <h1>Tower of Temptation Bot Status</h1>
        
        <div class="status {'running' if bot_process else 'stopped'}">
            <strong>Status:</strong> {process_status}
        </div>
        
        <div class="buttons">
            <a href="/" class="home">Back to Home</a>
            <a href="/stop" class="stop">Stop Process</a>
        </div>
        
        <h2>Output:</h2>
        <pre>{output_html}</pre>
    </body>
    </html>
    """
    
    return html

@app.route('/stop')
def stop_process():
    """Stop the currently running process."""
    global bot_process
    
    if bot_process:
        bot_process.terminate()
        bot_output.append("Process terminated by user.")
        bot_process = None
    
    return redirect(url_for('status'))

if __name__ == '__main__':
    # Start the Flask app
    app.run(host='0.0.0.0', port=5000)