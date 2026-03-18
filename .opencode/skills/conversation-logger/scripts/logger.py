#!/usr/bin/env python3
"""
Conversation Logger Script

Manages conversation recording state and writes to log files.
Usage:
    python logger.py start [log_file_path]  - Start recording
    python logger.py stop                   - Stop recording
    python logger.py log SPEAKER MESSAGE   - Log a message
    python logger.py status                - Check recording status
"""

import sys
import json
import os
from datetime import datetime

STATE_FILE = ".conversation_logger_state.json"
DEFAULT_LOG_FILE = "talk.log"

def get_state():
    """Load recording state from file."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {
        "recording": False,
        "log_file": DEFAULT_LOG_FILE,
        "session_start": None,
        "message_count": 0
    }

def save_state(state):
    """Save recording state to file."""
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

def get_timestamp():
    """Get current timestamp string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def start_recording(log_file=None):
    """Start recording conversation."""
    state = get_state()
    
    if state["recording"]:
        print(f"Recording already active (log: {state['log_file']})")
        return
    
    log_file = log_file or DEFAULT_LOG_FILE
    state["recording"] = True
    state["log_file"] = log_file
    state["session_start"] = get_timestamp()
    state["message_count"] = 0
    
    save_state(state)
    
    # Write session header
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"SESSION START: {state['session_start']}\n")
        f.write(f"{'='*60}\n\n")
    
    print(f"Recording started. Log file: {log_file}")

def stop_recording():
    """Stop recording conversation."""
    state = get_state()
    
    if not state["recording"]:
        print("Recording is not active")
        return
    
    # Write session footer
    with open(state["log_file"], 'a', encoding='utf-8') as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"SESSION END: {get_timestamp()}\n")
        f.write(f"Messages recorded: {state['message_count']}\n")
        f.write(f"{'='*60}\n\n")
    
    state["recording"] = False
    state["session_start"] = None
    
    count = state["message_count"]
    log_file = state["log_file"]
    
    save_state(state)
    
    print(f"Recording stopped. {count} messages saved to {log_file}")

def log_message(speaker, message):
    """Log a single message."""
    state = get_state()
    
    if not state["recording"]:
        return
    
    timestamp = get_timestamp()
    log_entry = f"[{timestamp}] [{speaker}]\n---\n{message}\n---\n\n"
    
    with open(state["log_file"], 'a', encoding='utf-8') as f:
        f.write(log_entry)
    
    state["message_count"] += 1
    save_state(state)

def get_status():
    """Get current recording status."""
    state = get_state()
    
    if state["recording"]:
        print(f"Recording: ACTIVE")
        print(f"Log file: {state['log_file']}")
        print(f"Session started: {state['session_start']}")
        print(f"Messages recorded: {state['message_count']}")
    else:
        print("Recording: INACTIVE")
        if state.get("log_file") and os.path.exists(state["log_file"]):
            print(f"Previous log: {state['log_file']}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python logger.py [start|stop|log|status] [args...]")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "start":
        log_file = sys.argv[2] if len(sys.argv) > 2 else None
        start_recording(log_file)
    
    elif command == "stop":
        stop_recording()
    
    elif command == "log":
        if len(sys.argv) < 4:
            print("Usage: python logger.py log SPEAKER MESSAGE")
            sys.exit(1)
        speaker = sys.argv[2]
        message = sys.argv[3]
        log_message(speaker, message)
    
    elif command == "status":
        get_status()
    
    else:
        print(f"Unknown command: {command}")
        print("Usage: python logger.py [start|stop|log|status] [args...]")
        sys.exit(1)

if __name__ == "__main__":
    main()