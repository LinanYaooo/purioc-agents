---
name: conversation-logger
description: Record and log conversation history to a file for later reference, auditing, or documentation. Use when users mention recording conversation, start logging, conversation history, or want to persist chat logs. Automatically captures both user messages and assistant responses with timestamps.
---

# Conversation Logger

A skill for recording and persisting conversation history with OpenCode to a file.

## Features

- **Automatic Recording**: Once activated, records all subsequent conversation turns
- **Persistent State**: Maintains recording state across the session
- **Timestamped Logs**: Each entry includes date and time
- **Speaker Identification**: Clearly labels USER and ASSISTANT messages
- **Configurable Output**: Saves to customizable log file path

## Usage

### Starting Recording

When the user wants to start recording:

1. Check if recording is already active
2. If not active, initialize the log file
3. Set recording state to active
4. Confirm recording has started

**Trigger phrases:**
- "start recording"
- "log conversation"
- "record this"
- "begin logging"
- "start conversation log"

### Stopping Recording

When the user wants to stop recording:

1. Check if recording is currently active
2. If active, set recording state to inactive
3. Provide summary of what was recorded
4. Confirm recording has stopped

**Trigger phrases:**
- "stop recording"
- "end logging"
- "stop conversation log"
- "finish recording"

### Recording Messages

When recording is active:

1. **For USER messages:**
   - Get the current timestamp
   - Format: `[YYYY-MM-DD HH:MM:SS] [USER]\n---\n[message content]\n---\n`
   - Append to log file

2. **For ASSISTANT responses:**
   - Get the current timestamp
   - Format: `[YYYY-MM-DD HH:MM:SS] [ASSISTANT]\n---\n[response content]\n---\n`
   - Append to log file

## Implementation

Use the bundled Python scripts for managing recording state and writing to log files.

### Scripts

1. **logger.py** - Main recording script
   ```python
   # Start recording: python scripts/logger.py start [log_file_path]
   # Stop recording: python scripts/logger.py stop
   # Log message: python scripts/logger.py log "SPEAKER" "message content"
   # Check status: python scripts/logger.py status
   ```

2. **state management** - Uses `.conversation_logger_state.json` to track:
   - Whether recording is active
   - Current log file path
   - Session start time
   - Message count

## Log Format

```
[2026-03-18 14:30:00] [USER]
---
Hello, I need help with...
---

[2026-03-18 14:30:15] [ASSISTANT]
---
I'd be happy to help! What do you need assistance with?
---
```

## Workflow

### When user says "start recording":

1. Check current recording status
2. If already recording → inform user
3. If not recording → activate recording
4. Create/open log file (default: `talk.log` in project root)
5. Write session header
6. Confirm recording started

### When user says "stop recording":

1. Check current recording status
2. If not recording → inform user
3. If recording → deactivate recording
4. Write session footer with summary
5. Confirm recording stopped
6. Show log file path

### During active recording:

Every message exchange (both user and assistant) is automatically appended to the log file with:
- Timestamp
- Speaker identification
- Message content

## Configuration

**Default settings:**
- Log file: `talk.log` (in working directory)
- Time format: `YYYY-MM-DD HH:MM:SS`
- State file: `.conversation_logger_state.json`

**Custom log path:**
Users can specify a custom path when starting recording:
- "start recording to /path/to/custom.log"

## Examples

**Example 1 - Start recording:**
```
User: "Start recording"
Process: Initialize log → Activate recording → Confirm
Output: "Recording started! All messages will be saved to talk.log"
```

**Example 2 - Active recording:**
```
User: "What's the weather?"
Logged: [2026-03-18 14:30:00] [USER] --- What's the weather? ---
Assistant: "It's sunny today!"
Logged: [2026-03-18 14:30:05] [ASSISTANT] --- It's sunny today! ---
```

**Example 3 - Stop recording:**
```
User: "Stop recording"
Process: Deactivate → Write footer → Confirm
Output: "Recording stopped. Log saved to talk.log (15 messages recorded)"
```

## Important Notes

- Recording persists only for the current session
- State is maintained in a hidden JSON file
- Log files are appended to, not overwritten
- Previous session logs are preserved
- Recording automatically captures ALL messages once activated