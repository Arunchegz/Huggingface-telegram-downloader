---
title: TGManager
emoji: 📁
colorFrom: blue
colorTo: purple
sdk: gradio
sdk_version: "4.0.0"
app_file: app.py
pinned: false
---

# TGFiles — Telegram File Browser for Hugging Face Spaces

Browse, search, and download files from your Telegram chats via a Gradio web UI.

## Setup

### 1. Generate a Pyrogram session string (run locally)

```python
from pyrogram import Client
with Client("gen", api_id=YOUR_API_ID, api_hash="YOUR_API_HASH") as c:
    print(c.export_session_string())
```

### 2. Set environment variables in HF Space Settings

| Variable | Description |
|---|---|
| `TG_API_ID` | Telegram API ID (from my.telegram.org) |
| `TG_API_HASH` | Telegram API Hash |
| `TG_SESSION_STRING` | Pyrogram StringSession |
| `PERSISTENT_STORAGE` | `/data` (HF persistent storage mount) |
| `MAX_CACHE_GB` | Max cache size in GB (default: 10) |

### 3. Deploy

- Space type: **Gradio**
- Python: 3.10+
- Entry point: `app.py`

## Features

- 👤 Account info + chat sync
- 📂 Browse files per chat with type filter
- 🔍 Search by filename, type, size, date
- ⬇ One-click download with progress
- 💾 Storage manager with LRU eviction
- 🕓 Recent downloads log
- ⭐ Favorite chats

## Architecture

```
Gradio UI (port 7860)
    └── FastAPI (port 7861, internal)
            └── Pyrogram MTProto client
            └── SQLite metadata DB
            └── HF Persistent Storage (/data)
```
