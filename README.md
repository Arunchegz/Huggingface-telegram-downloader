---
title: TGManager
emoji: 📁
colorFrom: blue
colorTo: purple
sdk: gradio
sdk_version: "3.50.2"
app_file: app.py
pinned: false
---

# TG Manager — Stremio Addon

Auto-downloads media from a Telegram channel and serves it as a Stremio addon (catalog, meta, stream, subtitles, range streaming).

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
| `CHANNEL_REF` | Channel username, invite link, or ID to auto-download from |
| `PERSISTENT_STORAGE` | `/data` (HF persistent storage mount) |
| `MAX_CACHE_GB` | Max cache size in GB (default: 10) |
| `TMDB_API_KEY` | TMDB API key (posters, IMDB IDs, better title matching) |
| `HF_TOKEN` | HF token for deleting bucket files when channel messages are deleted |
| `STORAGE_BUCKET_REPO` | HF repo (e.g. `arunchez/TGmanager`) holding uploaded files |
| `STORAGE_BUCKET_TYPE` | `space` (default), `dataset`, or `model` |
| `STORAGE_BUCKET_BASE` | Public base URL of the bucket (optional; else files stream from the addon) |
| `EXTRA_TOKENS` | Comma-separated bot tokens / session strings for extra download sessions |
| `AUTO_DOWNLOAD` | `1` to enable the channel watcher (default) |

### 3. Deploy

- Space type: **Gradio**
- Python: 3.10+
- Entry point: `app.py`

## Stremio install

Add the addon via `https://<your-space>.hf.space/manifest.json`.

Resources: catalog (`tgdm:`/`tgds:`), meta, stream (also on `tt` IMDB IDs), subtitles (OpenSubtitles v3), range streaming via `/tgfile/{chat_id}/{message_id}`.

## Architecture

```
FastAPI (Gradio server, port 7860)
    ├── Stremio addon routes (catalog / meta / stream / subtitles / tgfile)
    ├── Pyrogram MTProto client (instant new-message + delete-sync updates)
    ├── SQLite metadata DB
    └── HF Persistent Storage (/data)
```
