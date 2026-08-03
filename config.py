import os
from pathlib import Path

# Pyrogram
API_ID = int(os.environ.get("TG_API_ID") or 0)
API_HASH = os.environ.get("TG_API_HASH", "")
SESSION_STRING = os.environ.get("TG_SESSION_STRING", "")

# Storage
BASE_DIR = Path(os.environ.get("PERSISTENT_STORAGE", "/data"))
DOWNLOAD_DIR = BASE_DIR / "downloads"
DB_PATH = BASE_DIR / "database.db"
STATE_DIR = BASE_DIR / "state"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Extra download sessions (comma-separated): bot tokens or session strings.
# Each session has its own rate limits -> spreads GetFile flood waits.
EXTRA_TOKENS = [
    t.strip() for t in os.environ.get("EXTRA_TOKENS", "").split(",") if t.strip()
]

# Auto-downloader
CHANNEL_REF = os.environ.get("CHANNEL_REF", "").strip()
AUTO_DOWNLOAD = os.environ.get("AUTO_DOWNLOAD", "1") == "1"
INITIAL_SCAN_LIMIT = int(os.environ.get("INITIAL_SCAN_LIMIT", "500"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
# Default: one concurrent download per session (user + extras) so each
# account's independent rate-limit bucket is used in parallel.
_mcd = os.environ.get("MAX_CONCURRENT_DOWNLOADS", "").strip()
MAX_CONCURRENT_DOWNLOADS = (int(_mcd) if _mcd else 0) or 1 + len(EXTRA_TOKENS)

# TMDB (posters, IMDB IDs, title matching for the Stremio addon)
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")

# HF bucket (external repo holding uploaded files for streaming)
HF_TOKEN = os.environ.get("HF_TOKEN", "")
STORAGE_BUCKET_REPO = os.environ.get("STORAGE_BUCKET_REPO", "").strip()  # e.g. "arunchez/TGmanager"
STORAGE_BUCKET_TYPE = os.environ.get("STORAGE_BUCKET_TYPE", "space").strip()  # space | dataset | model

# Limits
MAX_CACHE_GB = float(os.environ.get("MAX_CACHE_GB", "10"))
MAX_CACHE_BYTES = MAX_CACHE_GB * 1024 ** 3
CHUNK_SIZE = 1024 * 1024  # 1 MB

# Media types
MEDIA_TYPES = {
    "video": [".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"],
    "audio": [".mp3", ".flac", ".wav", ".ogg", ".m4a", ".aac"],
    "document": [".pdf", ".docx", ".xlsx", ".pptx", ".txt", ".csv"],
    "image": [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"],
    "archive": [".zip", ".rar", ".7z", ".tar", ".gz", ".tar.gz"],
}

ALL_EXTENSIONS = {ext for exts in MEDIA_TYPES.values() for ext in exts}
