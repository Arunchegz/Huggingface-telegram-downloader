import os
from pathlib import Path

# Pyrogram
API_ID = int(os.environ.get("TG_API_ID", 0))
API_HASH = os.environ.get("TG_API_HASH", "")
SESSION_STRING = os.environ.get("TG_SESSION_STRING", "")

# Storage
BASE_DIR = Path(os.environ.get("PERSISTENT_STORAGE", "/data"))
DOWNLOAD_DIR = BASE_DIR / "downloads"
DB_PATH = BASE_DIR / "database.db"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

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
