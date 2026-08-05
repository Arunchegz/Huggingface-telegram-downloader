import os
import shutil
from pathlib import Path
from typing import Generator

import aiofiles

from config import DOWNLOAD_DIR, MAX_CACHE_BYTES
from database import get_conn


# ── disk helpers ──────────────────────────────────────────────────────────────

def get_all_cached_files() -> list[Path]:
    """All files under DOWNLOAD_DIR, sorted oldest-first."""
    files = [p for p in DOWNLOAD_DIR.rglob("*") if p.is_file()]
    files.sort(key=lambda p: p.stat().st_mtime)
    return files


def get_cache_size() -> int:
    """Total bytes used under DOWNLOAD_DIR."""
    return sum(p.stat().st_size for p in DOWNLOAD_DIR.rglob("*") if p.is_file())


def get_storage_stats() -> dict:
    total, used, free = shutil.disk_usage(DOWNLOAD_DIR)
    cache_used = get_cache_size()
    return {
        "disk_total_gb": round(total / 1024 ** 3, 2),
        "disk_used_gb": round(used / 1024 ** 3, 2),
        "disk_free_gb": round(free / 1024 ** 3, 2),
        "cache_used_gb": round(cache_used / 1024 ** 3, 3),
        "cache_limit_gb": round(MAX_CACHE_BYTES / 1024 ** 3, 2),
        "cache_file_count": len(get_all_cached_files()),
        "cache_pct": round(cache_used / MAX_CACHE_BYTES * 100, 1) if MAX_CACHE_BYTES else 0,
    }


# ── eviction ──────────────────────────────────────────────────────────────────

def evict_lru(needed_bytes: int = 0) -> list[str]:
    """
    Delete oldest files until cache is under MAX_CACHE_BYTES.
    If needed_bytes > 0, also ensure that much free space exists.
    Returns list of deleted file names.
    """
    files = get_all_cached_files()
    current_size = sum(p.stat().st_size for p in files)
    free_space = shutil.disk_usage(DOWNLOAD_DIR).free
    deleted = []

    for path in files:
        over_limit = current_size > MAX_CACHE_BYTES
        not_enough_free = needed_bytes > 0 and free_space < needed_bytes

        if not over_limit and not not_enough_free:
            break

        try:
            file_size = path.stat().st_size
            path.unlink()
            _clear_db_path(path)
            deleted.append(path.name)
            current_size -= file_size
            free_space += file_size
        except OSError:
            continue

    return deleted


def _clear_db_path(path: Path):
    with get_conn() as conn:
        conn.execute(
            "UPDATE files SET downloaded=0, local_path=NULL WHERE local_path=?",
            (str(path),)
        )


# ── manual delete ─────────────────────────────────────────────────────────────

def delete_cached_file(local_path: str) -> bool:
    """Delete a specific cached file by its local path."""
    p = Path(local_path)
    if p.exists() and p.is_file():
        try:
            p.unlink()
            _clear_db_path(p)
            return True
        except OSError:
            return False
    return False


def delete_chat_cache(chat_id: int) -> int:
    """Delete all cached files for a chat. Returns count deleted."""
    chat_dir = DOWNLOAD_DIR / str(chat_id)
    count = 0
    if chat_dir.exists():
        for p in chat_dir.rglob("*"):
            if p.is_file():
                try:
                    p.unlink()
                    _clear_db_path(p)
                    count += 1
                except OSError:
                    continue
        try:
            chat_dir.rmdir()
        except OSError:
            pass
    return count


def clear_all_cache() -> int:
    """Nuke everything under DOWNLOAD_DIR. Returns count deleted."""
    files = get_all_cached_files()
    count = 0
    for p in files:
        try:
            p.unlink()
            _clear_db_path(p)
            count += 1
        except OSError:
            continue
    for d in sorted(DOWNLOAD_DIR.rglob("*"), reverse=True):
        if d.is_dir():
            try:
                d.rmdir()
            except OSError:
                pass
    return count


# ── file serving ──────────────────────────────────────────────────────────────

async def iter_file_chunks(path: Path, start: int = 0, end: int = None, chunk: int = 65536):
    """Async generator: yield byte chunks for range-aware streaming without blocking thread pool."""
    size = path.stat().st_size
    end = end if end is not None else size - 1
    pos = start
    async with aiofiles.open(path, "rb") as f:
        await f.seek(start)
        while pos <= end:
            to_read = min(chunk, end - pos + 1)
            data = await f.read(to_read)
            if not data:
                break
            yield data
            pos += len(data)


def resolve_local_path(chat_id: int, message_id: int) -> Path | None:
    """Return cached path if file exists on disk."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT local_path FROM files WHERE chat_id=? AND message_id=? AND downloaded=1",
            (chat_id, message_id)
        ).fetchone()
    if row and row["local_path"]:
        p = Path(row["local_path"]).resolve()
        download_root = DOWNLOAD_DIR.resolve()
        if p.exists() and p.is_relative_to(download_root):
            return p
    return None
