import asyncio
import mimetypes
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Request
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config import DOWNLOAD_DIR
from database import (
    init_db, get_chats, get_files, toggle_favorite,
    recent_downloads, get_file_by_msg
)
from telegram_client import fetch_chats, scan_chat, download_file, get_me
from storage import (
    get_storage_stats, delete_cached_file, delete_chat_cache,
    clear_all_cache, evict_lru, resolve_local_path, iter_file_chunks
)

app = FastAPI(title="TGFiles API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Active download progress state (in-memory, per message)
_progress: dict[str, dict] = {}


# ── startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    init_db()


# ── account ───────────────────────────────────────────────────────────────────

@app.get("/me")
async def me():
    try:
        return await get_me()
    except Exception as e:
        raise HTTPException(503, f"Telegram error: {e}")


# ── chats ─────────────────────────────────────────────────────────────────────

@app.get("/chats")
async def list_chats(favorites: bool = False):
    rows = get_chats(favorites_only=favorites)
    return [dict(r) for r in rows]


@app.post("/chats/sync")
async def sync_chats():
    try:
        chats = await fetch_chats()
        return {"synced": len(chats)}
    except Exception as e:
        raise HTTPException(503, str(e))


@app.post("/chats/{chat_id}/favorite")
async def favorite_chat(chat_id: int):
    toggle_favorite(chat_id)
    return {"ok": True}


@app.post("/chats/{chat_id}/scan")
async def scan(chat_id: int, limit: int = 200):
    try:
        count = await scan_chat(chat_id, limit=limit)
        return {"indexed": count}
    except Exception as e:
        raise HTTPException(503, str(e))


# ── files ─────────────────────────────────────────────────────────────────────

@app.get("/files")
async def list_files(
    chat_id: Optional[int] = None,
    media_type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(default=100, le=500),
    offset: int = 0,
):
    rows = get_files(
        chat_id=chat_id,
        media_type=media_type,
        search=search,
        limit=limit,
        offset=offset,
    )
    return [dict(r) for r in rows]


# ── downloads ─────────────────────────────────────────────────────────────────

def _progress_key(chat_id, message_id):
    return f"{chat_id}:{message_id}"


async def _run_download(chat_id: int, message_id: int, file_name: str):
    key = _progress_key(chat_id, message_id)
    _progress[key] = {"current": 0, "total": 0, "done": False, "error": None}

    evict_lru()

    def on_progress(current, total):
        _progress[key]["current"] = current
        _progress[key]["total"] = total

    result = await download_file(
        chat_id=chat_id,
        message_id=message_id,
        file_name=file_name,
        progress_cb=on_progress,
    )

    if result:
        _progress[key]["done"] = True
        _progress[key]["local_path"] = str(result)
    else:
        _progress[key]["error"] = "Download failed"


@app.post("/download/{chat_id}/{message_id}")
async def start_download(chat_id: int, message_id: int, background_tasks: BackgroundTasks):
    row = get_file_by_msg(chat_id, message_id)
    if not row:
        raise HTTPException(404, "File not in index. Scan chat first.")

    key = _progress_key(chat_id, message_id)

    local = resolve_local_path(chat_id, message_id)
    if local:
        return {"status": "cached", "local_path": str(local)}

    if key in _progress and not _progress[key].get("done") and not _progress[key].get("error"):
        return {"status": "downloading", "progress": _progress[key]}

    background_tasks.add_task(_run_download, chat_id, message_id, row["file_name"])
    return {"status": "started"}


@app.get("/download/{chat_id}/{message_id}/progress")
async def download_progress(chat_id: int, message_id: int):
    key = _progress_key(chat_id, message_id)
    if key not in _progress:
        local = resolve_local_path(chat_id, message_id)
        if local:
            return {"done": True, "current": 0, "total": 0, "local_path": str(local)}
        return {"done": False, "current": 0, "total": 0}
    return _progress[key]


@app.get("/recent")
async def recent():
    rows = recent_downloads()
    return [dict(r) for r in rows]


# ── serve / stream ────────────────────────────────────────────────────────────

@app.get("/serve/{chat_id}/{message_id}")
async def serve_file(
    chat_id: int,
    message_id: int,
    request: Request,
):
    local = resolve_local_path(chat_id, message_id)
    if not local:
        raise HTTPException(404, "File not cached. Download first.")

    mime, _ = mimetypes.guess_type(str(local))
    mime = mime or "application/octet-stream"
    size = local.stat().st_size

    range_header = request.headers.get("Range")
    if range_header and range_header.startswith("bytes="):
        try:
            range_val = range_header[6:]
            start_str, end_str = range_val.split("-", 1)
            start = int(start_str)
            end = int(end_str) if end_str.strip() else size - 1
            if start > end or end >= size:
                return Response(
                    status_code=416,
                    headers={"Content-Range": f"bytes */{size}"},
                )
        except Exception:
            raise HTTPException(400, "Invalid Range header")

        headers = {
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Type": mime,
        }
        return StreamingResponse(
            iter_file_chunks(local, start=start, end=end),
            status_code=206,
            headers=headers,
            media_type=mime,
        )

    return FileResponse(
        path=str(local),
        media_type=mime,
        filename=local.name,
    )


# ── storage ───────────────────────────────────────────────────────────────────

@app.get("/storage")
async def storage_stats():
    return get_storage_stats()


@app.delete("/storage/file")
async def delete_file(local_path: str):
    ok = delete_cached_file(local_path)
    return {"deleted": ok}


@app.delete("/storage/chat/{chat_id}")
async def delete_chat_files(chat_id: int):
    count = delete_chat_cache(chat_id)
    return {"deleted": count}


@app.delete("/storage/all")
async def delete_all():
    count = clear_all_cache()
    return {"deleted": count}
