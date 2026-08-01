import asyncio
import os
from pathlib import Path
from datetime import datetime
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, FileReferenceExpired

from config import API_ID, API_HASH, SESSION_STRING, DOWNLOAD_DIR, CHUNK_SIZE, ALL_EXTENSIONS, MEDIA_TYPES
from database import upsert_chat, upsert_file, mark_downloaded, log_download, finish_download

_client: Client | None = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = Client(
            name="tgfiles",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=SESSION_STRING,
            in_memory=True,
        )
    return _client


async def ensure_started():
    client = get_client()
    if not client.is_connected:
        await client.start()


async def stop_client():
    global _client
    if _client and _client.is_connected:
        await _client.stop()
    _client = None


# ── helpers ──────────────────────────────────────────────────────────────────

def _detect_media_type(ext: str) -> str:
    for mtype, exts in MEDIA_TYPES.items():
        if ext.lower() in exts:
            return mtype
    return "document"


def _extract_file_meta(msg: Message) -> dict | None:
    """Pull filename/size/mime from any media type."""
    media = (
        msg.document
        or msg.video
        or msg.audio
        or msg.photo
        or msg.voice
        or msg.video_note
        or msg.animation
    )
    if media is None:
        return None

    # photos have no file_name attr
    file_name = getattr(media, "file_name", None) or f"photo_{msg.id}.jpg"
    file_size = getattr(media, "file_size", 0) or 0
    mime_type = getattr(media, "mime_type", "") or ""
    ext = Path(file_name).suffix.lower() or (
        "." + mime_type.split("/")[-1] if mime_type else ""
    )

    return {
        "chat_id": msg.chat.id,
        "message_id": msg.id,
        "file_name": file_name,
        "file_size": file_size,
        "mime_type": mime_type,
        "ext": ext,
        "media_type": _detect_media_type(ext),
        "date": msg.date.isoformat() if msg.date else datetime.utcnow().isoformat(),
    }


# ── public API ────────────────────────────────────────────────────────────────

async def fetch_chats() -> list[dict]:
    await ensure_started()
    client = get_client()
    chats = []
    async for dialog in client.get_dialogs():
        chat = dialog.chat
        row = {
            "id": chat.id,
            "title": chat.title or chat.first_name or str(chat.id),
            "type": str(chat.type),
            "username": chat.username or "",
            "last_synced": datetime.utcnow().isoformat(),
        }
        upsert_chat(row)
        chats.append(row)
    return chats


async def scan_chat(chat_id: int, limit: int = 200, progress_cb=None) -> int:
    """Scan chat messages and index media files into DB."""
    await ensure_started()
    client = get_client()
    count = 0
    async for msg in client.get_chat_history(chat_id, limit=limit):
        meta = _extract_file_meta(msg)
        if meta:
            upsert_file(meta)
            count += 1
            if progress_cb:
                progress_cb(count)
        await asyncio.sleep(0)  # yield to event loop
    return count


async def download_file(
    chat_id: int,
    message_id: int,
    file_name: str,
    progress_cb=None,
) -> Path | None:
    """Download file to DOWNLOAD_DIR. Returns local path or None on failure."""
    await ensure_started()
    client = get_client()

    dest = DOWNLOAD_DIR / str(chat_id)
    dest.mkdir(parents=True, exist_ok=True)
    out_path = dest / file_name

    # Already cached
    if out_path.exists():
        mark_downloaded(chat_id, message_id, str(out_path))
        return out_path

    log_download(chat_id, message_id, file_name, status="downloading")

    def _progress(current, total):
        if progress_cb and total:
            progress_cb(current, total)

    retries = 3
    for attempt in range(retries):
        try:
            msg = await client.get_messages(chat_id, message_id)
            if not msg or not msg.media:
                finish_download(chat_id, message_id, status="no_media")
                return None

            await client.download_media(
                msg,
                file_name=str(out_path),
                progress=_progress,
            )
            mark_downloaded(chat_id, message_id, str(out_path))
            finish_download(chat_id, message_id, status="done")
            return out_path

        except FileReferenceExpired:
            await asyncio.sleep(1)
            continue

        except FloodWait as e:
            await asyncio.sleep(e.value)
            continue

        except Exception as e:
            if attempt == retries - 1:
                finish_download(chat_id, message_id, status=f"error:{e}")
                return None
            await asyncio.sleep(2 ** attempt)

    return None


async def resolve_chat(ref: str) -> dict:
    """Resolve a chat by @username, invite link, or numeric id."""
    await ensure_started()
    client = get_client()
    ref = ref.strip()
    if ref.lstrip("-").isdigit():
        chat = await client.get_chat(int(ref))
    elif "t.me/" in ref:
        chat = await client.join_chat(ref)
    else:
        chat = await client.get_chat(ref.lstrip("@"))
    row = {
        "id": chat.id,
        "title": chat.title or chat.first_name or str(chat.id),
        "type": str(chat.type),
        "username": chat.username or "",
        "last_synced": datetime.utcnow().isoformat(),
    }
    upsert_chat(row)
    return row


async def get_me() -> dict:
    await ensure_started()
    me = await get_client().get_me()
    return {
        "id": me.id,
        "name": f"{me.first_name or ''} {me.last_name or ''}".strip(),
        "username": me.username or "",
        "phone": me.phone_number or "",
    }
