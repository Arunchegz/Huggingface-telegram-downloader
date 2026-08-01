import asyncio
import json
import os
import re
from pathlib import Path
from datetime import datetime
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, FileReferenceExpired

from config import (API_ID, API_HASH, SESSION_STRING, DOWNLOAD_DIR, CHUNK_SIZE,
                    ALL_EXTENSIONS, MEDIA_TYPES, STATE_DIR, INITIAL_SCAN_LIMIT,
                    POLL_INTERVAL, MAX_CACHE_BYTES, EXTRA_TOKENS,
                    MAX_CONCURRENT_DOWNLOADS)
from database import upsert_chat, upsert_file, mark_downloaded, log_download, finish_download, get_file_by_msg
from storage import get_cache_size

_client: Client | None = None
_watcher_client: Client | None = None


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


def get_watcher_client() -> Client:
    global _watcher_client
    if _watcher_client is None:
        _watcher_client = Client(
            name="tgfiles_watcher",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=SESSION_STRING,
            in_memory=True,
        )
    return _watcher_client


_extra_clients: list[Client] = []


def _is_bot_token(tok: str) -> bool:
    return bool(re.match(r"^\d+:[A-Za-z0-9_-]{20,}$", tok))


def get_download_clients() -> list[Client]:
    """Pool of clients used for downloads: watcher session + extra sessions."""
    global _extra_clients
    if not _extra_clients:
        for i, tok in enumerate(EXTRA_TOKENS):
            if _is_bot_token(tok):
                _extra_clients.append(Client(
                    name=f"tgfiles_bot_{i}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    bot_token=tok,
                    in_memory=True,
                ))
            else:
                _extra_clients.append(Client(
                    name=f"tgfiles_extra_{i}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    session_string=tok,
                    in_memory=True,
                ))
    return [get_watcher_client()] + _extra_clients


async def start_download_clients(ref: str, chat_id: int) -> None:
    """Connect every download client and warm its peer storage."""
    for c in get_download_clients():
        try:
            if not c.is_connected:
                await c.start()
            try:
                await c.get_chat(ref)
            except Exception:
                pass
        except Exception:
            continue


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
    client: Client | None = None,
) -> Path | None:
    """Download file to DOWNLOAD_DIR. Returns local path or None on failure."""
    # Use watcher client (download pool head) by default, not the browse client,
    # to avoid rate-limit collisions with scan/fetch operations.
    client = client or get_watcher_client()
    if not client.is_connected:
        await client.start()

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
    if "t.me/" in ref:
        try:
            chat = await client.get_chat(ref)
            if not getattr(chat, "id", None):
                raise ValueError(f"not a member yet: {ref}")
        except Exception:
            try:
                chat = await client.join_chat(ref)
            except Exception:
                pass
            chat = await client.get_chat(ref)
    elif ref.lstrip("-").isdigit():
        chat = await client.get_chat(int(ref))
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


# ── auto-downloader ───────────────────────────────────────────────────────────

def _state_file(chat_id: int) -> Path:
    return STATE_DIR / f"last_msg_{chat_id}.json"


def get_last_msg_id(chat_id: int) -> int:
    try:
        return json.loads(_state_file(chat_id).read_text()).get("last_msg_id", 0)
    except (OSError, ValueError):
        return 0


def _save_last_msg_id(chat_id: int, msg_id: int):
    try:
        _state_file(chat_id).write_text(json.dumps({"last_msg_id": msg_id}))
    except OSError:
        pass


def _is_downloaded(chat_id: int, msg_id: int) -> bool:
    row = get_file_by_msg(chat_id, msg_id)
    return bool(row) and bool(row["downloaded"])


def _cache_full(needed: int) -> bool:
    return get_cache_size() + needed > MAX_CACHE_BYTES


async def download_channel_all(
    chat_id: int,
    limit: int = INITIAL_SCAN_LIMIT,
    status_cb=None,
    client: Client | None = None,
) -> int:
    """Download every media file in the channel, skipping already-cached ones."""
    client = client or get_client()
    if not client.is_connected:
        await client.start()
    last_id = get_last_msg_id(chat_id)
    dl_clients = get_download_clients()

    pending = []
    async for msg in client.get_chat_history(chat_id, limit=limit):
        meta = _extract_file_meta(msg)
        if not meta:
            continue
        upsert_file(meta)
        if msg.id <= last_id and _is_downloaded(chat_id, msg.id):
            continue
        if _cache_full(meta["file_size"]):
            if status_cb:
                status_cb(f"Cache limit reached, stopping at {meta['file_name']}")
            break
        pending.append(meta)
        last_id = max(last_id, msg.id)
        await asyncio.sleep(0)

    _save_last_msg_id(chat_id, last_id)

    if not pending:
        return 0

    if status_cb:
        status_cb(f"🎯 {len(pending)} files to download (clients: {len(dl_clients)})")

    sem = asyncio.Semaphore(min(MAX_CONCURRENT_DOWNLOADS, len(dl_clients)))
    counter = {"n": 0}
    lock = asyncio.Lock()

    async def _one(meta: dict) -> bool:
        async with sem:
            try:
                if _cache_full(meta["file_size"]):
                    return False
                async with lock:
                    idx = counter["n"] % len(dl_clients)
                    counter["n"] += 1
                dl_client = dl_clients[idx]
                if status_cb:
                    status_cb(f"⬇ [{meta['message_id']}] {meta['file_name']} "
                              f"({meta['file_size']//1024//1024} MB) @ {dl_client.name}")
                path = await download_file(chat_id, meta["message_id"],
                                           meta["file_name"], client=dl_client)
                return path is not None
            except Exception as e:
                if status_cb:
                    status_cb(f"⚠ [{meta['message_id']}] {meta['file_name']}: {e}")
                return False

    results = await asyncio.gather(*[_one(m) for m in pending])
    return sum(1 for r in results if r)


async def watch_channel_new(
    chat_id: int,
    interval: int = POLL_INTERVAL,
    status_cb=None,
    client: Client | None = None,
):
    """Poll the channel and download newly added media files."""
    client = client or get_client()
    if not client.is_connected:
        await client.start()
    last_id = get_last_msg_id(chat_id)
    if status_cb:
        status_cb(f"👀 Watching channel (last msg {last_id}, poll {interval}s)")

    while True:
        try:
            newest_id = last_id
            dl_clients = get_download_clients()
            pending = []
            async for msg in client.get_chat_history(chat_id, limit=50):
                meta = _extract_file_meta(msg)
                if not meta:
                    continue
                upsert_file(meta)
                if msg.id <= last_id:
                    break
                if _cache_full(meta["file_size"]):
                    if status_cb:
                        status_cb(f"Cache limit reached, skipping {meta['file_name']}")
                    continue
                pending.append(meta)
                newest_id = max(newest_id, msg.id)
                await asyncio.sleep(0)
            if newest_id > last_id:
                last_id = newest_id
                _save_last_msg_id(chat_id, last_id)

            if pending:
                sem = asyncio.Semaphore(min(MAX_CONCURRENT_DOWNLOADS, len(dl_clients)))
                counter = {"n": 0}
                lock = asyncio.Lock()

                async def _one(meta: dict):
                    async with sem:
                        try:
                            async with lock:
                                idx = counter["n"] % len(dl_clients)
                                counter["n"] += 1
                            dl_client = dl_clients[idx]
                            if status_cb:
                                status_cb(f"⬇ NEW [{meta['message_id']}] {meta['file_name']} "
                                          f"@ {dl_client.name}")
                            path = await download_file(
                                chat_id, meta["message_id"], meta["file_name"],
                                client=dl_client)
                            if path and status_cb:
                                status_cb(f"✅ NEW [{meta['message_id']}] {meta['file_name']}")
                        except Exception as e:
                            if status_cb:
                                status_cb(f"⚠ NEW [{meta['message_id']}] {meta['file_name']}: {e}")

                await asyncio.gather(*[_one(m) for m in pending])
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception as e:
            if status_cb:
                status_cb(f"⚠ watch error: {e}")
        await asyncio.sleep(interval)


async def _chat_from_invite(client: Client, ref: str):
    """Resolve chat id from a t.me invite link without requiring a join."""
    m = re.search(r"t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)", ref)
    if not m:
        return None
    try:
        from pyrogram.raw import functions

        res = await client.invoke(functions.messages.CheckChatInvite(hash=m.group(1)))
    except Exception:
        return None
    raw = getattr(res, "chat", None)
    if raw is None:
        for raw in getattr(res, "chats", []) or []:
            break
    if raw is not None:
        try:
            await client.fetch_peers([raw])
        except Exception:
            pass
        cid = getattr(raw, "id", None)
        if cid is None:
            return None
        if getattr(raw, "channel", False) or getattr(raw, "megagroup", False):
            return -1000000000000 - cid
        return -cid
    cid = getattr(res, "chat_id", None)
    if cid is None:
        return None
    if getattr(res, "channel", False) or getattr(res, "broadcast", False):
        return -1000000000000 - cid
    return -cid


async def auto_download_main(status_cb=None) -> None:
    """Resolve CHANNEL_REF, download existing files, then watch for new ones."""
    if not os.environ.get("CHANNEL_REF"):
        if status_cb:
            status_cb("CHANNEL_REF not set; auto-downloader disabled.")
        return
    client = get_watcher_client()
    if not client.is_connected:
        await client.start()
    # Warm the watcher's peer storage so update handling and downloads
    # work for every chat the account follows.
    try:
        async for _ in client.get_dialogs():
            pass
    except Exception:
        pass
    ref = os.environ["CHANNEL_REF"].strip()
    if "t.me/" in ref:
        try:
            chat = await client.get_chat(ref)
            if not getattr(chat, "id", None):
                raise ValueError(f"not a member yet: {ref}")
        except Exception:
            try:
                chat = await client.join_chat(ref)
            except Exception:
                pass
            chat = await client.get_chat(ref)
    else:
        chat = await client.get_chat(ref.lstrip("@") if not ref.lstrip("-").isdigit() else int(ref))
    row = {
        "id": chat.id,
        "title": chat.title or chat.first_name or str(chat.id),
        "type": str(chat.type),
        "username": chat.username or "",
        "last_synced": datetime.utcnow().isoformat(),
    }
    upsert_chat(row)
    if status_cb:
        status_cb(f"📡 Channel: {row['title']} (ID {row['id']})")

    def _status(s):
        if status_cb:
            status_cb(s)

    extras = get_download_clients()[1:]
    if extras:
        if status_cb:
            status_cb(f"🔌 Extra download sessions: {len(extras)}")
        await start_download_clients(ref, row["id"])

    count = await download_channel_all(row["id"], status_cb=_status, client=client)
    if status_cb:
        status_cb(f"✅ Initial download done: {count} files.")
    await watch_channel_new(row["id"], status_cb=_status, client=client)


async def get_me() -> dict:
    await ensure_started()
    me = await get_client().get_me()
    return {
        "id": me.id,
        "name": f"{me.first_name or ''} {me.last_name or ''}".strip(),
        "username": me.username or "",
        "phone": me.phone_number or "",
    }
