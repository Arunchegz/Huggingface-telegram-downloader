import asyncio
import json
import os
import re
from pathlib import Path
import time
from datetime import datetime, timezone
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, FileReferenceExpired
from pyrogram.raw.types import UpdateDeleteChannelMessages, UpdateDeleteMessages
from pyrogram.handlers import RawUpdateHandler, MessageHandler

from config import (API_ID, API_HASH, SESSION_STRING, DOWNLOAD_DIR, CHUNK_SIZE,
                    ALL_EXTENSIONS, MEDIA_TYPES, STATE_DIR, INITIAL_SCAN_LIMIT,
                    POLL_INTERVAL, MAX_CACHE_BYTES, EXTRA_TOKENS,
                    MAX_CONCURRENT_DOWNLOADS)
from database import upsert_chat, upsert_file, mark_downloaded, log_download, finish_download, get_file_by_msg, delete_file_row
from bucket import delete_bucket_file
from storage import get_cache_size

_client: Client | None = None
_watcher_client: Client | None = None
_watching_chat_id: int | None = None
_in_flight: set[tuple[int, int]] = set()


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
    """The watcher is the SAME client as the browse client.

    Telegram only allows one live MTProto connection per auth key. Opening a
    second Client from the same session string kills the first connection with
    AuthKeyDuplicated, so both roles must share the single user-session client.
    """
    return get_client()


_extra_clients: list[Client] = []
_bot_clients: list[Client] = []
_rr_idx = 0
_broken: dict[str, float] = {}  # client_name -> expiry timestamp


class _RetryClient(Exception):
    """Internal signal: this client cannot fetch the message, try the next one."""


def _mark_broken(c: Client) -> None:
    _broken[c.name] = time.time() + 3600  # exclude for 1 hour


def _is_broken(c: Client) -> bool:
    expiry = _broken.get(c.name)
    if expiry is None:
        return False
    if time.time() > expiry:
        del _broken[c.name]
        return False
    return True


def _is_bot_token(tok: str) -> bool:
    return bool(re.match(r"^\d+:[A-Za-z0-9_-]{20,}$", tok))


def get_download_clients() -> list[Client]:
    """Pool of clients used for downloads: shared user client + extra sessions.

    EXTRA_TOKENS must contain DISTINCT accounts (bot tokens or session strings
    from other accounts). Any token equal to the main SESSION_STRING — or a
    duplicate of another token — is skipped, since connecting it alongside the
    main client triggers AuthKeyDuplicated.
    """
    global _extra_clients, _bot_clients
    if not _extra_clients:
        seen = {SESSION_STRING}
        for i, tok in enumerate(EXTRA_TOKENS):
            if not tok or tok in seen:
                continue
            seen.add(tok)
            if _is_bot_token(tok):
                c = Client(
                    name=f"tgfiles_bot_{i}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    bot_token=tok,
                    in_memory=True,
                )
                _bot_clients.append(c)
            else:
                c = Client(
                    name=f"tgfiles_extra_{i}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    session_string=tok,
                    in_memory=True,
                )
            _extra_clients.append(c)
    return [get_watcher_client()] + _extra_clients


def _pick_dl_client(clients: list[Client]) -> Client:
    """Round-robin pick for a download, preferring bot sessions.

    Bots have much higher upload.GetFile limits than user accounts, so they
    should carry downloads whenever available; the user session is only used
    when no bot/extra sessions exist. Clients that failed the startup health
    check (e.g. bots not added to the channel) are skipped.
    """
    global _rr_idx
    pool = [c for c in clients if not _is_broken(c)] or clients
    bots = [c for c in pool if c.name.startswith("tgfiles_bot_")]
    extras = [c for c in pool if c is not get_client()]
    target = bots or extras or pool
    _rr_idx = (_rr_idx + 1) % len(target)
    return target[_rr_idx]


async def start_download_clients(ref: str, chat_id: int) -> None:
    """Connect every download client and warm its peer storage.

    Clients that cannot read the target channel (e.g. bots not added as
    admins) are marked broken and excluded from the download pool.
    """
    for c in get_download_clients():
        try:
            if not c.is_connected:
                await c.start()
            try:
                await c.get_chat(ref)
            except Exception:
                _mark_broken(c)
                continue
            if chat_id:
                try:
                    async for _ in c.get_chat_history(chat_id, limit=1):
                        break
                except Exception:
                    _mark_broken(c)
        except Exception:
            _mark_broken(c)


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
        "date": msg.date.isoformat() if msg.date else datetime.now(timezone.utc).isoformat(),
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
            "last_synced": datetime.now(timezone.utc).isoformat(),
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
    # Prefer bot sessions for downloads (higher upload.GetFile limits),
    # not the browse client, to avoid the user session's flood waits.
    client = client or _pick_dl_client(get_download_clients())
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

    # If the picked client fails (e.g. bot without channel access), fall back
    # to the main user session, which is always a member of the channel.
    candidates = [client]
    for c in get_download_clients():
        if c is not client:
            candidates.append(c)

    retries = 3
    for try_client in candidates:
        for attempt in range(retries):
            try:
                msg = await try_client.get_messages(chat_id, message_id)
                if not msg or not msg.media:
                    if try_client is get_client():
                        # User session is a channel member: message is genuinely gone.
                        finish_download(chat_id, message_id, status="no_media")
                        return None
                    _mark_broken(try_client)
                    raise _RetryClient()
                await try_client.download_media(
                    msg,
                    file_name=str(out_path),
                    progress=_progress,
                )
                mark_downloaded(chat_id, message_id, str(out_path))
                finish_download(chat_id, message_id, status="done")
                return out_path

            except _RetryClient:
                break  # try the next client

            except FileReferenceExpired:
                await asyncio.sleep(1)
                continue

            except FloodWait as e:
                await asyncio.sleep(e.value)
                continue

            except Exception as e:
                if try_client is not get_client():
                    _mark_broken(try_client)
                if attempt == retries - 1:
                    break  # try the next client
                await asyncio.sleep(2 ** attempt)

    finish_download(chat_id, message_id, status="error:no_client_worked")
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
        "last_synced": datetime.now(timezone.utc).isoformat(),
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

    async def _one(meta: dict) -> bool:
        async with sem:
            try:
                if _cache_full(meta["file_size"]):
                    return False
                dl_client = _pick_dl_client(dl_clients)
                if status_cb:
                    status_cb(f"⬇ [{meta['message_id']}] {meta['file_name']} "
                              f"({meta['file_size']//1024//1024} MB) @ {dl_client.name}")
                path = await download_file(chat_id, meta["message_id"],
                                           meta["file_name"], client=dl_client)
                if path is None and status_cb:
                    status_cb(f"⚠ FAIL [{meta['message_id']}] {meta['file_name']} (all clients failed)")
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
                for meta in pending:
                    await _download_new_meta(chat_id, meta, status_cb=status_cb,
                                             clients=dl_clients)
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


async def _download_new_meta(chat_id: int, meta: dict, status_cb=None,
                             clients: list | None = None) -> None:
    """Download one newly posted file with in-flight dedupe (instant + poll share this)."""
    key = (chat_id, meta["message_id"])
    if key in _in_flight:
        return
    _in_flight.add(key)
    try:
        if _cache_full(meta["file_size"]):
            if status_cb:
                status_cb(f"Cache limit reached, skipping {meta['file_name']}")
            return
        clients = clients or get_download_clients()
        dl_client = _pick_dl_client(clients)
        if status_cb:
            status_cb(f"⬇ NEW [{meta['message_id']}] {meta['file_name']} @ {dl_client.name}")
        path = await download_file(chat_id, meta["message_id"], meta["file_name"],
                                   client=dl_client)
        if path and status_cb:
            status_cb(f"✅ NEW [{meta['message_id']}] {meta['file_name']}")
        elif path is None and status_cb:
            status_cb(f"⚠ FAIL NEW [{meta['message_id']}] {meta['file_name']} (all clients failed)")
    except Exception as e:
        if status_cb:
            status_cb(f"⚠ NEW [{meta['message_id']}] {meta['file_name']}: {e}")
    finally:
        _in_flight.discard(key)


async def _on_new_channel_message(client: Client, message: Message, status_cb=None):
    """Instant delivery: Pyrogram pushes channel posts over MTProto (no polling wait)."""
    chat = message.chat
    if chat is None or chat.id != _watching_chat_id:
        return
    meta = _extract_file_meta(message)
    if not meta:
        return
    upsert_file(meta)
    last_id = get_last_msg_id(chat.id)
    if message.id <= last_id:
        return
    _save_last_msg_id(chat.id, message.id)
    await _download_new_meta(chat.id, meta, status_cb=status_cb)


async def _remove_downloaded_file(chat_id: int, mid: int, status_cb=None) -> None:
    """Delete DB row + local file (mounted bucket) for (chat_id, mid)."""
    info = delete_file_row(chat_id, mid)
    if not info:
        return
    local_path, file_name = info["local_path"], info["file_name"]
    p = Path(local_path) if local_path else None
    if p and p.exists():
        try:
            p.unlink()
        except OSError as e:
            if status_cb:
                status_cb(f"⚠ delete local file failed [{chat_id}/{mid}]: {e}")
            return
    delete_bucket_file(chat_id, file_name)
    if status_cb:
        status_cb(f"🗑 Deleted [{chat_id}/{mid}] {file_name}")


async def sync_deletions(client: Client, chat_id: int, status_cb=None,
                         limit: int = INITIAL_SCAN_LIMIT) -> int:
    """Startup catch-up: remove files whose messages were deleted while offline.

    Fetches the newest `limit` message IDs of the channel; any downloaded DB row
    inside that range that is missing from the live set was deleted while the
    app was not running.
    """
    from database import get_downloaded_rows
    rows = get_downloaded_rows(chat_id)
    if not rows:
        return 0
    live_ids: set[int] = set()
    oldest = None
    async for msg in client.get_chat_history(chat_id, limit=limit):
        live_ids.add(msg.id)
        if oldest is None or msg.id < oldest:
            oldest = msg.id
    if not live_ids or oldest is None:
        return 0
    removed = 0
    for row in rows:
        mid = row["message_id"]
        if mid < oldest:
            continue  # outside fetched range, status unknown
        if mid not in live_ids:
            await _remove_downloaded_file(chat_id, mid, status_cb=status_cb)
            removed += 1
    return removed


async def verify_downloaded_files(chat_id: int, status_cb=None) -> int:
    """Startup integrity check: re-flag files whose bucket object vanished.

    Rows marked downloaded=1 whose local_path no longer exists on disk (HF
    eviction, manual deletion, storage reset) get their downloaded flag cleared
    so the initial scan re-downloads them.
    """
    from database import get_downloaded_rows, clear_downloaded
    rows = get_downloaded_rows(chat_id)
    missing = 0
    for row in rows:
        lp = row["local_path"]
        if lp and Path(lp).exists():
            continue
        clear_downloaded(chat_id, row["message_id"])
        missing += 1
    if missing and status_cb:
        status_cb(f"🔍 Integrity check: {missing} file(s) missing from storage, will re-download")
    return missing


async def _handle_delete_update(client: Client, update, users, chats, status_cb=None):
    """Pyrogram raw update handler — fires when messages are deleted in a channel."""
    if isinstance(update, UpdateDeleteChannelMessages):
        # Reconstruct Pyrogram-style channel peer ID
        chat_id = int(f"-100{update.channel_id}")
        msg_ids = update.messages
    elif isinstance(update, UpdateDeleteMessages):
        # Private/group messages: no channel_id in update, handle per stored rows
        chat_id = None
        msg_ids = update.messages
    else:
        return

    for mid in msg_ids:
        if chat_id is not None:
            await _remove_downloaded_file(chat_id, mid, status_cb=status_cb)
        else:
            # Try all chats for this message_id (rare: non-channel delete)
            from database import get_conn
            with get_conn() as conn:
                rows = conn.execute(
                    "SELECT chat_id FROM files WHERE message_id=? AND downloaded=1",
                    (mid,)
                ).fetchall()
            for row in rows:
                await _remove_downloaded_file(row["chat_id"], mid, status_cb=status_cb)


def _register_delete_handler(client: Client, status_cb=None):
    """Register raw update handler for message deletions on given client."""
    async def _handler(c, update, users, chats):
        await _handle_delete_update(c, update, users, chats, status_cb=status_cb)

    client.add_handler(RawUpdateHandler(_handler))


async def auto_download_main(status_cb=None) -> None:
    """Resolve CHANNEL_REF, download existing files, then watch for new ones."""
    if not os.environ.get("CHANNEL_REF"):
        if status_cb:
            status_cb("CHANNEL_REF not set; auto-downloader disabled.")
        return
    client = get_watcher_client()
    if not client.is_connected:
        await client.start()
    # Register delete-sync handler before warming peers
    _register_delete_handler(client, status_cb=status_cb)
    if status_cb:
        status_cb("🗑 Delete-sync handler registered")
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
        "last_synced": datetime.now(timezone.utc).isoformat(),
    }
    upsert_chat(row)
    if status_cb:
        status_cb(f"📡 Channel: {row['title']} (ID {row['id']})")

    def _status(s):
        if status_cb:
            status_cb(s)

    global _watching_chat_id
    _watching_chat_id = row["id"]

    async def _msg_handler(c, message):
        await _on_new_channel_message(c, message, status_cb=_status)

    client.add_handler(MessageHandler(_msg_handler, filters.channel))
    if status_cb:
        status_cb("⚡ Instant new-message handler registered (MTProto push)")

    extras = get_download_clients()[1:]
    if extras:
        if status_cb:
            status_cb(f"🔌 Extra download sessions: {len(extras)}")
        await start_download_clients(ref, row["id"])

    try:
        removed = await sync_deletions(client, row["id"], status_cb=_status)
        if removed:
            _status(f"🗑 Startup deletion sync: removed {removed} file(s) deleted while offline")
    except Exception as e:
        _status(f"⚠ Deletion sync failed: {e}")

    try:
        await verify_downloaded_files(row["id"], status_cb=_status)
    except Exception as e:
        _status(f"⚠ Integrity check failed: {e}")

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
