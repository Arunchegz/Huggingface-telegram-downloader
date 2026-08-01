import sqlite3
import json
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime
from config import DB_PATH


@contextmanager
def get_conn():
    """Context manager that opens, yields, commits/rolls back, and closes the connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS chats (
            id INTEGER PRIMARY KEY,
            title TEXT,
            type TEXT,
            username TEXT,
            is_favorite INTEGER DEFAULT 0,
            last_synced TEXT
        );

        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            message_id INTEGER,
            file_name TEXT,
            file_size INTEGER,
            mime_type TEXT,
            ext TEXT,
            media_type TEXT,
            date TEXT,
            local_path TEXT,
            downloaded INTEGER DEFAULT 0,
            UNIQUE(chat_id, message_id)
        );

        CREATE TABLE IF NOT EXISTS downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            message_id INTEGER,
            file_name TEXT,
            started_at TEXT,
            finished_at TEXT,
            status TEXT DEFAULT 'pending'
        );

        CREATE INDEX IF NOT EXISTS idx_files_chat ON files(chat_id);
        CREATE INDEX IF NOT EXISTS idx_files_type ON files(media_type);
        CREATE INDEX IF NOT EXISTS idx_files_name ON files(file_name);
        """)


def upsert_chat(chat: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO chats (id, title, type, username, last_synced)
            VALUES (:id, :title, :type, :username, :last_synced)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                type=excluded.type,
                username=excluded.username,
                last_synced=excluded.last_synced
        """, chat)


def upsert_file(file: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO files (chat_id, message_id, file_name, file_size, mime_type, ext, media_type, date)
            VALUES (:chat_id, :message_id, :file_name, :file_size, :mime_type, :ext, :media_type, :date)
            ON CONFLICT(chat_id, message_id) DO UPDATE SET
                file_name=excluded.file_name,
                file_size=excluded.file_size,
                mime_type=excluded.mime_type,
                ext=excluded.ext,
                media_type=excluded.media_type,
                date=excluded.date
        """, file)


def get_chats(favorites_only=False):
    with get_conn() as conn:
        if favorites_only:
            return conn.execute("SELECT * FROM chats WHERE is_favorite=1 ORDER BY title").fetchall()
        return conn.execute("SELECT * FROM chats ORDER BY is_favorite DESC, title").fetchall()


def toggle_favorite(chat_id: int):
    with get_conn() as conn:
        conn.execute("""
            UPDATE chats SET is_favorite = 1 - is_favorite WHERE id=?
        """, (chat_id,))


def get_files(chat_id=None, media_type=None, search=None, limit=100, offset=0):
    query = "SELECT * FROM files WHERE 1=1"
    params = []
    if chat_id:
        query += " AND chat_id=?"
        params.append(chat_id)
    if media_type and media_type != "all":
        query += " AND media_type=?"
        params.append(media_type)
    if search:
        query += " AND file_name LIKE ?"
        params.append(f"%{search}%")
    query += " ORDER BY date DESC LIMIT ? OFFSET ?"
    params += [limit, offset]
    with get_conn() as conn:
        return conn.execute(query, params).fetchall()


def mark_downloaded(chat_id: int, message_id: int, local_path: str):
    with get_conn() as conn:
        conn.execute("""
            UPDATE files SET downloaded=1, local_path=?
            WHERE chat_id=? AND message_id=?
        """, (local_path, chat_id, message_id))


def get_file_by_msg(chat_id: int, message_id: int):
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM files WHERE chat_id=? AND message_id=?",
            (chat_id, message_id)
        ).fetchone()


def log_download(chat_id, message_id, file_name, status="pending"):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO downloads (chat_id, message_id, file_name, started_at, status)
            VALUES (?, ?, ?, ?, ?)
        """, (chat_id, message_id, file_name, datetime.utcnow().isoformat(), status))


def finish_download(chat_id, message_id, status="done"):
    with get_conn() as conn:
        conn.execute("""
            UPDATE downloads SET finished_at=?, status=?
            WHERE chat_id=? AND message_id=? AND finished_at IS NULL
        """, (datetime.utcnow().isoformat(), status, chat_id, message_id))


def recent_downloads(limit=20):
    with get_conn() as conn:
        return conn.execute("""
            SELECT * FROM downloads ORDER BY started_at DESC LIMIT ?
        """, (limit,)).fetchall()
