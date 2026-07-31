from database import get_files, get_conn
from config import MEDIA_TYPES


# ── main search ───────────────────────────────────────────────────────────────

def search_files(
    query: str = "",
    chat_id: int = None,
    media_type: str = "all",
    min_size_mb: float = 0,
    max_size_mb: float = 0,
    date_from: str = None,
    date_to: str = None,
    downloaded_only: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    sql = "SELECT * FROM files WHERE 1=1"
    params = []

    if query:
        sql += " AND file_name LIKE ?"
        params.append(f"%{query}%")

    if chat_id:
        sql += " AND chat_id=?"
        params.append(chat_id)

    if media_type and media_type != "all":
        sql += " AND media_type=?"
        params.append(media_type)

    if min_size_mb > 0:
        sql += " AND file_size >= ?"
        params.append(int(min_size_mb * 1024 * 1024))

    if max_size_mb > 0:
        sql += " AND file_size <= ?"
        params.append(int(max_size_mb * 1024 * 1024))

    if date_from:
        sql += " AND date >= ?"
        params.append(date_from)

    if date_to:
        sql += " AND date <= ?"
        params.append(date_to + "T23:59:59")

    if downloaded_only:
        sql += " AND downloaded=1"

    sql += " ORDER BY date DESC LIMIT ? OFFSET ?"
    params += [limit, offset]

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [dict(r) for r in rows]


def count_results(
    query: str = "",
    chat_id: int = None,
    media_type: str = "all",
    downloaded_only: bool = False,
) -> int:
    sql = "SELECT COUNT(*) FROM files WHERE 1=1"
    params = []

    if query:
        sql += " AND file_name LIKE ?"
        params.append(f"%{query}%")

    if chat_id:
        sql += " AND chat_id=?"
        params.append(chat_id)

    if media_type and media_type != "all":
        sql += " AND media_type=?"
        params.append(media_type)

    if downloaded_only:
        sql += " AND downloaded=1"

    with get_conn() as conn:
        return conn.execute(sql, params).fetchone()[0]


# ── suggestions ───────────────────────────────────────────────────────────────

def suggest_filenames(partial: str, limit: int = 10) -> list[str]:
    if not partial or len(partial) < 2:
        return []
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT file_name FROM files WHERE file_name LIKE ? LIMIT ?",
            (f"%{partial}%", limit)
        ).fetchall()
    return [r["file_name"] for r in rows]


# ── stats ─────────────────────────────────────────────────────────────────────

def get_type_counts(chat_id: int = None) -> dict:
    sql = "SELECT media_type, COUNT(*) as cnt FROM files WHERE 1=1"
    params = []
    if chat_id:
        sql += " AND chat_id=?"
        params.append(chat_id)
    sql += " GROUP BY media_type"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    counts = {mtype: 0 for mtype in MEDIA_TYPES}
    counts["all"] = 0
    for row in rows:
        counts[row["media_type"]] = row["cnt"]
        counts["all"] += row["cnt"]
    return counts


def get_chat_file_stats() -> list[dict]:
    sql = """
        SELECT
            f.chat_id,
            c.title,
            COUNT(*) as file_count,
            SUM(f.file_size) as total_size,
            SUM(CASE WHEN f.downloaded=1 THEN 1 ELSE 0 END) as cached_count
        FROM files f
        LEFT JOIN chats c ON c.id = f.chat_id
        GROUP BY f.chat_id
        ORDER BY file_count DESC
    """
    with get_conn() as conn:
        rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]


# ── format helpers ────────────────────────────────────────────────────────────

def fmt_size(size_bytes: int) -> str:
    if not size_bytes:
        return "—"
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def fmt_date(iso: str) -> str:
    if not iso:
        return "—"
    return iso[:10]


def enrich_file_row(row: dict) -> dict:
    row = dict(row)
    row["size_fmt"] = fmt_size(row.get("file_size", 0))
    row["date_fmt"] = fmt_date(row.get("date", ""))
    row["cached"] = bool(row.get("downloaded"))
    return row
