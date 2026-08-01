"""Stremio addon serving files downloaded by TG Manager.

Endpoints:
  /manifest.json
  /catalog/{type}/{id}.json
  /meta/{type}/{id}.json
  /stream/{type}/{id}.json
  /tgfile/{chat_id}/{message_id}   (HTTP Range streaming)
"""
import mimetypes
import os
import re
import urllib.parse

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from config import DOWNLOAD_DIR
from database import get_conn
from storage import iter_file_chunks, resolve_local_path

BASE_URL = os.environ.get("ADDON_BASE_URL", "").rstrip("/")

MOVIE_PREFIX = "tgdm:"
SERIES_PREFIX = "tgds:"

MANIFEST = {
    "id": "org.tgmanager.files",
    "version": "1.0.0",
    "name": "TG Manager Files",
    "description": "Stream files downloaded by TG Manager from Telegram channels",
    "resources": ["catalog", "meta", "stream"],
    "types": ["movie", "series"],
    "catalogs": [
        {"type": "movie", "id": "tgmanager_movies", "name": "TG Downloaded Movies"},
        {"type": "series", "id": "tgmanager_series", "name": "TG Downloaded Series"},
    ],
}


# ── title parsing (mirrors Telegram-direct-addon/state.py) ────────────────────

def parse_title_year(filename: str) -> tuple[str, str]:
    name = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", filename)
    name = re.sub(r"[._]", " ", name)
    ym = re.search(r"\b(19|20)\d{2}\b", name)
    year = ym.group(0) if ym else ""
    cut = re.split(
        r"\b(?:19|20)\d{2}\b|\b(?:1080p|2160p|720p|480p|bluray|webrip|web dl|"
        r"bdrip|hdrip|remux|x264|x265|hevc|avc|h264|h265|aac|dts|atmos|10bit)\b",
        name, maxsplit=1, flags=re.IGNORECASE,
    )[0]
    return re.sub(r"\s+", " ", cut).strip().title(), year


IS_SERIES_RE = re.compile(
    r"[Ss]\d{1,2}[Ee]\d{1,3}"
    r"|[Ss]eason[\s._-]*\d+"
    r"|[Ee]pisode[\s._-]*\d+",
    re.IGNORECASE,
)


def parse_series(filename: str) -> dict | None:
    m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,3})", filename)
    if m:
        return {"season": int(m.group(1)), "episode": int(m.group(2))}
    m2 = re.search(r"[Ss]eason[\s._-]*(\d+)[\s\S]*?[Ee]pisode[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m2:
        return {"season": int(m2.group(1)), "episode": int(m2.group(2))}
    m3 = re.search(r"[Ss]eason[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m3:
        return {"season": int(m3.group(1)), "episode": 1}
    return None


def parse_show_title(filename: str) -> str:
    name = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", filename)
    name = re.sub(r"[._\-–—+]", " ", name)
    for pattern in [r"\b[Ss]\d{1,2}[Ee]\d{1,3}\b", r"\b[Ss]eason\s*\d+\b", r"\b[Ee]pisode\s*\d+\b"]:
        parts = re.split(pattern, name, flags=re.IGNORECASE)
        if len(parts) > 1:
            name = parts[0]
            break
    parts = re.split(r"\b(?:19|20)\d{2}\b", name)
    if len(parts) > 1:
        name = parts[0]
    name = re.split(
        r"\b(?:1080p|2160p|720p|480p|bluray|webrip|web dl|"
        r"bdrip|hdrip|remux|x264|x265|hevc|avc|h264|h265|aac|dts|atmos|10bit)\b",
        name, maxsplit=1, flags=re.IGNORECASE,
    )[0]
    return re.sub(r"\s+", " ", name).strip().title()


def show_id(filename: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", parse_show_title(filename).lower())


# ── DB helpers ────────────────────────────────────────────────────────────────

def _downloaded_files() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT chat_id, message_id, file_name, file_size, date "
            "FROM files WHERE downloaded=1 ORDER BY date DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def _series_groups(files: list[dict]) -> dict:
    groups: dict[str, dict] = {}
    for f in files:
        fn = f["file_name"]
        if not IS_SERIES_RE.search(fn):
            continue
        sid = show_id(fn)
        if sid not in groups:
            groups[sid] = {"title": parse_show_title(fn), "files": []}
        groups[sid]["files"].append(f)
    return groups


def _base_url(request: Request) -> str:
    if BASE_URL:
        return BASE_URL.rstrip("/")
    url = str(request.base_url).rstrip("/")
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    return url


def _file_url(base: str, f: dict) -> str:
    fname = urllib.parse.quote(f["file_name"])
    return f"{base}/tgfile/{f['chat_id']}/{f['message_id']}/{fname}"


# ── addon routes ──────────────────────────────────────────────────────────────

def add_routes(app: FastAPI):

    @app.get("/manifest.json")
    async def manifest():
        return JSONResponse(MANIFEST)

    @app.get("/catalog/{type}/{catalog_id}.json")
    async def catalog(type: str, catalog_id: str):
        files = _downloaded_files()
        metas = []
        if type == "movie":
            for f in files:
                if IS_SERIES_RE.search(f["file_name"]):
                    continue
                title, year = parse_title_year(f["file_name"])
                metas.append({
                    "id": f"{MOVIE_PREFIX}{f['chat_id']}:{f['message_id']}",
                    "type": "movie", "name": title or f["file_name"], "year": year,
                    "poster": "https://via.placeholder.com/300x450?text=No+Poster",
                    "posterShape": "poster",
                })
        else:
            for sid, g in _series_groups(files).items():
                metas.append({
                    "id": f"{SERIES_PREFIX}{sid}",
                    "type": "series", "name": g["title"],
                    "poster": "https://via.placeholder.com/300x450?text=No+Poster",
                    "posterShape": "poster",
                })
        return JSONResponse({"metas": metas})

    @app.get("/meta/{type}/{item_id}.json")
    async def meta(type: str, item_id: str):
        files = _downloaded_files()
        if item_id.startswith(MOVIE_PREFIX):
            parts = item_id[len(MOVIE_PREFIX):].split(":")
            chat_id, msg_id = int(parts[0]), int(parts[1])
            for f in files:
                if f["chat_id"] == chat_id and f["message_id"] == msg_id:
                    title, year = parse_title_year(f["file_name"])
                    return JSONResponse({"meta": {
                        "id": item_id, "type": "movie",
                        "name": title or f["file_name"], "year": year,
                        "poster": "https://via.placeholder.com/300x450?text=No+Poster",
                    }})
            return JSONResponse({"meta": None})

        if item_id.startswith(SERIES_PREFIX):
            sid = item_id[len(SERIES_PREFIX):]
            g = _series_groups(files).get(sid)
            if not g:
                return JSONResponse({"meta": None})
            videos = []
            for f in g["files"]:
                se = parse_series(f["file_name"])
                s = se["season"] if se else 1
                e = se["episode"] if se else 1
                videos.append({"id": f"{item_id}:{s}:{e}", "season": s, "episode": e,
                               "title": f["file_name"]})
            return JSONResponse({"meta": {
                "id": item_id, "type": "series", "name": g["title"],
                "poster": "https://via.placeholder.com/300x450?text=No+Poster",
                "videos": videos,
            }})

        return JSONResponse({"meta": None})

    @app.get("/stream/{type}/{item_id}.json")
    async def stream(type: str, item_id: str, request: Request):
        base = _base_url(request)
        files = _downloaded_files()
        streams = []

        if item_id.startswith(MOVIE_PREFIX):
            parts = item_id[len(MOVIE_PREFIX):].split(":")
            chat_id, msg_id = int(parts[0]), int(parts[1])
            for f in files:
                if f["chat_id"] == chat_id and f["message_id"] == msg_id:
                    streams.append({
                        "name": "TG Manager",
                        "title": f["file_name"],
                        "url": _file_url(base, f),
                    })
            return JSONResponse({"streams": streams})

        if item_id.startswith(SERIES_PREFIX):
            sid, season, episode = item_id[len(SERIES_PREFIX):], None, None
            parts = item_id[len(SERIES_PREFIX):].split(":")
            if len(parts) >= 3:
                sid, season, episode = parts[0], int(parts[1]), int(parts[2])
            g = _series_groups(files).get(sid)
            if g:
                for f in g["files"]:
                    se = parse_series(f["file_name"])
                    s = se["season"] if se else 1
                    e = se["episode"] if se else 1
                    if season is not None and (s != season or e != episode):
                        continue
                    streams.append({
                        "name": "TG Manager",
                        "title": f["file_name"],
                        "url": _file_url(base, f),
                    })
            return JSONResponse({"streams": streams})

        return JSONResponse({"streams": []})

    @app.api_route("/tgfile/{chat_id}/{message_id}", methods=["GET", "HEAD"])
    @app.api_route("/tgfile/{chat_id}/{message_id}/{file_name}", methods=["GET", "HEAD"])
    async def serve_file(chat_id: int, message_id: int, request: Request,
                         file_name: str = None):
        path = resolve_local_path(chat_id, message_id)
        if path is None:
            return Response(status_code=404, content="Not found")

        size = path.stat().st_size
        media_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        range_header = request.headers.get("range")
        status = 200
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(size),
            "Content-Type": media_type,
        }

        if range_header:
            m = re.match(r"bytes=(\d*)-(\d*)", range_header)
            if m:
                start_s, end_s = m.group(1), m.group(2)
                if start_s == "" and end_s != "":
                    # suffix range: last N bytes
                    length = min(int(end_s), size)
                    start = size - length
                    end = size - 1
                else:
                    start = int(start_s) if start_s else 0
                    end = int(end_s) if end_s else size - 1
                    end = min(end, size - 1)
                if start >= size or end < start:
                    return Response(
                        status_code=416,
                        headers={"Content-Range": f"bytes */{size}"},
                        content="Range Not Satisfiable",
                    )
                status = 206
                headers["Content-Range"] = f"bytes {start}-{end}/{size}"
                headers["Content-Length"] = str(end - start + 1)
                if request.method == "HEAD":
                    return Response(status_code=status, headers=headers)
                return StreamingResponse(
                    iter_file_chunks(path, start=start, end=end),
                    status_code=status, headers=headers,
                )

        if request.method == "HEAD":
            return Response(status_code=200, headers=headers)
        return StreamingResponse(
            iter_file_chunks(path),
            status_code=200, headers=headers,
        )
