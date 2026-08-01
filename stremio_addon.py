"""Stremio addon serving files downloaded by TG Manager.

Endpoints:
  /manifest.json
  /catalog/{type}/{id}.json
  /meta/{type}/{id}.json          — handles both tgdm:/tgds: and tt... IDs
  /stream/{type}/{id}.json        — handles both tgdm:/tgds: and tt... IDs
  /subtitles/{type}/{id}.json
  /tgfile/{chat_id}/{message_id}  (HTTP Range streaming)
"""
import mimetypes
import os
import re
import time
import unicodedata
import urllib.parse
from urllib.parse import quote_plus

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from config import DOWNLOAD_DIR
from database import get_conn
from storage import iter_file_chunks, resolve_local_path

BASE_URL = os.environ.get("ADDON_BASE_URL", "").rstrip("/")
STORAGE_BUCKET_BASE = os.environ.get("STORAGE_BUCKET_BASE", "").rstrip("/")

MOVIE_PREFIX = "tgdm:"
SERIES_PREFIX = "tgds:"

MANIFEST = {
    "id": "org.tgmanager.files",
    "version": "1.1.0",
    "name": "TG Manager Files",
    "description": "Stream files downloaded by TG Manager from Telegram channels",
    "resources": ["catalog", "meta", "stream", "subtitles"],
    "types": ["movie", "series"],
    "idPrefixes": ["tgdm:", "tgds:", "tt"],
    "catalogs": [
        {"type": "movie", "id": "tgmanager_movies", "name": "TG Downloaded Movies"},
        {"type": "series", "id": "tgmanager_series", "name": "TG Downloaded Series"},
    ],
}


# ── in-memory poster/imdb cache (TTL 24h, no Redis dependency) ───────────────

_cache: dict[str, tuple[float, str, str]] = {}  # key -> (expire_ts, poster, imdb_id)
_CACHE_TTL = 86400


def _cache_get(key: str):
    entry = _cache.get(key)
    if entry and entry[0] > time.time():
        return entry[1], entry[2]
    return None, None


def _cache_set(key: str, poster: str, imdb_id: str):
    _cache[key] = (time.time() + _CACHE_TTL, poster, imdb_id)


# ── title parsing ─────────────────────────────────────────────────────────────

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


def _cache_key(filename: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", filename.lower())[:64]


# ── VideoMatcher (ported from Telegram-direct-addon/state.py) ────────────────

_STOPWORDS = {"the", "a", "an", "of", "in", "on", "at", "to", "and", "or"}
_TECH_RE = re.compile(
    r"\b(?:1080p|2160p|720p|480p|bluray|webrip|web[ ._-]?dl|bdrip|hdrip|"
    r"remux|x264|x265|hevc|avc|h264|h265|aac|dts|atmos|10bit)\b",
    re.IGNORECASE,
)


def _normalize_title(title: str) -> str:
    if not title:
        return ""
    nfkd = unicodedata.normalize("NFD", title)
    title = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    roman_map = {
        "VIII": "8", "VII": "7", "VI": "6", "IV": "4", "IX": "9",
        "III": "3", "II": "2", "X": "10", "V": "5", "I": "1",
    }
    for roman, num in roman_map.items():
        title = re.sub(r"\b" + roman + r"\b", num, title, flags=re.IGNORECASE)
    return title.lower().strip()


def _clean_title_prefix(filename: str) -> str:
    name = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", filename)
    name = re.sub(r"[._\-–—+]", " ", name)
    name = re.sub(r"\b[Ss]\d{1,2}[Ee]\d{1,3}\b", "", name)
    name = re.sub(r"\b[Ss]eason\s*\d+\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\b[Ee]pisode\s*\d+\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\b(?:19|20)\d{2}\b", "", name)
    name = _TECH_RE.sub("", name)
    return re.sub(r"\s+", " ", name).strip().lower()


def _levenshtein_similarity(a: str, b: str) -> float:
    if len(a) < len(b):
        a, b = b, a
    if not b:
        return 1.0 if not a else 0.0
    prev = list(range(len(b) + 1))
    for ca in a:
        curr = [prev[0] + 1]
        for j, cb in enumerate(b):
            curr.append(min(prev[j + 1] + 1, curr[-1] + 1, prev[j] + (0 if ca == cb else 1)))
        prev = curr
    return 1 - prev[-1] / max(len(a), len(b))


def _matches_title(filename: str, title: str) -> bool:
    prefix = _clean_title_prefix(filename)
    norm_title = _normalize_title(title)
    norm_prefix = _normalize_title(prefix)
    if not norm_title or not norm_prefix:
        return False
    if norm_title == norm_prefix or norm_title in norm_prefix:
        return True
    # Compact-space: handles dots splitting a word ("Pallichat.Tambi" → "pallichattambi")
    compact_title = norm_title.replace(" ", "")
    compact_prefix = norm_prefix.replace(" ", "")
    if compact_title and (compact_title == compact_prefix
                          or compact_title in compact_prefix
                          or compact_prefix in compact_title):
        return True
    # Fuzzy single-word title: spelling variants ("Pallicchattambi" vs "Pallichattambi")
    if len(norm_title.split()) == 1 and len(compact_title) >= 6:
        sim = _levenshtein_similarity(compact_title, compact_prefix.split()[0] if compact_prefix else "")
        if sim >= 0.85:
            return True
    # Keyword overlap
    title_words = set(norm_title.split()) - _STOPWORDS or set(norm_title.split())
    prefix_words = set(norm_prefix.split())
    if not title_words:
        return False
    matches = sum(1 for w in title_words if w in prefix_words)
    return matches >= max(1, len(title_words) * 0.7)


def _parse_season_episode(filename: str) -> tuple[int | None, int | None]:
    m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,3})", filename)
    if m:
        return int(m.group(1)), int(m.group(2))
    m2 = re.search(r"[Ss]eason[\s._-]*(\d+)[\s\S]*?[Ee]pisode[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m2:
        return int(m2.group(1)), int(m2.group(2))
    m3 = re.search(r"[Ss]eason[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m3:
        return int(m3.group(1)), 1
    return None, None


def _match_score(filename: str, title: str, year: str,
                 season: int | None, episode: int | None) -> int:
    if not _matches_title(filename, title):
        return 0
    score = 20
    ym = re.search(r"\b(19|20)\d{2}\b", filename)
    file_year = int(ym.group(0)) if ym else None
    if year:
        try:
            meta_year = int(year)
            if file_year == meta_year:
                score += 20
            elif file_year and abs(file_year - meta_year) == 1:
                score += 5
            elif file_year:
                score -= 10
        except ValueError:
            pass
    else:
        if file_year:
            score += 5
    fs, fe = _parse_season_episode(filename)
    if season is not None and episode is not None:
        if fs == season and fe == episode:
            score += 20
        else:
            return 0
    return max(0, min(100, score))


MATCH_THRESHOLD = 35


# ── Cinemeta helpers ──────────────────────────────────────────────────────────

def _placeholder_poster(title: str) -> str:
    return f"https://via.placeholder.com/300x450?text={quote_plus(title or 'No+Poster')}"


async def _fetch_poster_and_imdb(filename: str) -> tuple[str, str]:
    is_series = bool(IS_SERIES_RE.search(filename))
    title = parse_show_title(filename) if is_series else parse_title_year(filename)[0]
    year = "" if is_series else parse_title_year(filename)[1]
    catalog_type = "series" if is_series else "movie"
    if not title:
        return _placeholder_poster(""), ""
    query = quote_plus(f"{title} {year}".strip())
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                f"https://v3-cinemeta.strem.io/catalog/{catalog_type}/top/search={query}.json"
            )
            metas = r.json().get("metas", [])
            if metas:
                poster = metas[0].get("poster") or _placeholder_poster(title)
                imdb_id = metas[0].get("id", "")
                if not imdb_id.startswith("tt"):
                    imdb_id = ""
                return poster, imdb_id
    except Exception as e:
        print(f"[poster] cinemeta failed for {filename!r}: {e}")
    return _placeholder_poster(title), ""


async def get_poster_and_imdb(filename: str) -> tuple[str, str]:
    key = _cache_key(filename)
    poster, imdb_id = _cache_get(key)
    if poster is not None:
        return poster, imdb_id
    poster, imdb_id = await _fetch_poster_and_imdb(filename)
    _cache_set(key, poster, imdb_id)
    return poster, imdb_id


async def get_cinemeta(type_name: str, imdb_id: str) -> tuple[str, str]:
    """Returns (title, year) from Cinemeta for a tt ID."""
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(f"https://v3-cinemeta.strem.io/meta/{type_name}/{imdb_id}.json")
            meta = r.json().get("meta", {}) or {}
            return meta.get("name", ""), str(meta.get("year", "") or "")
    except Exception as e:
        print(f"[cinemeta] failed for {imdb_id}: {e}")
    return "", ""


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
    if STORAGE_BUCKET_BASE:
        return f"{STORAGE_BUCKET_BASE}/{f['chat_id']}/{fname}"
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
                poster, _ = await get_poster_and_imdb(f["file_name"])
                metas.append({
                    "id": f"{MOVIE_PREFIX}{f['chat_id']}:{f['message_id']}",
                    "type": "movie", "name": title or f["file_name"], "year": year,
                    "poster": poster, "posterShape": "poster",
                })
        else:
            for sid, g in _series_groups(files).items():
                poster, _ = await get_poster_and_imdb(g["files"][0]["file_name"])
                metas.append({
                    "id": f"{SERIES_PREFIX}{sid}",
                    "type": "series", "name": g["title"],
                    "poster": poster, "posterShape": "poster",
                })
        return JSONResponse({"metas": metas})

    @app.get("/meta/{type}/{item_id}.json")
    async def meta(type: str, item_id: str):
        files = _downloaded_files()

        # ── tt IMDB ID: return Cinemeta meta + matched videos ──────────────
        if item_id.startswith("tt"):
            title, year = await get_cinemeta(type, item_id)
            if not title:
                return JSONResponse({"meta": None})
            meta_obj: dict = {"id": item_id, "type": type, "name": title, "year": year}
            if type == "series":
                videos = []
                seen = set()
                scored = []
                for f in files:
                    fn = f["file_name"]
                    fs, fe = _parse_season_episode(fn)
                    score = _match_score(fn, title, year, fs, fe)
                    if score >= MATCH_THRESHOLD:
                        scored.append((score, f, fs or 1, fe or 1))
                scored.sort(key=lambda x: x[0], reverse=True)
                for score, f, s, e in scored:
                    if (s, e) in seen:
                        continue
                    seen.add((s, e))
                    videos.append({
                        "id": f"{item_id}:{s}:{e}",
                        "season": s, "episode": e,
                        "title": f["file_name"],
                    })
                videos.sort(key=lambda x: (x["season"], x["episode"]))
                meta_obj["videos"] = videos
            return JSONResponse({"meta": meta_obj})

        # ── private tgdm: / tgds: IDs ─────────────────────────────────────
        if item_id.startswith(MOVIE_PREFIX):
            parts = item_id[len(MOVIE_PREFIX):].split(":")
            chat_id, msg_id = int(parts[0]), int(parts[1])
            for f in files:
                if f["chat_id"] == chat_id and f["message_id"] == msg_id:
                    title, year = parse_title_year(f["file_name"])
                    poster, imdb_id = await get_poster_and_imdb(f["file_name"])
                    meta_obj = {
                        "id": item_id, "type": "movie",
                        "name": title or f["file_name"], "year": year,
                        "poster": poster,
                    }
                    if imdb_id:
                        meta_obj["imdbId"] = imdb_id
                    return JSONResponse({"meta": meta_obj})
            return JSONResponse({"meta": None})

        if item_id.startswith(SERIES_PREFIX):
            sid = item_id[len(SERIES_PREFIX):]
            g = _series_groups(files).get(sid)
            if not g:
                return JSONResponse({"meta": None})
            poster, imdb_id = await get_poster_and_imdb(g["files"][0]["file_name"])
            videos = []
            for f in g["files"]:
                se = parse_series(f["file_name"])
                s = se["season"] if se else 1
                e = se["episode"] if se else 1
                videos.append({"id": f"{item_id}:{s}:{e}", "season": s, "episode": e,
                               "title": f["file_name"]})
            meta_obj = {
                "id": item_id, "type": "series", "name": g["title"],
                "poster": poster, "videos": videos,
            }
            if imdb_id:
                meta_obj["imdbId"] = imdb_id
            return JSONResponse({"meta": meta_obj})

        return JSONResponse({"meta": None})

    @app.get("/stream/{type}/{item_id}.json")
    async def stream(type: str, item_id: str, request: Request):
        base = _base_url(request)
        files = _downloaded_files()
        streams = []

        # ── tt IMDB ID: match by title ─────────────────────────────────────
        if item_id.startswith("tt"):
            parts = item_id.split(":")
            imdb_id = parts[0]
            season = int(parts[1]) if len(parts) > 1 else None
            episode = int(parts[2]) if len(parts) > 2 else None
            title, year = await get_cinemeta(type, imdb_id)
            if not title:
                return JSONResponse({"streams": []})
            scored = []
            for f in files:
                fn = f["file_name"]
                score = _match_score(fn, title, year, season, episode)
                if score >= MATCH_THRESHOLD:
                    scored.append((score, f))
            scored.sort(key=lambda x: x[0], reverse=True)
            for score, f in scored:
                size_gb = round(f["file_size"] / 1024 ** 3, 2) if f.get("file_size") else None
                size_str = f" | {size_gb} GB" if size_gb else ""
                streams.append({
                    "name": "TG Manager",
                    "title": f"{f['file_name']}{size_str}",
                    "url": _file_url(base, f),
                })
            return JSONResponse({"streams": streams})

        # ── private tgdm: / tgds: IDs ─────────────────────────────────────
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
            parts = item_id[len(SERIES_PREFIX):].split(":")
            sid = parts[0]
            season = int(parts[1]) if len(parts) >= 3 else None
            episode = int(parts[2]) if len(parts) >= 3 else None
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

    @app.get("/subtitles/{type}/{item_id}.json")
    async def subtitles(type: str, item_id: str):
        files = _downloaded_files()
        filename = ""
        season, episode = None, None

        if item_id.startswith("tt"):
            parts = item_id.split(":")
            imdb_id = parts[0]
            season = int(parts[1]) if len(parts) > 1 else None
            episode = int(parts[2]) if len(parts) > 2 else None
            # Find filename for IMDB-keyed requests
            title, year = await get_cinemeta(type, imdb_id)
            if title:
                for f in files:
                    score = _match_score(f["file_name"], title, year, season, episode)
                    if score >= MATCH_THRESHOLD:
                        filename = f["file_name"]
                        break
            # Can proxy directly with the imdb_id we already have
            os_id = imdb_id if type == "movie" else f"{imdb_id}:{season}:{episode}"
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.get(f"https://opensubtitles-v3.strem.io/subtitles/{type}/{os_id}.json")
                    if r.status_code == 200:
                        return JSONResponse(r.json())
            except Exception as e:
                print(f"[subtitles] opensubtitles failed for {imdb_id}: {e}")
            return JSONResponse({"subtitles": []})

        prefix = MOVIE_PREFIX if type == "movie" else SERIES_PREFIX
        if not item_id.startswith(prefix):
            return JSONResponse({"subtitles": []})

        if type == "movie":
            parts = item_id[len(MOVIE_PREFIX):].split(":")
            try:
                chat_id, msg_id = int(parts[0]), int(parts[1])
                for f in files:
                    if f["chat_id"] == chat_id and f["message_id"] == msg_id:
                        filename = f["file_name"]
                        break
            except (ValueError, IndexError):
                pass
        else:
            parts = item_id[len(SERIES_PREFIX):].split(":")
            if len(parts) >= 3:
                sid = parts[0]
                try:
                    season, episode = int(parts[1]), int(parts[2])
                except ValueError:
                    pass
                for f in files:
                    if show_id(f["file_name"]) == sid:
                        se = parse_series(f["file_name"])
                        if se and se["season"] == season and se["episode"] == episode:
                            filename = f["file_name"]
                            break

        if not filename:
            return JSONResponse({"subtitles": []})

        _, imdb_id = await get_poster_and_imdb(filename)
        if not imdb_id:
            return JSONResponse({"subtitles": []})

        os_id = imdb_id if type == "movie" else f"{imdb_id}:{season}:{episode}"
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(f"https://opensubtitles-v3.strem.io/subtitles/{type}/{os_id}.json")
                if r.status_code == 200:
                    return JSONResponse(r.json())
        except Exception as e:
            print(f"[subtitles] opensubtitles failed for {imdb_id}: {e}")

        return JSONResponse({"subtitles": []})

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
