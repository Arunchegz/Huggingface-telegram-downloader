"""Stremio addon serving files downloaded by TG Manager.

Endpoints:
  /manifest.json
  /catalog/{type}/{id}.json
  /meta/{type}/{id}.json          — handles both tgdm:/tgds: and tt... IDs
  /stream/{type}/{id}.json        — handles both tgdm:/tgds: and tt... IDs
  /subtitles/{type}/{id}.json
  /tgfile/{chat_id}/{message_id}  (HTTP Range streaming)
"""
import asyncio
import mimetypes
import os
import re
import time
import unicodedata
import urllib.parse
from datetime import datetime, timezone
from urllib.parse import quote_plus

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from config import DOWNLOAD_DIR, TMDB_API_KEY, HF_TOKEN, STORAGE_BUCKET_REPO, STORAGE_BUCKET_TYPE
from database import get_conn
from storage import iter_file_chunks, resolve_local_path

BASE_URL = os.environ.get("ADDON_BASE_URL", "").rstrip("/")
STORAGE_BUCKET_BASE = os.environ.get("STORAGE_BUCKET_BASE", "").rstrip("/")
OPENSUBTITLES_BASE = "https://opensubtitles-v3.strem.io"

# ── HF bucket S3-gateway presigned URLs ──────────────────────────────────────
# Preferred path for private buckets: the app SigV4-presigns a GET URL on
# https://s3.hf.co (the public S3-compatible gateway). The player hits the
# gateway directly and is 302-redirected to the PUBLIC HF CDN (us.aws.cdn.hf.co)
# because the request originates from the player's network — avoiding the
# internal-only cas-bridge host that the /buckets/resolve redirect uses for
# requests made from inside HF's network.
# Requires S3 credentials generated from a HF access token (Settings → Access
# Tokens → Generate S3 credentials) exposed as HF_S3_ACCESS_KEY/HF_S3_SECRET_KEY.

import hashlib
import hmac

HF_S3_ACCESS_KEY = os.environ.get("HF_S3_ACCESS_KEY", "")
HF_S3_SECRET_KEY = os.environ.get("HF_S3_SECRET_KEY", "")
HF_S3_ENDPOINT = os.environ.get("HF_S3_ENDPOINT", "https://s3.hf.co").rstrip("/")
HF_S3_REGION = "us-east-1"
HF_S3_SERVICE = "s3"
HF_S3_EXPIRES = int(os.environ.get("HF_S3_EXPIRES", "3600"))
_S3_HOST = urllib.parse.urlparse(HF_S3_ENDPOINT).netloc


def _sigv4_sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _sigv4_signing_key(secret: str, date_stamp: str) -> bytes:
    k_date = _sigv4_sign(("AWS4" + secret).encode("utf-8"), date_stamp)
    k_region = _sigv4_sign(k_date, HF_S3_REGION)
    k_service = _sigv4_sign(k_region, HF_S3_SERVICE)
    return _sigv4_sign(k_service, "aws4_request")


def presign_s3_url(chat_id: int, file_name: str, now=None) -> str:
    """SigV4-presign a GET of bucket file via the public S3 gateway.

    Returns the full presigned URL, or "" if S3 credentials are not configured.
    """
    if not (HF_S3_ACCESS_KEY and HF_S3_SECRET_KEY and STORAGE_BUCKET_REPO):
        return ""
    owner, bucket = STORAGE_BUCKET_REPO.split("/", 1)
    key = f"{bucket}/downloads/{chat_id}/{file_name}"
    canonical_uri = "/" + owner + "/" + urllib.parse.quote(key, safe="/~")

    t = now or datetime.now(timezone.utc)
    amz_date = t.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = t.strftime("%Y%m%d")
    scope = f"{date_stamp}/{HF_S3_REGION}/{HF_S3_SERVICE}/aws4_request"

    params = [
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        ("X-Amz-Credential", f"{HF_S3_ACCESS_KEY}/{scope}"),
        ("X-Amz-Date", amz_date),
        ("X-Amz-Expires", str(HF_S3_EXPIRES)),
        ("X-Amz-SignedHeaders", "host"),
    ]
    params.sort()
    qs = "&".join(
        f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
        for k, v in params
    )

    canonical_headers = f"host:{_S3_HOST}\n"
    canonical_request = "\n".join([
        "GET", canonical_uri, qs,
        canonical_headers, "host", "UNSIGNED-PAYLOAD",
    ])
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    signature = hmac.new(
        _sigv4_signing_key(HF_S3_SECRET_KEY, date_stamp),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{HF_S3_ENDPOINT}{canonical_uri}?{qs}&X-Amz-Signature={signature}"


HF_BUCKET_COLLECTION = os.environ.get("HF_BUCKET_COLLECTION", "downloads").strip()


def _hf_bucket_url(chat_id: int, file_name: str) -> str:
    """Build the HF bucket resolve URL for a file."""
    repo = STORAGE_BUCKET_REPO
    if not repo:
        return ""
    repo_type = STORAGE_BUCKET_TYPE or "space"
    # HF bucket resolve endpoint: /buckets/{owner}/{repo}/resolve/{collection}/{path}
    # repo_type is only needed for dataset/model buckets; space buckets use /buckets/ directly
    encoded = urllib.parse.quote(file_name, safe="")
    return (
        f"https://huggingface.co/buckets/{repo}/resolve"
        f"/{HF_BUCKET_COLLECTION}/{chat_id}/{encoded}"
    )


async def get_hf_cdn_url(chat_id: int, file_name: str) -> str:
    """
    Resolve a private HF bucket file to a CloudFront pre-signed CDN URL.
    HF returns HTTP 302 → Location header contains the signed CDN URL.
    Returns empty string on failure (caller falls back to local proxy).
    """
    if not STORAGE_BUCKET_REPO or not HF_TOKEN:
        return ""
    bucket_url = _hf_bucket_url(chat_id, file_name)
    if not bucket_url:
        return ""
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=False) as c:
            r = await c.get(
                bucket_url,
                headers={"Authorization": f"Bearer {HF_TOKEN}"},
                params={"download": "true"},
            )
            if r.status_code in (301, 302, 303, 307, 308):
                cdn_url = r.headers.get("location", "")
                if cdn_url:
                    return cdn_url
            # Some HF responses return 200 with a redirect body — try anyway
            print(f"[hf-bucket] unexpected status {r.status_code} for {file_name}")
    except Exception as e:
        print(f"[hf-bucket] CDN URL resolve failed for {file_name!r}: {e}")
    return ""

MOVIE_PREFIX = "tgdm:"
SERIES_PREFIX = "tgds:"

MANIFEST = {
    "id": "org.tgmanager.files",
    "version": "1.2.0",
    "name": "TG Manager Files + Subtitles",
    "description": "Stream files downloaded by TG Manager from Telegram channels, with OpenSubtitles v3 subtitles built in",
    "resources": ["catalog", "meta", "stream", "subtitles"],
    "types": ["movie", "series"],
    "catalogs": [
        {"type": "movie", "id": "tgmanager_movies", "name": "TG Downloaded Movies"},
        {"type": "series", "id": "tgmanager_series", "name": "TG Downloaded Series"},
    ],
    "idPrefixes": ["tgdm:", "tgds:", "tt"],
}


# ── in-memory poster/imdb cache (TTL 24h, no Redis dependency) ───────────────

_cache: dict[str, tuple[float, str, str]] = {}  # key -> (expire_ts, poster, imdb_id)
_CACHE_TTL = 86400
_title_cache: dict[str, tuple[float, str, str, list]] = {}  # imdb_id -> (expire_ts, name, year, candidates)


def _cache_get(key: str):
    entry = _cache.get(key)
    if entry and entry[0] > time.time():
        return entry[1], entry[2]
    return None, None


def _cache_set(key: str, poster: str, imdb_id: str):
    # Evict expired entries periodically to prevent unbounded growth
    if len(_cache) > 500:
        now = time.time()
        expired = [k for k, v in _cache.items() if v[0] <= now]
        for k in expired:
            del _cache[k]
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


# ── TMDB + Cinemeta helpers ───────────────────────────────────────────────────

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG = "https://image.tmdb.org/t/p/w500"


def _placeholder_poster(title: str) -> str:
    return f"https://via.placeholder.com/300x450?text={quote_plus(title or 'No+Poster')}"


def _tmdb_media(is_series: bool) -> str:
    return "tv" if is_series else "movie"


def _clean_alt_title(title: str) -> str:
    title = re.sub(r"\s*[\(\[]?(?:19|20)\d{2}[\)\]]?\s*$", "", title.strip())
    return title.strip()


def _tmdb_result_year(r: dict) -> str:
    return (r.get("release_date") or r.get("first_air_date") or "")[:4]


async def _tmdb_search(filename: str) -> dict | None:
    """TMDB search (primary): poster + imdb_id via external_ids + title candidates."""
    if not TMDB_API_KEY:
        return None
    is_series = bool(IS_SERIES_RE.search(filename))
    title = parse_show_title(filename) if is_series else parse_title_year(filename)[0]
    year = "" if is_series else parse_title_year(filename)[1]
    if not title:
        return None
    media = _tmdb_media(is_series)
    params = {"api_key": TMDB_API_KEY, "query": title, "language": "en-US"}
    if year:
        params["first_air_date_year" if is_series else "year"] = year
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(f"{TMDB_BASE}/search/{media}", params=params)
            if r.status_code == 429:
                await asyncio.sleep(2.0)
                r = await c.get(f"{TMDB_BASE}/search/{media}", params=params)
            results = r.json().get("results", [])
            if not results:
                return None
            best = results[0]
            if year:
                exact = [res for res in results if _tmdb_result_year(res) == year]
                if exact:
                    best = exact[0]
            tmdb_id = best["id"]
            name = (best.get("title") or best.get("name")
                    or best.get("original_title") or best.get("original_name") or title)
            original = best.get("original_title") or best.get("original_name") or name
            poster = ""
            if best.get("poster_path"):
                poster = f"{TMDB_IMG}{best['poster_path']}"
            # external_ids → IMDB ID directly
            imdb_id = ""
            r2 = await c.get(f"{TMDB_BASE}/{media}/{tmdb_id}/external_ids",
                             params={"api_key": TMDB_API_KEY})
            if r2.status_code == 429:
                await asyncio.sleep(2.0)
                r2 = await c.get(f"{TMDB_BASE}/{media}/{tmdb_id}/external_ids",
                                 params={"api_key": TMDB_API_KEY})
            imdb_id = r2.json().get("imdb_id") or ""
            alt_titles = []
            r3 = await c.get(f"{TMDB_BASE}/{media}/{tmdb_id}/alternative_titles",
                             params={"api_key": TMDB_API_KEY})
            if r3.status_code == 429:
                await asyncio.sleep(2.0)
                r3 = await c.get(f"{TMDB_BASE}/{media}/{tmdb_id}/alternative_titles",
                                 params={"api_key": TMDB_API_KEY})
            entries = r3.json().get("titles") or r3.json().get("results") or []
            for e in entries:
                t = _clean_alt_title(e.get("title", ""))
                if t and t.lower() not in {name.lower(), original.lower()}:
                    alt_titles.append(t)
            candidates = []
            for t in [name, original] + alt_titles:
                if t and t not in candidates:
                    candidates.append(t)
            return {"poster": poster, "imdb_id": imdb_id,
                    "title": name, "candidates": candidates}
    except Exception as e:
        print(f"[tmdb] search failed for {filename!r}: {e}")
    return None


async def _fetch_poster_and_imdb(filename: str) -> tuple[str, str]:
    """TMDB search (primary) → poster + imdb_id; Cinemeta search as fallback."""
    is_series = bool(IS_SERIES_RE.search(filename))
    title = parse_show_title(filename) if is_series else parse_title_year(filename)[0]
    if not title:
        return _placeholder_poster(""), ""
    tmdb = await _tmdb_search(filename)
    if tmdb and (tmdb["poster"] or tmdb["imdb_id"]):
        return tmdb["poster"] or _placeholder_poster(title), tmdb["imdb_id"]
    year = "" if is_series else parse_title_year(filename)[1]
    query = quote_plus(f"{title} {year}".strip())
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                f"https://v3-cinemeta.strem.io/catalog/"
                f"{'series' if is_series else 'movie'}/top/search={query}.json"
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


async def get_title_info(type_name: str, imdb_id: str) -> tuple[str, str, list[str]]:
    """Returns (name, year, candidate_titles) for a tt ID.

    TMDB find is primary (localized + original_title + alternative_titles);
    Cinemeta is the fallback. Results are cached for 24h.
    """
    if not imdb_id.startswith("tt"):
        return "", "", []
    cache_key = f"{type_name}:{imdb_id}"
    entry = _title_cache.get(cache_key)
    if entry and entry[0] > time.time():
        return entry[1], entry[2], entry[3]
    if TMDB_API_KEY:
        media = _tmdb_media(type_name == "series")
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"{TMDB_BASE}/find/{imdb_id}",
                                params={"api_key": TMDB_API_KEY,
                                        "external_source": "imdb_id"})
                results = r.json().get("movie_results") or r.json().get("tv_results") or []
                if results:
                    best = results[0]
                    tmdb_id = best["id"]
                    name = best.get("title") or best.get("name") or ""
                    original = best.get("original_title") or best.get("original_name") or name
                    year = _tmdb_result_year(best)
                    candidates = []
                    for t in [name, original]:
                        if t and t not in candidates:
                            candidates.append(t)
                    try:
                        r2 = await c.get(
                            f"{TMDB_BASE}/{media}/{tmdb_id}/alternative_titles",
                            params={"api_key": TMDB_API_KEY})
                        entries = r2.json().get("titles") or r2.json().get("results") or []
                        known = {c.lower() for c in candidates}
                        for e in entries:
                            t = _clean_alt_title(e.get("title", ""))
                            if t and t.lower() not in known:
                                candidates.append(t)
                                known.add(t.lower())
                    except Exception as e:
                        print(f"[tmdb] alt titles failed for {imdb_id}: {e}")
                    result = (name or original, year, candidates)
                    _title_cache[cache_key] = (time.time() + _CACHE_TTL, result[0], result[1], result[2])
                    return result
        except Exception as e:
            print(f"[tmdb] find failed for {imdb_id}: {e}")
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(f"https://v3-cinemeta.strem.io/meta/{type_name}/{imdb_id}.json")
            meta = r.json().get("meta", {}) or {}
            name = meta.get("name", "")
            result = (name, str(meta.get("year", "") or ""), [name] if name else [])
            _title_cache[cache_key] = (time.time() + _CACHE_TTL, result[0], result[1], result[2])
            return result
    except Exception as e:
        print(f"[cinemeta] failed for {imdb_id}: {e}")
    return "", "", []


def _match_any(filename: str, titles: list[str], year: str,
               season: int | None, episode: int | None) -> int:
    for t in titles:
        if not t:
            continue
        score = _match_score(filename, t, year, season, episode)
        if score >= MATCH_THRESHOLD:
            return score
    return 0


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


async def _file_url(base: str, f: dict) -> str:
    """
    Resolve the stream URL for a file:
    1. If HF_S3_ACCESS_KEY/SECRET set → SigV4-presigned S3 gateway URL (private bucket, public CDN).
    2. If STORAGE_BUCKET_REPO + HF_TOKEN set → resolve private bucket to CDN URL (legacy).
    3. Elif STORAGE_BUCKET_BASE set → static public bucket URL (legacy).
    4. Else → local /tgfile proxy.
    """
    if HF_S3_ACCESS_KEY and HF_S3_SECRET_KEY:
        presigned = presign_s3_url(f["chat_id"], f["file_name"])
        if presigned:
            return presigned
    if STORAGE_BUCKET_REPO and HF_TOKEN:
        cdn_url = await get_hf_cdn_url(f["chat_id"], f["file_name"])
        if cdn_url:
            return cdn_url
        # Fall through to local proxy if CDN resolution failed
    if STORAGE_BUCKET_BASE:
        fname = urllib.parse.quote(f["file_name"])
        return f"{STORAGE_BUCKET_BASE}/{f['chat_id']}/{fname}"
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

        # ── tt IMDB ID: return meta + matched videos ──────────────────────
        if item_id.startswith("tt"):
            title, year, title_candidates = await get_title_info(type, item_id)
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
                    score = _match_any(fn, title_candidates, year, fs, fe)
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
            title, year, title_candidates = await get_title_info(type, imdb_id)
            if not title:
                return JSONResponse({"streams": []})
            scored = []
            for f in files:
                fn = f["file_name"]
                score = _match_any(fn, title_candidates, year, season, episode)
                if score >= MATCH_THRESHOLD:
                    scored.append((score, f))
            scored.sort(key=lambda x: x[0], reverse=True)
            for score, f in scored:
                size_gb = round(f["file_size"] / 1024 ** 3, 2) if f.get("file_size") else None
                size_str = f" | {size_gb} GB" if size_gb else ""
                streams.append({
                    "name": "TG Manager",
                    "title": f"{f['file_name']}{size_str}",
                    "url": await _file_url(base, f),
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
                        "url": await _file_url(base, f),
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
                        "url": await _file_url(base, f),
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
            title, year, title_candidates = await get_title_info(type, imdb_id)
            if title:
                for f in files:
                    score = _match_any(f["file_name"], title_candidates, year, season, episode)
                    if score >= MATCH_THRESHOLD:
                        filename = f["file_name"]
                        break
            # Can proxy directly with the imdb_id we already have
            os_id = imdb_id if type == "movie" else f"{imdb_id}:{season}:{episode}"
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.get(f"{OPENSUBTITLES_BASE}/subtitles/{type}/{os_id}.json")
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
                r = await c.get(f"{OPENSUBTITLES_BASE}/subtitles/{type}/{os_id}.json")
                if r.status_code == 200:
                    return JSONResponse(r.json())
        except Exception as e:
            print(f"[subtitles] opensubtitles failed for {imdb_id}: {e}")

        return JSONResponse({"subtitles": []})

    async def _serve_file_impl(chat_id: int, message_id: int, request: Request,
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

    @app.api_route("/tgfile/{chat_id}/{message_id}", methods=["GET", "HEAD"],
                   operation_id="serve_file_no_name")
    async def serve_file(chat_id: int, message_id: int, request: Request):
        return await _serve_file_impl(chat_id, message_id, request)

    @app.api_route("/tgfile/{chat_id}/{message_id}/{file_name}", methods=["GET", "HEAD"],
                   operation_id="serve_file_with_name")
    async def serve_file_named(chat_id: int, message_id: int, file_name: str, request: Request):
        return await _serve_file_impl(chat_id, message_id, request, file_name)
