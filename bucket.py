"""Bucket file deletion.

Files are downloaded into `DOWNLOAD_DIR` (`/data/downloads/{chat}/{file}`) and
served to clients from the space's storage bucket via the public S3 gateway
(`s3.hf.co/{owner}/{bucket}/downloads/{chat}/{file}`, SigV4-presigned GET).

Deleting a channel message must remove the object from that bucket, so this
module:

  1. unlinks the local file under DOWNLOAD_DIR (also covers the case where
     `/data` is the mounted bucket itself),
  2. SigV4-signs a DELETE against the same gateway object that gets served,
  3. falls back to a HF Hub `delete_file` for dataset/model-style backends.
"""

import hashlib
import hmac
import os
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

from config import DOWNLOAD_DIR


import logging

logger = logging.getLogger("tgmanager.bucket")

_bucket_cache: str | None = None
_bucket_cache_set = False


def resolve_storage_bucket() -> str:
    """Return the storage bucket repo id ("owner/name"), for this Space.

    Priority:
      1. STORAGE_BUCKET_REPO env var (explicit override).
      2. The bucket volume mounted on this Space, discovered from the HF
         runtime API (GET /api/spaces/{SPACE_ID} → runtime.volumes[]).
      3. Empty string (bucket not resolvable).

    Result is cached for the process lifetime once resolved.
    """
    global _bucket_cache, _bucket_cache_set
    if _bucket_cache_set and _bucket_cache:
        return _bucket_cache

    explicit = os.environ.get("STORAGE_BUCKET_REPO", "").strip()
    if explicit:
        _bucket_cache, _bucket_cache_set = explicit, True
        return explicit

    discovered = ""
    space_id = (os.environ.get("SPACE_ID") or "").strip()
    hf_token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    if space_id and hf_token:
        # Method A: HfApi get_space_runtime
        try:
            from huggingface_hub import HfApi
            api = HfApi(token=hf_token)
            runtime = api.get_space_runtime(repo_id=space_id)
            for vol in getattr(runtime, "volumes", []) or []:
                vtype = getattr(vol, "type", None) or (vol.get("type") if isinstance(vol, dict) else None)
                vsource = getattr(vol, "source", None) or (vol.get("source") if isinstance(vol, dict) else None)
                if vtype in ("storage", "bucket", "storage_bucket") and vsource:
                    discovered = vsource
                    break
        except Exception as e:
            logger.debug(f"HfApi get_space_runtime discovery: {e}")

        # Method B: Direct HTTP API query
        if not discovered:
            try:
                import httpx
                r = httpx.get(
                    f"https://huggingface.co/api/spaces/{space_id}",
                    headers={"Authorization": f"Bearer {hf_token}"},
                    timeout=15,
                )
                if r.status_code == 200:
                    runtime = r.json().get("runtime") or {}
                    for vol in runtime.get("volumes") or []:
                        vol_type = vol.get("type", "")
                        if vol_type in ("storage", "bucket", "storage_bucket") and vol.get("source"):
                            discovered = vol["source"]
                            break
                else:
                    logger.warning(f"bucket discovery: space API {r.status_code}")
            except Exception as e:
                logger.warning(f"bucket discovery failed: {e}")

    if discovered:
        _bucket_cache, _bucket_cache_set = discovered, True
        logger.info(f"Resolved storage bucket: {discovered}")
        return discovered
    return ""


def _parse_repo_owner_bucket(repo: str) -> tuple[str, str]:
    """Split 'owner/bucket' safely, defaulting owner from SPACE_ID if missing."""
    if "/" in repo:
        return repo.split("/", 1)
    space_id = (os.environ.get("SPACE_ID") or "").strip()
    owner = space_id.split("/")[0] if "/" in space_id else ""
    return owner, repo


def _sigv4_sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _sigv4_signing_key(secret: str, date_stamp: str) -> bytes:
    region = os.environ.get("HF_S3_REGION", "us-east-1").strip()
    service = os.environ.get("HF_S3_SERVICE", "s3").strip()
    k_date = _sigv4_sign(("AWS4" + secret).encode("utf-8"), date_stamp)
    k_region = _sigv4_sign(k_date, region)
    k_service = _sigv4_sign(k_region, service)
    return _sigv4_sign(k_service, "aws4_request")


def _s3_delete_url(chat_id: int, file_name: str, now=None) -> str:
    """SigV4-presign a DELETE of the same bucket object the addon serves.

    Path layout mirrors `presign_s3_url` in stremio_addon.py so the object we
    remove is exactly the one that was streamed. Returns "" when S3 gateway
    credentials are not configured (caller falls back to other strategies).
    """
    access = os.environ.get("HF_S3_ACCESS_KEY", "")
    secret = os.environ.get("HF_S3_SECRET_KEY", "")
    repo = resolve_storage_bucket()
    endpoint = os.environ.get("HF_S3_ENDPOINT", "https://s3.hf.co").rstrip("/")
    if not (access and secret and repo):
        return ""

    owner, bucket = _parse_repo_owner_bucket(repo)
    collection = os.environ.get("HF_BUCKET_COLLECTION", "downloads").strip()
    key = f"{bucket}/{collection}/{chat_id}/{file_name}"
    canonical_uri = (f"/{owner}/" if owner else "/") + urllib.parse.quote(key, safe="/~")
    host = urllib.parse.urlparse(endpoint).netloc
    region = os.environ.get("HF_S3_REGION", "us-east-1").strip()
    service = os.environ.get("HF_S3_SERVICE", "s3").strip()
    expires = os.environ.get("HF_S3_EXPIRES", "300").strip()

    t = now or datetime.now(timezone.utc)
    amz_date = t.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = t.strftime("%Y%m%d")
    scope = f"{date_stamp}/{region}/{service}/aws4_request"

    params = [
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        ("X-Amz-Credential", f"{access}/{scope}"),
        ("X-Amz-Date", amz_date),
        ("X-Amz-Expires", expires),
        ("X-Amz-SignedHeaders", "host"),
    ]
    params.sort()
    qs = "&".join(
        f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
        for k, v in params
    )

    canonical_headers = f"host:{host}\n"
    canonical_request = "\n".join([
        "DELETE", canonical_uri, qs,
        canonical_headers, "host", "UNSIGNED-PAYLOAD",
    ])
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    signature = hmac.new(
        _sigv4_signing_key(secret, date_stamp),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{endpoint}{canonical_uri}?{qs}&X-Amz-Signature={signature}"


def delete_bucket_file(chat_id: int, file_name: str) -> bool:
    """Delete a file from the storage bucket (best-effort).

    Strategy:
      1. Unlink the local copy under DOWNLOAD_DIR if still present (covers mounted bucket volume).
      2. If S3 gateway credentials are configured, SigV4-DELETE the exact
         bucket object that gets served (`downloads/{chat}/{file}`).
      3. HF Hub Storage Bucket API (batch_bucket_files with delete=[paths]).
      4. Else fallback to HF Hub delete_file for dataset/model/space git repos.

    Returns True on success or when local file was unlinked, False on complete failure.
    """
    deleted_any = False

    # Guard: wipe local file if somehow still present (also covers /data bucket mounts)
    cid_variants = {str(chat_id), str(abs(chat_id))}
    if str(abs(chat_id)).startswith("100"):
        cid_variants.add(str(abs(chat_id))[3:])
    for cid in cid_variants:
        loc = DOWNLOAD_DIR / cid / file_name
        if loc.exists():
            try:
                loc.unlink()
                deleted_any = True
                logger.info(f"Unlinked local file {loc}")
            except OSError as e:
                logger.warning(f"Failed to unlink local file {loc}: {e}")

    # Real bucket delete: SigV4-signed DELETE against the S3 gateway object.
    delete_url = _s3_delete_url(chat_id, file_name)
    if delete_url:
        try:
            import httpx
            r = httpx.delete(delete_url, timeout=30)
            if r.status_code in (200, 202, 204, 404):
                logger.info(f"S3 DELETE ok {chat_id}/{file_name} ({r.status_code})")
                return True
            logger.warning(
                f"S3 DELETE unexpected status {r.status_code} for "
                f"{chat_id}/{file_name}: {r.text[:200]}"
            )
        except Exception as e:
            logger.warning(f"S3 DELETE failed {chat_id}/{file_name}: {e}")

    hf_token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    repo = resolve_storage_bucket()
    collection = os.environ.get("HF_BUCKET_COLLECTION", "downloads").strip()

    if hf_token and repo:
        paths_to_delete = [f"{collection}/{chat_id}/{file_name}"]
        alt_cid = str(chat_id).replace("-100", "").replace("-", "")
        if alt_cid != str(chat_id):
            paths_to_delete.append(f"{collection}/{alt_cid}/{file_name}")

        # Primary: HF Storage Bucket API (batch_bucket_files)
        try:
            from huggingface_hub import batch_bucket_files
            batch_bucket_files(
                bucket_id=repo,
                delete=paths_to_delete,
                token=hf_token,
            )
            logger.info(f"HF Hub batch_bucket_files deleted [{repo}: {paths_to_delete}]")
            return True
        except ImportError:
            logger.debug("batch_bucket_files not available in huggingface_hub")
        except Exception as e:
            logger.warning(f"HF batch_bucket_files failed for {repo}: {e}")

        # Fallback: Git-based repository delete_file (dataset, space, model)
        repo_type = os.environ.get("STORAGE_BUCKET_TYPE", "space").strip()
        try:
            from huggingface_hub import HfApi
            api = HfApi(token=hf_token)
            for rpath in paths_to_delete:
                try:
                    api.delete_file(
                        path_in_repo=rpath,
                        repo_id=repo,
                        repo_type=repo_type,
                    )
                    deleted_any = True
                    logger.info(f"HF Hub delete_file ok [{repo}:{rpath}]")
                except Exception:
                    pass
            if deleted_any:
                return True
        except Exception as e:
            logger.debug(f"HF Hub delete_file fallback failed [{repo}]: {e}")

    return deleted_any


def list_bucket_files(chat_id: int) -> list[str]:
    """List all file names stored in the bucket under downloads/{chat_id}/.

    Tries S3 ListObjectsV2 first (if S3 creds configured), then falls back to
    HF Hub list_bucket_tree for Space bucket backends, and checks local mount.
    Returns bare file names (no path prefix), or [] on error / not configured.
    """
    collection = os.environ.get("HF_BUCKET_COLLECTION", "downloads").strip()
    prefix_path = f"{collection}/{chat_id}/"

    # ── S3 ListObjectsV2 ─────────────────────────────────────────────────────
    access = os.environ.get("HF_S3_ACCESS_KEY", "")
    secret = os.environ.get("HF_S3_SECRET_KEY", "")
    repo   = resolve_storage_bucket()
    endpoint = os.environ.get("HF_S3_ENDPOINT", "https://s3.hf.co").rstrip("/")
    region   = os.environ.get("HF_S3_REGION", "us-east-1").strip()
    service  = os.environ.get("HF_S3_SERVICE", "s3").strip()

    if access and secret and repo:
        try:
            import httpx
            owner, bucket = _parse_repo_owner_bucket(repo)
            key_prefix = f"{bucket}/{prefix_path}"
            host = urllib.parse.urlparse(endpoint).netloc
            now = datetime.now(timezone.utc)
            amz_date   = now.strftime("%Y%m%dT%H%M%SZ")
            date_stamp = now.strftime("%Y%m%d")
            scope = f"{date_stamp}/{region}/{service}/aws4_request"
            params = [
                ("list-type", "2"),
                ("prefix",    key_prefix),
                ("X-Amz-Algorithm",     "AWS4-HMAC-SHA256"),
                ("X-Amz-Credential",    f"{access}/{scope}"),
                ("X-Amz-Date",          amz_date),
                ("X-Amz-Expires",       "60"),
                ("X-Amz-SignedHeaders", "host"),
            ]
            params.sort()
            qs = "&".join(
                f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
                for k, v in params
            )
            canonical_uri     = f"/{owner}/" if owner else "/"
            canonical_headers = f"host:{host}\n"
            canonical_request = "\n".join([
                "GET", canonical_uri, qs,
                canonical_headers, "host", "UNSIGNED-PAYLOAD",
            ])
            string_to_sign = "\n".join([
                "AWS4-HMAC-SHA256", amz_date, scope,
                hashlib.sha256(canonical_request.encode()).hexdigest(),
            ])
            sig = hmac.new(
                _sigv4_signing_key(secret, date_stamp),
                string_to_sign.encode(),
                hashlib.sha256,
            ).hexdigest()
            url = f"{endpoint}{canonical_uri}?{qs}&X-Amz-Signature={sig}"
            r = httpx.get(url, timeout=30)
            if r.status_code == 200:
                import xml.etree.ElementTree as ET
                ns   = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
                root = ET.fromstring(r.text)
                files = []
                for obj in root.findall("s3:Contents", ns):
                    key_el = obj.find("s3:Key", ns) or obj.find("Key")
                    key = (key_el.text or "") if key_el is not None else ""
                    if key.startswith(key_prefix):
                        files.append(key[len(key_prefix):])
                return [f for f in files if f]
            logger.warning(f"S3 list {r.status_code}: {r.text[:200]}")
        except Exception as e:
            logger.warning(f"S3 list error: {e}")

    # ── HF Hub list_bucket_tree (Space bucket API) ───────────────────────────
    hf_token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    bucket_id = resolve_storage_bucket()

    if hf_token and bucket_id:
        try:
            from huggingface_hub import HfApi
            from huggingface_hub import BucketFile
            api = HfApi(token=hf_token)
            items = api.list_bucket_tree(
                bucket_id=bucket_id,
                prefix=prefix_path,
                recursive=True,
                token=hf_token,
            )
            files = []
            for item in items:
                if isinstance(item, BucketFile):
                    name = item.path.split("/")[-1]
                    if name:
                        files.append(name)
            if files:
                return files
        except ImportError:
            logger.warning("list_bucket_tree not available in this huggingface_hub version")
        except Exception as e:
            logger.warning(f"HF list_bucket_tree error: {e}")

    # ── Local directory fallback (if bucket is mounted as volume at /data) ───
    for cid in (str(chat_id), str(abs(chat_id))):
        local_dir = DOWNLOAD_DIR / cid
        if local_dir.exists() and local_dir.is_dir():
            local_files = [p.name for p in local_dir.iterdir() if p.is_file()]
            if local_files:
                return local_files

    return []
