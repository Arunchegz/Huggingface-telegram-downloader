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
    repo = os.environ.get("STORAGE_BUCKET_REPO", "")
    endpoint = os.environ.get("HF_S3_ENDPOINT", "https://s3.hf.co").rstrip("/")
    if not (access and secret and repo):
        return ""

    owner, bucket = repo.split("/", 1)
    key = f"{bucket}/downloads/{chat_id}/{file_name}"
    canonical_uri = "/" + owner + "/" + urllib.parse.quote(key, safe="/~")
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
      1. Unlink the local copy under DOWNLOAD_DIR if still present.
      2. If S3 gateway credentials are configured, SigV4-DELETE the exact
         bucket object that gets served (`downloads/{chat}/{file}`).
      3. Else, if HF_TOKEN + a repo are present, attempt HF Hub delete_file
         (mainly useful for dataset/model-style bucket backends).

    Returns True on success or when no action was needed, False on error.
    """
    # Guard: wipe local file if somehow still present
    local_path = DOWNLOAD_DIR / str(chat_id) / file_name
    if local_path.exists():
        try:
            local_path.unlink()
        except OSError as e:
            logger.warning(f"Failed to unlink local file {local_path}: {e}")

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
    repo = os.environ.get("STORAGE_BUCKET_REPO") or os.environ.get("SPACE_ID")
    repo_type = os.environ.get("STORAGE_BUCKET_TYPE", "space").strip()
    collection = os.environ.get("HF_BUCKET_COLLECTION", "downloads").strip()

    if hf_token and repo:
        try:
            from huggingface_hub import HfApi
            api = HfApi(token=hf_token)
            repo_path = f"{collection}/{chat_id}/{file_name}"
            api.delete_file(
                path_in_repo=repo_path,
                repo_id=repo,
                repo_type=repo_type,
            )
        except Exception as e:
            # File may already be gone or not present in bucket; log info
            logger.info(f"HF Hub delete_file [{repo_path}]: {e}")

    return True


def list_bucket_files(chat_id: int) -> list[str]:
    """List all file names stored in the bucket under downloads/{chat_id}/.

    Tries S3 ListObjectsV2 first (if S3 creds configured), then falls back to
    HF Hub list_repo_tree for dataset/model/space backends.
    Returns bare file names (no path prefix), or [] on error / not configured.
    """
    collection = os.environ.get("HF_BUCKET_COLLECTION", "downloads").strip()
    prefix_path = f"{collection}/{chat_id}/"

    # ── S3 ListObjectsV2 ─────────────────────────────────────────────────────
    access = os.environ.get("HF_S3_ACCESS_KEY", "")
    secret = os.environ.get("HF_S3_SECRET_KEY", "")
    repo   = os.environ.get("STORAGE_BUCKET_REPO", "")
    endpoint = os.environ.get("HF_S3_ENDPOINT", "https://s3.hf.co").rstrip("/")
    region   = os.environ.get("HF_S3_REGION", "us-east-1").strip()
    service  = os.environ.get("HF_S3_SERVICE", "s3").strip()

    if access and secret and repo:
        try:
            import httpx
            owner, bucket = repo.split("/", 1)
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
            canonical_uri     = f"/{owner}/"
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
    # bucket_id is the Space/repo ID that owns the bucket (e.g. "owner/MySpace")
    bucket_id = os.environ.get("STORAGE_BUCKET_REPO") or os.environ.get("SPACE_ID")

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
            return files
        except ImportError:
            # older huggingface_hub without list_bucket_tree
            logger.warning("list_bucket_tree not available in this huggingface_hub version")
        except Exception as e:
            logger.warning(f"HF list_bucket_tree error: {e}")

    return []
