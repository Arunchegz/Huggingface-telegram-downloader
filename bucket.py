"""Bucket file deletion.

On HF Spaces the bucket is mounted read-write at PERSISTENT_STORAGE (/data),
so `p.unlink()` on the local path already removes the object — no API call
needed.  This module is therefore a no-op in that environment.

On any other backend (S3, R2, custom) you can swap in real delete logic here.
The caller (`_remove_downloaded_file`) always invokes this AFTER the local
`unlink()` succeeds, so on HF Spaces the bucket is already clean by the time
we arrive.
"""

import os
from pathlib import Path

from config import DOWNLOAD_DIR


import logging

logger = logging.getLogger("tgmanager.bucket")


def delete_bucket_file(chat_id: int, file_name: str) -> bool:
    """Delete a file from the bucket.

    Strategy:
      1. If the file still exists under DOWNLOAD_DIR (shouldn't happen after
         `unlink`, but guards against races), delete it now.
      2. If HF_TOKEN + SPACE_ID are present, attempt a HF Hub API delete so
         that bucket objects are removed even when the local mount path has
         already been unlinked (e.g. race between eviction and delete update).
      3. Otherwise return True — the local unlink by the caller is sufficient
         for HF Spaces mounted buckets.

    Returns True on success or when no action was needed, False on error.
    """
    # Guard: wipe local file if somehow still present
    local_path = DOWNLOAD_DIR / str(chat_id) / file_name
    if local_path.exists():
        try:
            local_path.unlink()
        except OSError as e:
            logger.warning(f"Failed to unlink local file {local_path}: {e}")

    hf_token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    space_id = os.environ.get("SPACE_ID")  # e.g. "username/space-name"

    if hf_token and space_id:
        try:
            from huggingface_hub import HfApi
            api = HfApi(token=hf_token)
            repo_path = f"{chat_id}/{file_name}"
            api.delete_file(
                path_in_repo=repo_path,
                repo_id=space_id,
                repo_type="space",
            )
        except Exception as e:
            # File may already be gone; log warning
            logger.warning(f"HF Hub delete_file failed for {chat_id}/{file_name}: {e}")

    return True
