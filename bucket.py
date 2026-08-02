"""Bucket file deletion.

The Space mounts a Hugging Face Storage Bucket read-write at /data
(PERSISTENT_STORAGE), so downloads land in the bucket directly and
deleting the local file under DOWNLOAD_DIR removes the object from the
bucket automatically. No HF API call is needed (buckets have no
per-file git delete API; delete_bucket would destroy the whole bucket).
"""


def delete_bucket_file(chat_id: int, file_name: str) -> bool:
    """No-op: local file deletion (handled by the caller) syncs to the mounted bucket."""
    return True
