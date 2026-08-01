"""Delete files from the external HuggingFace bucket repo (Space/Dataset)."""

from config import HF_TOKEN, STORAGE_BUCKET_REPO, STORAGE_BUCKET_TYPE


def delete_bucket_file(chat_id: int, file_name: str) -> bool:
    """Delete {chat_id}/{file_name} from the HF bucket repo. Best-effort."""
    if not HF_TOKEN or not STORAGE_BUCKET_REPO:
        return False
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_TOKEN)
        api.delete_file(
            path_in_repo=f"{chat_id}/{file_name}",
            repo_id=STORAGE_BUCKET_REPO,
            repo_type=STORAGE_BUCKET_TYPE,
            commit_message=f"tgmanager: delete {chat_id}/{file_name}",
        )
        return True
    except Exception as e:
        print(f"[bucket] delete failed {chat_id}/{file_name}: {e}")
        return False
