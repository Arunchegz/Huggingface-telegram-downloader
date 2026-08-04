"""TG Manager Stremio addon server (no UI) — downloads from Telegram and serves them via Stremio."""

import asyncio
import threading

import gradio as gr
import spaces

from database import init_db
from telegram_client import auto_download_main
from config import AUTO_DOWNLOAD, CHANNEL_REF

init_db()


@spaces.GPU
def _dummy_gpu():
    pass


# ── auto-downloader (background) ──────────────────────────────────────────────

def start_auto_downloader():
    if not AUTO_DOWNLOAD or not CHANNEL_REF:
        return

    def _status(msg):
        print(f"[auto] {msg}")

    def _run():
        try:
            asyncio.run(auto_download_main(status_cb=_status))
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[auto] FATAL: {e}")

    t = threading.Thread(target=_run, daemon=True, name="auto-downloader")
    t.start()


start_auto_downloader()


if __name__ == "__main__":
    import os
    from stremio_addon import add_routes

    port = int(os.environ.get("PORT", "7860"))

    # demo.launch() must run for the @spaces.GPU startup check;
    # addon routes go on gradio's FastAPI app, main thread parks forever.
    with gr.Blocks(title="TG Manager Stremio Addon") as demo:
        pass
    demo.launch(server_name="0.0.0.0", server_port=port, share=False,
                prevent_thread_lock=True)
    add_routes(demo.app)
    threading.Event().wait()
