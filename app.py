import asyncio
import threading
import time
import uvicorn
import gradio as gr
from pathlib import Path

from backend import app as fastapi_app
from database import init_db
from search import search_files, get_type_counts, get_chat_file_stats, enrich_file_row, fmt_size
from storage import get_storage_stats, delete_cached_file, clear_all_cache
from telegram_client import fetch_chats, scan_chat
from database import get_chats, toggle_favorite, recent_downloads

# ── start FastAPI in background thread ────────────────────────────────────────

def _run_api():
    uvicorn.run(fastapi_app, host="0.0.0.0", port=7861, log_level="warning")

threading.Thread(target=_run_api, daemon=True).start()
init_db()


# ── async runner ──────────────────────────────────────────────────────────────

def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


# ── helpers ───────────────────────────────────────────────────────────────────

def chats_to_choices():
    rows = get_chats()
    return [(r["title"], str(r["id"])) for r in rows]


def files_to_table(rows):
    out = []
    for r in rows:
        r = enrich_file_row(r)
        out.append([
            r["file_name"],
            r["media_type"],
            r["size_fmt"],
            r["date_fmt"],
            "✅" if r["cached"] else "⬜",
            str(r["chat_id"]),
            str(r["message_id"]),
        ])
    return out


HEADERS = ["File Name", "Type", "Size", "Date", "Cached", "Chat ID", "Msg ID"]


# ── tab: Account ──────────────────────────────────────────────────────────────

def sync_chats_action():
    try:
        chats = run_async(fetch_chats())
        return f"✅ Synced {len(chats)} chats.", gr.update(choices=chats_to_choices())
    except Exception as e:
        return f"❌ {e}", gr.update()


def load_account_info():
    from telegram_client import get_me
    try:
        info = run_async(get_me())
        return f"👤 {info['name']} | @{info['username']} | 📱 {info['phone']}"
    except Exception as e:
        return f"❌ Not connected: {e}"


# ── tab: Browse ───────────────────────────────────────────────────────────────

def browse_chat(chat_id_str, media_type_filter):
    if not chat_id_str:
        return [], "Select a chat first."
    try:
        chat_id = int(chat_id_str)
        rows = search_files(chat_id=chat_id, media_type=media_type_filter, limit=200)
        counts = get_type_counts(chat_id)
        summary = " | ".join(f"{k}: {v}" for k, v in counts.items() if v > 0)
        return files_to_table(rows), summary
    except Exception as e:
        return [], f"❌ {e}"


def scan_chat_action(chat_id_str, scan_limit):
    if not chat_id_str:
        return "Select a chat first."
    try:
        chat_id = int(chat_id_str)
        count = run_async(scan_chat(chat_id, limit=int(scan_limit)))
        return f"✅ Indexed {count} files from chat."
    except Exception as e:
        return f"❌ {e}"


def toggle_fav_action(chat_id_str):
    if not chat_id_str:
        return "Select a chat first.", gr.update()
    toggle_favorite(int(chat_id_str))
    return "⭐ Toggled favorite.", gr.update(choices=chats_to_choices())


# ── tab: Search ───────────────────────────────────────────────────────────────

def do_search(query, media_type, min_mb, max_mb, date_from, date_to, cached_only):
    rows = search_files(
        query=query,
        media_type=media_type,
        min_size_mb=float(min_mb or 0),
        max_size_mb=float(max_mb or 0),
        date_from=date_from or None,
        date_to=date_to or None,
        downloaded_only=cached_only,
        limit=200,
    )
    return files_to_table(rows), f"{len(rows)} results"


# ── tab: Download ─────────────────────────────────────────────────────────────

def start_download_action(chat_id_str, msg_id_str):
    if not chat_id_str or not msg_id_str:
        return "Enter Chat ID and Message ID."
    try:
        chat_id = int(chat_id_str)
        msg_id = int(msg_id_str)

        result_holder = {}

        async def _dl():
            from telegram_client import download_file
            from database import get_file_by_msg
            row = get_file_by_msg(chat_id, msg_id)
            if not row:
                result_holder["msg"] = "❌ File not in index. Scan chat first."
                return

            def on_prog(cur, tot):
                pct = round(cur / tot * 100, 1) if tot else 0
                result_holder["progress"] = pct

            path = await download_file(chat_id, msg_id, row["file_name"], progress_cb=on_prog)
            if path:
                result_holder["msg"] = f"✅ Downloaded: {path.name}"
                result_holder["path"] = str(path)
            else:
                result_holder["msg"] = "❌ Download failed."

        run_async(_dl())
        return result_holder.get("msg", "Done.")
    except Exception as e:
        return f"❌ {e}"


def download_from_table(evt: gr.SelectData, table_data):
    if not table_data or evt.index[0] >= len(table_data):
        return "No row selected.", "", ""
    row = table_data[evt.index[0]]
    chat_id = row[5]
    msg_id = row[6]
    file_name = row[0]
    return f"Selected: {file_name}", str(chat_id), str(msg_id)


# ── tab: Storage ──────────────────────────────────────────────────────────────

def load_storage():
    s = get_storage_stats()
    text = (
        f"📦 Cache: {s['cache_used_gb']} GB / {s['cache_limit_gb']} GB ({s['cache_pct']}%)\n"
        f"💽 Disk: {s['disk_used_gb']} GB used, {s['disk_free_gb']} GB free\n"
        f"📄 Cached files: {s['cache_file_count']}"
    )
    stats = get_chat_file_stats()
    rows = [[r["title"] or str(r["chat_id"]), r["file_count"], fmt_size(r["total_size"] or 0), r["cached_count"]] for r in stats]
    return text, rows


def clear_all_action():
    n = clear_all_cache()
    return f"🗑️ Deleted {n} cached files."


# ── tab: Recent ───────────────────────────────────────────────────────────────

def load_recent():
    rows = recent_downloads(20)
    return [[r["file_name"], r["status"], r["started_at"][:16], r["finished_at"][:16] if r["finished_at"] else "—"] for r in rows]


# ── build UI ──────────────────────────────────────────────────────────────────

with gr.Blocks(title="TGFiles", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 📁 TGFiles — Telegram File Browser")

    with gr.Tabs():

        # ── Account ──────────────────────────────────────────────────────────
        with gr.TabItem("👤 Account"):
            acct_info = gr.Textbox(label="Account", interactive=False)
            with gr.Row():
                btn_load_me = gr.Button("Load Account Info")
                btn_sync = gr.Button("🔄 Sync Chats", variant="primary")
            sync_status = gr.Textbox(label="Status", interactive=False)
            chat_dropdown_acct = gr.Dropdown(label="Chats (refreshed after sync)", choices=chats_to_choices())

            btn_load_me.click(load_account_info, outputs=acct_info)
            btn_sync.click(sync_chats_action, outputs=[sync_status, chat_dropdown_acct])

        # ── Browse ────────────────────────────────────────────────────────────
        with gr.TabItem("📂 Browse"):
            with gr.Row():
                chat_dd = gr.Dropdown(label="Chat", choices=chats_to_choices(), scale=3)
                type_dd = gr.Dropdown(label="Filter Type", choices=["all", "video", "audio", "document", "image", "archive"], value="all", scale=1)
            with gr.Row():
                btn_browse = gr.Button("Browse", variant="primary")
                scan_limit = gr.Slider(50, 1000, value=200, step=50, label="Scan limit")
                btn_scan = gr.Button("🔍 Scan Chat")
                btn_fav = gr.Button("⭐ Toggle Favorite")
            browse_status = gr.Textbox(label="Status", interactive=False)
            browse_table = gr.Dataframe(headers=HEADERS, interactive=False, wrap=True)

            sel_file_label = gr.Textbox(label="Selected File", interactive=False)
            with gr.Row():
                sel_chat_id = gr.Textbox(label="Chat ID", interactive=False)
                sel_msg_id = gr.Textbox(label="Message ID", interactive=False)
            btn_dl_selected = gr.Button("⬇ Download Selected", variant="primary")
            dl_status_browse = gr.Textbox(label="Download Status", interactive=False)

            btn_browse.click(browse_chat, inputs=[chat_dd, type_dd], outputs=[browse_table, browse_status])
            btn_scan.click(scan_chat_action, inputs=[chat_dd, scan_limit], outputs=browse_status)
            btn_fav.click(toggle_fav_action, inputs=chat_dd, outputs=[browse_status, chat_dd])
            browse_table.select(download_from_table, inputs=browse_table, outputs=[sel_file_label, sel_chat_id, sel_msg_id])
            btn_dl_selected.click(start_download_action, inputs=[sel_chat_id, sel_msg_id], outputs=dl_status_browse)

        # ── Search ────────────────────────────────────────────────────────────
        with gr.TabItem("🔍 Search"):
            with gr.Row():
                search_q = gr.Textbox(label="Filename search", scale=3)
                search_type = gr.Dropdown(["all", "video", "audio", "document", "image", "archive"], value="all", label="Type", scale=1)
            with gr.Row():
                min_mb = gr.Number(label="Min size (MB)", value=0, precision=1)
                max_mb = gr.Number(label="Max size (MB)", value=0, precision=1)
                date_from = gr.Textbox(label="Date from (YYYY-MM-DD)")
                date_to = gr.Textbox(label="Date to (YYYY-MM-DD)")
            cached_only = gr.Checkbox(label="Cached only")
            btn_search = gr.Button("Search", variant="primary")
            search_count = gr.Textbox(label="Results", interactive=False)
            search_table = gr.Dataframe(headers=HEADERS, interactive=False, wrap=True)

            s_file_label = gr.Textbox(label="Selected", interactive=False)
            with gr.Row():
                s_chat_id = gr.Textbox(label="Chat ID", interactive=False)
                s_msg_id = gr.Textbox(label="Message ID", interactive=False)
            btn_dl_search = gr.Button("⬇ Download Selected", variant="primary")
            dl_status_search = gr.Textbox(label="Download Status", interactive=False)

            btn_search.click(do_search, inputs=[search_q, search_type, min_mb, max_mb, date_from, date_to, cached_only], outputs=[search_table, search_count])
            search_table.select(download_from_table, inputs=search_table, outputs=[s_file_label, s_chat_id, s_msg_id])
            btn_dl_search.click(start_download_action, inputs=[s_chat_id, s_msg_id], outputs=dl_status_search)

        # ── Download ──────────────────────────────────────────────────────────
        with gr.TabItem("⬇ Download"):
            gr.Markdown("Manually enter Chat ID + Message ID, or click a row in Browse/Search tabs.")
            with gr.Row():
                dl_chat = gr.Textbox(label="Chat ID")
                dl_msg = gr.Textbox(label="Message ID")
            btn_dl = gr.Button("⬇ Download", variant="primary")
            dl_out = gr.Textbox(label="Status", interactive=False)
            btn_dl.click(start_download_action, inputs=[dl_chat, dl_msg], outputs=dl_out)

        # ── Storage ───────────────────────────────────────────────────────────
        with gr.TabItem("💾 Storage"):
            btn_refresh_storage = gr.Button("🔄 Refresh")
            storage_info = gr.Textbox(label="Cache Info", lines=3, interactive=False)
            storage_table = gr.Dataframe(headers=["Chat", "Files", "Total Size", "Cached"], interactive=False)
            btn_clear_all = gr.Button("🗑️ Clear All Cache", variant="stop")
            clear_status = gr.Textbox(label="Status", interactive=False)

            btn_refresh_storage.click(load_storage, outputs=[storage_info, storage_table])
            btn_clear_all.click(clear_all_action, outputs=clear_status)

        # ── Recent ────────────────────────────────────────────────────────────
        with gr.TabItem("🕓 Recent"):
            btn_refresh_recent = gr.Button("🔄 Refresh")
            recent_table = gr.Dataframe(headers=["File", "Status", "Started", "Finished"], interactive=False)
            btn_refresh_recent.click(load_recent, outputs=recent_table)

    demo.load(load_recent, outputs=recent_table)


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860, share=False)
