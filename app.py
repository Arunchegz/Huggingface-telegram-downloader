import asyncio
import gradio as gr
from pathlib import Path

from database import init_db
from search import search_files, get_type_counts, get_chat_file_stats, enrich_file_row, fmt_size
from storage import get_storage_stats, clear_all_cache
from telegram_client import fetch_chats, scan_chat, download_file, get_me
from database import get_chats, toggle_favorite, recent_downloads, get_file_by_msg

init_db()

# ── async runner ──────────────────────────────────────────────────────────────

def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError
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

# ── account ───────────────────────────────────────────────────────────────────

def load_account_info():
    try:
        info = run_async(get_me())
        return f"👤 {info['name']} | @{info['username']} | 📱 {info['phone']}"
    except Exception as e:
        return f"❌ Not connected: {e}"

def sync_chats_action():
    try:
        chats = run_async(fetch_chats())
        choices = chats_to_choices()
        return f"✅ Synced {len(chats)} chats.", gr.update(choices=choices), gr.update(choices=choices)
    except Exception as e:
        return f"❌ {e}", gr.update(), gr.update()

# ── browse ────────────────────────────────────────────────────────────────────

def browse_chat(chat_id_str, media_type_filter):
    if not chat_id_str:
        return [], "Select a chat first."
    try:
        rows = search_files(chat_id=int(chat_id_str), media_type=media_type_filter, limit=200)
        counts = get_type_counts(int(chat_id_str))
        summary = " | ".join(f"{k}: {v}" for k, v in counts.items() if v > 0)
        return files_to_table(rows), summary
    except Exception as e:
        return [], f"❌ {e}"

def scan_chat_action(chat_id_str, scan_limit):
    if not chat_id_str:
        return "Select a chat first."
    try:
        count = run_async(scan_chat(int(chat_id_str), limit=int(scan_limit)))
        return f"✅ Indexed {count} files."
    except Exception as e:
        return f"❌ {e}"

def toggle_fav_action(chat_id_str):
    if not chat_id_str:
        return "Select a chat first.", gr.update()
    toggle_favorite(int(chat_id_str))
    choices = chats_to_choices()
    return "⭐ Toggled.", gr.update(choices=choices)

# ── search ────────────────────────────────────────────────────────────────────

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

# ── download ──────────────────────────────────────────────────────────────────

def start_download_action(chat_id_str, msg_id_str):
    if not chat_id_str or not msg_id_str:
        return "Enter Chat ID and Message ID."
    try:
        chat_id, msg_id = int(chat_id_str), int(msg_id_str)
        row = get_file_by_msg(chat_id, msg_id)
        if not row:
            return "❌ File not indexed. Scan chat first."

        result = {}
        async def _dl():
            def on_prog(cur, tot):
                result["pct"] = round(cur/tot*100, 1) if tot else 0
            path = await download_file(chat_id, msg_id, row["file_name"], progress_cb=on_prog)
            result["path"] = str(path) if path else None

        run_async(_dl())
        if result.get("path"):
            return f"✅ Done: {Path(result['path']).name}"
        return "❌ Download failed."
    except Exception as e:
        return f"❌ {e}"

def select_row(evt: gr.SelectData, table_data):
    if not table_data or evt.index[0] >= len(table_data):
        return "—", "", ""
    row = table_data[evt.index[0]]
    return f"Selected: {row[0]}", str(row[5]), str(row[6])

# ── storage ───────────────────────────────────────────────────────────────────

def load_storage():
    s = get_storage_stats()
    text = (
        f"📦 Cache: {s['cache_used_gb']} GB / {s['cache_limit_gb']} GB ({s['cache_pct']}%)\n"
        f"💽 Disk free: {s['disk_free_gb']} GB\n"
        f"📄 Files: {s['cache_file_count']}"
    )
    stats = get_chat_file_stats()
    rows = [[r["title"] or str(r["chat_id"]), r["file_count"],
             fmt_size(r["total_size"] or 0), r["cached_count"]] for r in stats]
    return text, rows

def clear_all_action():
    return f"🗑️ Deleted {clear_all_cache()} files."

# ── recent ────────────────────────────────────────────────────────────────────

def load_recent():
    rows = recent_downloads(20)
    return [[r["file_name"], r["status"],
             r["started_at"][:16],
             r["finished_at"][:16] if r["finished_at"] else "—"] for r in rows]

# ── UI ────────────────────────────────────────────────────────────────────────

with gr.Blocks(title="TGFiles", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 📁 TGFiles — Telegram File Browser")

    with gr.Tabs():

        # Account
        with gr.TabItem("👤 Account"):
            acct_box = gr.Textbox(label="Account", interactive=False)
            with gr.Row():
                btn_me = gr.Button("Load Info")
                btn_sync = gr.Button("🔄 Sync Chats", variant="primary")
            sync_status = gr.Textbox(label="Status", interactive=False)
            # shared dropdowns updated after sync
            acct_chat_dd = gr.Dropdown(label="Chats", choices=chats_to_choices())
            btn_me.click(load_account_info, outputs=acct_box)

        # Browse
        with gr.TabItem("📂 Browse"):
            with gr.Row():
                browse_chat_dd = gr.Dropdown(label="Chat", choices=chats_to_choices(), scale=3)
                type_dd = gr.Dropdown(["all","video","audio","document","image","archive"],
                                      value="all", label="Type", scale=1)
            with gr.Row():
                btn_browse = gr.Button("Browse", variant="primary")
                scan_limit = gr.Slider(50, 1000, value=200, step=50, label="Scan limit")
                btn_scan = gr.Button("🔍 Scan")
                btn_fav = gr.Button("⭐ Fav")
            browse_status = gr.Textbox(label="", interactive=False)
            browse_table = gr.Dataframe(headers=HEADERS, interactive=False, wrap=True)
            sel_label = gr.Textbox(label="Selected", interactive=False)
            with gr.Row():
                sel_chat = gr.Textbox(label="Chat ID", interactive=False)
                sel_msg  = gr.Textbox(label="Msg ID",  interactive=False)
            btn_dl_b = gr.Button("⬇ Download", variant="primary")
            dl_status_b = gr.Textbox(label="Status", interactive=False)

            btn_browse.click(browse_chat, [browse_chat_dd, type_dd], [browse_table, browse_status])
            btn_scan.click(scan_chat_action, [browse_chat_dd, scan_limit], browse_status)
            btn_fav.click(toggle_fav_action, browse_chat_dd, [browse_status, browse_chat_dd])
            browse_table.select(select_row, browse_table, [sel_label, sel_chat, sel_msg])
            btn_dl_b.click(start_download_action, [sel_chat, sel_msg], dl_status_b)

        # Search
        with gr.TabItem("🔍 Search"):
            with gr.Row():
                sq = gr.Textbox(label="Filename", scale=3)
                st = gr.Dropdown(["all","video","audio","document","image","archive"],
                                 value="all", label="Type", scale=1)
            with gr.Row():
                smin = gr.Number(label="Min MB", value=0, precision=1)
                smax = gr.Number(label="Max MB", value=0, precision=1)
                sdf  = gr.Textbox(label="From (YYYY-MM-DD)")
                sdt  = gr.Textbox(label="To (YYYY-MM-DD)")
            sc_only = gr.Checkbox(label="Cached only")
            btn_search = gr.Button("Search", variant="primary")
            search_count = gr.Textbox(label="", interactive=False)
            search_table = gr.Dataframe(headers=HEADERS, interactive=False, wrap=True)
            ssel_label = gr.Textbox(label="Selected", interactive=False)
            with gr.Row():
                ssel_chat = gr.Textbox(label="Chat ID", interactive=False)
                ssel_msg  = gr.Textbox(label="Msg ID",  interactive=False)
            btn_dl_s = gr.Button("⬇ Download", variant="primary")
            dl_status_s = gr.Textbox(label="Status", interactive=False)

            btn_search.click(do_search, [sq,st,smin,smax,sdf,sdt,sc_only], [search_table, search_count])
            search_table.select(select_row, search_table, [ssel_label, ssel_chat, ssel_msg])
            btn_dl_s.click(start_download_action, [ssel_chat, ssel_msg], dl_status_s)

        # Manual Download
        with gr.TabItem("⬇ Download"):
            gr.Markdown("Enter Chat ID + Message ID manually, or use Browse/Search tabs.")
            with gr.Row():
                dl_chat = gr.Textbox(label="Chat ID")
                dl_msg  = gr.Textbox(label="Message ID")
            btn_dl = gr.Button("⬇ Download", variant="primary")
            dl_out = gr.Textbox(label="Status", interactive=False)
            btn_dl.click(start_download_action, [dl_chat, dl_msg], dl_out)

        # Storage
        with gr.TabItem("💾 Storage"):
            btn_ref_s = gr.Button("🔄 Refresh")
            store_info = gr.Textbox(label="Cache Info", lines=3, interactive=False)
            store_table = gr.Dataframe(headers=["Chat","Files","Size","Cached"], interactive=False)
            btn_clr = gr.Button("🗑️ Clear All", variant="stop")
            clr_status = gr.Textbox(label="", interactive=False)
            btn_ref_s.click(load_storage, outputs=[store_info, store_table])
            btn_clr.click(clear_all_action, outputs=clr_status)

        # Recent
        with gr.TabItem("🕓 Recent"):
            btn_ref_r = gr.Button("🔄 Refresh")
            recent_table = gr.Dataframe(
                headers=["File","Status","Started","Finished"], interactive=False)
            btn_ref_r.click(load_recent, outputs=recent_table)

    # wire sync to update both chat dropdowns
    btn_sync.click(sync_chats_action, outputs=[sync_status, acct_chat_dd, browse_chat_dd])
    demo.load(load_recent, outputs=recent_table)

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860, share=False)
