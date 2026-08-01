import asyncio
import threading
import gradio as gr
import spaces
from pathlib import Path

from database import init_db
from search import search_files, get_type_counts, get_chat_file_stats, enrich_file_row, fmt_size
from storage import get_storage_stats, clear_all_cache
from telegram_client import fetch_chats, scan_chat, download_file, get_me, resolve_chat, auto_download_main
from database import get_chats, toggle_favorite, recent_downloads, get_file_by_msg
from config import AUTO_DOWNLOAD, CHANNEL_REF

init_db()

@spaces.GPU
def _dummy_gpu():
    pass

# ── async runner ──────────────────────────────────────────────────────────────

def run_async(coro):
    """Run a coroutine from sync context, even if an event loop is already running."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # Already inside an event loop (e.g. Gradio async worker) — run in thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    else:
        return asyncio.run(coro)

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

# ── HTML table helpers ────────────────────────────────────────────────────────

def rows_to_html(rows, selected_chat_id=None, selected_msg_id=None):
    """Render file list as HTML table with click-to-select via JS."""
    if not rows:
        return "<p style='color:#888'>No files found.</p>"

    html = """
    <style>
    .tg-table { width:100%; border-collapse:collapse; font-size:13px; }
    .tg-table th { background:#2d2d2d; color:#fff; padding:6px 8px; text-align:left; }
    .tg-table td { padding:5px 8px; border-bottom:1px solid #333; }
    .tg-table tr:hover { background:#1a1a2e; cursor:pointer; }
    .tg-table tr.selected { background:#0f3460; }
    .cached-yes { color:#4caf50; font-weight:bold; }
    .cached-no  { color:#888; }
    </style>
    <table class='tg-table'>
    <thead><tr>
      <th>File Name</th><th>Type</th><th>Size</th><th>Date</th><th>Cached</th>
      <th style='display:none'>chat_id</th><th style='display:none'>msg_id</th>
    </tr></thead><tbody>
    """
    for r in rows:
        r = enrich_file_row(r)
        cached_cls = "cached-yes" if r["cached"] else "cached-no"
        cached_sym = "✅" if r["cached"] else "⬜"
        chat_id = r["chat_id"]
        msg_id  = r["message_id"]
        sel_cls = "selected" if (str(chat_id) == str(selected_chat_id) and
                                  str(msg_id)  == str(selected_msg_id)) else ""
        html += f"""
        <tr class='{sel_cls}' onclick="
            document.getElementById('hidden_chat').value='{chat_id}';
            document.getElementById('hidden_msg').value='{msg_id}';
            document.getElementById('hidden_fname').value='{r['file_name'].replace(chr(39),'')}';
            document.querySelectorAll('.tg-table tr').forEach(t=>t.classList.remove('selected'));
            this.classList.add('selected');
        ">
          <td title='{r["file_name"]}'>{r["file_name"][:60]}{"…" if len(r["file_name"])>60 else ""}</td>
          <td>{r["media_type"]}</td>
          <td>{r["size_fmt"]}</td>
          <td>{r["date_fmt"]}</td>
          <td class='{cached_cls}'>{cached_sym}</td>
        </tr>"""
    html += "</tbody></table>"
    return html


def recent_to_html(rows):
    if not rows:
        return "<p style='color:#888'>No recent downloads.</p>"
    html = """
    <style>
    .tg-table{width:100%;border-collapse:collapse;font-size:13px;}
    .tg-table th{background:#2d2d2d;color:#fff;padding:6px 8px;text-align:left;}
    .tg-table td{padding:5px 8px;border-bottom:1px solid #333;}
    </style>
    <table class='tg-table'><thead><tr>
    <th>File</th><th>Status</th><th>Started</th><th>Finished</th>
    </tr></thead><tbody>"""
    for r in rows:
        html += f"<tr><td>{r['file_name']}</td><td>{r['status']}</td><td>{r['started_at'][:16]}</td><td>{r['finished_at'][:16] if r['finished_at'] else '—'}</td></tr>"
    html += "</tbody></table>"
    return html


def storage_to_html(stats):
    if not stats:
        return "<p style='color:#888'>No data.</p>"
    html = """
    <style>
    .tg-table{width:100%;border-collapse:collapse;font-size:13px;}
    .tg-table th{background:#2d2d2d;color:#fff;padding:6px 8px;text-align:left;}
    .tg-table td{padding:5px 8px;border-bottom:1px solid #333;}
    </style>
    <table class='tg-table'><thead><tr>
    <th>Chat</th><th>Files</th><th>Size</th><th>Cached</th>
    </tr></thead><tbody>"""
    for r in stats:
        html += f"<tr><td>{r['title'] or r['chat_id']}</td><td>{r['file_count']}</td><td>{fmt_size(r['total_size'] or 0)}</td><td>{r['cached_count']}</td></tr>"
    html += "</tbody></table>"
    return html

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
        upd = gr.update(choices=choices)
        return f"✅ Synced {len(chats)} chats.", upd, upd
    except Exception as e:
        return f"❌ {e}", gr.update(), gr.update()

def chats_to_choices():
    rows = get_chats()
    return [(r["title"], str(r["id"])) for r in rows]

# ── browse ────────────────────────────────────────────────────────────────────

def browse_chat(chat_id_str, media_type_filter):
    if not chat_id_str:
        return "<p>Select a chat first.</p>", "—"
    try:
        rows = search_files(chat_id=int(chat_id_str), media_type=media_type_filter, limit=200)
        counts = get_type_counts(int(chat_id_str))
        summary = " | ".join(f"{k}: {v}" for k, v in counts.items() if v > 0)
        return rows_to_html(rows), summary
    except Exception as e:
        return f"<p>❌ {e}</p>", "—"

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
    return "⭐ Toggled.", gr.update(choices=chats_to_choices())

# ── search ────────────────────────────────────────────────────────────────────

def do_search(query, media_type, min_mb, max_mb, date_from, date_to, cached_only):
    rows = search_files(
        query=query, media_type=media_type,
        min_size_mb=float(min_mb or 0), max_size_mb=float(max_mb or 0),
        date_from=date_from or None, date_to=date_to or None,
        downloaded_only=cached_only, limit=200,
    )
    return rows_to_html(rows), f"{len(rows)} results"

# ── download ──────────────────────────────────────────────────────────────────

def start_download_action(chat_id_str, msg_id_str):
    if not chat_id_str or not msg_id_str:
        return "Enter Chat ID and Message ID."
    try:
        chat_id, msg_id = int(chat_id_str.strip()), int(msg_id_str.strip())
        row = get_file_by_msg(chat_id, msg_id)
        if not row:
            return "❌ File not indexed. Scan chat first."
        result = {}
        async def _dl():
            path = await download_file(chat_id, msg_id, row["file_name"])
            result["path"] = str(path) if path else None
        run_async(_dl())
        if result.get("path"):
            return f"✅ Done: {Path(result['path']).name}"
        return "❌ Download failed."
    except Exception as e:
        return f"❌ {e}"

def resolve_chat_action(ref):
    if not ref or not ref.strip():
        return "Enter @username, invite link, or chat ID.", gr.update(), gr.update()
    try:
        chat = run_async(resolve_chat(ref.strip()))
        return (
            f"✅ Resolved: {chat['title']} → ID {chat['id']}",
            str(chat["id"]),
            gr.update(choices=chats_to_choices()),
        )
    except Exception as e:
        return f"❌ {e}", "", gr.update()

# ── storage ───────────────────────────────────────────────────────────────────

def load_storage():
    s = get_storage_stats()
    text = (f"📦 Cache: {s['cache_used_gb']} GB / {s['cache_limit_gb']} GB ({s['cache_pct']}%)\n"
            f"💽 Disk free: {s['disk_free_gb']} GB  |  📄 Files: {s['cache_file_count']}")
    return text, storage_to_html(get_chat_file_stats())

def clear_all_action():
    return f"🗑️ Deleted {clear_all_cache()} files."

# ── recent ────────────────────────────────────────────────────────────────────

def load_recent():
    return recent_to_html(recent_downloads(20))

# ── UI ────────────────────────────────────────────────────────────────────────

JS_HIDDEN = """
<input type='text' id='hidden_chat' style='display:none'>
<input type='text' id='hidden_msg'  style='display:none'>
<input type='text' id='hidden_fname' style='display:none'>
"""

with gr.Blocks(title="TGFiles", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 📁 TGFiles — Telegram File Browser")
    gr.HTML(JS_HIDDEN)

    with gr.Tabs():

        # ── Account ──────────────────────────────────────────────────────────
        with gr.TabItem("👤 Account"):
            acct_box = gr.Textbox(label="Account", interactive=False)
            with gr.Row():
                btn_me   = gr.Button("Load Info")
                btn_sync = gr.Button("🔄 Sync Chats", variant="primary")
            sync_status = gr.Textbox(label="Status", interactive=False)
            btn_me.click(load_account_info, outputs=acct_box)

        # ── Browse ────────────────────────────────────────────────────────────
        with gr.TabItem("📂 Browse"):
            with gr.Row():
                browse_chat_dd = gr.Dropdown(label="Chat", choices=chats_to_choices(), scale=3)
                type_dd = gr.Dropdown(["all","video","audio","document","image","archive"],
                                      value="all", label="Type", scale=1)
            with gr.Row():
                btn_browse = gr.Button("Browse", variant="primary")
                scan_limit = gr.Slider(50, 1000, value=200, step=50, label="Scan limit")
                btn_scan   = gr.Button("🔍 Scan")
                btn_fav    = gr.Button("⭐ Fav")
            browse_status = gr.Textbox(label="", interactive=False)
            browse_html   = gr.HTML()
            gr.Markdown("**Selected file** — paste Chat ID + Msg ID from table row, then download:")
            with gr.Row():
                sel_chat = gr.Textbox(label="Chat ID")
                sel_msg  = gr.Textbox(label="Msg ID")
            btn_dl_b    = gr.Button("⬇ Download", variant="primary")
            dl_status_b = gr.Textbox(label="Status", interactive=False)

            btn_browse.click(browse_chat, [browse_chat_dd, type_dd], [browse_html, browse_status])
            btn_scan.click(scan_chat_action, [browse_chat_dd, scan_limit], browse_status)
            btn_fav.click(toggle_fav_action, browse_chat_dd, [browse_status, browse_chat_dd])
            btn_dl_b.click(start_download_action, [sel_chat, sel_msg], dl_status_b)

        # ── Search ────────────────────────────────────────────────────────────
        with gr.TabItem("🔍 Search"):
            with gr.Row():
                search_chat_dd = gr.Dropdown(label="Chat (optional)", choices=[("All", "")] + chats_to_choices(), value="", scale=2)
                sq = gr.Textbox(label="Filename", scale=3)
                st = gr.Dropdown(["all","video","audio","document","image","archive"],
                                 value="all", label="Type", scale=1)
            with gr.Row():
                smin = gr.Number(label="Min MB", value=0, precision=1)
                smax = gr.Number(label="Max MB", value=0, precision=1)
                sdf  = gr.Textbox(label="From (YYYY-MM-DD)")
                sdt  = gr.Textbox(label="To (YYYY-MM-DD)")
            sc_only      = gr.Checkbox(label="Cached only")
            btn_search   = gr.Button("Search", variant="primary")
            search_count = gr.Textbox(label="", interactive=False)
            search_html  = gr.HTML()
            with gr.Row():
                ssel_chat = gr.Textbox(label="Chat ID")
                ssel_msg  = gr.Textbox(label="Msg ID")
            btn_dl_s    = gr.Button("⬇ Download", variant="primary")
            dl_status_s = gr.Textbox(label="Status", interactive=False)

            btn_search.click(do_search, [sq,st,smin,smax,sdf,sdt,sc_only], [search_html, search_count])
            btn_dl_s.click(start_download_action, [ssel_chat, ssel_msg], dl_status_s)

        # ── Manual Download ───────────────────────────────────────────────────
        with gr.TabItem("⬇ Download"):
            gr.Markdown("Enter Chat ID + Message ID from Browse/Search.")
            with gr.Row():
                ch_name = gr.Textbox(label="Channel (@username / invite link / ID)", scale=3)
                btn_res = gr.Button("🔎 Load Channel")
            ch_status = gr.Textbox(label="", interactive=False)
            with gr.Row():
                dl_chat = gr.Textbox(label="Chat ID")
                dl_msg  = gr.Textbox(label="Message ID")
            btn_dl = gr.Button("⬇ Download", variant="primary")
            dl_out = gr.Textbox(label="Status", interactive=False)
            btn_dl.click(start_download_action, [dl_chat, dl_msg], dl_out)
            btn_res.click(resolve_chat_action, ch_name, [ch_status, dl_chat, browse_chat_dd])

        # ── Storage ───────────────────────────────────────────────────────────
        with gr.TabItem("💾 Storage"):
            btn_ref_s  = gr.Button("🔄 Refresh")
            store_info = gr.Textbox(label="Cache Info", lines=2, interactive=False)
            store_html = gr.HTML()
            btn_clr    = gr.Button("🗑️ Clear All", variant="stop")
            clr_status = gr.Textbox(label="", interactive=False)
            btn_ref_s.click(load_storage, outputs=[store_info, store_html])
            btn_clr.click(clear_all_action, outputs=clr_status)

        # ── Recent ────────────────────────────────────────────────────────────
        with gr.TabItem("🕓 Recent"):
            btn_ref_r    = gr.Button("🔄 Refresh")
            recent_html  = gr.HTML()
            btn_ref_r.click(load_recent, outputs=recent_html)

    # sync updates both chat dropdowns
    def _sync_with_search():
        msg, upd_browse, _ = sync_chats_action()
        choices_with_all = [("All", "")] + chats_to_choices()
        return msg, upd_browse, gr.update(choices=choices_with_all)

    btn_sync.click(_sync_with_search, outputs=[sync_status, browse_chat_dd, search_chat_dd])
    demo.load(load_recent, outputs=recent_html)

if __name__ == "__main__":
    import os
    import threading
    from stremio_addon import add_routes

    port = int(os.environ.get("PORT", "7860"))

    # demo.launch() must run for the @spaces.GPU startup check.
    # prevent_thread_lock keeps serving in background; addon routes go
    # on gradio's FastAPI app; main thread parks forever.
    demo.launch(server_name="0.0.0.0", server_port=port, share=True,
                prevent_thread_lock=True)
    add_routes(demo.app)
    threading.Event().wait()
