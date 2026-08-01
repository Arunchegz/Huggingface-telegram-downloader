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

def rows_to_html(rows, selected_chat_id=None, selected_msg_id=None, base_url=""):
    """Render file list as file-manager style table with copy-link and open buttons."""
    if not rows:
        return "<p style='color:#888'>No files found.</p>"

    TYPE_ICON = {
        "video": "🎬", "audio": "🎵", "document": "📄",
        "image": "🖼️", "archive": "🗜️",
    }

    from config import STORAGE_BUCKET_BASE as _BUCKET

    html = """
    <style>
    .fm-wrap{font-family:monospace;font-size:13px;}
    .fm-table{width:100%;border-collapse:collapse;}
    .fm-table th{background:#1e1e2e;color:#cdd6f4;padding:7px 10px;text-align:left;font-weight:600;border-bottom:2px solid #45475a;}
    .fm-table td{padding:6px 10px;border-bottom:1px solid #313244;vertical-align:middle;}
    .fm-table tr:hover td{background:#181825;}
    .fm-table tr.fm-sel td{background:#1e3a5f;}
    .fm-name{color:#cdd6f4;cursor:pointer;max-width:360px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block;}
    .fm-name:hover{color:#89b4fa;}
    .fm-size{color:#a6e3a1;white-space:nowrap;}
    .fm-date{color:#9399b2;white-space:nowrap;font-size:11px;}
    .fm-type{color:#cba6f7;text-align:center;font-size:16px;}
    .fm-cached-y{color:#a6e3a1;text-align:center;font-size:15px;}
    .fm-cached-n{color:#45475a;text-align:center;font-size:15px;}
    .fm-actions{white-space:nowrap;}
    .fm-btn{display:inline-block;padding:3px 10px;border-radius:4px;border:none;cursor:pointer;font-size:11px;font-weight:600;margin-right:4px;}
    .fm-open{background:#313244;color:#cdd6f4;}
    .fm-open:hover{background:#45475a;}
    .fm-copy{background:#1e3a5f;color:#89b4fa;}
    .fm-copy:hover{background:#2a4a7f;}
    .fm-toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:#a6e3a1;color:#1e1e2e;padding:7px 20px;border-radius:6px;font-size:13px;font-weight:700;z-index:9999;display:none;pointer-events:none;}
    </style>
    <div class='fm-wrap'>
    <div id='fm-toast' class='fm-toast'>🔗 Link copied!</div>
    <table class='fm-table'>
    <thead><tr>
      <th>Name</th><th>Type</th><th>Size</th><th>Date</th><th>●</th><th>Actions</th>
    </tr></thead><tbody>
    """

    for r in rows:
        r = enrich_file_row(r)
        chat_id = r["chat_id"]
        msg_id  = r["message_id"]
        fname   = r["file_name"].replace("'", "").replace('"', "")
        icon    = TYPE_ICON.get(r["media_type"], "📁")
        cached  = r["cached"]
        sel_cls = "fm-sel" if (str(chat_id) == str(selected_chat_id) and
                               str(msg_id)  == str(selected_msg_id)) else ""

        if _BUCKET:
            dl_url = f"{_BUCKET.rstrip('/')}/{chat_id}/{fname}"
        else:
            dl_url = f"{base_url}/tgfile/{chat_id}/{msg_id}/{fname}" if base_url else f"/tgfile/{chat_id}/{msg_id}/{fname}"

        dl_url_js = dl_url.replace("'", "\\'")
        display_name = r["file_name"][:70] + ("…" if len(r["file_name"]) > 70 else "")

        html += f"""
        <tr class='{sel_cls}' id='fm-{chat_id}-{msg_id}'>
          <td>
            <span class='fm-name' title='{r["file_name"]}' onclick="
              document.querySelectorAll('.fm-table tr').forEach(t=>t.classList.remove('fm-sel'));
              document.getElementById('fm-{chat_id}-{msg_id}').classList.add('fm-sel');
              document.getElementById('hidden_chat').value='{chat_id}';
              document.getElementById('hidden_msg').value='{msg_id}';
              document.getElementById('hidden_fname').value='{fname}';
            ">{icon} {display_name}</span>
          </td>
          <td class='fm-type'>{r["media_type"]}</td>
          <td class='fm-size'>{r["size_fmt"]}</td>
          <td class='fm-date'>{r["date_fmt"]}</td>
          <td class='{"fm-cached-y" if cached else "fm-cached-n"}'>{"●" if cached else "○"}</td>
          <td class='fm-actions'>
            <button class='fm-btn fm-open' onclick="window.open('{dl_url_js}','_blank')">⬇ Open</button>
            <button class='fm-btn fm-copy' onclick="navigator.clipboard.writeText('{dl_url_js}').then(()=>{{var t=document.getElementById('fm-toast');t.style.display='block';setTimeout(()=>t.style.display='none',1800)}})">🔗 Copy</button>
          </td>
        </tr>"""

    html += "</tbody></table></div>"
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

def browse_chat(chat_id_str, media_type_filter, request: gr.Request = None):
    if not chat_id_str:
        return "<p>Select a chat first.</p>", "—"
    try:
        rows = search_files(chat_id=int(chat_id_str), media_type=media_type_filter, limit=200)
        counts = get_type_counts(int(chat_id_str))
        summary = " | ".join(f"{k}: {v}" for k, v in counts.items() if v > 0)
        base = ""
        if request:
            base = f"{request.request.url.scheme}://{request.request.url.netloc}"
        return rows_to_html(rows, base_url=base), summary
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
    gr.HTML(JS_HIDDEN)

    with gr.Row():
        chat_dd   = gr.Dropdown(label="Chat", choices=chats_to_choices(), scale=4)
        type_dd   = gr.Dropdown(["all","video","audio","document","image","archive"],
                                 value="all", label="Type", scale=1)
        btn_sync  = gr.Button("🔄", scale=0, min_width=48)
        btn_scan  = gr.Button("⚡ Scan", scale=0, min_width=80)
        scan_limit = gr.Slider(50, 1000, value=200, step=50, label="Limit", scale=1)

    status_bar = gr.Textbox(label="", interactive=False, show_label=False, max_lines=1)
    file_html  = gr.HTML()

    def do_browse(chat_id_str, media_type, request: gr.Request = None):
        if not chat_id_str:
            return "", "<p style='color:#888'>Select a chat to browse.</p>"
        try:
            rows = search_files(chat_id=int(chat_id_str), media_type=media_type, limit=500)
            counts = get_type_counts(int(chat_id_str))
            summary = "  ".join(f"{k} {v}" for k, v in counts.items() if v > 0)
            base = ""
            if request:
                base = f"{request.request.url.scheme}://{request.request.url.netloc}"
            return f"{len(rows)} files  |  {summary}", rows_to_html(rows, base_url=base)
        except Exception as e:
            return f"❌ {e}", ""

    def do_sync(request: gr.Request = None):
        try:
            chats = run_async(fetch_chats())
            return f"✅ Synced {len(chats)} chats.", gr.update(choices=chats_to_choices())
        except Exception as e:
            return f"❌ {e}", gr.update()

    def do_scan(chat_id_str, limit):
        if not chat_id_str:
            return "Select a chat first."
        try:
            count = run_async(scan_chat(int(chat_id_str), limit=int(limit)))
            return f"✅ Indexed {count} files."
        except Exception as e:
            return f"❌ {e}"

    btn_sync.click(do_sync, outputs=[status_bar, chat_dd])
    btn_scan.click(do_scan, [chat_dd, scan_limit], status_bar)
    chat_dd.change(do_browse, [chat_dd, type_dd], [status_bar, file_html])
    type_dd.change(do_browse, [chat_dd, type_dd], [status_bar, file_html])

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
