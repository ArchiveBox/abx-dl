#!/usr/bin/env python3
"""
abx-dl Web Download Server

A simple web server that lets users submit URLs for downloading via abx-dl.
Each request gets a unique session directory. Results are cleaned up after 24 hours.

Usage:
    python server.py [--host 0.0.0.0] [--port 8484] [--data-dir /tmp/abx-dl-sessions]

REST API:
    POST /api/download        - Submit a new download (JSON body: {"url": "...", "plugins": ["wget","title"]})
    GET  /api/sessions        - List all active sessions
    GET  /api/session/<id>    - Get session status, progress, and file listing
    GET  /download/<id>/<path> - Download an individual result file
    GET  /download/<id>/zip   - Download entire session as a zip archive

Web UI:
    GET  /                    - Homepage with submission form and API docs
    GET  /session/<id>        - Session detail page with live progress
"""

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from typing import Any

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

# ---------------------------------------------------------------------------
# App configuration
# ---------------------------------------------------------------------------

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8484
DEFAULT_DATA_DIR = os.path.join(tempfile.gettempdir(), "abx-dl-sessions")
SESSION_TTL_HOURS = 24
DEFAULT_TIMEOUT = 120

# Plugins safe to expose publicly (extraction/snapshot plugins only, not system-level ones)
ALLOWED_PLUGINS = [
    "title",
    "favicon",
    "screenshot",
    "pdf",
    "dom",
    "wget",
    "singlefile",
    "readability",
    "headers",
    "hashes",
    "git",
    "ytdlp",
    "gallerydl",
    "forumdl",
    "mercury",
    "htmltotext",
    "trafilatura",
    "defuddle",
    "media",
    "archivedotorg",
    "dns",
    "ssl",
    "seo",
    "accessibility",
    "redirects",
    "responses",
]

DEFAULT_PLUGINS = ["title", "favicon", "screenshot", "pdf", "dom", "wget", "singlefile", "readability"]

app = Flask(__name__, template_folder="templates")

# In-memory session store  {session_id: SessionInfo}
sessions: dict[str, dict[str, Any]] = {}
sessions_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_data_dir() -> Path:
    return Path(app.config.get("DATA_DIR", DEFAULT_DATA_DIR))


def new_session_id() -> str:
    return uuid.uuid4().hex[:12]


def session_dir(session_id: str) -> Path:
    return get_data_dir() / session_id


def create_session(url: str, plugins: list[str], timeout: int = DEFAULT_TIMEOUT) -> dict[str, Any]:
    sid = new_session_id()
    sdir = session_dir(sid)
    sdir.mkdir(parents=True, exist_ok=True)

    info: dict[str, Any] = {
        "id": sid,
        "url": url,
        "plugins": plugins,
        "timeout": timeout,
        "status": "starting",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "exit_code": None,
        "pid": None,
        "error": None,
    }

    with sessions_lock:
        sessions[sid] = info

    # Persist session metadata
    (sdir / "session.json").write_text(json.dumps(info, indent=2))

    # Spawn abx-dl in background
    thread = threading.Thread(target=_run_download, args=(sid, url, plugins, timeout), daemon=True)
    thread.start()

    return info


def _run_download(sid: str, url: str, plugins: list[str], timeout: int) -> None:
    sdir = session_dir(sid)
    cmd = ["abx-dl", "dl", "--output", str(sdir)]
    if plugins:
        cmd += ["--plugins", ",".join(plugins)]
    cmd += ["--timeout", str(timeout)]
    cmd += [url]

    stdout_path = sdir / "abx-dl.stdout.log"
    stderr_path = sdir / "abx-dl.stderr.log"

    try:
        with open(stdout_path, "w") as stdout_f, open(stderr_path, "w") as stderr_f:
            proc = subprocess.Popen(
                cmd,
                stdout=stdout_f,
                stderr=stderr_f,
                cwd=str(sdir),
            )
            with sessions_lock:
                sessions[sid]["status"] = "running"
                sessions[sid]["pid"] = proc.pid
            _persist_session(sid)

            proc.wait(timeout=timeout + 30)

            with sessions_lock:
                sessions[sid]["exit_code"] = proc.returncode
                sessions[sid]["status"] = "completed" if proc.returncode == 0 else "failed"
                sessions[sid]["finished_at"] = datetime.now(timezone.utc).isoformat()
            _persist_session(sid)

    except subprocess.TimeoutExpired:
        proc.kill()
        with sessions_lock:
            sessions[sid]["status"] = "timeout"
            sessions[sid]["error"] = f"Process timed out after {timeout + 30}s"
            sessions[sid]["finished_at"] = datetime.now(timezone.utc).isoformat()
        _persist_session(sid)

    except Exception as e:
        with sessions_lock:
            sessions[sid]["status"] = "failed"
            sessions[sid]["error"] = str(e)
            sessions[sid]["finished_at"] = datetime.now(timezone.utc).isoformat()
        _persist_session(sid)


def _persist_session(sid: str) -> None:
    sdir = session_dir(sid)
    with sessions_lock:
        info = dict(sessions.get(sid, {}))
    if info and sdir.exists():
        (sdir / "session.json").write_text(json.dumps(info, indent=2))


def get_session_info(sid: str) -> dict[str, Any] | None:
    with sessions_lock:
        info = sessions.get(sid)
    if info:
        return dict(info)

    # Try loading from disk (server restart recovery)
    meta_path = session_dir(sid) / "session.json"
    if meta_path.exists():
        info = json.loads(meta_path.read_text())
        # If it was "running" but we restarted, mark as failed
        if info.get("status") in ("starting", "running"):
            info["status"] = "failed"
            info["error"] = "Server restarted while download was in progress"
            info["finished_at"] = datetime.now(timezone.utc).isoformat()
        with sessions_lock:
            sessions[sid] = info
        return dict(info)

    return None


def list_session_files(sid: str) -> list[dict[str, Any]]:
    """List downloadable result files for a session (excludes internal logs)."""
    sdir = session_dir(sid)
    if not sdir.exists():
        return []

    skip_names = {"session.json", "abx-dl.stdout.log", "abx-dl.stderr.log"}
    files = []
    for p in sorted(sdir.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(sdir)
        # Skip internal files at root level
        if str(rel) in skip_names:
            continue
        # Skip hook internal files
        if rel.name.endswith((".pid", ".sh", ".meta.json", ".stdout.log", ".stderr.log")):
            continue
        files.append({
            "path": str(rel),
            "name": rel.name,
            "size": p.stat().st_size,
            "plugin": str(rel.parts[0]) if len(rel.parts) > 1 else None,
        })
    return files


def get_session_logs(sid: str) -> dict[str, str]:
    sdir = session_dir(sid)
    logs = {}
    for name in ("abx-dl.stdout.log", "abx-dl.stderr.log"):
        path = sdir / name
        if path.exists():
            logs[name] = path.read_text(errors="replace")
    return logs


def get_session_jsonl(sid: str) -> list[dict[str, Any]]:
    """Parse index.jsonl for structured progress info."""
    path = session_dir(sid) / "index.jsonl"
    if not path.exists():
        return []
    records = []
    for line in path.read_text(errors="replace").strip().splitlines():
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


def cleanup_expired_sessions() -> None:
    """Remove sessions older than SESSION_TTL_HOURS."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SESSION_TTL_HOURS)

    to_remove = []
    with sessions_lock:
        for sid, info in list(sessions.items()):
            created = info.get("created_at", "")
            try:
                created_dt = datetime.fromisoformat(created)
                if created_dt < cutoff:
                    to_remove.append(sid)
            except (ValueError, TypeError):
                continue

    # Also check filesystem for sessions not in memory
    data_dir = get_data_dir()
    if data_dir.exists():
        for entry in data_dir.iterdir():
            if not entry.is_dir():
                continue
            meta_path = entry / "session.json"
            if meta_path.exists():
                try:
                    info = json.loads(meta_path.read_text())
                    created_dt = datetime.fromisoformat(info.get("created_at", ""))
                    if created_dt < cutoff and entry.name not in to_remove:
                        to_remove.append(entry.name)
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue

    for sid in to_remove:
        sdir = session_dir(sid)
        if sdir.exists():
            shutil.rmtree(sdir, ignore_errors=True)
        with sessions_lock:
            sessions.pop(sid, None)


def _cleanup_loop() -> None:
    """Background thread that runs cleanup every hour."""
    while True:
        time.sleep(3600)
        try:
            cleanup_expired_sessions()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Web UI routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template(
        "index.html",
        allowed_plugins=ALLOWED_PLUGINS,
        default_plugins=DEFAULT_PLUGINS,
        default_timeout=DEFAULT_TIMEOUT,
    )


@app.route("/session/<sid>")
def session_page(sid: str):
    info = get_session_info(sid)
    if not info:
        abort(404)
    files = list_session_files(sid)
    logs = get_session_logs(sid)
    records = get_session_jsonl(sid)
    return render_template(
        "session.html",
        session=info,
        files=files,
        logs=logs,
        records=records,
        format_size=format_size,
    )


@app.route("/submit", methods=["POST"])
def submit_form():
    """Handle form submission from the web UI."""
    url = request.form.get("url", "").strip()
    if not url:
        return redirect(url_for("index"))

    plugins = request.form.getlist("plugins")
    # Validate plugins
    plugins = [p for p in plugins if p in ALLOWED_PLUGINS]
    if not plugins:
        plugins = list(DEFAULT_PLUGINS)

    timeout = DEFAULT_TIMEOUT
    try:
        timeout = int(request.form.get("timeout", DEFAULT_TIMEOUT))
        timeout = max(10, min(600, timeout))
    except (ValueError, TypeError):
        pass

    info = create_session(url, plugins, timeout)
    return redirect(url_for("session_page", sid=info["id"]))


# ---------------------------------------------------------------------------
# REST API routes
# ---------------------------------------------------------------------------

@app.route("/api/download", methods=["POST"])
def api_download():
    """
    Submit a new download.

    Request JSON:
        {
            "url": "https://example.com",
            "plugins": ["title", "wget", "screenshot"],  // optional, defaults to common set
            "timeout": 120  // optional, seconds, default 120, max 600
        }

    Response JSON:
        {
            "id": "a1b2c3d4e5f6",
            "url": "https://example.com",
            "plugins": ["title", "wget", "screenshot"],
            "status": "starting",
            "session_url": "/session/a1b2c3d4e5f6",
            "api_url": "/api/session/a1b2c3d4e5f6"
        }
    """
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "Missing required field: url"}), 400

    plugins = data.get("plugins", list(DEFAULT_PLUGINS))
    if not isinstance(plugins, list):
        return jsonify({"error": "plugins must be a list of strings"}), 400
    plugins = [p for p in plugins if isinstance(p, str) and p in ALLOWED_PLUGINS]
    if not plugins:
        plugins = list(DEFAULT_PLUGINS)

    timeout = DEFAULT_TIMEOUT
    try:
        timeout = int(data.get("timeout", DEFAULT_TIMEOUT))
        timeout = max(10, min(600, timeout))
    except (ValueError, TypeError):
        pass

    info = create_session(url, plugins, timeout)
    return jsonify({
        "id": info["id"],
        "url": info["url"],
        "plugins": info["plugins"],
        "status": info["status"],
        "session_url": f"/session/{info['id']}",
        "api_url": f"/api/session/{info['id']}",
    }), 201


@app.route("/api/sessions")
def api_sessions():
    """List all active sessions."""
    # Merge in-memory and on-disk sessions
    all_sessions = {}
    with sessions_lock:
        for sid, info in sessions.items():
            all_sessions[sid] = dict(info)

    data_dir = get_data_dir()
    if data_dir.exists():
        for entry in sorted(data_dir.iterdir(), reverse=True):
            if entry.is_dir() and entry.name not in all_sessions:
                meta = entry / "session.json"
                if meta.exists():
                    try:
                        all_sessions[entry.name] = json.loads(meta.read_text())
                    except json.JSONDecodeError:
                        continue

    result = sorted(all_sessions.values(), key=lambda s: s.get("created_at", ""), reverse=True)
    return jsonify(result)


@app.route("/api/session/<sid>")
def api_session(sid: str):
    """
    Get session details including status, files, and progress records.

    Response JSON:
        {
            "id": "...",
            "url": "...",
            "plugins": [...],
            "status": "running" | "completed" | "failed" | "timeout",
            "created_at": "...",
            "finished_at": "..." | null,
            "files": [{"path": "title/title.txt", "name": "title.txt", "size": 42, "plugin": "title"}, ...],
            "records": [...]  // parsed index.jsonl entries
        }
    """
    info = get_session_info(sid)
    if not info:
        return jsonify({"error": "Session not found"}), 404

    files = list_session_files(sid)
    records = get_session_jsonl(sid)

    return jsonify({
        **info,
        "files": files,
        "records": records,
    })


@app.route("/download/<sid>/<path:filepath>")
def download_file(sid: str, filepath: str):
    """Download an individual result file."""
    info = get_session_info(sid)
    if not info:
        abort(404)

    sdir = session_dir(sid)
    target = (sdir / filepath).resolve()

    # Prevent path traversal
    if not str(target).startswith(str(sdir.resolve())):
        abort(403)

    if not target.is_file():
        abort(404)

    return send_file(target, as_attachment=True)


@app.route("/download/<sid>/zip")
def download_zip(sid: str):
    """Download the entire session output as a zip archive."""
    info = get_session_info(sid)
    if not info:
        abort(404)

    files = list_session_files(sid)
    if not files:
        abort(404)

    sdir = session_dir(sid)
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            fpath = sdir / f["path"]
            if fpath.is_file():
                zf.write(fpath, f["path"])
    buf.seek(0)

    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"abx-dl-{sid}.zip",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="abx-dl Web Download Server")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Bind host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Bind port (default: {DEFAULT_PORT})")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help=f"Session data directory (default: {DEFAULT_DATA_DIR})")
    args = parser.parse_args()

    app.config["DATA_DIR"] = args.data_dir
    Path(args.data_dir).mkdir(parents=True, exist_ok=True)

    # Run initial cleanup
    cleanup_expired_sessions()

    # Start background cleanup thread
    cleanup_thread = threading.Thread(target=_cleanup_loop, daemon=True)
    cleanup_thread.start()

    print(f"abx-dl Web Download Server")
    print(f"  Listening on http://{args.host}:{args.port}")
    print(f"  Data directory: {args.data_dir}")
    print(f"  Sessions expire after {SESSION_TTL_HOURS} hours")
    print()

    app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
