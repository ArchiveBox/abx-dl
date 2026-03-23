from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from server.server_utils import (
    RECOVERED_SESSION_ERROR,
    get_safe_external_url,
    get_visible_log_entries,
    normalize_recovered_session_info,
    resolve_public_session_download,
)


def test_normalize_recovered_session_marks_interrupted_runs_failed() -> None:
    now = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)

    info = normalize_recovered_session_info(
        {
            "id": "demo",
            "status": "running",
            "finished_at": None,
            "error": None,
        },
        now=now,
    )

    assert info["status"] == "failed"
    assert info["error"] == RECOVERED_SESSION_ERROR
    assert info["finished_at"] == now.isoformat()


def test_normalize_recovered_session_preserves_completed_runs() -> None:
    info = normalize_recovered_session_info({"status": "completed", "finished_at": "done"})

    assert info == {"status": "completed", "finished_at": "done"}


def test_get_safe_external_url_only_allows_http_and_https() -> None:
    assert get_safe_external_url("https://example.com/path") == "https://example.com/path"
    assert get_safe_external_url("http://example.com") == "http://example.com"
    assert get_safe_external_url("javascript:alert(1)") is None
    assert get_safe_external_url("data:text/html,hello") is None


def test_get_visible_log_entries_filters_whitespace_only_logs() -> None:
    entries = get_visible_log_entries(
        {
            "abx-dl.stdout.log": "   \n\t",
            "abx-dl.stderr.log": "real output\n",
        },
    )

    assert entries == [("abx-dl.stderr.log", "real output\n")]


def test_resolve_public_session_download_rejects_traversal_and_internal_files(tmp_path: Path) -> None:
    session_root = tmp_path / "session"
    session_root.mkdir()
    public_file = session_root / "title" / "title.txt"
    public_file.parent.mkdir()
    public_file.write_text("ok")
    internal_file = session_root / "session.json"
    internal_file.write_text("{}")
    outside_dir = tmp_path / "session-escaped"
    outside_dir.mkdir()
    outside_file = outside_dir / "secret.txt"
    outside_file.write_text("nope")

    allowed_paths = {"title/title.txt"}

    assert (
        resolve_public_session_download(
            session_root,
            "title/title.txt",
            allowed_relative_paths=allowed_paths,
        )
        == public_file.resolve()
    )
    assert (
        resolve_public_session_download(
            session_root,
            "session.json",
            allowed_relative_paths=allowed_paths,
        )
        is None
    )
    assert (
        resolve_public_session_download(
            session_root,
            "../session-escaped/secret.txt",
            allowed_relative_paths=allowed_paths,
        )
        is None
    )


flask = pytest.importorskip("flask")
server_module = importlib.import_module("server.server")


def _write_session(data_dir: Path, sid: str, **overrides: object) -> dict[str, object]:
    session_root = data_dir / sid
    session_root.mkdir(parents=True, exist_ok=True)
    info: dict[str, object] = {
        "id": sid,
        "url": "https://example.com",
        "plugins": ["title"],
        "timeout": 120,
        "status": "completed",
        "created_at": "2026-03-15T12:00:00+00:00",
        "finished_at": "2026-03-15T12:01:00+00:00",
        "exit_code": 0,
        "pid": None,
        "error": None,
    }
    info.update(overrides)
    (session_root / "session.json").write_text(json.dumps(info, indent=2))
    return info


@pytest.fixture()
def server_client(tmp_path: Path):
    data_dir = tmp_path / "sessions"
    data_dir.mkdir(parents=True, exist_ok=True)
    server_module.app.config.update(TESTING=True, DATA_DIR=str(data_dir))
    with server_module.sessions_lock:
        server_module.sessions.clear()
    with server_module.app.test_client() as client:
        yield client
    with server_module.sessions_lock:
        server_module.sessions.clear()


def test_api_sessions_normalizes_restarted_runs(server_client) -> None:
    data_dir = Path(server_module.app.config["DATA_DIR"])
    _write_session(data_dir, "restartdemo", status="running", finished_at=None, error=None)

    response = server_client.get("/api/sessions")

    assert response.status_code == 200
    session = next(item for item in response.get_json() if item["id"] == "restartdemo")
    assert session["status"] == "failed"
    assert session["error"] == RECOVERED_SESSION_ERROR
    assert session["finished_at"] is not None


def test_session_page_and_download_only_expose_safe_public_outputs(server_client) -> None:
    data_dir = Path(server_module.app.config["DATA_DIR"])
    sid = "unsafe"
    _write_session(data_dir, sid, url="javascript:alert(1)")

    session_root = data_dir / sid
    public_file = session_root / "title" / "title.txt"
    public_file.parent.mkdir(parents=True, exist_ok=True)
    public_file.write_text("ok")
    (session_root / "abx-dl.stdout.log").write_text("   \n")

    sibling_root = data_dir / f"{sid}-leak"
    sibling_root.mkdir(parents=True, exist_ok=True)
    (sibling_root / "secret.txt").write_text("secret")

    page = server_client.get(f"/session/{sid}")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert 'href="javascript:alert(1)"' not in html
    assert "No log output yet." in html

    public_download = server_client.get(f"/download/{sid}/title/title.txt")
    assert public_download.status_code == 200
    assert public_download.data == b"ok"

    internal_download = server_client.get(f"/download/{sid}/abx-dl.stdout.log")
    assert internal_download.status_code == 404

    traversal_download = server_client.get(f"/download/{sid}/../{sid}-leak/secret.txt")
    assert traversal_download.status_code == 404


def test_run_download_reaps_timed_out_child(server_client, monkeypatch, tmp_path: Path) -> None:
    sid = "timeoutdemo"
    data_dir = Path(server_module.app.config["DATA_DIR"])
    session_root = data_dir / sid
    session_root.mkdir(parents=True, exist_ok=True)

    with server_module.sessions_lock:
        server_module.sessions[sid] = {
            "id": sid,
            "url": "https://example.com",
            "plugins": ["title"],
            "timeout": 1,
            "status": "starting",
            "created_at": "2026-03-15T12:00:00+00:00",
            "finished_at": None,
            "exit_code": None,
            "pid": None,
            "error": None,
        }

    class FakeProc:
        def __init__(self) -> None:
            self.pid = 4321
            self.returncode = -9
            self.kill_called = False
            self.wait_calls = 0

        def wait(self, timeout: int | float | None = None) -> int:
            self.wait_calls += 1
            if self.wait_calls == 1:
                assert timeout is not None
                raise server_module.subprocess.TimeoutExpired(cmd=["abx-dl"], timeout=timeout)
            return self.returncode

        def kill(self) -> None:
            self.kill_called = True

    fake_proc = FakeProc()
    monkeypatch.setattr(server_module.subprocess, "Popen", lambda *args, **kwargs: fake_proc)

    server_module._run_download(sid, "https://example.com", ["title"], timeout=1)

    assert fake_proc.kill_called is True
    assert fake_proc.wait_calls == 2
    with server_module.sessions_lock:
        info = dict(server_module.sessions[sid])
    assert info["status"] == "timeout"
    assert info["exit_code"] == -9
