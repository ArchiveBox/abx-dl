import asyncio
import importlib.metadata
import io
import json
import os
import pty
import re
import select
import signal
import termios
import shutil
import subprocess
import sys
import time
import psutil
import pytest
from datetime import UTC, datetime
from pathlib import Path

import abx_dl.cli as cli_module
from abxpkg.binary_service import BinaryRequestEvent, BinaryService
from abx_dl.cli import _build_archive_results_table, _compact_output, _format_archive_result_line, _format_elapsed
from abx_dl.cli import cli as cli_group
from abx_dl.events import (
    ArchiveResultEvent,
    CrawlSetupEvent,
    ProcessCompletedEvent,
    ProcessEvent,
    ProcessStartedEvent,
    ProcessStdoutEvent,
    SnapshotEvent,
)
from abx_dl.limits import parse_filesize_to_bytes
from abx_dl.catalog import PluginCatalog
from abx_dl.models import ArchiveResult
from abx_dl.orchestrator import create_bus
from abx_dl.output_files import OutputFile
from rich.console import Console
from rich.progress import Progress

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("choice", ["skip", "retry", "abort", "ctrl-c", "noninteractive"])
def test_cli_interrupts_active_hook(tmp_path: Path, choice: str) -> None:
    master, slave = pty.openpty()
    output = bytearray()
    output_dir = tmp_path / "capture"
    env = _cli_env(tmp_path)
    # A real TTY with TERM=dumb still supports input. Rich suppresses live
    # frames there, so this also protects the plain-text prompt fallback.
    env.update(CHROME_DELAY_AFTER_LOAD="60", CHROME_TIMEOUT="120", CHROME_HEADLESS="True", TERM="dumb")
    process = subprocess.Popen(
        [sys.executable, "-m", "abx_dl", "dl", "--plugins=chrome", "--dir", str(output_dir), "https://example.com"],
        cwd=tmp_path,
        env=env,
        stdin=subprocess.DEVNULL if choice == "noninteractive" else slave,
        stdout=slave,
        stderr=slave,
        start_new_session=True,
    )

    def read_until(predicate, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if select.select([master], [], [], 0.1)[0]:
                output.extend(os.read(master, 65536))
            if predicate():
                return
        raise AssertionError(output.decode(errors="replace"))

    def active_hook():
        for pid_file in output_dir.rglob("on_Snapshot__30_chrome_navigate.*.pid"):
            pid_text = pid_file.read_text().strip()
            if pid_text and psutil.pid_exists(int(pid_text)):
                return int(pid_text)
        return None

    try:
        read_until(lambda: active_hook() is not None)
        pid = active_hook()
        assert pid is not None
        process.send_signal(signal.SIGINT)
        if choice != "noninteractive":
            read_until(lambda: b"Choice [skip]:" in output)
            read_until(lambda: not termios.tcgetattr(slave)[3] & termios.ICANON)
            assert not psutil.pid_exists(pid)
            if choice == "retry":
                os.write(master, b"r")
                read_until(lambda: (next_pid := active_hook()) is not None and next_pid != pid)
                process.send_signal(signal.SIGINT)
                read_until(lambda: output.count(b"Choice [skip]:") == 2)
                read_until(lambda: not termios.tcgetattr(slave)[3] & termios.ICANON)
            if choice == "ctrl-c":
                process.send_signal(signal.SIGINT)
            else:
                os.write(master, b"a" if choice == "abort" else b"\r")
        read_until(lambda: process.poll() is not None)
        assert process.returncode == (1 if choice in {"abort", "ctrl-c", "noninteractive"} else 0), output.decode(errors="replace")
        assert not psutil.pid_exists(pid)
        if choice == "noninteractive":
            assert b"Choice [skip]:" not in output
        assert b"Traceback" not in output
        records = [json.loads(line) for path in output_dir.rglob("index.jsonl") for line in path.read_text().splitlines() if line.strip()]
        interrupted = [
            record for record in records if record.get("type") == "Process" and record.get("hook_name") == "on_Snapshot__30_chrome_navigate"
        ]
        assert len(interrupted) == (2 if choice == "retry" else 1)
        assert all(record["exit_code"] == 130 and record["stderr"] == "Hook interrupted by user" for record in interrupted)
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=15)
        os.close(slave)
        os.close(master)


@pytest.mark.parametrize("phase", ["startup", "running"])
@pytest.mark.parametrize("choice", ["skip", "retry", "ctrl-c", "noninteractive"])
def test_cli_interrupts_background_only_capture(tmp_path: Path, choice: str, phase: str) -> None:
    """The outer CLI must prompt even when no foreground hook owns a wait loop."""
    master, slave = pty.openpty()
    termios.tcsetwinsize(slave, (40, 200))
    output = bytearray()
    output_dir = tmp_path / "capture"
    process = subprocess.Popen(
        [sys.executable, "-m", "abx_dl", "dl", "--plugins=forumdl", "--dir", str(output_dir), "https://news.ycombinator.com"],
        cwd=tmp_path,
        env=_cli_env(tmp_path),
        stdin=subprocess.DEVNULL if choice == "noninteractive" else slave,
        stdout=slave,
        stderr=slave,
        start_new_session=True,
    )

    def read_until(predicate, timeout=45):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if select.select([master], [], [], 0.1)[0]:
                output.extend(os.read(master, 65536))
            if predicate():
                return
        raise AssertionError(output.decode(errors="replace"))

    def active_hook():
        for path in output_dir.rglob("on_Snapshot__33_forumdl.*.pid"):
            value = path.read_text().strip()
            if value and psutil.pid_exists(int(value)):
                return int(value)
        return None

    try:
        read_until(lambda: active_hook() is not None)
        pid = active_hook()
        assert pid is not None
        children = psutil.Process(process.pid).children(recursive=True)
        if phase == "running":
            read_until(lambda: any("INFO:root:GET" in log.read_text() for log in output_dir.rglob("on_Snapshot__33_forumdl.*.stderr.log")))
        process.send_signal(signal.SIGINT)
        if choice == "noninteractive":
            read_until(lambda: process.poll() is not None, timeout=25)
            assert process.returncode == 1
            assert b"Choice [skip]:" not in output
            assert not psutil.pid_exists(pid)
            assert not [child.pid for child in children if child.is_running() and child.status() != psutil.STATUS_ZOMBIE]
            return
        read_until(lambda: b"Choice [skip]:" in output, timeout=20)
        read_until(lambda: not termios.tcgetattr(slave)[3] & termios.ICANON)
        assert not psutil.pid_exists(pid)
        if choice == "retry":
            os.write(master, b"r")
            read_until(lambda: (new_pid := active_hook()) is not None and new_pid != pid)
            children.extend(psutil.Process(process.pid).children(recursive=True))
            process.send_signal(signal.SIGINT)
            read_until(lambda: output.count(b"Choice [skip]:") == 2, timeout=20)
            read_until(lambda: not termios.tcgetattr(slave)[3] & termios.ICANON)
        os.write(master, b"\r" if choice == "skip" else b"\x03")
        read_until(lambda: process.poll() is not None, timeout=25)
        assert process.returncode == (0 if choice == "skip" else 1), output.decode(errors="replace")
        assert b"Traceback" not in output
        assert not [child.pid for child in children if child.is_running() and child.status() != psutil.STATUS_ZOMBIE]
        records = [json.loads(line) for path in output_dir.rglob("index.jsonl") for line in path.read_text().splitlines() if line.strip()]
        interrupted = [record for record in records if record.get("type") == "Process" and record.get("plugin") == "forumdl"]
        assert len(interrupted) == (2 if choice == "retry" else 1)
        assert all(record["exit_code"] == 130 for record in interrupted)
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=20)
        os.close(slave)
        os.close(master)


def test_cli_abort_stops_long_running_background_hook(tmp_path: Path) -> None:
    master, slave = pty.openpty()
    termios.tcsetwinsize(slave, (40, 200))
    output = bytearray()
    output_dir = tmp_path / "capture"
    env = _cli_env(tmp_path)
    env.update(INFINISCROLL_SCROLL_DELAY="10000", CHROME_HEADLESS="True", TERM="xterm-256color", COLUMNS="200")
    process = subprocess.Popen(
        [sys.executable, "-m", "abx_dl", "dl", "--plugins=forumdl,infiniscroll", "--dir", str(output_dir), "https://news.ycombinator.com"],
        cwd=tmp_path,
        env=env,
        stdin=slave,
        stdout=slave,
        stderr=slave,
        start_new_session=True,
    )

    def read_until(predicate, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if select.select([master], [], [], 0.1)[0]:
                output.extend(os.read(master, 65536))
            if predicate():
                return
        raise AssertionError(output.decode(errors="replace"))

    def active_hook():
        for pid_file in output_dir.rglob("on_Snapshot__45_infiniscroll.*.pid"):
            pid_text = pid_file.read_text().strip()
            if pid_text and psutil.pid_exists(int(pid_text)):
                return int(pid_text)
        return None

    try:
        read_until(lambda: active_hook() is not None)
        pid = active_hook()
        assert pid is not None
        read_until(lambda: "running pid=" in re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", output.decode(errors="replace")))
        process.send_signal(signal.SIGINT)
        read_until(lambda: b"Choice [skip]:" in output)
        read_until(lambda: not termios.tcgetattr(slave)[3] & termios.ICANON)
        children = psutil.Process(process.pid).children(recursive=True)
        assert any("forumdl" in " ".join(child.cmdline()) for child in children)
        abort_offset = len(output)
        os.write(master, b"\x03")
        read_until(lambda: b"Aborting crawl" in output[abort_offset:], timeout=3)
        read_until(lambda: process.poll() is not None, timeout=25)
        assert not [child.pid for child in children if child.is_running() and child.status() != psutil.STATUS_ZOMBIE]
        assert process.returncode == 1, output.decode(errors="replace")
        assert not psutil.pid_exists(pid)
        assert any("INFO:root:GET" in log.read_text() for log in output_dir.rglob("on_Snapshot__33_forumdl.*.stderr.log"))
        assert b"Stopped during crawl abort" in output
        assert b"INFO:root:GET" not in output[abort_offset:]
        assert b"Traceback" not in output
        records = [json.loads(line) for path in output_dir.rglob("index.jsonl") for line in path.read_text().splitlines() if line.strip()]
        interrupted = [
            record for record in records if record.get("type") == "Process" and record.get("hook_name") == "on_Snapshot__45_infiniscroll"
        ]
        assert len(interrupted) == 1
        assert all(record["exit_code"] == 130 and record["stderr"] == "Hook interrupted by user" for record in interrupted)
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=15)
        os.close(slave)
        os.close(master)


@pytest.mark.parametrize("choice", ["typed", "signal"])
def test_cli_third_interrupt_forces_aborted_crawl_to_exit(tmp_path: Path, choice: str) -> None:
    """After abort was chosen, another Ctrl+C cannot wait for hook grace periods."""
    master, slave = pty.openpty()
    termios.tcsetwinsize(slave, (40, 200))
    output = bytearray()
    output_dir = tmp_path / "capture"
    env = _cli_env(tmp_path)
    env.update(INFINISCROLL_SCROLL_DELAY="10000", CHROME_HEADLESS="True", TERM="dumb")
    process = subprocess.Popen(
        [sys.executable, "-m", "abx_dl", "dl", "--plugins=forumdl,infiniscroll", "--dir", str(output_dir), "https://news.ycombinator.com"],
        cwd=tmp_path,
        env=env,
        stdin=slave,
        stdout=slave,
        stderr=slave,
        start_new_session=True,
    )

    def read_until(predicate, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if select.select([master], [], [], 0.1)[0]:
                output.extend(os.read(master, 65536))
            if predicate():
                return
        raise AssertionError(output.decode(errors="replace"))

    try:
        read_until(lambda: any(path.read_text().strip() for path in output_dir.rglob("on_Snapshot__45_infiniscroll.*.pid")))
        owned = psutil.Process(process.pid).children(recursive=True)
        process.send_signal(signal.SIGINT)
        read_until(lambda: b"Choice [skip]:" in output)
        read_until(lambda: not termios.tcgetattr(slave)[3] & termios.ICANON)
        if choice == "typed":
            os.write(master, b"a")
        else:
            process.send_signal(signal.SIGINT)
        read_until(lambda: b"Aborting crawl" in output, timeout=5)
        forced_at = time.monotonic()
        process.send_signal(signal.SIGINT)
        read_until(lambda: process.poll() is not None, timeout=5)
        assert time.monotonic() - forced_at < 2.0
        assert process.returncode == 130, output.decode(errors="replace")
        assert not [child.pid for child in owned if child.is_running() and child.status() != psutil.STATUS_ZOMBIE]
        assert b"Traceback" not in output
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=15)
        os.close(slave)
        os.close(master)


ABX_ENV_KEYS = {
    "CHECK_SSL_VALIDITY",
    "CONFIG_DIR",
    "COOKIES_FILE",
    "CRAWL_DIR",
    "DATA_DIR",
    "ABXPKG_LIB_DIR",
    "PERSONAS_DIR",
    "SNAP_DIR",
    "TIMEOUT",
    "TMP_DIR",
    "USER_AGENT",
}
for plugin in PluginCatalog.discover().values():
    ABX_ENV_KEYS.update(plugin.config.properties.keys())


def _cli_env(tmp_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    for key in ABX_ENV_KEYS:
        env.pop(key, None)
    config_dir = tmp_path / "config"
    pythonpath_entries = [str(REPO_ROOT)]
    for sibling in ("abx-plugins", "abxpkg"):
        sibling_path = REPO_ROOT.parent / sibling
        if sibling_path.exists():
            pythonpath_entries.append(str(sibling_path))
    if env.get("PYTHONPATH"):
        pythonpath_entries.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)
    env["CONFIG_DIR"] = str(config_dir)
    env["ABXPKG_LIB_DIR"] = os.environ.get("ABXPKG_LIB_DIR", str(config_dir / "lib"))
    env["PERSONAS_DIR"] = str(config_dir / "personas")
    env["DATA_DIR"] = str(tmp_path / "data")
    env["TMP_DIR"] = str(tmp_path / "tmp")
    env["HOME"] = str(tmp_path / "home")
    env["CHROME_SANDBOX"] = "false"
    path_entries = [entry for entry in os.environ["PATH"].split(os.pathsep) if entry]
    for common_dir in (
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/opt/homebrew/opt/node/bin",
    ):
        if common_dir not in path_entries:
            path_entries.insert(0, common_dir)
    env["PATH"] = os.pathsep.join(path_entries)
    return env


def _run_cli(tmp_path: Path, *args: str, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    cwd = tmp_path / "cwd"
    cwd.mkdir(parents=True, exist_ok=True)
    try:
        return subprocess.run(
            [sys.executable, "-m", "abx_dl", *args],
            cwd=cwd,
            env=_cli_env(tmp_path),
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as err:
        diagnostics = [
            f"CLI timed out after {timeout}s: {err.cmd}",
            f"partial stdout:\n{err.stdout or ''}",
            f"partial stderr:\n{err.stderr or ''}",
        ]
        artifact_suffixes = (
            ".pid",
            ".sh",
            ".stdout.log",
            ".stderr.log",
            "index.jsonl",
        )
        for artifact in sorted(tmp_path.rglob("*")):
            if not artifact.is_file() or not artifact.name.endswith(artifact_suffixes):
                continue
            try:
                contents = artifact.read_text(errors="replace")[-16_000:]
            except OSError as artifact_err:
                contents = f"<{type(artifact_err).__name__}: {artifact_err}>"
            diagnostics.append(f"{artifact.relative_to(tmp_path)}:\n{contents}")
            if artifact.suffix != ".pid":
                continue
            try:
                pid = int(artifact.read_text().strip())
            except (OSError, ValueError):
                continue
            proc_dir = Path("/proc") / str(pid)
            for proc_name in ("cmdline", "wchan", "status"):
                proc_path = proc_dir / proc_name
                try:
                    proc_contents = proc_path.read_text(errors="replace")[-8_000:]
                except OSError as proc_err:
                    proc_contents = f"<{type(proc_err).__name__}: {proc_err}>"
                diagnostics.append(f"{proc_path}:\n{proc_contents}")
        raise AssertionError("\n\n".join(diagnostics)) from err


def _hook_names(plugin_name: str, event_name: str) -> list[str]:
    plugin = PluginCatalog.discover()[plugin_name]
    return [hook.name for hook in sorted(plugin.hooks, key=lambda hook: hook.sort_key) if event_name in hook.name]


def _real_hook_path(plugin_name: str, hook_name: str) -> str:
    plugin = PluginCatalog.discover()[plugin_name]
    hook = next(hook for hook in plugin.hooks if hook.name == hook_name)
    assert hook.path.is_file()
    return str(hook.path)


async def _completed_real_hook_process() -> asyncio.subprocess.Process:
    process = await asyncio.create_subprocess_exec(
        _real_hook_path("parse_txt_urls", "on_Snapshot__71_parse_txt_urls"),
        "--url=https://example.com",
        env=os.environ.copy(),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    assert await process.wait() == 0
    return process


def test_compact_output_collapses_whitespace_and_truncates() -> None:
    assert _compact_output("line one\n\nline two\tline three", limit=20) == "line one line two..."


def test_format_install_output_flattens_and_strips_double_quotes() -> None:
    output = cli_module._format_install_output('{"status": "ok"}\n"value"')
    assert output.plain == "{status: ok} value"


def test_format_table_output_strips_double_quotes_without_flattening() -> None:
    output = cli_module._format_table_output('{"status": "ok"}\n"value"', flatten=False)
    assert output.plain == "{status: ok}\nvalue"


def test_format_table_output_humanizes_binary_records() -> None:
    output = cli_module._format_table_output(
        '{"type": "Binary","name":"forum-dl","abspath":"/tmp/forum-dl","binproviders":"env,brew,apt","machine_id":"ignored"}',
        flatten=True,
    )
    assert output.plain == "{type: Binary,name:forum-dl,abspath:/tmp/forum-dl,binproviders:env,brew,apt,machine_id:ignored}"


def test_latest_active_hook_name_prefers_most_recent_still_running_hook() -> None:
    live_results = {
        "row-1": cli_module._LiveProcessRecord(id="row-1", plugin="wget", hook_name="install", timeout=60),
        "row-2": cli_module._LiveProcessRecord(
            id="row-2",
            plugin="chrome",
            hook_name="on_CrawlSetup__90_chrome_launch.daemon.bg",
            timeout=60,
        ),
    }
    assert cli_module._latest_active_hook_name(["row-1", "row-2"], live_results) == "on_CrawlSetup__90_chrome_launch.daemon.bg"
    assert cli_module._latest_active_hook_name(["row-1"], live_results) == "install"
    assert cli_module._latest_active_hook_name([], live_results) is None


def test_render_record_output_compacts_live_process_output() -> None:
    record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="wget",
        hook_name="install",
        timeout=60,
        output="line one\nline two " + ("x" * 200),
    )
    rendered = cli_module._render_record_output(record)
    assert "\n" not in rendered
    assert rendered.endswith("...")


def test_render_record_output_flattens_archive_result_output() -> None:
    record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="headers",
        hook_name="on_Snapshot__80_headers",
        timeout=60,
        output="line one\nline two",
        final_output="line one\nline two",
        final_output_is_archive_result=True,
    )
    assert cli_module._render_record_output(record) == "line one line two"


def test_build_archive_results_table_shows_output_size_column() -> None:
    record = ArchiveResult(
        snapshot_id="snap-1",
        plugin="wget",
        hook_name="on_Snapshot__35_wget.finite.bg",
        status="succeeded",
        output_str="wget/example.com/index.html",
        output_files=[
            OutputFile(path="wget/example.com/index.html", size=1536),
            OutputFile(path="wget/example.com/favicon.ico", size=512),
        ],
        start_ts="2026-03-25T12:00:00",
        end_ts="2026-03-25T12:00:02",
    )

    buffer = io.StringIO()
    rendered_table = _build_archive_results_table([record], timeout_seconds=60, show_header=True, stream=True)
    Console(file=buffer, force_terminal=False, color_system=None, width=160).print(rendered_table)
    rendered = buffer.getvalue()

    assert "Size" in rendered
    assert "2KB" in rendered


def test_render_output_size_cell_uses_expected_threshold_styles() -> None:
    assert cli_module._render_output_size_cell(99 * 1024).style == cli_module.SIZE_GREEN_STYLE
    assert cli_module._render_output_size_cell(100 * 1024).style == cli_module.SIZE_GREEN_STYLE
    assert cli_module._render_output_size_cell(2 * 1024 * 1024).style == cli_module.SIZE_YELLOW_STYLE
    assert cli_module._render_output_size_cell(10 * 1024 * 1024).style == cli_module.SIZE_YELLOW_STYLE
    assert cli_module._render_output_size_cell(50 * 1024 * 1024).style == cli_module.SIZE_ORANGE_STYLE
    assert cli_module._render_output_size_cell(100 * 1024 * 1024).style == cli_module.SIZE_RED_STYLE
    assert cli_module._render_output_size_cell(1024 * 1024 * 1024).style == cli_module.SIZE_FLASHING_STYLE


def test_format_output_size_rounds_kb_and_promotes_near_1mb() -> None:
    assert cli_module._format_output_size(2048) == "2KB"
    assert cli_module._format_output_size(int(1002.1 * 1024)) == "1MB"


def test_process_completed_uses_last_non_json_line_for_live_output() -> None:
    bus = create_bus(total_timeout=10.0, name="process_completed_last_line")
    output = io.StringIO()
    live_ui = cli_module.LiveBusUI(
        bus,
        total_hooks=1,
        timeout_seconds=60,
        ui_console=Console(file=output, force_terminal=False, color_system=None),
        interactive_tty=True,
    )
    event = ProcessCompletedEvent(
        plugin_name="infiniscroll",
        hook_name="on_Snapshot__45_infiniscroll",
        hook_path=_real_hook_path("infiniscroll", "on_Snapshot__45_infiniscroll"),
        hook_args=[],
        env={},
        timeout=60,
        stdout='{"type":"ArchiveResult","status":"succeeded"}\n',
        stderr="Starting infinite scroll on https://yahoo.com\nClicked 16 load more buttons\n",
        exit_code=1,
        status="failed",
        output_dir="/tmp",
        output_files=[],
        start_ts="2026-03-25T12:00:00",
        end_ts="2026-03-25T12:00:01",
    )

    asyncio.run(live_ui.on_ProcessCompletedEvent(event))

    assert not live_ui.live_results
    rendered = output.getvalue()
    assert "Clicked 16 load more b" in rendered
    assert "Starting infinite scroll on https://yahoo.com" not in rendered


def test_noninteractive_live_ui_streams_process_lifecycle() -> None:
    bus = create_bus(total_timeout=10.0, name="noninteractive_process_lifecycle")
    output = io.StringIO()
    cli_module.LiveBusUI(
        bus,
        total_hooks=1,
        timeout_seconds=60,
        ui_console=Console(file=output, force_terminal=False, color_system=None),
        interactive_tty=False,
    )

    async def run() -> None:
        process = await _completed_real_hook_process()
        started_event = ProcessStartedEvent(
            plugin_name="parse_txt_urls",
            hook_name="on_Snapshot__71_parse_txt_urls",
            hook_path=_real_hook_path("parse_txt_urls", "on_Snapshot__71_parse_txt_urls"),
            hook_args=["--url=https://example.com"],
            output_dir="/tmp",
            env={},
            timeout=60,
            pid=process.pid or 0,
            subprocess=process,
            stdout_file=Path("/tmp/noninteractive_process_lifecycle.stdout.log"),
            stderr_file=Path("/tmp/noninteractive_process_lifecycle.stderr.log"),
            pid_file=Path("/tmp/noninteractive_process_lifecycle.pid"),
            cmd_file=Path("/tmp/noninteractive_process_lifecycle.sh"),
            files_before=set(),
            start_ts="2026-03-25T12:00:00",
        )
        completed_event = ProcessCompletedEvent(
            plugin_name=started_event.plugin_name,
            hook_name=started_event.hook_name,
            hook_path=started_event.hook_path,
            hook_args=started_event.hook_args,
            env={},
            timeout=60,
            stdout='{"type":"ArchiveResult","status":"succeeded","output_str":"1 URL"}\n',
            stderr="",
            exit_code=0,
            status="succeeded",
            output_dir="/tmp",
            output_files=[],
            start_ts=started_event.start_ts,
            end_ts="2026-03-25T12:00:01",
            event_parent_id=started_event.event_id,
        )
        await bus.emit(started_event).now()
        await bus.emit(completed_event).now()
        await bus.wait_until_idle()
        await bus.destroy(clear=False)

    asyncio.run(run())

    rendered = output.getvalue()
    assert "STARTED" in rendered
    assert "parse_txt_urls" in rendered
    assert "on_Snapshot__71_parse_txt_urls" in rendered
    assert "succeeded" in rendered


def test_noninteractive_intro_and_summary_preserve_literal_brackets() -> None:
    bus = create_bus(total_timeout=10.0, name="noninteractive_literal_brackets")
    output = io.StringIO()
    live_ui = cli_module.LiveBusUI(
        bus,
        total_hooks=0,
        timeout_seconds=60,
        ui_console=Console(file=output, force_terminal=False, color_system=None, width=200),
        interactive_tty=False,
    )
    # Non-TTY output is consumed as plain logs. Rich markup-like text can be
    # supplied by both the URL and output path, so every bracket must survive.
    output_dir = Path("/tmp/[bold]archive[/bold]")
    live_ui.print_intro(
        url="https://example.com/[red]literal[/red]",
        output_dir=output_dir,
        plugins_label="title",
    )
    live_ui.print_summary(output_dir=output_dir, archive_results=[])

    rendered = output.getvalue()
    assert "[STARTED] https://example.com/[red]literal[/red] -> /tmp/[bold]archive[/bold]" in rendered
    assert "[COMPLETED] 0 succeeded, 0 noresult, 0 failed, 0 skipped -> /tmp/[bold]archive[/bold]" in rendered


def test_process_completed_success_ignores_stderr_for_live_output() -> None:
    bus = create_bus(total_timeout=10.0, name="process_completed_success_stderr")
    output = io.StringIO()
    live_ui = cli_module.LiveBusUI(
        bus,
        total_hooks=1,
        timeout_seconds=60,
        ui_console=Console(file=output, force_terminal=False, color_system=None),
        interactive_tty=True,
    )
    event = ProcessCompletedEvent(
        plugin_name="chrome",
        hook_name="on_CrawlSetup__90_chrome_launch.daemon.bg",
        hook_path=_real_hook_path("chrome", "on_CrawlSetup__90_chrome_launch.daemon.bg"),
        hook_args=[],
        env={},
        timeout=360,
        stdout='{"type":"ArchiveResult","status":"succeeded","output_str":"chrome is running"}\n',
        stderr="[chromium:stderr] noisy warning\n",
        exit_code=0,
        status="succeeded",
        output_dir="/tmp",
        output_files=[],
        start_ts="2026-03-25T12:00:00",
        end_ts="2026-03-25T12:02:34",
    )

    asyncio.run(live_ui.on_ProcessCompletedEvent(event))

    rendered = output.getvalue()
    assert "chrome is running" in rendered
    assert "noisy warning" not in rendered


def test_process_completed_success_uses_stderr_when_stdout_is_empty() -> None:
    bus = create_bus(total_timeout=10.0, name="process_completed_success_empty_stdout")
    output = io.StringIO()
    live_ui = cli_module.LiveBusUI(
        bus,
        total_hooks=1,
        timeout_seconds=60,
        ui_console=Console(file=output, force_terminal=False, color_system=None),
        interactive_tty=True,
    )
    event = ProcessCompletedEvent(
        plugin_name="twocaptcha",
        hook_name="on_CrawlSetup__95_twocaptcha_config",
        hook_path=_real_hook_path("twocaptcha", "on_CrawlSetup__95_twocaptcha_config"),
        hook_args=[],
        env={},
        timeout=60,
        stdout="",
        stderr="2captcha already configured\n",
        exit_code=0,
        status="succeeded",
        output_dir="/tmp",
        output_files=[],
        start_ts="2026-03-25T12:00:00",
        end_ts="2026-03-25T12:00:01",
    )

    asyncio.run(live_ui.on_ProcessCompletedEvent(event))

    rendered = output.getvalue()
    assert "2captcha already confi" in rendered


def test_process_completed_records_output_file_sizes_for_live_output() -> None:
    bus = create_bus(total_timeout=10.0, name="process_completed_output_files")
    output = io.StringIO()
    live_ui = cli_module.LiveBusUI(
        bus,
        total_hooks=1,
        timeout_seconds=60,
        ui_console=Console(file=output, force_terminal=False, color_system=None),
        interactive_tty=True,
    )
    event = ProcessCompletedEvent(
        plugin_name="headers",
        hook_name="on_Snapshot__27_headers.daemon.bg",
        hook_path=_real_hook_path("headers", "on_Snapshot__27_headers.daemon.bg"),
        hook_args=[],
        env={},
        timeout=60,
        stdout='{"type":"ArchiveResult","status":"succeeded","output_str":"headers/headers.json"}\n',
        stderr="",
        exit_code=0,
        status="succeeded",
        output_dir="/tmp",
        output_files=[OutputFile(path="headers.json", size=2048)],
        start_ts="2026-03-25T12:00:00",
        end_ts="2026-03-25T12:00:01",
    )

    asyncio.run(live_ui.on_ProcessCompletedEvent(event))

    rendered = output.getvalue()
    assert "2KB" in rendered


def test_process_completed_preserves_output_files_when_inline_archive_result_has_none() -> None:
    bus = create_bus(total_timeout=10.0, name="process_completed_preserve_output_files")
    output = io.StringIO()
    live_ui = cli_module.LiveBusUI(
        bus,
        total_hooks=1,
        timeout_seconds=60,
        ui_console=Console(file=output, force_terminal=False, color_system=None),
        interactive_tty=True,
    )

    async def run() -> None:
        process = await _completed_real_hook_process()
        started_event = ProcessStartedEvent(
            plugin_name="pdf",
            hook_name="on_Snapshot__52_pdf",
            hook_path=_real_hook_path("pdf", "on_Snapshot__52_pdf"),
            hook_args=[],
            output_dir="/tmp",
            env={},
            timeout=60,
            pid=process.pid or 0,
            subprocess=process,
            stdout_file=Path("/tmp/process_completed_preserve_output_files.stdout.log"),
            stderr_file=Path("/tmp/process_completed_preserve_output_files.stderr.log"),
            pid_file=Path("/tmp/process_completed_preserve_output_files.pid"),
            cmd_file=Path("/tmp/process_completed_preserve_output_files.sh"),
            files_before=set(),
            start_ts="2026-03-25T12:00:00",
        )
        archive_result_event = ArchiveResultEvent(
            snapshot_id="snap-1",
            plugin="pdf",
            hook_name="on_Snapshot__52_pdf",
            status="succeeded",
            output_str="pdf/output.pdf",
            output_files=[],
            event_parent_id=started_event.event_id,
        )
        completed_event = ProcessCompletedEvent(
            plugin_name="pdf",
            hook_name="on_Snapshot__52_pdf",
            hook_path=_real_hook_path("pdf", "on_Snapshot__52_pdf"),
            hook_args=[],
            env={},
            timeout=60,
            stdout='{"type":"ArchiveResult","status":"succeeded","output_str":"pdf/output.pdf"}\n',
            stderr="",
            exit_code=0,
            status="succeeded",
            output_dir="/tmp",
            output_files=[OutputFile(path="output.pdf", size=4096)],
            start_ts="2026-03-25T12:00:00",
            end_ts="2026-03-25T12:00:01",
            event_parent_id=started_event.event_id,
        )
        await bus.emit(started_event).now()
        await bus.emit(archive_result_event).now()
        await bus.emit(completed_event).now()

    asyncio.run(run())

    assert not live_ui.live_results
    rendered = output.getvalue()
    assert "4KB" in rendered


def test_process_stdout_updates_live_row_with_last_non_json_line() -> None:
    bus = create_bus(total_timeout=10.0, name="process_stdout_live_row")
    live_ui = cli_module.LiveBusUI(
        bus,
        total_hooks=1,
        timeout_seconds=60,
        ui_console=Console(file=io.StringIO(), force_terminal=False, color_system=None),
        interactive_tty=True,
    )

    async def run() -> None:
        process = await _completed_real_hook_process()
        started_event = ProcessStartedEvent(
            plugin_name="chrome",
            hook_name="on_CrawlSetup__90_chrome_launch.daemon.bg",
            hook_path=_real_hook_path("chrome", "on_CrawlSetup__90_chrome_launch.daemon.bg"),
            hook_args=[],
            output_dir="/tmp",
            env={},
            timeout=360,
            pid=process.pid or 0,
            is_background=True,
            subprocess=process,
            stdout_file=Path("/tmp/process_stdout_live_row.stdout.log"),
            stderr_file=Path("/tmp/process_stdout_live_row.stderr.log"),
            pid_file=Path("/tmp/process_stdout_live_row.pid"),
            cmd_file=Path("/tmp/process_stdout_live_row.sh"),
            files_before=set(),
            start_ts="2026-03-25T12:00:00",
        )
        stdout_event = ProcessStdoutEvent(
            line="[*] Chromium launch hook staying alive to handle cleanup...",
            plugin_name=started_event.plugin_name,
            hook_name=started_event.hook_name,
            output_dir=started_event.output_dir,
            event_parent_id=started_event.event_id,
        )
        await live_ui.on_ProcessStartedEvent(started_event)
        await live_ui.on_ProcessStdoutEvent(stdout_event)

    asyncio.run(run())

    row = live_ui.live_results["process:1"]
    assert isinstance(row, cli_module._LiveProcessRecord)
    assert row.output == "[*] Chromium launch hook staying alive to handle cleanup..."


def test_parse_filesize_to_bytes_accepts_human_units() -> None:
    assert parse_filesize_to_bytes("45mb") == 45 * 1024 * 1024
    assert parse_filesize_to_bytes("2GB") == 2 * 1024 * 1024 * 1024
    assert parse_filesize_to_bytes("123") == 123


def test_normalize_archive_result_output_relativizes_absolute_path(tmp_path: Path) -> None:
    cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        output_path = tmp_path / "example.com" / "index.html"
        assert cli_module._normalize_archive_result_output(str(output_path)) == "example.com/index.html"
    finally:
        os.chdir(cwd)


def test_render_record_output_relativizes_live_archive_result_absolute_path(tmp_path: Path) -> None:
    cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        output_path = tmp_path / "example.com" / "index.html"
        record = cli_module._LiveProcessRecord(
            id="proc-1",
            plugin="wget",
            hook_name="on_Snapshot__35_wget.finite.bg",
            timeout=60,
            output=str(output_path),
            final_output=str(output_path),
            final_output_is_archive_result=True,
        )
        assert cli_module._render_record_output(record) == "example.com/index.html"
    finally:
        os.chdir(cwd)


def test_phase_label_for_event_uses_ancestor_phase_event() -> None:
    bus = create_bus(total_timeout=10.0, name="phase_label_ancestor")
    phase_event = CrawlSetupEvent(url="https://example.com", snapshot_id="snap", output_dir="/tmp")
    process_event = ProcessEvent(
        plugin_name="wget",
        hook_name="install",
        hook_path=_real_hook_path("wget", "on_Snapshot__35_wget.finite.bg"),
        hook_args=["wget"],
        is_background=False,
        output_dir="/tmp",
        env={},
        event_parent_id=phase_event.event_id,
    )

    async def run() -> None:
        await bus.emit(phase_event).now()

    asyncio.run(run())
    assert cli_module._phase_label_for_event(bus, process_event) == "CrawlSetup"


def test_phase_label_for_event_walks_nested_event_ancestors() -> None:
    bus = create_bus(total_timeout=10.0, name="phase_label_nested")
    phase_event = SnapshotEvent(url="https://example.com", snapshot_id="snap", output_dir="/tmp")
    provider_process = ProcessEvent(
        plugin_name="chrome",
        hook_name="on_CrawlSetup__90_chrome_launch",
        hook_path=_real_hook_path("chrome", "on_CrawlSetup__90_chrome_launch.daemon.bg"),
        hook_args=["chromium"],
        is_background=False,
        output_dir="/tmp",
        env={},
        event_parent_id=phase_event.event_id,
    )

    async def run() -> None:
        await bus.emit(phase_event).now()

    asyncio.run(run())
    assert cli_module._phase_label_for_event(bus, provider_process) == "Snapshot"


def test_render_record_output_cell_highlights_compacted_live_output() -> None:
    record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="wget",
        hook_name="install",
        timeout=60,
        output='{"status": "ok"}\n"value"',
    )
    assert cli_module._render_record_output_cell(record).plain == "{status: ok} value"


def test_render_record_output_uses_exit_code_for_failed_empty_live_row() -> None:
    record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="wget",
        hook_name="install",
        timeout=60,
        status="failed",
        exit_code=1,
    )
    assert cli_module._render_record_output(record) == "exit=1"


@pytest.mark.parametrize("status", ["started", "succeeded", "failed", "skipped", "noresult"])
def test_render_record_output_is_always_single_line(status: str) -> None:
    record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="wget",
        hook_name="install",
        timeout=60,
        status=status,
        output="line one\nline two " + ("x" * 200),
    )
    assert cli_module._render_record_output(record) == "line one line two " + "x" * 99 + "..."
    cell = cli_module._render_record_output_cell(record)
    assert "\n" not in cell.plain
    assert cell.no_wrap and cell.overflow == "ellipsis"

    # No status may bypass the invariant, and narrow terminals must ellipsize
    # rather than wrap. Assert physical rendered lines, not only the cell text.
    output = io.StringIO()
    Console(file=output, width=100, color_system=None).print(
        _build_archive_results_table([record], timeout_seconds=60, stream=True, show_header=False),
    )
    assert len(output.getvalue().splitlines()) == 1


def test_binary_record_display_output_prefers_abspath_then_version() -> None:
    record = cli_module._BinaryRecord(
        name="wget",
        abspath="/opt/homebrew/bin/wget",
        version="1.25.0",
        plugin="brew",
        hook_name="-",
        status="installed",
    )
    assert record.display_output == "/opt/homebrew/bin/wget 1.25.0"


def test_format_archive_result_line_includes_requested_fields() -> None:
    result = ArchiveResult(
        snapshot_id="snap",
        plugin="chrome",
        hook_name="on_Snapshot__01_chrome_tab.bg",
        status="failed",
        output_str="",
        error="No Chrome session found",
    )
    line = _format_archive_result_line(result)
    assert "ArchiveResult" in line
    assert "on_Snapshot__01_chrome_tab.bg" in line
    assert "failed" in line
    assert "No Chrome session found" in line


def test_format_elapsed_uses_running_or_completed_timestamps() -> None:
    now = datetime(2026, 3, 11, 12, 0, 15, tzinfo=UTC)
    assert _format_elapsed("2026-03-11T12:00:00", None, 60, now=now) == "15.0s/60s"
    assert _format_elapsed("2026-03-11T12:00:00", "2026-03-11T12:00:05", 60, now=now) == "5.0s/60s"


def test_format_elapsed_accepts_mixed_timezone_awareness() -> None:
    assert _format_elapsed("2026-03-11T12:00:00+00:00", "2026-03-11T12:00:05", 60) == "5.0s/60s"
    assert _format_elapsed("2026-03-11T12:00:00", "2026-03-11T12:00:05+00:00", 60) == "5.0s/60s"


def test_advance_progress_expands_total_for_extra_completed_hooks() -> None:
    progress = Progress(console=Console(file=io.StringIO(), force_terminal=False, color_system=None))
    task_id = progress.add_task("Running plugins...", total=1)
    cli_module._advance_progress(progress, task_id, "hook one")
    cli_module._advance_progress(progress, task_id, "binary provider")
    task = progress.tasks[task_id]
    assert task.total == 2
    assert task.completed == 2


def test_build_archive_results_table_includes_elapsed_column() -> None:
    result = ArchiveResult(
        snapshot_id="snap",
        plugin="chrome",
        hook_name="on_Snapshot__01_chrome_tab.bg",
        status="started",
        start_ts="2026-03-11T12:00:00",
    )
    table = _build_archive_results_table([result], timeout_seconds=60, now=datetime(2026, 3, 11, 12, 0, 5, tzinfo=UTC))
    assert [column.header for column in table.columns] == ["Currently Running", "Phase", "Status", "Size", "Elapsed", "Output"]
    assert [str(cell) for cell in table.columns[3]._cells] == ["-"]
    assert table.columns[4]._cells == ["5.0s/60s"]


def test_record_status_style_uses_darker_started_color_for_background_hooks() -> None:
    bg_record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="chrome",
        hook_name="on_Snapshot__00_chrome_launch.daemon.bg",
        timeout=60,
        status="started",
    )
    fg_record = cli_module._LiveProcessRecord(
        id="proc-2",
        plugin="wget",
        hook_name="on_Snapshot__35_wget",
        timeout=60,
        status="started",
    )

    assert cli_module._record_status_style(bg_record) == cli_module.BG_STARTED_STYLE
    assert cli_module._record_status_style(fg_record) == "yellow"


@pytest.mark.parametrize(
    ("status", "expected_style"),
    [("cancelled", "red"), ("noresult", "dim"), ("noresults", "dim"), ("skipped", "dim")],
)
def test_archive_result_status_colors_match_cli_palette(status: str, expected_style: str) -> None:
    result = ArchiveResult(snapshot_id="snap", plugin="wget", hook_name="on_Snapshot__35_wget", status=status)

    table = _build_archive_results_table([result], timeout_seconds=60, stream=True)
    cells = table.columns[1]._cells
    assert len(cells) == 1
    cell = cells[0]
    assert isinstance(cell, cli_module.Text)
    assert (cell.plain, cell.style) == (status, expected_style)
    assert cli_module._record_muted_style(result) == ("dim" if status in {"noresult", "noresults", "skipped"} else None)
    output = io.StringIO()
    Console(file=output, force_terminal=True, color_system="standard", no_color=False, width=140).print(table)
    assert f"\x1b[{31 if expected_style == 'red' else 2}m{status}" in output.getvalue()


def test_live_ui_closes_fast_real_binary_requests_without_stale_started_rows(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=30.0, name="real_binary_ui_lifecycle")
    BinaryService(bus, auto_install=False)
    output = io.StringIO()
    live_ui = cli_module.LiveBusUI(
        bus,
        total_hooks=2,
        timeout_seconds=30,
        ui_console=Console(file=output, force_terminal=False, color_system=None),
        interactive_tty=False,
    )

    async def run() -> None:
        for _ in range(2):
            request = bus.emit(
                BinaryRequestEvent(
                    name=sys.executable,
                    binproviders="env",
                    auto_install=False,
                    lib_dir=tmp_path / "lib",
                    no_cache=True,
                ),
            )
            await request.now(timeout=30)
            await request.wait(timeout=30)
        await bus.wait_until_idle()

    asyncio.run(run())

    assert live_ui.live_results == {}
    assert live_ui.active_row_keys == []
    assert output.getvalue().count("succeeded") == 2
    assert "[STARTED] Install" not in output.getvalue()


def test_live_ui_tracks_concurrent_real_binary_requests_with_same_name(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=30.0, name="concurrent_binary_ui_lifecycle")
    output = io.StringIO()
    live_ui = cli_module.LiveBusUI(
        bus,
        total_hooks=2,
        timeout_seconds=30,
        ui_console=Console(file=output, force_terminal=False, color_system=None),
        interactive_tty=False,
    )
    BinaryService(bus, auto_install=False)

    async def run() -> None:
        requests = [
            bus.emit(
                BinaryRequestEvent(
                    name=sys.executable,
                    binproviders="env",
                    auto_install=False,
                    lib_dir=tmp_path / "lib",
                    no_cache=True,
                ),
            )
            for _ in range(2)
        ]
        await asyncio.gather(*(request.now(timeout=30) for request in requests))
        await asyncio.gather(*(request.wait(timeout=30) for request in requests))
        await bus.wait_until_idle()

    asyncio.run(run())

    assert live_ui.live_results == {}
    assert live_ui.active_row_keys == []
    assert not live_ui.pending_binary_rows
    assert output.getvalue().count("succeeded") == 2
    assert output.getvalue().count("[STARTED] Install") == 2


def test_live_ui_marks_unresolved_real_binary_request_failed(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=10.0, name="missing_binary_ui_lifecycle")
    BinaryService(bus, auto_install=False)
    output = io.StringIO()
    live_ui = cli_module.LiveBusUI(
        bus,
        total_hooks=1,
        timeout_seconds=10,
        ui_console=Console(file=output, force_terminal=False, color_system=None),
        interactive_tty=False,
    )

    async def run() -> None:
        request = bus.emit(
            BinaryRequestEvent(
                name=str(tmp_path / "missing-real-binary"),
                binproviders="env",
                auto_install=False,
                lib_dir=tmp_path / "lib",
                no_cache=True,
            ),
        )
        await request.now(timeout=10)
        await request.wait(timeout=10)
        await bus.wait_until_idle()
        await asyncio.sleep(0)

    asyncio.run(run())

    assert live_ui.live_results == {}
    assert live_ui.active_row_keys == []
    assert "failed" in output.getvalue()


def test_default_group_routes_bare_url_and_top_level_dl_options() -> None:
    assert cli_group._should_default_to_dl(["https://example.com"]) is True
    assert cli_group._should_default_to_dl(["--debug", "https://example.com"]) is True
    assert cli_group._should_default_to_dl(["--plugins=wget", "https://example.com"]) is True
    assert cli_group._should_default_to_dl(["--timeout=120", "https://example.com"]) is True
    assert cli_group._should_default_to_dl(["plugins", "wget"]) is False
    assert cli_group._should_default_to_dl(["example.com"]) is False
    assert cli_group._should_default_to_dl(["nonsense"]) is False
    assert cli_group._should_default_to_dl(["--help"]) is False


def test_default_group_leaves_unknown_non_url_as_subcommand_error(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "nonsense")
    assert result.returncode != 0
    assert "No such command" in result.stderr or "No such command" in result.stdout
    assert '"url": "nonsense"' not in result.stdout


def test_dl_refuses_to_write_crawl_output_into_source_checkout_root(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "abx_dl",
            "dl",
            "--plugins=wget",
            "https://example.com",
        ],
        cwd=REPO_ROOT,
        env=_cli_env(tmp_path),
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )

    assert result.returncode != 0
    assert "Refusing to write crawl output into the abx-dl source checkout root" in result.stderr
    assert not (REPO_ROOT / "index.jsonl").exists()
    assert not (REPO_ROOT / "wget").exists()

    for metadata_args in (("--help",), ("--version",), ("plugins", "wget")):
        metadata_result = subprocess.run(
            [sys.executable, "-m", "abx_dl", *metadata_args],
            cwd=REPO_ROOT,
            env=_cli_env(tmp_path),
            text=True,
            capture_output=True,
            timeout=180,
            check=False,
        )
        assert metadata_result.returncode == 0

    output_dir = tmp_path / "outside-checkout"
    download_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "abx_dl",
            "dl",
            "--plugins=wget",
            f"--dir={output_dir}",
            "https://example.com",
        ],
        cwd=REPO_ROOT,
        env=_cli_env(tmp_path),
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )
    assert download_result.returncode == 0
    assert (output_dir / "index.jsonl").is_file()
    assert "Example Domain" in (output_dir / "wget" / "example.com" / "index.html").read_text()


def test_help_aliases_match_top_level_help(tmp_path: Path) -> None:
    help_result = _run_cli(tmp_path, "--help")
    command_help_result = _run_cli(tmp_path, "help")
    short_help_result = _run_cli(tmp_path, "-h")
    assert help_result.returncode == 0
    assert command_help_result.returncode == 0
    assert short_help_result.returncode == 0
    assert "Usage:" in command_help_result.stdout
    assert "Usage:" in short_help_result.stdout
    assert "Commands" in command_help_result.stdout
    assert "Commands" in short_help_result.stdout


def test_version_outputs_only_raw_version(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "--version")
    assert result.returncode == 0
    assert result.stdout.strip() == importlib.metadata.version("abx-dl")


def test_readme_config_commands_round_trip_in_isolated_config_dir(tmp_path: Path) -> None:
    set_result = _run_cli(tmp_path, "config", "--set", "TIMEOUT=120")
    assert set_result.returncode == 0
    assert "TIMEOUT=120" in set_result.stdout
    assert "Saved to" in set_result.stderr

    get_result = _run_cli(tmp_path, "config", "--get", "TIMEOUT")
    assert get_result.returncode == 0
    assert get_result.stdout.strip() == "TIMEOUT=120"
    assert (tmp_path / "config" / "config.env").read_text().strip() == "TIMEOUT=120"


def test_readme_plugins_command_lists_real_wget_hooks(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "plugins", "wget")
    expected_hook_names = _hook_names("wget", "Crawl") + _hook_names("wget", "Snapshot")
    resolved_wget = Path(_cli_env(tmp_path)["ABXPKG_LIB_DIR"]) / "env" / "bin" / "wget"
    assert result.returncode == 0
    assert "wget" in result.stdout
    assert "Archive pages and their requisites with wget" in result.stdout
    assert "text/html" in result.stdout
    assert resolved_wget.is_symlink()
    version_result = subprocess.run(
        [resolved_wget, "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert version_result.stdout.startswith("GNU Wget")
    for hook_name in expected_hook_names:
        assert hook_name in result.stdout


def test_plugins_single_plugin_shows_metadata_from_config(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "plugins", "headers")
    normalized = " ".join(result.stdout.split())
    assert result.returncode == 0
    assert "headers" in normalized.lower()
    assert "Capture HTTP headers for the main document response" in normalized
    assert "chrome" in normalized
    assert "application/json" in normalized


def test_plugins_list_includes_requested_plugins(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "plugins", "headers", "chrome")
    normalized = " ".join(result.stdout.split())
    assert result.returncode == 0
    assert "chrome" in normalized
    assert "headers" in normalized


def test_plugins_list_resolves_enabled_aliases(tmp_path: Path) -> None:
    set_result = _run_cli(tmp_path, "config", "--set", "MEDIA_ENABLED=false")
    result = _run_cli(tmp_path, "plugins", "ytdlp")

    assert set_result.returncode == 0
    assert "YTDLP_ENABLED=false" in set_result.stdout
    assert result.returncode == 0
    assert "disabled" in result.stdout


def test_readme_install_command_runs_real_install_pipeline(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "plugins", "--install", "wget")
    resolved_wget = Path(_cli_env(tmp_path)["ABXPKG_LIB_DIR"]) / "env" / "bin" / "wget"
    assert result.returncode == 0
    assert "wget" in result.stdout
    assert resolved_wget.is_symlink()
    version_result = subprocess.run(
        [resolved_wget, "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert version_result.stdout.startswith("GNU Wget")


def test_readme_dl_command_downloads_example_dot_com_with_real_output(tmp_path: Path) -> None:
    output_dir = tmp_path / "downloads"
    result = _run_cli(
        tmp_path,
        "dl",
        "--plugins=wget",
        f"--dir={output_dir}",
        "https://example.com",
    )
    assert result.returncode == 0

    stdout_records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    assert any(record["type"] == "Snapshot" and record["url"] == "https://example.com" for record in stdout_records)

    downloaded_html = (output_dir / "wget" / "example.com" / "index.html").read_text()
    assert "Example Domain" in downloaded_html
    assert "This domain is for use in documentation examples" in downloaded_html

    index_records = [json.loads(line) for line in (output_dir / "index.jsonl").read_text().splitlines() if line.startswith("{")]
    wget_results = [record for record in index_records if record["type"] == "ArchiveResult" and record["plugin"] == "wget"]
    assert any(record["output_str"] == "wget/example.com/index.html" for record in wget_results)

    assert not (output_dir / ".abx-dl").exists()

    wget_processes = [
        record
        for record in index_records
        if record["type"] == "Process" and record.get("plugin") == "wget" and record.get("hook_name") == "on_Snapshot__35_wget.finite.bg"
    ]
    assert wget_processes
    reported_results = [
        json.loads(line)
        for process in wget_processes
        for line in process.get("stdout", "").splitlines()
        if line.startswith("{") and json.loads(line).get("type") == "ArchiveResult"
    ]
    assert any(
        record.get("status") == "succeeded" and record.get("output_str") == "wget/example.com/index.html" for record in reported_results
    )


def test_dl_hooks_find_dependency_commands_inside_active_install(tmp_path: Path) -> None:
    output_dir = tmp_path / "downloads"
    env = _cli_env(tmp_path)
    env["PATH"] = os.pathsep.join(entry for entry in env["PATH"].split(os.pathsep) if entry and not (Path(entry) / "abxpkg").exists())
    assert shutil.which("abxpkg", path=env["PATH"]) is None

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "abx_dl",
            "dl",
            "--plugins=parse_txt_urls",
            f"--dir={output_dir}",
            "https://example.com",
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    records = [json.loads(line) for line in (output_dir / "index.jsonl").read_text().splitlines()]
    process = next(record for record in records if record["type"] == "Process" and record["plugin"] == "parse_txt_urls")
    assert process["status"] == "succeeded"
    assert "No such file or directory" not in process["stderr"]


def test_dl_independent_output_directories_need_no_accounting(tmp_path: Path) -> None:
    output_dirs = [tmp_path / "first-output", tmp_path / "second-output"]

    for output_dir in output_dirs:
        result = _run_cli(
            tmp_path,
            "dl",
            "--plugins=wget",
            f"--dir={output_dir}",
            "https://example.com",
        )

        assert result.returncode == 0, result.stderr
        assert "denied by crawl limits" not in result.stderr
        assert "Traceback" not in result.stderr
        assert not (output_dir / ".abx-dl").exists()
        assert "Example Domain" in (output_dir / "wget" / "example.com" / "index.html").read_text()


def test_dl_ignores_legacy_crawl_accounting(tmp_path: Path) -> None:
    output_dir = tmp_path / "exhausted-output"
    state_dir = output_dir / ".abx-dl"
    state_dir.mkdir(parents=True)
    (state_dir / "limits.json").write_text(
        json.dumps(
            {
                "admission_key": "snapshot_id",
                "admitted_snapshot_ids": ["already-admitted"],
                "counted_event_ids": [],
                "snapshot_sizes": {},
                "snapshot_stop_reasons": {},
                "started_at": time.time(),
                "total_size": 0,
                "stop_reason": "crawl_max_urls",
            },
        ),
        encoding="utf-8",
    )

    result = _run_cli(
        tmp_path,
        "dl",
        "--plugins=wget",
        f"--dir={output_dir}",
        "https://example.com",
    )

    assert result.returncode == 0, result.stderr
    assert "Example Domain" in (output_dir / "wget/example.com/index.html").read_text()
    assert json.loads((state_dir / "limits.json").read_text())["admitted_snapshot_ids"] == ["already-admitted"]
    assert not (state_dir / "limits.lock").exists()


def test_dl_relative_dir_keeps_shared_hook_paths_in_run_dir(tmp_path: Path) -> None:
    result = _run_cli(
        tmp_path,
        "dl",
        "--plugins=title,wget",
        "--dir=out",
        "https://example.com/",
    )
    assert result.returncode == 0, result.stderr

    output_dir = tmp_path / "cwd" / "out"
    records = [json.loads(line) for line in (output_dir / "index.jsonl").read_text().splitlines() if line.startswith("{")]
    archive_results = [record for record in records if record["type"] == "ArchiveResult"]
    failed_results = [record for record in archive_results if record["status"] == "failed"]
    assert failed_results == []

    title_result = next(record for record in archive_results if record["plugin"] == "title")
    wget_result = next(record for record in archive_results if record["plugin"] == "wget")
    assert title_result["status"] == "succeeded"
    assert title_result["output_str"] == "Example Domain"
    assert wget_result["status"] == "succeeded"
    assert wget_result["output_str"] == "wget/example.com/index.html"
    assert (output_dir / "title" / "title.txt").read_text() == "Example Domain"
    assert "Example Domain" in (output_dir / "wget" / "example.com" / "index.html").read_text()
    assert not (output_dir / "chrome" / "out").exists()
    assert not (output_dir / "wget" / "out").exists()


def test_dl_reruns_same_directory_without_crawl_accounting(tmp_path: Path) -> None:
    output_dir = tmp_path / "downloads"
    for _attempt in range(2):
        result = _run_cli(tmp_path, "dl", "--plugins=wget", f"--dir={output_dir}", "https://example.com")
        assert result.returncode == 0, result.stderr
        assert "Example Domain" in (output_dir / "wget/example.com/index.html").read_text()
    assert not (output_dir / ".abx-dl").exists()


def test_dl_snapshot_size_budget_is_fresh_on_retry(tmp_path: Path) -> None:
    output_dir = tmp_path / "limited"
    source = output_dir / "staticfile/input.txt"
    source.parent.mkdir(parents=True)
    source.write_text("https://example.com/one\nhttps://example.com/two\n")
    for _attempt in range(2):
        result = _run_cli(
            tmp_path,
            "dl",
            "--plugins=parse_txt_urls,hashes",
            "--snapshot-max-size=1",
            f"--dir={output_dir}",
            "https://example.com",
        )
        assert result.returncode == 0, result.stderr
        records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
        results = [record for record in records if record["type"] == "ArchiveResult"]
        assert [(record["plugin"], record["status"]) for record in results] == [("parse_txt_urls", "succeeded")]
        urls = [json.loads(line)["url"] for line in (output_dir / "parse_txt_urls/urls.jsonl").read_text().splitlines()]
        assert urls == ["https://example.com/one", "https://example.com/two"]
        assert not (output_dir / "hashes").exists()
        assert not (output_dir / ".abx-dl").exists()
