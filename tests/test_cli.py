import asyncio
import io
import importlib.metadata
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.progress import Progress

import abx_dl.cli as cli_module
from abx_dl.cli import _build_archive_results_table, _compact_output, _format_archive_result_line, _format_elapsed, cli as cli_group
from abx_dl.events import CrawlSetupEvent, ProcessCompletedEvent, ProcessEvent, ProcessStartedEvent, ProcessStdoutEvent, SnapshotEvent
from abx_dl.limits import CrawlLimitState, parse_filesize_to_bytes
from abx_dl.models import ArchiveResult, discover_plugins
from abx_dl.orchestrator import create_bus
from abx_dl.events import ArchiveResultEvent
from abx_dl.output_files import OutputFile


REPO_ROOT = Path(__file__).resolve().parents[1]
ABX_ENV_KEYS = {
    "CHECK_SSL_VALIDITY",
    "CONFIG_DIR",
    "COOKIES_FILE",
    "CRAWL_DIR",
    "DATA_DIR",
    "LIB_DIR",
    "NODE_MODULES_DIR",
    "NODE_PATH",
    "NPM_BIN_DIR",
    "NPM_HOME",
    "PERSONAS_DIR",
    "PIP_BIN_DIR",
    "PIP_HOME",
    "SNAP_DIR",
    "TIMEOUT",
    "TMP_DIR",
    "USER_AGENT",
}
for plugin in discover_plugins().values():
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
    if "PYTHONPATH" in env and env["PYTHONPATH"]:
        pythonpath_entries.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)
    env["CONFIG_DIR"] = str(config_dir)
    env["LIB_DIR"] = str(config_dir / "lib")
    env["PERSONAS_DIR"] = str(config_dir / "personas")
    env["DATA_DIR"] = str(tmp_path / "data")
    env["TMP_DIR"] = str(tmp_path / "tmp")
    env["HOME"] = str(tmp_path / "home")
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
    return subprocess.run(
        [sys.executable, "-m", "abx_dl", *args],
        cwd=cwd,
        env=_cli_env(tmp_path),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _hook_names(plugin_name: str, event_name: str) -> list[str]:
    plugin = discover_plugins()[plugin_name]
    return [hook.name for hook in sorted(plugin.hooks, key=lambda hook: hook.sort_key) if event_name in hook.name]


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


def test_render_record_output_keeps_archive_result_output_untruncated() -> None:
    record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="headers",
        hook_name="on_Snapshot__80_headers",
        timeout=60,
        output="line one\nline two",
        final_output="line one\nline two",
        final_output_is_archive_result=True,
    )
    assert cli_module._render_record_output(record) == "line one\nline two"


def test_build_archive_results_table_shows_output_size_column() -> None:
    record = ArchiveResult(
        snapshot_id="snap-1",
        plugin="wget",
        hook_name="on_Snapshot__06_wget.finite.bg",
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
        hook_path="/bin/echo",
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
        hook_path="/bin/echo",
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
        hook_path="/bin/echo",
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
        hook_path="/bin/echo",
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
        process = await asyncio.create_subprocess_exec("/bin/sh", "-c", "true")
        await process.wait()
        started_event = ProcessStartedEvent(
            plugin_name="pdf",
            hook_name="on_Snapshot__52_pdf",
            hook_path="/bin/echo",
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
            hook_path="/bin/echo",
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
        await live_ui.on_ProcessStartedEvent(started_event)
        await live_ui.on_ArchiveResultEvent(archive_result_event)
        bus.event_history[archive_result_event.event_id] = archive_result_event
        await live_ui.on_ProcessCompletedEvent(completed_event)

    asyncio.run(run())

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
        process = await asyncio.create_subprocess_exec("/bin/sh", "-c", "true")
        await process.wait()
        started_event = ProcessStartedEvent(
            plugin_name="chrome",
            hook_name="on_CrawlSetup__90_chrome_launch.daemon.bg",
            hook_path="/bin/echo",
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


def test_crawl_limit_state_blocks_snapshots_after_max_urls(tmp_path: Path) -> None:
    limit_state = CrawlLimitState(crawl_dir=tmp_path, max_urls=2, max_size=0)
    assert limit_state.admit_snapshot("snap-1").allowed is True
    assert limit_state.admit_snapshot("snap-2").allowed is True
    third = limit_state.admit_snapshot("snap-3")
    assert third.allowed is False
    assert third.stop_reason == "max_urls"


def test_crawl_limit_state_stops_after_max_size(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "snapshot" / "wget"
    plugin_dir.mkdir(parents=True)
    output_file = plugin_dir / "index.html"
    output_file.write_bytes(b"x" * 32)

    limit_state = CrawlLimitState(crawl_dir=tmp_path, max_urls=0, max_size=16)
    assert limit_state.record_process_output("proc-1", plugin_dir, ["index.html"]) == "max_size"


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
            hook_name="on_Snapshot__06_wget.finite.bg",
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
        hook_path="/bin/echo",
        hook_args=["wget"],
        is_background=False,
        output_dir="/tmp",
        env={},
        event_parent_id=phase_event.event_id,
    )
    bus.event_history[phase_event.event_id] = phase_event
    assert cli_module._phase_label_for_event(bus, process_event) == "CrawlSetup"


def test_phase_label_for_event_walks_nested_event_ancestors() -> None:
    bus = create_bus(total_timeout=10.0, name="phase_label_nested")
    phase_event = SnapshotEvent(url="https://example.com", snapshot_id="snap", output_dir="/tmp")
    provider_process = ProcessEvent(
        plugin_name="puppeteer",
        hook_name="on_BinaryRequest__12_puppeteer_install",
        hook_path="/bin/echo",
        hook_args=["chromium"],
        is_background=False,
        output_dir="/tmp",
        env={},
        event_parent_id=phase_event.event_id,
    )
    bus.event_history[phase_event.event_id] = phase_event
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


def test_render_record_output_keeps_failed_output_untruncated() -> None:
    record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="wget",
        hook_name="install",
        timeout=60,
        status="failed",
        output="line one\nline two " + ("x" * 200),
    )
    assert cli_module._render_record_output(record) == record.output
    assert cli_module._render_record_output_cell(record).plain == record.output.replace('"', "")


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
        hook_name="on_Snapshot__10_chrome_tab.bg",
        status="failed",
        output_str="",
        error="No Chrome session found",
    )
    line = _format_archive_result_line(result)
    assert "ArchiveResult" in line
    assert "on_Snapshot__10_chrome_tab.bg" in line
    assert "failed" in line
    assert "No Chrome session found" in line


def test_format_elapsed_uses_running_or_completed_timestamps() -> None:
    now = datetime(2026, 3, 11, 12, 0, 15)
    assert _format_elapsed("2026-03-11T12:00:00", None, 60, now=now) == "15.0s/60s"
    assert _format_elapsed("2026-03-11T12:00:00", "2026-03-11T12:00:05", 60, now=now) == "5.0s/60s"


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
        hook_name="on_Snapshot__10_chrome_tab.bg",
        status="started",
        start_ts="2026-03-11T12:00:00",
    )
    table = _build_archive_results_table([result], timeout_seconds=60, now=datetime(2026, 3, 11, 12, 0, 5))
    assert [column.header for column in table.columns] == ["Currently Running", "Phase", "Status", "Size", "Elapsed", "Output"]
    assert [str(cell) for cell in table.columns[3]._cells] == ["-"]
    assert table.columns[4]._cells == ["5.0s/60s"]


def test_record_status_style_uses_darker_started_color_for_background_hooks() -> None:
    bg_record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="chrome",
        hook_name="on_Snapshot__09_chrome_launch.daemon.bg",
        timeout=60,
        status="started",
    )
    fg_record = cli_module._LiveProcessRecord(
        id="proc-2",
        plugin="wget",
        hook_name="on_Snapshot__06_wget",
        timeout=60,
        status="started",
    )

    assert cli_module._record_status_style(bg_record) == cli_module.BG_STARTED_STYLE
    assert cli_module._record_status_style(fg_record) == "yellow"


def test_default_group_routes_bare_url_and_top_level_dl_options() -> None:
    assert cli_group._should_default_to_dl(["https://example.com"]) is True
    assert cli_group._should_default_to_dl(["--debug", "https://example.com"]) is True
    assert cli_group._should_default_to_dl(["--plugins=wget", "https://example.com"]) is True
    assert cli_group._should_default_to_dl(["--timeout=120", "https://example.com"]) is True
    assert cli_group._should_default_to_dl(["archivebox://install"]) is True
    assert cli_group._should_default_to_dl(["plugins", "wget"]) is False
    assert cli_group._should_default_to_dl(["example.com"]) is False
    assert cli_group._should_default_to_dl(["nonsense"]) is False
    assert cli_group._should_default_to_dl(["--help"]) is False


def test_default_group_leaves_unknown_non_url_as_subcommand_error(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "nonsense")
    assert result.returncode != 0
    assert "No such command" in result.stderr or "No such command" in result.stdout
    assert '"url": "nonsense"' not in result.stdout


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
    assert result.returncode == 0
    assert "wget" in result.stdout
    assert "Archive pages and their requisites with wget" in result.stdout
    assert "Outputs:" in result.stdout
    assert "text/html" in result.stdout
    for hook_name in expected_hook_names:
        assert hook_name in result.stdout


def test_plugins_single_plugin_shows_metadata_from_config(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "plugins", "headers")
    normalized = " ".join(result.stdout.split())
    assert result.returncode == 0
    assert "Headers" in normalized
    assert "Capture HTTP headers for the main document response" in normalized
    assert "Depends on: chrome" in normalized
    assert "Outputs: application/json" in normalized


def test_plugins_list_includes_metadata_summary_columns(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "plugins", "headers", "chrome")
    normalized = " ".join(result.stdout.split())
    assert result.returncode == 0
    assert "Deps" in normalized
    assert "Outputs" in normalized
    assert "Info" in normalized
    assert "chrome" in normalized
    assert "headers" in normalized
    assert "application/json" in normalized


def test_readme_install_command_runs_real_install_pipeline(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "plugins", "--install", "wget")
    assert result.returncode == 0
    assert "Installing plugin dependencies" in result.stdout
    assert "wget" in result.stdout
    if "Install Results" in result.stdout:
        assert "Binary" in result.stdout
        assert "✅" in result.stdout
        assert "⬇️" in result.stdout
        assert "binproviders: env,apt,brew" in result.stdout
    else:
        assert "No required binaries declared for the selected plugins." in result.stdout


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
