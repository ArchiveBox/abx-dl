import io
import importlib.metadata
import json
import os
import subprocess
import sys
from typing import Any
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner
from rich.console import Console
from rich.progress import Progress

import abx_dl.cli as cli_module
from abx_dl.cli import _build_archive_results_table, _compact_output, _format_archive_result_line, _format_elapsed, cli as cli_group
from abx_dl.events import (
    ArchiveResultEvent,
    BinaryRequestEvent,
    BinaryEvent,
    CrawlSetupEvent,
    ProcessCompletedEvent,
    ProcessEvent,
    ProcessStdoutEvent,
    SnapshotEvent,
)
from abx_dl.limits import CrawlLimitState, parse_filesize_to_bytes
from abx_dl.models import ArchiveResult
from abx_dl.models import discover_plugins


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
    ABX_ENV_KEYS.update(plugin.config_schema.keys())


def _cli_env(tmp_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    for key in ABX_ENV_KEYS:
        env.pop(key, None)
    config_dir = tmp_path / "config"
    pythonpath_entries = [str(REPO_ROOT)]
    for sibling in ("abx-plugins", "abx-pkg"):
        sibling_path = REPO_ROOT.parent / sibling
        if sibling_path.exists():
            pythonpath_entries.append(str(sibling_path))
    if env.get("PYTHONPATH"):
        pythonpath_entries.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)
    env["CONFIG_DIR"] = str(config_dir)
    env["LIB_DIR"] = str(config_dir / "lib")
    env["PERSONAS_DIR"] = str(config_dir / "personas")
    env["DATA_DIR"] = str(tmp_path / "data")
    env["TMP_DIR"] = str(tmp_path / "tmp")
    env["HOME"] = str(tmp_path / "home")
    path_entries = [entry for entry in env.get("PATH", "").split(os.pathsep) if entry]
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
    text = "line one\n\nline two\tline three"
    assert _compact_output(text, limit=20) == "line one line two..."


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
    assert output.plain == "Installed binary: forum-dl (/tmp/forum-dl)"


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


def test_render_record_output_humanizes_long_binary_json_before_compacting() -> None:
    record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="npm",
        hook_name="install",
        timeout=60,
        output='{"type": "BinaryRequest","name":"npm","binproviders":"env,apt,brew","machine_id":"","overrides":{"apt":{"install_args":["nodejs","npm"]},"brew":{"install_args":["node"]}}}',
    )
    rendered = cli_module._render_record_output(record)
    assert rendered == "Binary requested: npm binproviders: env,apt,brew"


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


def test_dl_dry_run_passes_flag_to_download(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, Any] = {}

    class DummyBus:
        pass

    class DummyLiveUI:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def print_intro(self, *args, **kwargs) -> None:
            pass

        def print_summary(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    async def fake_download(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr(cli_module, "create_bus", lambda total_timeout: DummyBus())
    monkeypatch.setattr(cli_module, "LiveBusUI", DummyLiveUI)
    monkeypatch.setattr(cli_module, "download", fake_download)
    monkeypatch.setattr(cli_module, "_run_with_debug_bus_log", lambda bus, debug, func: func())

    runner = CliRunner()
    result = runner.invoke(cli_group, ["dl", "--dry-run", "https://example.com"])

    assert result.exit_code == 0, result.output
    assert captured["kwargs"]["dry_run"] is True
    assert captured["args"][4] == {"DRY_RUN": True}


def test_dl_forwards_max_url_and_size_limits(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class DummyBus:
        pass

    class DummyLiveUI:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def print_intro(self, *args, **kwargs) -> None:
            pass

        def print_summary(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    async def fake_download(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr(cli_module, "create_bus", lambda total_timeout: DummyBus())
    monkeypatch.setattr(cli_module, "LiveBusUI", DummyLiveUI)
    monkeypatch.setattr(cli_module, "download", fake_download)
    monkeypatch.setattr(cli_module, "_run_with_debug_bus_log", lambda bus, debug, func: func())

    result = CliRunner().invoke(cli_group, ["dl", "--max-urls=3", "--max-size=45mb", "https://example.com"])

    assert result.exit_code == 0, result.output
    assert captured["args"][4]["MAX_URLS"] == 3
    assert captured["args"][4]["MAX_SIZE"] == 45 * 1024 * 1024


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
    stop_reason = limit_state.record_process_output("proc-1", plugin_dir, ["index.html"])

    assert stop_reason == "max_size"


def test_install_dry_run_forwards_to_plugins_install(monkeypatch) -> None:
    plugin = cli_module.Plugin(name="demo", path=Path("."), hooks=[], config_schema={})
    captured: dict[str, object] = {}

    monkeypatch.setattr(cli_module, "discover_plugins", lambda: {"demo": plugin})

    def fake_run_plugin_install(*args, **kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(cli_module, "_run_plugin_install", fake_run_plugin_install)

    runner = CliRunner()
    result = runner.invoke(cli_group, ["install", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert captured["dry_run"] is True


def test_normalize_archive_result_output_relativizes_absolute_path(monkeypatch, tmp_path: Path) -> None:
    output_path = tmp_path / "example.com" / "index.html"
    monkeypatch.chdir(tmp_path)
    assert cli_module._normalize_archive_result_output(str(output_path)) == "example.com/index.html"


def test_render_record_output_relativizes_live_archive_result_absolute_path(monkeypatch, tmp_path: Path) -> None:
    output_path = tmp_path / "example.com" / "index.html"
    monkeypatch.chdir(tmp_path)
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


def test_phase_label_for_event_uses_ancestor_phase_event() -> None:
    phase_event = CrawlSetupEvent(url="https://example.com", snapshot_id="snap", output_dir="/tmp")
    process_event = ProcessEvent(
        plugin_name="wget",
        hook_name="install",
        hook_path="/bin/echo",
        hook_args=["wget"],
        is_background=False,
        output_dir="/tmp",
        env={},
        snapshot_id="snap",
        event_parent_id=phase_event.event_id,
    )
    bus = SimpleNamespace(event_history={phase_event.event_id: phase_event})
    assert cli_module._phase_label_for_event(bus, process_event) == "CrawlSetup"


def test_phase_label_for_event_walks_nested_event_ancestors() -> None:
    phase_event = SnapshotEvent(url="https://example.com", snapshot_id="snap", output_dir="/tmp")
    parent_process = ProcessEvent(
        plugin_name="chrome",
        hook_name="on_Snapshot__11_chrome_wait",
        hook_path="/bin/echo",
        hook_args=["chrome"],
        is_background=False,
        output_dir="/tmp",
        env={},
        snapshot_id="snap",
        event_parent_id=phase_event.event_id,
    )
    stdout_event = ProcessStdoutEvent(
        line='{"type": "BinaryRequest","name":"chromium"}',
        plugin_name="chrome",
        hook_name="on_Snapshot__11_chrome_wait",
        output_dir="/tmp",
        snapshot_id="snap",
        event_parent_id=parent_process.event_id,
    )
    provider_process = ProcessEvent(
        plugin_name="puppeteer",
        hook_name="on_BinaryRequest__12_puppeteer_install",
        hook_path="/bin/echo",
        hook_args=["chromium"],
        is_background=False,
        output_dir="/tmp",
        env={},
        snapshot_id="snap",
        event_parent_id=stdout_event.event_id,
    )
    bus = SimpleNamespace(
        event_history={
            phase_event.event_id: phase_event,
            parent_process.event_id: parent_process,
            stdout_event.event_id: stdout_event,
        },
    )
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


def test_render_record_output_uses_process_args_for_failed_empty_live_row() -> None:
    record = cli_module._LiveProcessRecord(
        id="proc-1",
        plugin="wget",
        hook_name="install",
        timeout=60,
        status="failed",
        cmd=["/bin/echo", "--flag", "value"],
    )
    assert cli_module._render_record_output(record) == "['--flag', 'value']"


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


def test_parse_emitted_binary_names_reads_binary_stdout_records() -> None:
    proc = cli_module.Process(
        cmd=[],
        stdout='{"type": "Binary","name":"wget","abspath":"/opt/homebrew/bin/wget"}\n{"type": "BinaryRequest","name":"node"}\n',
    )
    assert cli_module._parse_emitted_binary_names(proc) == ["wget"]


def test_parse_hook_status_marker_recognizes_skipped_prefix() -> None:
    status, output = cli_module._parse_hook_status_marker(
        "line one\nSKIPPED: CLAUDECHROME_ENABLED=False\n",
        "",
    )
    assert status == "skipped"
    assert output == "CLAUDECHROME_ENABLED=False"


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

    running = _format_elapsed("2026-03-11T12:00:00", None, 60, now=now)
    completed = _format_elapsed("2026-03-11T12:00:00", "2026-03-11T12:00:05", 60, now=now)

    assert running == "15.0s/60s"
    assert completed == "5.0s/60s"


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

    assert [column.header for column in table.columns] == ["Currently Running", "Phase", "Status", "Elapsed", "Output"]
    elapsed_cells = table.columns[3]._cells
    assert elapsed_cells == ["5.0s/60s"]


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
    assert expected_hook_names
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
    assert "chrome" in normalized
    assert "application/json" in normalized


def test_readme_install_command_runs_real_install_pipeline(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, "plugins", "--install", "wget")

    assert result.returncode == 0
    assert "Installing plugin dependencies" in result.stdout
    assert "wget" in result.stdout
    if "Install Results" in result.stdout:
        assert "Binary" in result.stdout or "Binary" in result.stdout
        assert "✅" in result.stdout
        assert "⬇️" in result.stdout
        assert "providers=env,apt,brew" in result.stdout
    else:
        assert "No required binaries declared for the selected plugins." in result.stdout


def test_run_plugin_install_keeps_provider_plugins_but_only_selected_required_binaries(monkeypatch) -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ["forumdl", "puppeteer"]}

    async def fake_install_plugins(*args, **kwargs):
        install_plugins = kwargs["plugins"]
        assert install_plugins["forumdl"].binaries
        assert install_plugins["puppeteer"].binaries
        assert install_plugins["puppeteer"].get_binary_request_hooks()
        return []

    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=io.StringIO(), force_terminal=False, color_system=None, width=120),
    )

    exit_code = cli_module._run_plugin_install(selected, visible_plugins={"forumdl"})

    assert exit_code == 0


def test_run_plugin_install_uses_declared_required_binaries(monkeypatch) -> None:
    plugin = discover_plugins()["npm"]

    async def fake_install_plugins(*args, **kwargs):
        install_plugins = kwargs["plugins"]
        assert install_plugins["npm"].binaries
        return []

    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=io.StringIO(), force_terminal=False, color_system=None, width=120),
    )

    exit_code = cli_module._run_plugin_install({"npm": plugin}, visible_plugins={"npm"})

    assert exit_code == 0


def test_run_plugin_install_passes_through_failed_binary_hook_stderr(monkeypatch) -> None:
    plugin = discover_plugins()["chrome"]
    console_output = io.StringIO()
    sandbox_error = "\n".join(
        [
            "npm ERR! getaddrinfo EAI_AGAIN storage.googleapis.com",
            "HINT: Override NO_PROXY before retrying.",
            'HINT: NO_PROXY="localhost,127.0.0.1"',
            "DETAIL: " + ("proxy-blocked " * 30).strip(),
        ],
    )

    async def fake_install_plugins(*args, **kwargs):
        bus = kwargs.get("bus")
        # Emit events on the bus so cli handlers receive them
        if bus:
            await bus.emit(
                BinaryRequestEvent(
                    name="chromium",
                    plugin_name="chrome",
                    binproviders="puppeteer",
                ),
            )
            await bus.emit(
                ProcessCompletedEvent(
                    plugin_name="puppeteer",
                    hook_name="on_BinaryRequest__12_puppeteer_install",
                    exit_code=1,
                    stdout="",
                    stderr=sandbox_error,
                    output_dir="",
                    process_id="proc-1",
                ),
            )
        return []

    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=console_output, force_terminal=False, color_system=None, width=120),
    )

    exit_code = cli_module._run_plugin_install({"chrome": plugin})

    rendered = console_output.getvalue()
    table_section = rendered.split("Failure details:")[0]
    normalized_table = " ".join(table_section.split())
    assert exit_code == 1
    assert "Installing plugin dependencies for chrome" in rendered
    assert "on_BinaryRequest__12_puppeteer_install" in rendered
    assert "Binary" in rendered
    assert "❌" in rendered
    assert "storage.googleapis.com" in normalized_table
    assert "NO_PROXY=localhost,127.0.0.1" in normalized_table


def test_run_plugin_install_fails_when_binary_request_never_resolves(monkeypatch) -> None:
    plugin = discover_plugins()["chrome"]
    console_output = io.StringIO()

    async def fake_install_plugins(*args, **kwargs):
        bus = kwargs.get("bus")
        if bus:
            await bus.emit(
                BinaryRequestEvent(
                    name="chromium",
                    plugin_name="chrome",
                    binproviders="puppeteer",
                ),
            )
        return []

    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=console_output, force_terminal=False, color_system=None, width=120),
    )

    exit_code = cli_module._run_plugin_install({"chrome": plugin})

    rendered = console_output.getvalue()
    assert exit_code == 1
    assert "BinaryRequested" in rendered
    assert "❌" in rendered
    assert "chromium" in rendered
    assert "Requested binary not resolved" in rendered


def test_run_plugin_install_binary_rows_show_provider_hook_name(monkeypatch) -> None:
    plugin = discover_plugins()["chrome"]
    console_output = io.StringIO()

    async def fake_install_plugins(*args, **kwargs):
        bus = kwargs.get("bus")
        if bus:
            await bus.emit(
                BinaryRequestEvent(
                    name="wget",
                    plugin_name="env",
                    hook_name="on_BinaryRequest__09_env_discover",
                ),
            )
            await bus.emit(
                BinaryEvent(
                    name="wget",
                    plugin_name="env",
                    hook_name="on_BinaryRequest__09_env_discover",
                    abspath="/opt/homebrew/bin/wget",
                    version="1.25.0",
                    binprovider="env",
                ),
            )
            await bus.emit(
                ProcessCompletedEvent(
                    plugin_name="env",
                    hook_name="on_BinaryRequest__09_env_discover",
                    exit_code=0,
                    stdout='{"type": "Binary","name":"wget","abspath":"/opt/homebrew/bin/wget","version":"1.25.0"}',
                    stderr="",
                    output_dir="",
                    process_id="proc-1",
                ),
            )
        return []

    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=console_output, force_terminal=False, color_system=None, width=160),
    )

    exit_code = cli_module._run_plugin_install({"chrome": plugin})

    rendered = console_output.getvalue()
    assert exit_code == 0
    assert "Binary" in rendered
    assert "on_BinaryRequest__09_env_discover" in rendered
    assert "/opt/homebrew/bin/wget 1.25.0" in rendered


def test_run_plugin_install_hides_failed_provider_row_after_later_success(monkeypatch) -> None:
    plugin = discover_plugins()["wget"]
    console_output = io.StringIO()
    crawl_hook = "install"
    env_hook = "on_BinaryRequest__09_env_discover"
    pip_hook = "on_BinaryRequest__11_pip_install"
    request_stdout = json.dumps(
        {
            "type": "BinaryRequest",
            "name": "opendataloader-pdf",
            "binproviders": "env,pip",
        },
    )

    async def fake_install_plugins(*args, **kwargs):
        bus = kwargs.get("bus")
        if bus:
            await bus.emit(
                BinaryRequestEvent(
                    name="opendataloader-pdf",
                    plugin_name="wget",
                    hook_name=crawl_hook,
                    binproviders="env,pip",
                ),
            )
            await bus.emit(
                ProcessCompletedEvent(
                    plugin_name="env",
                    hook_name=env_hook,
                    exit_code=1,
                    stdout="",
                    stderr="opendataloader-pdf not found in PATH",
                    output_dir="",
                    process_id="env-proc",
                ),
            )
            await bus.emit(
                BinaryEvent(
                    name="opendataloader-pdf",
                    plugin_name="pip",
                    hook_name=pip_hook,
                    abspath="/tmp/lib/pip/venv/bin/opendataloader-pdf",
                    version="2.0.2",
                    binprovider="pip",
                ),
            )
            await bus.emit(
                ProcessCompletedEvent(
                    plugin_name="pip",
                    hook_name=pip_hook,
                    exit_code=0,
                    stdout='{"type": "Binary","name":"opendataloader-pdf","abspath":"/tmp/lib/pip/venv/bin/opendataloader-pdf","version":"2.0.2"}',
                    stderr="",
                    output_dir="",
                    process_id="pip-proc",
                ),
            )
            await bus.emit(
                ProcessCompletedEvent(
                    plugin_name="wget",
                    hook_name=crawl_hook,
                    exit_code=0,
                    stdout=request_stdout,
                    stderr="",
                    output_dir="",
                    process_id="crawl-proc",
                ),
            )
        return []

    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=console_output, force_terminal=False, color_system=None, width=160),
    )

    exit_code = cli_module._run_plugin_install({"wget": plugin})

    rendered = console_output.getvalue()
    assert exit_code == 0
    assert "opendataloader-pdf" in rendered
    assert "/tmp/lib/pip/venv/bin/opendataloader-pdf 2.0.2" in rendered
    assert "opendataloader-pdf not found in PATH" not in rendered


def test_run_plugin_install_scopes_provider_failure_to_requested_binary(monkeypatch) -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ["trafilatura", "opendataloader", "env", "pip"]}
    console_output = io.StringIO()

    async def fake_install_plugins(*args, **kwargs):
        bus = kwargs.get("bus")
        if bus:
            await bus.emit(
                BinaryRequestEvent(
                    name="trafilatura",
                    plugin_name="trafilatura",
                    hook_name="install",
                    binproviders="env,pip",
                ),
            )
            await bus.emit(
                BinaryRequestEvent(
                    name="opendataloader-pdf",
                    plugin_name="opendataloader",
                    hook_name="install",
                    binproviders="env,pip",
                ),
            )
            await bus.emit(
                ProcessCompletedEvent(
                    plugin_name="env",
                    hook_name="on_BinaryRequest__00_env_discover",
                    hook_args=["--name=trafilatura", "--binproviders=env,pip"],
                    exit_code=1,
                    stdout="",
                    stderr="trafilatura not found in PATH",
                    output_dir="",
                    process_id="env-proc",
                ),
            )
        return []

    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=console_output, force_terminal=False, color_system=None, width=160),
    )

    exit_code = cli_module._run_plugin_install(selected, visible_plugins={"trafilatura", "opendataloader"})

    rendered = console_output.getvalue()
    assert exit_code == 1
    assert "trafilatura not found in PATH" in rendered
    assert "Requested binary not resolved: opendataloader-pdf" in rendered
    assert "opendataloader / install\ntrafilatura not found in PATH" not in rendered


def test_run_plugin_install_renders_rows_in_chronological_order(monkeypatch) -> None:
    plugin = discover_plugins()["wget"]
    console_output = io.StringIO()
    crawl_hook = "install"
    provider_hook = "on_BinaryRequest__09_env_discover"
    request_line = json.dumps(
        {
            "type": "BinaryRequest",
            "name": "wget",
            "binproviders": "env,apt,brew",
        },
    )

    async def fake_install_plugins(*args, **kwargs):
        bus = kwargs.get("bus")
        if bus:
            await bus.emit(
                BinaryRequestEvent(
                    name="wget",
                    plugin_name="wget",
                    hook_name=crawl_hook,
                    binproviders="env,apt,brew",
                ),
            )
            await bus.emit(
                BinaryEvent(
                    name="wget",
                    plugin_name="env",
                    hook_name=provider_hook,
                    abspath="/opt/homebrew/bin/wget",
                    version="1.25.0",
                    binprovider="env",
                ),
            )
            await bus.emit(
                ProcessCompletedEvent(
                    plugin_name="env",
                    hook_name=provider_hook,
                    exit_code=0,
                    stdout='{"type": "Binary","name":"wget","abspath":"/opt/homebrew/bin/wget","version":"1.25.0"}',
                    stderr="",
                    output_dir="",
                    process_id="provider-proc",
                ),
            )
            await bus.emit(
                ProcessCompletedEvent(
                    plugin_name="wget",
                    hook_name=crawl_hook,
                    exit_code=0,
                    stdout=request_line,
                    stderr="",
                    output_dir="",
                    process_id="crawl-proc",
                ),
            )
        return []

    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=console_output, force_terminal=False, color_system=None, width=160),
    )

    exit_code = cli_module._run_plugin_install({"wget": plugin})

    rendered = console_output.getvalue()
    assert exit_code == 0
    assert rendered.index(crawl_hook) < rendered.index(provider_hook)


def test_run_plugin_install_live_view_does_not_print_no_hooks_footer(monkeypatch) -> None:
    plugin = discover_plugins()["wget"]
    console_output = io.StringIO()

    async def fake_install_plugins(*args, **kwargs):
        bus = kwargs.get("bus")
        if bus:
            await bus.emit(
                BinaryRequestEvent(
                    name="wget",
                    plugin_name="wget",
                    hook_name="install",
                    binproviders="env,apt,brew",
                ),
            )
            await bus.emit(
                BinaryEvent(
                    name="wget",
                    plugin_name="env",
                    hook_name="on_BinaryRequest__09_env_discover",
                    abspath="/opt/homebrew/bin/wget",
                    version="1.25.0",
                    binprovider="env",
                ),
            )
        return []

    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=console_output, force_terminal=True, color_system=None, width=160),
    )

    exit_code = cli_module._run_plugin_install({"wget": plugin})

    rendered = console_output.getvalue()
    assert exit_code == 0
    assert "No required binaries declared for the selected plugins." not in rendered


def test_dl_debug_logs_bus_tree_on_normal_exit(monkeypatch) -> None:
    plugin = discover_plugins()["wget"]
    log_calls: list[str] = []

    class FakeBus:
        def on(self, *_args, **_kwargs) -> None:
            return None

        def off(self, *_args, **_kwargs) -> None:
            return None

        def log_tree(self) -> str:
            log_calls.append("logged")
            return "tree"

    async def fake_download(*args, **kwargs):
        return []

    monkeypatch.setattr(cli_module, "discover_plugins", lambda: {"wget": plugin})
    monkeypatch.setattr(cli_module, "create_bus", lambda **kwargs: FakeBus())
    monkeypatch.setattr(cli_module, "download", fake_download)

    result = CliRunner().invoke(cli_group, ["dl", "--debug", "https://example.com"])

    assert result.exit_code == 0
    assert log_calls == ["logged"]


def test_dl_debug_logs_bus_tree_on_abort(monkeypatch) -> None:
    plugin = discover_plugins()["wget"]
    log_calls: list[str] = []

    class FakeBus:
        def on(self, *_args, **_kwargs) -> None:
            return None

        def off(self, *_args, **_kwargs) -> None:
            return None

        def log_tree(self) -> str:
            log_calls.append("logged")
            return "tree"

    async def fake_download(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(cli_module, "discover_plugins", lambda: {"wget": plugin})
    monkeypatch.setattr(cli_module, "create_bus", lambda **kwargs: FakeBus())
    monkeypatch.setattr(cli_module, "download", fake_download)

    result = CliRunner().invoke(cli_group, ["dl", "--debug", "https://example.com"])

    assert result.exit_code != 0
    assert log_calls == ["logged"]


def test_dl_live_view_shows_process_rows_without_archive_results(monkeypatch, tmp_path: Path) -> None:
    plugin = discover_plugins()["wget"]
    console_output = io.StringIO()

    class FakeBus:
        def __init__(self) -> None:
            self.handlers: dict[type, list] = {}

        def on(self, event_type, handler) -> None:
            self.handlers.setdefault(event_type, []).append(handler)

        async def emit(self, event) -> None:
            for event_type, handlers in self.handlers.items():
                if isinstance(event, event_type):
                    for handler in handlers:
                        await handler(event)

        def log_tree(self) -> str:
            return "tree"

    async def fake_download(*args, **kwargs):
        bus = kwargs["bus"]
        await bus.emit(
            ProcessEvent(
                plugin_name="wget",
                hook_name="install",
                hook_path="/bin/echo",
                hook_args=["wget"],
                is_background=True,
                output_dir=str(tmp_path),
                env={},
                timeout=60,
            ),
        )
        await bus.emit(
            ProcessCompletedEvent(
                plugin_name="wget",
                hook_name="install",
                stdout="wget installed",
                stderr="",
                exit_code=0,
                output_dir=str(tmp_path),
                is_background=True,
                process_id="proc-1",
                start_ts="2026-03-20T10:00:00",
                end_ts="2026-03-20T10:00:01",
            ),
        )
        return []

    monkeypatch.setattr(cli_module, "create_bus", lambda **kwargs: FakeBus())
    monkeypatch.setattr(cli_module, "download", fake_download)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=console_output, force_terminal=True, color_system=None, width=160),
    )
    monkeypatch.setattr(
        cli_module,
        "stderr_console",
        Console(file=io.StringIO(), force_terminal=False, color_system=None, width=160),
    )

    class _TTY:
        def isatty(self) -> bool:
            return True

    class _NoTTY:
        def isatty(self) -> bool:
            return False

    monkeypatch.setattr(cli_module.sys, "stdout", _TTY())
    monkeypatch.setattr(cli_module.sys, "stderr", _NoTTY())

    ctx = cli_module.click.Context(cli_group, obj={"plugins": {"wget": plugin}})
    callback = cli_module.dl.callback
    assert callback is not None
    with ctx:
        callback("https://example.com", None, str(tmp_path), None, False, False)

    rendered = console_output.getvalue()
    assert "Downloading: https://example.com" in rendered
    assert tmp_path.name in rendered


def test_dl_live_view_prefers_archive_result_output_for_snapshot_hooks(monkeypatch, tmp_path: Path) -> None:
    plugin = discover_plugins()["headers"]
    console_output = io.StringIO()

    class FakeBus:
        def __init__(self) -> None:
            self.handlers: dict[type, list] = {}

        def on(self, event_type, handler) -> None:
            self.handlers.setdefault(event_type, []).append(handler)

        async def emit(self, event) -> None:
            for event_type, handlers in self.handlers.items():
                if isinstance(event, event_type):
                    for handler in handlers:
                        await handler(event)

        async def find(self, event_type, where=None, child_of=None, **event_fields):
            return None

        def log_tree(self) -> str:
            return "tree"

    async def fake_download(*args, **kwargs):
        bus = kwargs["bus"]
        await bus.emit(
            ProcessEvent(
                plugin_name="headers",
                hook_name="on_Snapshot__80_headers",
                hook_path="/bin/echo",
                hook_args=["headers"],
                is_background=False,
                output_dir=str(tmp_path),
                env={},
                snapshot_id="snap-1",
                timeout=60,
            ),
        )
        await bus.emit(
            ProcessStdoutEvent(
                line="fetching headers...",
                plugin_name="headers",
                hook_name="on_Snapshot__80_headers",
                output_dir=str(tmp_path),
                snapshot_id="snap-1",
                process_id="proc-2",
                start_ts="2026-03-20T10:00:00",
            ),
        )
        await bus.emit(
            ArchiveResultEvent(
                snapshot_id="snap-1",
                plugin="headers",
                hook_name="on_Snapshot__80_headers",
                status="noresult",
                process_id="proc-2",
                output_str="No headers found",
            ),
        )
        await bus.emit(
            ProcessCompletedEvent(
                plugin_name="headers",
                hook_name="on_Snapshot__80_headers",
                stdout="fetching headers...\n",
                stderr="",
                exit_code=0,
                output_dir=str(tmp_path),
                snapshot_id="snap-1",
                process_id="proc-2",
                start_ts="2026-03-20T10:00:00",
                end_ts="2026-03-20T10:00:01",
            ),
        )
        return []

    monkeypatch.setattr(cli_module, "create_bus", lambda **kwargs: FakeBus())
    monkeypatch.setattr(cli_module, "download", fake_download)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=console_output, force_terminal=True, color_system=None, width=160),
    )
    monkeypatch.setattr(
        cli_module,
        "stderr_console",
        Console(file=io.StringIO(), force_terminal=False, color_system=None, width=160),
    )

    class _TTY:
        def isatty(self) -> bool:
            return True

    class _NoTTY:
        def isatty(self) -> bool:
            return False

    monkeypatch.setattr(cli_module.sys, "stdout", _TTY())
    monkeypatch.setattr(cli_module.sys, "stderr", _NoTTY())

    ctx = cli_module.click.Context(cli_group, obj={"plugins": {"headers": plugin}})
    callback = cli_module.dl.callback
    assert callback is not None
    with ctx:
        callback("https://example.com", None, str(tmp_path), None, False, False)

    rendered = console_output.getvalue()
    assert "on_Snapshot__80_headers" in rendered
    assert "noresult" in rendered
    assert "No headers found" in rendered


def test_dl_live_view_hides_binary_provider_substeps(monkeypatch, tmp_path: Path) -> None:
    plugin = discover_plugins()["wget"]
    console_output = io.StringIO()

    class FakeBus:
        def __init__(self) -> None:
            self.handlers: dict[type, list] = {}

        def on(self, event_type, handler) -> None:
            self.handlers.setdefault(event_type, []).append(handler)

        async def emit(self, event) -> None:
            for event_type, handlers in self.handlers.items():
                if isinstance(event, event_type):
                    for handler in handlers:
                        await handler(event)

        async def find(self, event_type, where=None, child_of=None, **event_fields):
            return None

        def log_tree(self) -> str:
            return "tree"

    async def fake_download(*args, **kwargs):
        bus = kwargs["bus"]
        await bus.emit(
            ProcessEvent(
                plugin_name="wget",
                hook_name="install",
                hook_path="/bin/echo",
                hook_args=["wget"],
                is_background=True,
                output_dir=str(tmp_path),
                env={},
                timeout=60,
            ),
        )
        await bus.emit(
            ProcessEvent(
                plugin_name="env",
                hook_name="on_BinaryRequest__00_env_discover",
                hook_path="/bin/echo",
                hook_args=["--name=wget"],
                is_background=False,
                output_dir=str(tmp_path / "env"),
                env={},
                timeout=300,
            ),
        )
        await bus.emit(
            ProcessCompletedEvent(
                plugin_name="env",
                hook_name="on_BinaryRequest__00_env_discover",
                stdout="",
                stderr="wget not found in PATH",
                exit_code=1,
                output_dir=str(tmp_path / "env"),
                process_id="binary-proc",
                start_ts="2026-03-20T10:00:00",
                end_ts="2026-03-20T10:00:01",
            ),
        )
        await bus.emit(
            ProcessCompletedEvent(
                plugin_name="wget",
                hook_name="install",
                stdout="wget installed",
                stderr="",
                exit_code=0,
                output_dir=str(tmp_path),
                is_background=True,
                process_id="crawl-proc",
                start_ts="2026-03-20T10:00:00",
                end_ts="2026-03-20T10:00:02",
            ),
        )
        return []

    monkeypatch.setattr(cli_module, "create_bus", lambda **kwargs: FakeBus())
    monkeypatch.setattr(cli_module, "download", fake_download)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=console_output, force_terminal=True, color_system=None, width=160),
    )
    monkeypatch.setattr(
        cli_module,
        "stderr_console",
        Console(file=io.StringIO(), force_terminal=False, color_system=None, width=160),
    )

    class _TTY:
        def isatty(self) -> bool:
            return True

    class _NoTTY:
        def isatty(self) -> bool:
            return False

    monkeypatch.setattr(cli_module.sys, "stdout", _TTY())
    monkeypatch.setattr(cli_module.sys, "stderr", _NoTTY())

    ctx = cli_module.click.Context(cli_group, obj={"plugins": {"wget": plugin}})
    callback = cli_module.dl.callback
    assert callback is not None
    with ctx:
        callback("https://example.com", None, str(tmp_path), None, False, False)

    rendered = console_output.getvalue()
    assert "Downloading: https://example.com" in rendered
    assert tmp_path.name in rendered
    assert "on_BinaryRequest__00_env_discover" not in rendered
    assert "wget not found in PATH" not in rendered


def test_run_plugin_install_debug_logs_bus_tree_on_normal_exit(monkeypatch) -> None:
    plugin = discover_plugins()["wget"]
    log_calls: list[str] = []

    class FakeBus:
        def on(self, *_args, **_kwargs) -> None:
            return None

        def log_tree(self) -> str:
            log_calls.append("logged")
            return "tree"

    async def fake_install_plugins(*args, **kwargs):
        return []

    monkeypatch.setattr(cli_module, "create_bus", lambda **kwargs: FakeBus())
    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=io.StringIO(), force_terminal=False, color_system=None, width=120),
    )

    exit_code = cli_module._run_plugin_install({"wget": plugin}, debug=True)

    assert exit_code == 0
    assert log_calls == ["logged"]


def test_run_plugin_install_debug_logs_bus_tree_on_abort(monkeypatch) -> None:
    plugin = discover_plugins()["wget"]
    log_calls: list[str] = []

    class FakeBus:
        def on(self, *_args, **_kwargs) -> None:
            return None

        def log_tree(self) -> str:
            log_calls.append("logged")
            return "tree"

    async def fake_install_plugins(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(cli_module, "create_bus", lambda **kwargs: FakeBus())
    monkeypatch.setattr(cli_module, "install_plugins", fake_install_plugins)
    monkeypatch.setattr(
        cli_module,
        "console",
        Console(file=io.StringIO(), force_terminal=False, color_system=None, width=120),
    )

    with pytest.raises(KeyboardInterrupt):
        cli_module._run_plugin_install({"wget": plugin}, debug=True)

    assert log_calls == ["logged"]


def test_readme_dl_command_downloads_example_dot_com_with_real_output(tmp_path: Path) -> None:
    output_dir = tmp_path / "downloads"
    result = _run_cli(
        tmp_path,
        "dl",
        "--plugins=wget",
        f"--output={output_dir}",
        "https://example.com",
    )

    assert result.returncode == 0

    stdout_records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    assert any(record.get("type") == "Snapshot" and record.get("url") == "https://example.com" for record in stdout_records)
    assert any(
        record.get("type") == "ArchiveResult" and record.get("plugin") == "wget" and record.get("status") == "succeeded"
        for record in stdout_records
    )

    downloaded_html = (output_dir / "wget" / "example.com" / "index.html").read_text()
    assert "Example Domain" in downloaded_html
    assert "This domain is for use in documentation examples" in downloaded_html

    index_records = [json.loads(line) for line in (output_dir / "index.jsonl").read_text().splitlines() if line.startswith("{")]
    wget_results = [record for record in index_records if record.get("type") == "ArchiveResult" and record.get("plugin") == "wget"]
    assert any(record.get("output_str") == "wget/example.com/index.html" for record in wget_results)
