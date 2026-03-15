import io
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console

import abx_dl.cli as cli_module
from abx_dl.cli import _build_archive_results_table, _compact_output, _format_archive_result_line, _format_elapsed, cli as cli_group
from abx_dl.models import ArchiveResult, Process
from abx_dl.plugins import discover_plugins


REPO_ROOT = Path(__file__).resolve().parents[1]
ABX_ENV_KEYS = {
    'CHECK_SSL_VALIDITY',
    'CONFIG_DIR',
    'COOKIES_FILE',
    'CRAWL_DIR',
    'DATA_DIR',
    'LIB_DIR',
    'NODE_MODULES_DIR',
    'NODE_PATH',
    'NPM_BIN_DIR',
    'NPM_HOME',
    'PERSONAS_DIR',
    'PIP_BIN_DIR',
    'PIP_HOME',
    'SNAP_DIR',
    'TIMEOUT',
    'TMP_DIR',
    'USER_AGENT',
}
for plugin in discover_plugins().values():
    ABX_ENV_KEYS.update(plugin.config_schema.keys())


def _cli_env(tmp_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    for key in ABX_ENV_KEYS:
        env.pop(key, None)
    config_dir = tmp_path / 'config'
    env['CONFIG_DIR'] = str(config_dir)
    env['LIB_DIR'] = str(config_dir / 'lib')
    env['PERSONAS_DIR'] = str(config_dir / 'personas')
    env['DATA_DIR'] = str(tmp_path / 'data')
    env['HOME'] = str(tmp_path / 'home')
    return env


def _run_cli(tmp_path: Path, *args: str, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, '-m', 'abx_dl', *args],
        cwd=REPO_ROOT,
        env=_cli_env(tmp_path),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _hook_names(plugin_name: str, event_name: str) -> list[str]:
    plugin = discover_plugins()[plugin_name]
    return [
        hook.name
        for hook in sorted(plugin.hooks, key=lambda hook: hook.sort_key)
        if event_name in hook.name
    ]


def test_compact_output_collapses_whitespace_and_truncates() -> None:
    text = "line one\n\nline two\tline three"
    assert _compact_output(text, limit=20) == "line one line two..."


def test_format_archive_result_line_includes_requested_fields() -> None:
    result = ArchiveResult(
        snapshot_id='snap',
        plugin='chrome',
        hook_name='on_Snapshot__10_chrome_tab.bg',
        status='failed',
        output_str='',
        error='No Chrome session found',
    )

    line = _format_archive_result_line(result)

    assert 'ArchiveResult' in line
    assert 'on_Snapshot__10_chrome_tab.bg' in line
    assert 'failed' in line
    assert 'No Chrome session found' in line


def test_format_elapsed_uses_running_or_completed_timestamps() -> None:
    now = datetime(2026, 3, 11, 12, 0, 15)

    running = _format_elapsed('2026-03-11T12:00:00', None, 60, now=now)
    completed = _format_elapsed('2026-03-11T12:00:00', '2026-03-11T12:00:05', 60, now=now)

    assert running == '15.0s/60s'
    assert completed == '5.0s/60s'


def test_build_archive_results_table_includes_elapsed_column() -> None:
    result = ArchiveResult(
        snapshot_id='snap',
        plugin='chrome',
        hook_name='on_Snapshot__10_chrome_tab.bg',
        status='started',
        start_ts='2026-03-11T12:00:00',
    )

    table = _build_archive_results_table([result], timeout_seconds=60, now=datetime(2026, 3, 11, 12, 0, 5))

    assert [column.header for column in table.columns] == ['Type', 'Hook Name', 'Status', 'Elapsed', 'Output']
    elapsed_cells = table.columns[3]._cells
    assert elapsed_cells == ['5.0s/60s']


def test_default_group_routes_bare_url_and_top_level_dl_options() -> None:
    assert cli_group._should_default_to_dl(['https://example.com']) is True
    assert cli_group._should_default_to_dl(['--plugins=wget', 'https://example.com']) is True
    assert cli_group._should_default_to_dl(['--timeout=120', 'https://example.com']) is True
    assert cli_group._should_default_to_dl(['plugins', 'wget']) is False
    assert cli_group._should_default_to_dl(['--help']) is False


def test_readme_config_commands_round_trip_in_isolated_config_dir(tmp_path: Path) -> None:
    set_result = _run_cli(tmp_path, 'config', '--set', 'TIMEOUT=120')

    assert set_result.returncode == 0
    assert 'TIMEOUT=120' in set_result.stdout
    assert 'Saved to' in set_result.stderr

    get_result = _run_cli(tmp_path, 'config', '--get', 'TIMEOUT')

    assert get_result.returncode == 0
    assert get_result.stdout.strip() == 'TIMEOUT=120'
    assert (tmp_path / 'config' / 'config.env').read_text().strip() == 'TIMEOUT=120'


def test_readme_plugins_command_lists_real_wget_hooks(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, 'plugins', 'wget')
    expected_hook_names = _hook_names('wget', 'Crawl') + _hook_names('wget', 'Snapshot')

    assert result.returncode == 0
    assert 'wget' in result.stdout
    assert expected_hook_names
    for hook_name in expected_hook_names:
        assert hook_name in result.stdout


def test_readme_install_command_runs_real_install_hooks(tmp_path: Path) -> None:
    result = _run_cli(tmp_path, 'plugins', '--install', 'wget')
    install_hook_names = _hook_names('wget', 'Crawl')

    assert result.returncode == 0
    assert 'Installing plugin dependencies' in result.stdout
    assert 'Install Results' in result.stdout
    assert 'wget' in result.stdout
    assert install_hook_names
    for hook_name in install_hook_names:
        assert hook_name in result.stdout
    assert 'succeeded' in result.stdout


def test_run_plugin_install_passes_through_failed_binary_hook_stderr(monkeypatch) -> None:
    plugin = discover_plugins()['chrome']
    console_output = io.StringIO()
    sandbox_error = "\n".join(
        [
            "npm ERR! getaddrinfo EAI_AGAIN storage.googleapis.com",
            "HINT: Override NO_PROXY before retrying.",
            'HINT: NO_PROXY="localhost,127.0.0.1"',
        ]
    )

    def fake_download(*args, **kwargs):
        yield Process(
            cmd=['python', 'on_Binary__12_puppeteer_install.py'],
            plugin='puppeteer',
            hook_name='on_Binary__12_puppeteer_install',
            exit_code=1,
            stderr=sandbox_error,
        )
        yield ArchiveResult(
            snapshot_id='snap',
            plugin='chrome',
            hook_name='on_Crawl__70_chrome_install.finite.bg',
            status='succeeded',
            output_str='chromium requested',
        )

    monkeypatch.setattr(cli_module, 'download', fake_download)
    monkeypatch.setattr(
        cli_module,
        'console',
        Console(file=console_output, force_terminal=False, color_system=None, width=120),
    )

    exit_code = cli_module._run_plugin_install({'chrome': plugin})

    rendered = console_output.getvalue()
    assert exit_code == 1
    assert 'on_Binary__12_puppeteer_install' in rendered
    assert sandbox_error in rendered


def test_readme_dl_command_downloads_example_dot_com_with_real_output(tmp_path: Path) -> None:
    output_dir = tmp_path / 'downloads'
    result = _run_cli(
        tmp_path,
        'dl',
        '--plugins=wget',
        f'--output={output_dir}',
        'https://example.com',
    )

    assert result.returncode == 0

    stdout_records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
    assert any(record.get('type') == 'Snapshot' and record.get('url') == 'https://example.com' for record in stdout_records)
    assert any(
        record.get('type') == 'ArchiveResult'
        and record.get('plugin') == 'wget'
        and record.get('status') == 'succeeded'
        for record in stdout_records
    )

    downloaded_html = (output_dir / 'wget' / 'example.com' / 'index.html').read_text()
    assert 'Example Domain' in downloaded_html
    assert 'This domain is for use in documentation examples' in downloaded_html

    index_records = [
        json.loads(line)
        for line in (output_dir / 'index.jsonl').read_text().splitlines()
        if line.startswith('{')
    ]
    wget_results = [
        record for record in index_records
        if record.get('type') == 'ArchiveResult' and record.get('plugin') == 'wget'
    ]
    assert any(record.get('output_str') == 'example.com/index.html' for record in wget_results)
