import asyncio
import json
from pathlib import Path

from bubus import EventBus

from abx_dl.orchestrator import download
from abx_dl.events import BinaryInstalledEvent, BinaryLoadedEvent, ArchiveResultEvent
from abx_dl.models import ArchiveResult
from abx_dl.models import discover_plugins


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if content and not content.startswith('#!'):
        content = '#!/usr/bin/env python3\n' + content
    path.write_text(content)
    path.chmod(0o755)


def _run_download(*args, **kwargs):
    """Helper to run async download() from sync test code."""
    return asyncio.run(download(*args, **kwargs))


def test_download_dispatches_binary_hooks_and_applies_machine_updates(tmp_path: Path) -> None:
    plugins_root = tmp_path / 'plugins'

    _write(
        plugins_root / 'producer' / 'on_Crawl__00_emit.py',
        '\n'.join(
            [
                'import json',
                'print(json.dumps({"type": "Binary", "name": "demo", "binproviders": "provider"}))',
                'print(json.dumps({"type": "Machine", "config": {"DEMO_FLAG": "ready"}}))',
            ]
        )
        + '\n',
    )
    _write(
        plugins_root / 'provider' / 'on_Binary__10_provider_install.py',
        '\n'.join(
            [
                'import json',
                'from pathlib import Path',
                'import argparse',
                'parser = argparse.ArgumentParser()',
                'parser.add_argument("--binary-id", required=True)',
                'parser.add_argument("--machine-id", required=True)',
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                'parser.add_argument("--overrides", default=None)',
                'args = parser.parse_args()',
                'bin_path = Path.cwd() / "bin" / args.name',
                'bin_path.parent.mkdir(parents=True, exist_ok=True)',
                'bin_path.write_text("demo")',
                'node_modules = Path.cwd() / "lib" / "node_modules"',
                'node_modules.mkdir(parents=True, exist_ok=True)',
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path), "binary_id": args.binary_id, "machine_id": args.machine_id, "binprovider": "provider"}))',
                'print(json.dumps({"type": "Machine", "config": {"NODE_MODULES_DIR": str(node_modules), "NODE_PATH": str(node_modules)}}))',
            ]
        )
        + '\n',
    )
    _write(
        plugins_root / 'consumer' / 'on_Crawl__01_check.py',
        '\n'.join(
            [
                'import json',
                'import os',
                'print(json.dumps({',
                '    "type": "ArchiveResult",',
                '    "status": "succeeded",',
                '    "output_str": "|".join([',
                '        os.environ.get("DEMO_BINARY", ""),',
                '        os.environ.get("DEMO_FLAG", ""),',
                '        os.environ.get("NODE_PATH", ""),',
                '    ]),',
                '}))',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)

    assert next(result for result in results if result.plugin == 'producer').status == 'succeeded'
    assert results[-1].output_str.endswith('|ready|' + str(tmp_path / 'run' / 'provider' / 'lib' / 'node_modules'))

    index_lines = [json.loads(line) for line in (tmp_path / 'run' / 'index.jsonl').read_text().splitlines()]
    assert any(
        line.get('type') == 'Process' and 'on_Binary__10_provider_install.py' in ' '.join(line.get('cmd', []))
        for line in index_lines
    )


def test_download_applies_side_effects_from_completed_background_hooks(tmp_path: Path) -> None:
    plugins_root = tmp_path / 'plugins'

    _write(
        plugins_root / 'producer' / 'on_Crawl__00_emit.bg.py',
        '\n'.join(
            [
                'import json',
                'print(json.dumps({"type": "Binary", "name": "demo", "binproviders": "provider"}), flush=True)',
                'print(json.dumps({"type": "Machine", "config": {"DEMO_FLAG": "ready"}}), flush=True)',
            ]
        )
        + '\n',
    )
    _write(
        plugins_root / 'delay' / 'on_Crawl__01_wait.py',
        '\n'.join(
            [
                'import time',
                'time.sleep(0.2)',
            ]
        )
        + '\n',
    )
    _write(
        plugins_root / 'provider' / 'on_Binary__10_provider_install.py',
        '\n'.join(
            [
                'import json',
                'from pathlib import Path',
                'import argparse',
                'parser = argparse.ArgumentParser()',
                'parser.add_argument("--binary-id", required=True)',
                'parser.add_argument("--machine-id", required=True)',
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                'parser.add_argument("--overrides", default=None)',
                'args = parser.parse_args()',
                'bin_path = Path.cwd() / "bin" / args.name',
                'bin_path.parent.mkdir(parents=True, exist_ok=True)',
                'bin_path.write_text("demo")',
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path), "binary_id": args.binary_id, "machine_id": args.machine_id, "binprovider": "provider"}))',
            ]
        )
        + '\n',
    )
    _write(
        plugins_root / 'consumer' / 'on_Crawl__02_check.py',
        '\n'.join(
            [
                'import json',
                'import os',
                'print(json.dumps({',
                '    "type": "ArchiveResult",',
                '    "status": "succeeded",',
                '    "output_str": "|".join([',
                '        os.environ.get("DEMO_BINARY", ""),',
                '        os.environ.get("DEMO_FLAG", ""),',
                '    ]),',
                '}))',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)

    consumer_result = next(result for result in results if result.plugin == 'consumer')
    assert consumer_result.output_str == str(tmp_path / 'run' / 'provider' / 'bin' / 'demo') + '|ready'
    assert any(result.plugin == 'producer' and result.status == 'succeeded' for result in results)


def test_download_finalizes_background_hooks_after_sigterm(tmp_path: Path) -> None:
    plugins_root = tmp_path / 'plugins'

    _write(
        plugins_root / 'bgdemo' / 'on_Snapshot__05_wait.bg.py',
        '\n'.join(
            [
                'import json',
                'import signal',
                'import sys',
                'import time',
                '',
                'def handle_sigterm(signum, frame):',
                '    print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "bg cleaned up"}), flush=True)',
                '    sys.exit(0)',
                '',
                'signal.signal(signal.SIGTERM, handle_sigterm)',
                'print("[*] background hook ready", flush=True)',
                'while True:',
                '    time.sleep(0.1)',
            ]
        )
        + '\n',
    )
    _write(
        plugins_root / 'bgdemo' / 'on_Snapshot__10_delay.py',
        '\n'.join(
            [
                'import time',
                'time.sleep(0.3)',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    all_results = _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)
    results = [
        result for result in all_results
        if result.plugin == 'bgdemo' and result.hook_name == 'on_Snapshot__05_wait.bg'
    ]

    # Only the hook's own SIGTERM handler emits the ArchiveResult — no synthetic records.
    assert len(results) == 1
    assert results[0].status == 'succeeded'
    assert results[0].output_str == 'bg cleaned up'

    index_lines = [json.loads(line) for line in (tmp_path / 'run' / 'index.jsonl').read_text().splitlines()]
    final_result = next(
        line for line in reversed(index_lines)
        if line.get('type') == 'ArchiveResult' and line.get('plugin') == 'bgdemo'
    )
    final_process = next(
        line for line in reversed(index_lines)
        if line.get('type') == 'Process' and 'on_Snapshot__05_wait.bg.py' in ' '.join(line.get('cmd', []))
    )

    assert final_result['status'] == 'succeeded'
    assert final_result['process_id'] == results[0].process_id
    assert final_process['id'] == results[0].process_id
    assert final_process['exit_code'] == 0


def test_download_preserves_full_hook_stderr_in_archive_result(tmp_path: Path) -> None:
    plugins_root = tmp_path / 'plugins'
    full_error = 'ERROR: ' + ('proxy-blocked ' * 80) + 'storage.googleapis.com'

    _write(
        plugins_root / 'broken' / 'on_Crawl__00_fail.py',
        '\n'.join(
            [
                'import sys',
                f'sys.stderr.write({full_error!r})',
                'raise SystemExit(1)',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)

    failure = next(result for result in results if result.plugin == 'broken')
    assert failure.status == 'failed'
    assert failure.error == full_error


def test_download_applies_background_side_effects_in_hook_lifecycle_order(tmp_path: Path) -> None:
    plugins_root = tmp_path / 'plugins'

    _write(
        plugins_root / 'crawlbg' / 'on_Crawl__90_emit.bg.py',
        '\n'.join(
            [
                'import json',
                'import time',
                'time.sleep(0.1)',
                'print(json.dumps({"type": "Machine", "config": {"HOOK_ORDER": "crawl"}}), flush=True)',
            ]
        )
        + '\n',
    )
    _write(
        plugins_root / 'snapshotbg' / 'on_Snapshot__05_emit.bg.py',
        '\n'.join(
            [
                'import json',
                'import time',
                'time.sleep(0.1)',
                'print(json.dumps({"type": "Machine", "config": {"HOOK_ORDER": "snapshot"}}), flush=True)',
            ]
        )
        + '\n',
    )
    _write(
        plugins_root / 'waiter' / 'on_Snapshot__08_wait.py',
        '\n'.join(
            [
                'import time',
                'time.sleep(0.2)',
            ]
        )
        + '\n',
    )
    _write(
        plugins_root / 'consumer' / 'on_Snapshot__09_check.py',
        '\n'.join(
            [
                'import json',
                'import os',
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": os.environ.get("HOOK_ORDER", "")}))',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)

    consumer_result = next(result for result in results if result.plugin == 'consumer')
    assert consumer_result.output_str == 'snapshot'


def test_download_can_suppress_jsonl_stdout(tmp_path: Path, capsys) -> None:
    plugins_root = tmp_path / 'plugins'

    _write(
        plugins_root / 'demo' / 'on_Snapshot__10_echo.py',
        '\n'.join(
            [
                'import json',
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "ok"}))',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download(
        'https://example.com',
        plugins,
        tmp_path / 'run',
        auto_install=True,
        emit_jsonl=False,
    )

    captured = capsys.readouterr()

    assert [result.status for result in results] == ['succeeded']
    assert captured.out == ''


def test_cleanup_does_not_duplicate_failed_foreground_results(tmp_path: Path) -> None:
    plugins_root = tmp_path / 'plugins'

    _write(
        plugins_root / 'foreground' / 'on_Snapshot__10_fail.py',
        '\n'.join(
            [
                'import sys',
                'print("foreground failed", file=sys.stderr)',
                'sys.exit(1)',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = [result for result in _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True) if result.plugin == 'foreground']

    assert [result.status for result in results] == ['failed']

    index_lines = [json.loads(line) for line in (tmp_path / 'run' / 'index.jsonl').read_text().splitlines()]
    foreground_results = [
        line for line in index_lines
        if line.get('type') == 'ArchiveResult' and line.get('plugin') == 'foreground'
    ]

    assert len(foreground_results) == 1
    assert foreground_results[0]['status'] == 'failed'


def test_successful_hook_with_only_logs_is_succeeded(tmp_path: Path) -> None:
    plugins_root = tmp_path / 'plugins'

    _write(
        plugins_root / 'cachedemo' / 'on_Crawl__10_install.py',
        '\n'.join(
            [
                'print("[*] extension already installed (using cache)")',
                'print("[+] extension setup complete")',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)

    assert [result.status for result in results] == ['succeeded']


def test_successful_hook_with_skipping_log_stays_succeeded_without_explicit_skip_result(tmp_path: Path) -> None:
    plugins_root = tmp_path / 'plugins'

    _write(
        plugins_root / 'skipdemo' / 'on_Snapshot__10_skip.py',
        '\n'.join(
            [
                'import sys',
                'print("Skipping skipdemo (SKIPDEMO_ENABLED=False)", file=sys.stderr)',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)

    assert [result.status for result in results] == ['succeeded']


def test_output_str_absolute_paths_are_stored_relative_to_hook_output_dir(tmp_path: Path) -> None:
    plugins_root = tmp_path / 'plugins'

    _write(
        plugins_root / 'demo' / 'on_Snapshot__10_emit.py',
        '\n'.join(
            [
                'import json',
                'from pathlib import Path',
                'output = Path.cwd() / "media.mp4"',
                'output.write_text("demo")',
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": str(output)}))',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = [
        result
        for result in _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)
        if isinstance(result, ArchiveResult)
    ]

    assert [result.output_str for result in results] == ['media.mp4']


def test_cleanup_runs_after_all_hooks_not_interleaved(tmp_path: Path) -> None:
    """Verify that cleanup events fire after all hooks, not interleaved.

    A bg daemon hook writes a marker file on SIGTERM. A fg hook runs after it.
    The cleanup (SIGTERM) must happen only after ALL hooks have run, so the
    marker file must NOT exist when the fg hook runs but MUST exist after
    download() returns.
    """
    plugins_root = tmp_path / 'plugins'
    marker = tmp_path / 'run' / 'bgdemo' / 'sigtermed.marker'

    # bg daemon that creates a marker file when SIGTERMed
    _write(
        plugins_root / 'bgdemo' / 'on_Snapshot__05_daemon.bg.py',
        '\n'.join(
            [
                'import json',
                'import signal',
                'import sys',
                'import time',
                'from pathlib import Path',
                '',
                'def handle_sigterm(signum, frame):',
                f'    Path({str(marker)!r}).write_text("killed")',
                '    print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "daemon cleaned up"}), flush=True)',
                '    sys.exit(0)',
                '',
                'signal.signal(signal.SIGTERM, handle_sigterm)',
                'print("[*] daemon ready", flush=True)',
                'while True:',
                '    time.sleep(0.1)',
            ]
        )
        + '\n',
    )

    # fg hook that checks the marker file does NOT exist yet (cleanup hasn't fired)
    _write(
        plugins_root / 'checker' / 'on_Snapshot__10_check.py',
        '\n'.join(
            [
                'import json',
                'from pathlib import Path',
                f'marker_exists = Path({str(marker)!r}).exists()',
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": f"marker_during_hooks={marker_exists}"}))',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)

    # The fg hook must see no marker (cleanup hasn't happened yet)
    checker_result = next(r for r in results if r.plugin == 'checker')
    assert checker_result.output_str == 'marker_during_hooks=False', (
        'Cleanup fired before fg hooks finished — ordering is broken'
    )

    # After download() returns, the marker must exist (cleanup did fire)
    assert marker.exists(), (
        'Cleanup event never SIGTERMed the bg daemon — cleanup event not emitted'
    )


def test_binary_loaded_vs_installed_events(tmp_path: Path, monkeypatch) -> None:
    """Verify BinaryLoadedEvent for pre-existing and BinaryInstalledEvent for provider-installed.

    Sets up two binaries in the same run:
    - "preloaded": hook outputs Binary with abspath already set (detected on disk)
    - "installme": hook outputs Binary without abspath (needs provider to install)

    Expected event flow:
    - preloaded: BinaryLoadedEvent only (no install needed)
    - installme: BinaryLoadedEvent (from provider's nested BinaryEvent with abspath)
                 + BinaryInstalledEvent (from original event after provider resolved it)

    Uses monkeypatched EventBus.__init__ to capture informational events.
    """
    plugins_root = tmp_path / 'plugins'
    captured: list[tuple[str, str, str]] = []

    _orig_init = EventBus.__init__

    def _capturing_init(self, *a, **kw):
        _orig_init(self, *a, **kw)

        async def on_loaded(e: BinaryLoadedEvent) -> None:
            captured.append(('loaded', e.name, e.abspath))

        async def on_installed(e: BinaryInstalledEvent) -> None:
            captured.append(('installed', e.name, e.abspath))

        self.on(BinaryLoadedEvent, on_loaded)
        self.on(BinaryInstalledEvent, on_installed)

    monkeypatch.setattr(EventBus, '__init__', _capturing_init)

    # Hook that emits two Binary requests: one pre-existing, one needing install
    preloaded_path = str(tmp_path / 'bin' / 'preloaded')
    (tmp_path / 'bin').mkdir()
    Path(preloaded_path).write_text('stub')

    _write(
        plugins_root / 'emitter' / 'on_Crawl__00_emit.py',
        '\n'.join(
            [
                'import json',
                # Pre-existing binary — abspath already known
                f'print(json.dumps({{"type": "Binary", "name": "preloaded", "abspath": {preloaded_path!r}}}))',
                # Binary that needs provider installation
                'print(json.dumps({"type": "Binary", "name": "installme", "binproviders": "provider"}))',
            ]
        )
        + '\n',
    )

    # Provider hook that "installs" the binary
    _write(
        plugins_root / 'provider' / 'on_Binary__10_provider_install.py',
        '\n'.join(
            [
                'import json',
                'import argparse',
                'from pathlib import Path',
                'parser = argparse.ArgumentParser()',
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binary-id", required=True)',
                'parser.add_argument("--machine-id", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                'parser.add_argument("--overrides", default=None)',
                'parser.add_argument("--custom-cmd", default=None)',
                'args = parser.parse_args()',
                'bin_path = Path.cwd() / "bin" / args.name',
                'bin_path.parent.mkdir(parents=True, exist_ok=True)',
                'bin_path.write_text("installed")',
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path),'
                ' "binary_id": args.binary_id, "machine_id": args.machine_id,'
                ' "binprovider": "provider"}))',
            ]
        )
        + '\n',
    )

    # Consumer hook verifies both binaries are available via env vars
    _write(
        plugins_root / 'consumer' / 'on_Crawl__01_check.py',
        '\n'.join(
            [
                'import json',
                'import os',
                'print(json.dumps({',
                '    "type": "ArchiveResult",',
                '    "status": "succeeded",',
                '    "output_str": "|".join([',
                '        os.environ.get("PRELOADED_BINARY", "missing"),',
                '        os.environ.get("INSTALLME_BINARY", "missing"),',
                '    ]),',
                '}))',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)

    # --- Verify informational events ---

    loaded_preloaded = [e for e in captured if e[0] == 'loaded' and e[1] == 'preloaded']
    installed_preloaded = [e for e in captured if e[0] == 'installed' and e[1] == 'preloaded']

    assert len(loaded_preloaded) == 1, (
        f'Expected exactly 1 BinaryLoadedEvent for preloaded, got {len(loaded_preloaded)}: {loaded_preloaded}'
    )
    assert loaded_preloaded[0][2] == preloaded_path, (
        f'BinaryLoadedEvent abspath mismatch: {loaded_preloaded[0][2]!r} != {preloaded_path!r}'
    )
    assert len(installed_preloaded) == 0, (
        f'Pre-existing binary should not emit BinaryInstalledEvent, got: {installed_preloaded}'
    )

    loaded_installme = [e for e in captured if e[0] == 'loaded' and e[1] == 'installme']
    installed_installme = [e for e in captured if e[0] == 'installed' and e[1] == 'installme']

    assert len(loaded_installme) == 1, (
        f'Expected 1 BinaryLoadedEvent for installme (from provider nested event), '
        f'got {len(loaded_installme)}: {loaded_installme}'
    )
    assert len(installed_installme) == 1, (
        f'Expected 1 BinaryInstalledEvent for installme, got {len(installed_installme)}: {installed_installme}'
    )
    # Both events should report the same resolved path
    assert loaded_installme[0][2] == installed_installme[0][2], (
        f'BinaryLoadedEvent path {loaded_installme[0][2]!r} != '
        f'BinaryInstalledEvent path {installed_installme[0][2]!r}'
    )

    # --- Verify config propagation (both binaries visible to subsequent hooks) ---

    consumer = next(r for r in results if r.plugin == 'consumer')
    parts = consumer.output_str.split('|')
    assert parts[0] == preloaded_path, (
        f'PRELOADED_BINARY env var mismatch: {parts[0]!r}'
    )
    assert parts[1] != 'missing', (
        'INSTALLME_BINARY not set in consumer env — install chain broken'
    )


def test_archive_result_events_inline_vs_enriched(tmp_path: Path, monkeypatch) -> None:
    """Verify inline ArchiveResultEvent (no process_id) and enriched (with process_id).

    A hook outputs an inline ArchiveResult during execution. After the process
    completes, ArchiveResultService emits an enriched ArchiveResultEvent with
    process metadata. The orchestrator only collects the enriched one.
    """
    plugins_root = tmp_path / 'plugins'
    ar_events: list[tuple[str, str, str]] = []  # (process_id, hook_name, status)

    _orig_init = EventBus.__init__

    def _capturing_init(self, *a, **kw):
        _orig_init(self, *a, **kw)

        async def on_ar(e: ArchiveResultEvent) -> None:
            ar_events.append((e.process_id, e.hook_name, e.status))

        self.on(ArchiveResultEvent, on_ar)

    monkeypatch.setattr(EventBus, '__init__', _capturing_init)

    _write(
        plugins_root / 'demo' / 'on_Snapshot__10_emit.py',
        '\n'.join(
            [
                'import json',
                # Inline ArchiveResult during execution (informational)
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "partial"}))',
            ]
        )
        + '\n',
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download('https://example.com', plugins, tmp_path / 'run', auto_install=True)

    # Should have at least 2 ArchiveResultEvents for this hook:
    # 1 inline (no process_id), 1 enriched (with process_id)
    demo_events = [e for e in ar_events if e[1] == 'on_Snapshot__10_emit']
    inline = [e for e in demo_events if not e[0]]
    enriched = [e for e in demo_events if e[0]]

    assert len(inline) >= 1, (
        f'Expected at least 1 inline ArchiveResultEvent, got {len(inline)}'
    )
    assert len(enriched) == 1, (
        f'Expected exactly 1 enriched ArchiveResultEvent, got {len(enriched)}: {enriched}'
    )
    assert enriched[0][2] == 'succeeded'

    # Only the enriched event should produce results in the return list
    demo_results = [r for r in results if isinstance(r, ArchiveResult) and r.hook_name == 'on_Snapshot__10_emit']
    assert len(demo_results) == 1, (
        f'Expected 1 ArchiveResult in results list (from enriched event only), got {len(demo_results)}'
    )
