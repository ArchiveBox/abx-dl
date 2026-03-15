import json
from pathlib import Path

from abx_dl.executor import download
from abx_dl.models import ArchiveResult
from abx_dl.plugins import discover_plugins


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


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
    results = list(download('https://example.com', plugins, tmp_path / 'run', auto_install=True))

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
    results = list(download('https://example.com', plugins, tmp_path / 'run', auto_install=True))

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
    results = [
        result
        for result in download('https://example.com', plugins, tmp_path / 'run', auto_install=True)
        if result.plugin == 'bgdemo' and result.hook_name == 'on_Snapshot__05_wait.bg'
    ]

    assert [result.status for result in results] == ['started', 'succeeded']
    assert results[-1].output_str == 'bg cleaned up'
    assert results[0].process_id == results[-1].process_id
    assert results[0].start_ts == results[-1].start_ts

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
    results = list(download('https://example.com', plugins, tmp_path / 'run', auto_install=True))

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
    results = list(download('https://example.com', plugins, tmp_path / 'run', auto_install=True))

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
    results = list(
        download(
            'https://example.com',
            plugins,
            tmp_path / 'run',
            auto_install=True,
            emit_jsonl=False,
        )
    )

    captured = capsys.readouterr()

    assert [result.status for result in results] == ['succeeded']
    assert captured.out == ''


def test_cleanup_does_not_refinalize_failed_foreground_hooks(tmp_path: Path) -> None:
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
    results = [result for result in download('https://example.com', plugins, tmp_path / 'run', auto_install=True) if result.plugin == 'foreground']

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
    results = [result for result in download('https://example.com', plugins, tmp_path / 'run', auto_install=True)]

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
    results = [result for result in download('https://example.com', plugins, tmp_path / 'run', auto_install=True)]

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
        for result in download('https://example.com', plugins, tmp_path / 'run', auto_install=True)
        if isinstance(result, ArchiveResult)
    ]

    assert [result.output_str for result in results] == ['media.mp4']
