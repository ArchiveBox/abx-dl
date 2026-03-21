import asyncio
import json
from pathlib import Path

from abx_dl.orchestrator import create_bus, download
from abx_dl.events import ArchiveResultEvent, BinaryInstalledEvent, SnapshotEvent
from abx_dl.models import ArchiveResult, Snapshot
from abx_dl.models import discover_plugins


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix != ".json" and content and not content.startswith("#!"):
        content = "#!/usr/bin/env python3\n" + content
    path.write_text(content)
    path.chmod(0o755)


def _run_download(*args, **kwargs):
    """Helper to run async download() from sync test code."""
    return asyncio.run(download(*args, **kwargs))


def test_download_dispatches_binary_hooks_and_applies_machine_updates(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "producer" / "on_Crawl__00_emit.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "Binary", "name": "demo", "binproviders": "provider"}))',
                'print(json.dumps({"type": "Machine", "config": {"DEMO_FLAG": "ready"}}))',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "provider" / "on_Binary__10_provider_install.py",
        "\n".join(
            [
                "import json",
                "import os",
                "from pathlib import Path",
                "import argparse",
                "parser = argparse.ArgumentParser()",
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                'parser.add_argument("--overrides", default=None)',
                "args = parser.parse_args()",
                'context = json.loads(os.environ["EXTRA_CONTEXT"])',
                'bin_path = Path.cwd() / "bin" / args.name',
                "bin_path.parent.mkdir(parents=True, exist_ok=True)",
                'bin_path.write_text("demo")',
                'node_modules = Path.cwd() / "lib" / "node_modules"',
                "node_modules.mkdir(parents=True, exist_ok=True)",
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path), "binary_id": context["binary_id"], "machine_id": context["machine_id"], "plugin_name": context["plugin_name"], "hook_name": context["hook_name"], "binprovider": "provider"}))',
                'print(json.dumps({"type": "Machine", "config": {"NODE_MODULES_DIR": str(node_modules), "NODE_PATH": str(node_modules)}}))',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "consumer" / "on_Snapshot__01_check.py",
        "\n".join(
            [
                "import json",
                "import os",
                "print(json.dumps({",
                '    "type": "ArchiveResult",',
                '    "status": "succeeded",',
                '    "output_str": "|".join([',
                '        os.environ.get("DEMO_BINARY", ""),',
                '        os.environ.get("DEMO_FLAG", ""),',
                '        os.environ.get("NODE_PATH", ""),',
                "    ]),",
                "}))",
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    # producer hook is an on_Crawl hook (emits Binary/Machine records) — these
    # don't produce ArchiveResults (only on_Snapshot hooks do)

    # consumer hook emits an ArchiveResult during snapshot after crawl setup propagated env vars
    consumer_result = next(result for result in results if result.plugin == "consumer")
    assert consumer_result.status == "succeeded"
    assert consumer_result.output_str.endswith("|ready|" + str(tmp_path / "run" / "provider" / "lib" / "node_modules"))

    index_lines = [json.loads(line) for line in (tmp_path / "run" / "index.jsonl").read_text().splitlines()]
    assert any(
        line.get("type") == "Process" and "on_Binary__10_provider_install.py" in " ".join(line.get("cmd", [])) for line in index_lines
    )
    provider_process = next(
        line
        for line in index_lines
        if line.get("type") == "Process" and "on_Binary__10_provider_install.py" in " ".join(line.get("cmd", []))
    )
    assert "--binary-id=" not in " ".join(provider_process["cmd"])
    assert "--machine-id=" not in " ".join(provider_process["cmd"])
    assert "--plugin-name=" not in " ".join(provider_process["cmd"])
    assert "--hook-name=" not in " ".join(provider_process["cmd"])


def test_download_forwards_binary_min_version_to_provider_hooks(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "producer" / "on_Crawl__00_emit.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "Binary", "name": "java", "binproviders": "provider", "min_version": "11.0.0"}))',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "provider" / "on_Binary__10_provider_install.py",
        "\n".join(
            [
                "import argparse",
                "import json",
                "import os",
                "from pathlib import Path",
                "parser = argparse.ArgumentParser()",
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                'parser.add_argument("--min-version", default="")',
                'parser.add_argument("--overrides", default=None)',
                "args = parser.parse_args()",
                'context = json.loads(os.environ["EXTRA_CONTEXT"])',
                'bin_path = Path.cwd() / "bin" / args.name',
                "bin_path.parent.mkdir(parents=True, exist_ok=True)",
                'bin_path.write_text("demo")',
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path), "binary_id": context["binary_id"], "machine_id": context["machine_id"], "plugin_name": context["plugin_name"], "hook_name": context["hook_name"], "binprovider": "provider"}))',
                'print(json.dumps({"type": "Machine", "config": {"JAVA_VERSION_CONSTRAINT": args.min_version}}))',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "consumer" / "on_Snapshot__01_check.py",
        "\n".join(
            [
                "import json",
                "import os",
                "print(json.dumps({",
                '    "type": "ArchiveResult",',
                '    "status": "succeeded",',
                '    "output_str": os.environ.get("JAVA_VERSION_CONSTRAINT", ""),',
                "}))",
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    consumer_result = next(result for result in results if result.plugin == "consumer")
    assert consumer_result.status == "succeeded"
    assert consumer_result.output_str == "11.0.0"


def test_snapshot_hooks_receive_snapshot_id_via_extra_context_env(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "demo" / "on_Snapshot__01_check.py",
        "\n".join(
            [
                "import argparse",
                "import json",
                "import os",
                "parser = argparse.ArgumentParser()",
                'parser.add_argument("--url", required=True)',
                "args = parser.parse_args()",
                'context = json.loads(os.environ["EXTRA_CONTEXT"])',
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": args.url + "|" + context.get("snapshot_id", "")}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    result = next(r for r in results if r.plugin == "demo")
    url, snapshot_id = result.output_str.split("|", 1)
    assert url == "https://example.com"
    assert snapshot_id

    index_lines = [json.loads(line) for line in (tmp_path / "run" / "index.jsonl").read_text().splitlines()]
    process = next(line for line in index_lines if line.get("type") == "Process" and line.get("plugin") == "demo")
    assert "--snapshot-id=" not in " ".join(process["cmd"])


def test_download_sets_plugin_specific_binary_env_from_binary_default(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "demo" / "config.json",
        "\n".join(
            [
                "{",
                '  "properties": {',
                '    "DEMO_BINARY": {',
                '      "type": "string",',
                '      "default": "demo-tool"',
                "    }",
                "  }",
                "}",
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "demo" / "on_Crawl__00_emit.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "Binary", "name": "demo-tool", "binproviders": "provider"}))',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "demo" / "on_Snapshot__01_check.py",
        "\n".join(
            [
                "import json",
                "import os",
                "print(json.dumps({",
                '    "type": "ArchiveResult",',
                '    "status": "succeeded",',
                '    "output_str": "|".join([',
                '        os.environ.get("DEMO_BINARY", ""),',
                '        os.environ.get("DEMO_TOOL_BINARY", ""),',
                "    ]),",
                "}))",
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "provider" / "on_Binary__10_provider_install.py",
        "\n".join(
            [
                "import argparse",
                "import json",
                "import os",
                "from pathlib import Path",
                "parser = argparse.ArgumentParser()",
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                'parser.add_argument("--min-version", default="")',
                'parser.add_argument("--overrides", default=None)',
                "args = parser.parse_args()",
                'context = json.loads(os.environ["EXTRA_CONTEXT"])',
                'bin_path = Path.cwd() / "bin" / args.name',
                "bin_path.parent.mkdir(parents=True, exist_ok=True)",
                'bin_path.write_text("demo")',
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path), "binary_id": context["binary_id"], "machine_id": context["machine_id"], "plugin_name": context["plugin_name"], "hook_name": context["hook_name"], "binprovider": "provider"}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    demo_result = next(result for result in results if result.plugin == "demo")
    generic_key = str(tmp_path / "run" / "provider" / "bin" / "demo-tool")
    assert demo_result.status == "succeeded"
    assert demo_result.output_str == f"{generic_key}|{generic_key}"


def test_download_applies_side_effects_from_completed_background_hooks(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "producer" / "on_Crawl__00_emit.bg.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "Binary", "name": "demo", "binproviders": "provider"}), flush=True)',
                'print(json.dumps({"type": "Machine", "config": {"DEMO_FLAG": "ready"}}), flush=True)',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "delay" / "on_Crawl__01_wait.py",
        "\n".join(
            [
                "import time",
                "time.sleep(0.2)",
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "provider" / "on_Binary__10_provider_install.py",
        "\n".join(
            [
                "import json",
                "import os",
                "from pathlib import Path",
                "import argparse",
                "parser = argparse.ArgumentParser()",
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                'parser.add_argument("--overrides", default=None)',
                "args = parser.parse_args()",
                'context = json.loads(os.environ["EXTRA_CONTEXT"])',
                'bin_path = Path.cwd() / "bin" / args.name',
                "bin_path.parent.mkdir(parents=True, exist_ok=True)",
                'bin_path.write_text("demo")',
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path), "binary_id": context["binary_id"], "machine_id": context["machine_id"], "plugin_name": context["plugin_name"], "hook_name": context["hook_name"], "binprovider": "provider"}))',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "consumer" / "on_Snapshot__02_check.py",
        "\n".join(
            [
                "import json",
                "import os",
                "print(json.dumps({",
                '    "type": "ArchiveResult",',
                '    "status": "succeeded",',
                '    "output_str": "|".join([',
                '        os.environ.get("DEMO_BINARY", ""),',
                '        os.environ.get("DEMO_FLAG", ""),',
                "    ]),",
                "}))",
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    consumer_result = next(result for result in results if result.plugin == "consumer")
    assert consumer_result.output_str == str(tmp_path / "run" / "provider" / "bin" / "demo") + "|ready"
    # producer is an on_Crawl hook — no ArchiveResult expected (only on_Snapshot hooks get them)


def test_download_finalizes_background_hooks_after_sigterm(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "bgdemo" / "on_Snapshot__05_wait.bg.py",
        "\n".join(
            [
                "import json",
                "import signal",
                "import sys",
                "import time",
                "",
                "def handle_sigterm(signum, frame):",
                '    print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "bg cleaned up"}), flush=True)',
                "    sys.exit(0)",
                "",
                "signal.signal(signal.SIGTERM, handle_sigterm)",
                'print("[*] background hook ready", flush=True)',
                "while True:",
                "    time.sleep(0.1)",
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "bgdemo" / "on_Snapshot__10_delay.py",
        "\n".join(
            [
                "import time",
                "time.sleep(0.3)",
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    all_results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)
    results = [result for result in all_results if result.plugin == "bgdemo" and result.hook_name == "on_Snapshot__05_wait.bg"]

    # Only the hook's own SIGTERM handler emits the ArchiveResult — no synthetic records.
    assert len(results) == 1
    assert results[0].status == "succeeded"
    assert results[0].output_str == "bg cleaned up"

    index_lines = [json.loads(line) for line in (tmp_path / "run" / "index.jsonl").read_text().splitlines()]
    final_result = next(line for line in reversed(index_lines) if line.get("type") == "ArchiveResult" and line.get("plugin") == "bgdemo")
    final_process = next(
        line
        for line in reversed(index_lines)
        if line.get("type") == "Process" and "on_Snapshot__05_wait.bg.py" in " ".join(line.get("cmd", []))
    )

    assert final_result["status"] == "succeeded"
    assert final_process["exit_code"] == 0


def test_download_preserves_full_hook_stderr_in_archive_result(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"
    full_error = "ERROR: " + ("proxy-blocked " * 80) + "storage.googleapis.com"

    _write(
        plugins_root / "broken" / "on_Snapshot__00_fail.py",
        "\n".join(
            [
                "import sys",
                f"sys.stderr.write({full_error!r})",
                "raise SystemExit(1)",
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    failure = next(result for result in results if result.plugin == "broken")
    assert failure.status == "failed"
    assert failure.error == full_error


def test_pid_file_removed_after_failed_hook_exits(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "broken" / "on_Snapshot__00_fail.py",
        "\n".join(
            [
                "raise SystemExit(1)",
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    pid_file = tmp_path / "run" / "broken" / "on_Snapshot__00_fail.pid"
    assert not pid_file.exists(), "stale pid file left behind after process exit"


def test_download_applies_background_side_effects_in_hook_lifecycle_order(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "crawlbg" / "on_Crawl__90_emit.bg.py",
        "\n".join(
            [
                "import json",
                "import time",
                "time.sleep(0.1)",
                'print(json.dumps({"type": "Machine", "config": {"HOOK_ORDER": "crawl"}}), flush=True)',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "snapshotbg" / "on_Snapshot__05_emit.bg.py",
        "\n".join(
            [
                "import json",
                "import time",
                "time.sleep(0.1)",
                'print(json.dumps({"type": "Machine", "config": {"HOOK_ORDER": "snapshot"}}), flush=True)',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "waiter" / "on_Snapshot__08_wait.py",
        "\n".join(
            [
                "import time",
                "time.sleep(0.2)",
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "consumer" / "on_Snapshot__09_check.py",
        "\n".join(
            [
                "import json",
                "import os",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": os.environ.get("HOOK_ORDER", "")}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    consumer_result = next(result for result in results if result.plugin == "consumer")
    assert consumer_result.output_str == "snapshot"


def test_download_can_suppress_jsonl_stdout(tmp_path: Path, capsys) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "demo" / "on_Snapshot__10_echo.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "ok"}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download(
        "https://example.com",
        plugins,
        tmp_path / "run",
        auto_install=True,
        emit_jsonl=False,
    )

    captured = capsys.readouterr()

    assert [result.status for result in results] == ["succeeded"]
    assert captured.out == ""


def test_cleanup_does_not_duplicate_failed_foreground_results(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "foreground" / "on_Snapshot__10_fail.py",
        "\n".join(
            [
                "import sys",
                'print("foreground failed", file=sys.stderr)',
                "sys.exit(1)",
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = [
        result
        for result in _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)
        if result.plugin == "foreground"
    ]

    assert [result.status for result in results] == ["failed"]

    index_lines = [json.loads(line) for line in (tmp_path / "run" / "index.jsonl").read_text().splitlines()]
    foreground_results = [line for line in index_lines if line.get("type") == "ArchiveResult" and line.get("plugin") == "foreground"]

    assert len(foreground_results) == 1
    assert foreground_results[0]["status"] == "failed"


def test_successful_hook_with_only_logs_produces_noresult(tmp_path: Path) -> None:
    """A hook that exits 0 but doesn't emit ArchiveResult or create content files produces noresult."""
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "cachedemo" / "on_Snapshot__10_install.py",
        "\n".join(
            [
                'print("[*] extension already installed (using cache)")',
                'print("[+] extension setup complete")',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    assert len(results) == 1
    assert results[0].plugin == "cachedemo"
    assert results[0].status == "noresult"


def test_successful_hook_with_skipping_log_produces_noresult(tmp_path: Path) -> None:
    """A hook that exits 0 with only stderr output and no content files produces noresult."""
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "skipdemo" / "on_Snapshot__10_skip.py",
        "\n".join(
            [
                "import sys",
                'print("Skipping skipdemo (SKIPDEMO_ENABLED=False)", file=sys.stderr)',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    assert len(results) == 1
    assert results[0].plugin == "skipdemo"
    assert results[0].status == "noresult"


def test_output_str_is_stored_verbatim_from_hook(tmp_path: Path) -> None:
    """ArchiveResult output_str is stored exactly as the hook reported it."""
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "demo" / "on_Snapshot__10_emit.py",
        "\n".join(
            [
                "import json",
                "from pathlib import Path",
                'output = Path.cwd() / "media.mp4"',
                'output.write_text("demo")',
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": str(output)}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = [
        result
        for result in _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)
        if isinstance(result, ArchiveResult)
    ]

    assert len(results) == 1
    # output_str is stored verbatim — the hook reported an absolute path
    assert results[0].output_str == str(tmp_path / "run" / "demo" / "media.mp4")


def test_cleanup_runs_after_all_hooks_not_interleaved(tmp_path: Path) -> None:
    """Verify that cleanup events fire after all hooks, not interleaved.

    A bg daemon hook writes a marker file on SIGTERM. A fg hook runs after it.
    The cleanup (SIGTERM) must happen only after ALL hooks have run, so the
    marker file must NOT exist when the fg hook runs but MUST exist after
    download() returns.
    """
    plugins_root = tmp_path / "plugins"
    marker = tmp_path / "run" / "bgdemo" / "sigtermed.marker"

    # bg daemon that creates a marker file when SIGTERMed
    _write(
        plugins_root / "bgdemo" / "on_Snapshot__05_daemon.bg.py",
        "\n".join(
            [
                "import json",
                "import signal",
                "import sys",
                "import time",
                "from pathlib import Path",
                "",
                "def handle_sigterm(signum, frame):",
                f'    Path({str(marker)!r}).write_text("killed")',
                '    print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "daemon cleaned up"}), flush=True)',
                "    sys.exit(0)",
                "",
                "signal.signal(signal.SIGTERM, handle_sigterm)",
                'print("[*] daemon ready", flush=True)',
                "while True:",
                "    time.sleep(0.1)",
            ],
        )
        + "\n",
    )

    # fg hook that checks the marker file does NOT exist yet (cleanup hasn't fired)
    _write(
        plugins_root / "checker" / "on_Snapshot__10_check.py",
        "\n".join(
            [
                "import json",
                "from pathlib import Path",
                f"marker_exists = Path({str(marker)!r}).exists()",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": f"marker_during_hooks={marker_exists}"}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    # The fg hook must see no marker (cleanup hasn't happened yet)
    checker_result = next(r for r in results if r.plugin == "checker")
    assert checker_result.output_str == "marker_during_hooks=False", "Cleanup fired before fg hooks finished — ordering is broken"

    # After download() returns, the marker must exist (cleanup did fire)
    assert marker.exists(), "Cleanup event never SIGTERMed the bg daemon — cleanup event not emitted"


def test_bg_daemon_survives_past_timeout(tmp_path: Path) -> None:
    """Background daemons must not be killed by the per-hook timeout.

    Sets up a bg daemon with a 1-second timeout, plus a fg hook that sleeps
    for 2 seconds (longer than the daemon's timeout). The daemon must still be
    alive when the fg hook runs, proving the timeout doesn't apply to bg hooks.
    """
    plugins_root = tmp_path / "plugins"
    alive_marker = tmp_path / "run" / "bgdemo" / "daemon_alive.marker"

    # bg daemon with a very short timeout — writes a marker every 0.1s
    _write(
        plugins_root / "bgdemo" / "on_Snapshot__05_daemon.bg.py",
        "\n".join(
            [
                "import json",
                "import signal",
                "import sys",
                "import time",
                "from pathlib import Path",
                "",
                "def handle_sigterm(signum, frame):",
                "    sys.exit(0)",
                "",
                "signal.signal(signal.SIGTERM, handle_sigterm)",
                'print("[*] daemon ready", flush=True)',
                "while True:",
                f"    Path({str(alive_marker)!r}).write_text(str(time.time()))",
                "    time.sleep(0.1)",
            ],
        )
        + "\n",
    )

    # fg hook that sleeps 2s (longer than the 1s timeout), then checks daemon
    _write(
        plugins_root / "checker" / "on_Snapshot__10_check.py",
        "\n".join(
            [
                "import json",
                "import time",
                "from pathlib import Path",
                "time.sleep(2)",
                f"alive = Path({str(alive_marker)!r}).exists()",
                # Read mtime to verify daemon wrote recently (within last 1s)
                f"mtime = Path({str(alive_marker)!r}).stat().st_mtime if alive else 0",
                "recent = (time.time() - mtime) < 1.0 if alive else False",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": f"alive={alive},recent={recent}"}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    # BGDEMO_TIMEOUT=1 sets a 1-second timeout for the bgdemo plugin only.
    # The bg daemon must survive past this timeout (killed only at cleanup).
    results = _run_download(
        "https://example.com",
        plugins,
        tmp_path / "run",
        auto_install=True,
        config_overrides={"BGDEMO_TIMEOUT": "1"},
    )

    checker_result = next(r for r in results if r.plugin == "checker")
    assert checker_result.output_str == "alive=True,recent=True", (
        f"Background daemon was killed by timeout instead of surviving: {checker_result.output_str}"
    )


def test_crawl_bg_daemon_does_not_block_snapshot_phase(tmp_path: Path) -> None:
    """Crawl-scoped bg daemons must not block transition into snapshot hooks."""
    plugins_root = tmp_path / "plugins"
    alive_marker = tmp_path / "run" / "crawlbg" / "alive.marker"
    cleaned_marker = tmp_path / "run" / "crawlbg" / "cleaned.marker"

    _write(
        plugins_root / "crawlbg" / "on_Crawl__05_daemon.bg.py",
        "\n".join(
            [
                "import signal",
                "import sys",
                "import time",
                "from pathlib import Path",
                "",
                "def handle_sigterm(signum, frame):",
                f"    Path({str(cleaned_marker)!r}).write_text('cleaned')",
                "    sys.exit(0)",
                "",
                "signal.signal(signal.SIGTERM, handle_sigterm)",
                'print("[*] crawl daemon ready", flush=True)',
                "while True:",
                f"    Path({str(alive_marker)!r}).write_text(str(time.time()))",
                "    time.sleep(0.05)",
            ],
        )
        + "\n",
    )

    _write(
        plugins_root / "checker" / "on_Snapshot__01_check.py",
        "\n".join(
            [
                "import json",
                "import time",
                "from pathlib import Path",
                "time.sleep(0.2)",
                f"alive = Path({str(alive_marker)!r}).exists()",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": f"crawlbg_alive={alive}"}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    checker_result = next(r for r in results if r.plugin == "checker")
    assert checker_result.output_str == "crawlbg_alive=True"
    assert cleaned_marker.exists(), "Crawl cleanup never SIGTERMed the crawl-scoped bg daemon"


def test_explicit_depth_one_snapshot_runs_snapshot_hooks(tmp_path: Path) -> None:
    """Explicit child snapshot runs should still execute snapshot hooks."""
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "childcheck" / "on_Snapshot__10_check.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "child-depth-hook-ran"}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    child_snapshot = Snapshot(url="https://example.com/child", depth=1, id="child-depth-1")
    results = _run_download(
        "https://example.com/child",
        plugins,
        tmp_path / "run",
        auto_install=True,
        snapshot=child_snapshot,
        skip_crawl_setup=True,
        skip_crawl_cleanup=True,
    )

    checker_result = next(r for r in results if r.plugin == "childcheck")
    assert checker_result.output_str == "child-depth-hook-ran"


def test_binary_installed_events(tmp_path: Path) -> None:
    """Verify BinaryInstalledEvent fires for both pre-existing and provider-installed binaries.

    Sets up two binaries in the same run:
    - "preloaded": hook outputs Binary with abspath already set (detected on disk)
    - "installme": hook outputs Binary without abspath (needs provider to install)

    Both should emit BinaryInstalledEvent with the resolved abspath.
    """
    plugins_root = tmp_path / "plugins"
    captured: list[tuple[str, str]] = []  # (name, abspath)
    bus = create_bus(total_timeout=60.0)

    async def on_installed(e: BinaryInstalledEvent) -> None:
        captured.append((e.name, e.abspath))

    bus.on(BinaryInstalledEvent, on_installed)

    # Hook that emits two Binary requests: one pre-existing, one needing install
    preloaded_path = str(tmp_path / "bin" / "preloaded")
    (tmp_path / "bin").mkdir()
    Path(preloaded_path).write_text("stub")

    _write(
        plugins_root / "emitter" / "on_Crawl__00_emit.py",
        "\n".join(
            [
                "import json",
                # Pre-existing binary — abspath already known
                f'print(json.dumps({{"type": "Binary", "name": "preloaded", "abspath": {preloaded_path!r}}}))',
                # Binary that needs provider installation
                'print(json.dumps({"type": "Binary", "name": "installme", "binproviders": "provider"}))',
            ],
        )
        + "\n",
    )

    # Provider hook that "installs" the binary
    _write(
        plugins_root / "provider" / "on_Binary__10_provider_install.py",
        "\n".join(
            [
                "import json",
                "import argparse",
                "import os",
                "from pathlib import Path",
                "parser = argparse.ArgumentParser()",
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                'parser.add_argument("--overrides", default=None)',
                'parser.add_argument("--custom-cmd", default=None)',
                "args = parser.parse_args()",
                'context = json.loads(os.environ["EXTRA_CONTEXT"])',
                'bin_path = Path.cwd() / "bin" / args.name',
                "bin_path.parent.mkdir(parents=True, exist_ok=True)",
                'bin_path.write_text("installed")',
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path),'
                ' "binary_id": context["binary_id"], "machine_id": context["machine_id"],'
                ' "plugin_name": context["plugin_name"], "hook_name": context["hook_name"],'
                ' "binprovider": "provider"}))',
            ],
        )
        + "\n",
    )

    # Consumer hook verifies both binaries are available via env vars
    _write(
        plugins_root / "consumer" / "on_Snapshot__01_check.py",
        "\n".join(
            [
                "import json",
                "import os",
                "print(json.dumps({",
                '    "type": "ArchiveResult",',
                '    "status": "succeeded",',
                '    "output_str": "|".join([',
                '        os.environ.get("PRELOADED_BINARY", "missing"),',
                '        os.environ.get("INSTALLME_BINARY", "missing"),',
                "    ]),",
                "}))",
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True, bus=bus)

    # --- Verify BinaryInstalledEvent for both binaries ---

    preloaded_events = [(n, p) for n, p in captured if n == "preloaded"]
    installme_events = [(n, p) for n, p in captured if n == "installme"]

    assert len(preloaded_events) == 1, f"Expected BinaryInstalledEvent for preloaded, got: {preloaded_events}"
    assert preloaded_events[0][1] == preloaded_path, (
        f"BinaryInstalledEvent abspath mismatch: {preloaded_events[0][1]!r} != {preloaded_path!r}"
    )

    assert len(installme_events) == 1, f"Expected BinaryInstalledEvent for installme, got: {installme_events}"

    # --- Verify config propagation (both binaries visible to subsequent hooks) ---

    consumer = next(r for r in results if r.plugin == "consumer")
    parts = consumer.output_str.split("|")
    assert parts[0] == preloaded_path, f"PRELOADED_BINARY env var mismatch: {parts[0]!r}"
    assert parts[1] != "missing", "INSTALLME_BINARY not set in consumer env — install chain broken"


def test_archive_result_events_no_synthetic_when_inline_reported(tmp_path: Path) -> None:
    """When a hook reports an inline ArchiveResult, no synthetic one is emitted.

    A hook that outputs ``{"type": "ArchiveResult", ...}`` JSONL during execution
    gets exactly one ArchiveResultEvent (the inline one). ProcessCompletedEvent
    sees the existing result via bus.find and skips synthetic emission.
    """
    plugins_root = tmp_path / "plugins"
    ar_events: list[tuple[str, str]] = []  # (hook_name, status)
    bus = create_bus(total_timeout=60.0)

    async def on_ar(e: ArchiveResultEvent) -> None:
        ar_events.append((e.hook_name, e.status))

    bus.on(ArchiveResultEvent, on_ar)

    _write(
        plugins_root / "demo" / "on_Snapshot__10_emit.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "partial"}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True, bus=bus)

    # Should have exactly 1 ArchiveResultEvent — the inline one.
    # No synthetic duplicate from ProcessCompletedEvent.
    demo_events = [e for e in ar_events if e[0] == "on_Snapshot__10_emit"]
    assert len(demo_events) == 1, f"Expected exactly 1 ArchiveResultEvent (inline only), got {len(demo_events)}: {demo_events}"
    assert demo_events[0][1] == "succeeded"

    demo_results = [r for r in results if isinstance(r, ArchiveResult) and r.hook_name == "on_Snapshot__10_emit"]
    assert len(demo_results) == 1, f"Expected 1 ArchiveResult in results list, got {len(demo_results)}"


def test_nested_snapshot_events_are_emitted_but_ignored_by_snapshot_hooks(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"
    bus = create_bus(total_timeout=60.0)
    seen_snapshot_events: list[tuple[int, str]] = []

    async def on_snapshot(event: SnapshotEvent) -> None:
        seen_snapshot_events.append((event.depth, event.url))

    bus.on(SnapshotEvent, on_snapshot)

    _write(
        plugins_root / "producer" / "on_Snapshot__10_emit_nested.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "Snapshot", "url": "https://example.com/child", "depth": 1}))',
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "producer"}))',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "observer" / "on_Snapshot__20_record.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "observer"}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True, bus=bus)

    assert seen_snapshot_events == [
        (0, "https://example.com"),
        (1, "https://example.com/child"),
    ]

    producer_results = [result for result in results if result.plugin == "producer"]
    observer_results = [result for result in results if result.plugin == "observer"]

    assert len(producer_results) == 1
    assert producer_results[0].output_str == "producer"
    assert len(observer_results) == 1
    assert observer_results[0].output_str == "observer"
