import asyncio
import json
from pathlib import Path

from abxbus import EventBus

from abx_dl.orchestrator import create_bus, download, install_plugins
from abx_dl.events import ArchiveResultEvent, BinaryEvent, BinaryInstalledEvent, SnapshotEvent
from abx_dl.models import ArchiveResult, Plugin, Snapshot
from abx_dl.models import INSTALL_URL, discover_plugins
from abx_dl.output_files import OutputFile
from abx_dl.services.binary_service import BinaryService
from abx_dl.services.machine_service import MachineService


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


def test_install_url_runs_finite_background_crawl_hooks_to_completion(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"
    captured: list[tuple[str, str]] = []
    bus = create_bus(total_timeout=120.0)

    async def on_installed(event: BinaryInstalledEvent) -> None:
        captured.append((event.name, event.abspath))

    bus.on(BinaryInstalledEvent, on_installed)

    _write(
        plugins_root / "emitter" / "on_Crawl__10_emit_install.finite.bg.py",
        "\n".join(
            [
                "import json",
                "import time",
                "time.sleep(0.1)",
                'print(json.dumps({"type": "Binary", "name": "demo", "binproviders": "provider"}), flush=True)',
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
                "from pathlib import Path",
                "parser = argparse.ArgumentParser()",
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                'parser.add_argument("--overrides", default=None)',
                "args = parser.parse_args()",
                'bin_path = Path.cwd() / "bin" / args.name',
                "bin_path.parent.mkdir(parents=True, exist_ok=True)",
                'bin_path.write_text("demo")',
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path), "version": "1.0.0", "binprovider": "provider"}), flush=True)',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    asyncio.run(download(INSTALL_URL, plugins, tmp_path / "install-run", auto_install=True, emit_jsonl=False, bus=bus))

    assert captured
    assert captured[-1][0] == "demo"
    assert Path(captured[-1][1]).is_absolute()


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


def test_binary_installed_event_preserves_child_provider_metadata(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "producer" / "on_Crawl__00_emit.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "Binary", "name": "demo", "binproviders": "provider"}))',
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
                'parser.add_argument("--overrides", default=None)',
                "args = parser.parse_args()",
                'context = json.loads(os.environ["EXTRA_CONTEXT"])',
                'bin_path = Path.cwd() / "bin" / args.name',
                "bin_path.parent.mkdir(parents=True, exist_ok=True)",
                'bin_path.write_text("demo")',
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path), "version": "1.2.3", "sha256": "abc123", "binary_id": context["binary_id"], "machine_id": context["machine_id"], "plugin_name": context["plugin_name"], "hook_name": context["hook_name"], "binprovider": "provider"}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    bus = EventBus(name="binary_installed_metadata_test")
    installed_events: list[BinaryInstalledEvent] = []

    async def on_installed(event: BinaryInstalledEvent) -> None:
        installed_events.append(event)

    bus.on(BinaryInstalledEvent, on_installed)

    _run_download(
        "https://example.com",
        plugins,
        tmp_path / "run",
        auto_install=True,
        bus=bus,
    )

    demo_events = [event for event in installed_events if event.name == "demo"]
    assert demo_events
    assert all(event.version == "1.2.3" for event in demo_events)
    assert all(event.sha256 == "abc123" for event in demo_events)
    assert all(event.binproviders == "provider" for event in demo_events)
    assert all(event.binprovider == "provider" for event in demo_events)


def test_binary_installed_event_uses_machine_config_seeded_from_persistent_config() -> None:
    from abx_dl.config import set_config

    set_config(None, MERCURY_BINARY="/opt/homebrew/bin/postlight-parser")

    async def run() -> list[BinaryInstalledEvent]:
        bus = create_bus(total_timeout=10.0)
        machine = MachineService(bus)
        BinaryService(
            bus,
            machine=machine,
            plugins={"mercury": Plugin(name="mercury", path=Path("."), hooks=[], config_schema={})},
            auto_install=True,
        )

        installed_events: list[BinaryInstalledEvent] = []

        async def on_installed(event: BinaryInstalledEvent) -> None:
            installed_events.append(event)

        bus.on(BinaryInstalledEvent, on_installed)
        try:
            await bus.emit(
                BinaryEvent(
                    name="postlight-parser",
                    plugin_name="mercury",
                    hook_name="on_Crawl__40_mercury_install.finite.bg",
                    output_dir=".",
                    binproviders="env",
                    overrides={"env": {"abspath": "/opt/homebrew/bin/postlight-parser"}},
                ),
            )
            return installed_events
        finally:
            await bus.stop()

    demo_events = [event for event in asyncio.run(run()) if event.name == "postlight-parser"]
    assert demo_events
    assert Path(str(demo_events[-1].abspath)).name == "postlight-parser"
    assert demo_events[-1].binproviders == "env"


def test_binary_installed_event_resolves_config_backed_command_name() -> None:
    async def run() -> list[BinaryInstalledEvent]:
        bus = create_bus(total_timeout=10.0)
        machine = MachineService(bus, initial_config={"DEMO_BINARY": "python3"})
        BinaryService(
            bus,
            machine=machine,
            plugins={
                "demo": Plugin(
                    name="demo",
                    path=Path("."),
                    hooks=[],
                    config_schema={"DEMO_BINARY": {"type": "string", "default": "python3"}},
                ),
            },
            auto_install=True,
        )

        installed_events: list[BinaryInstalledEvent] = []

        async def on_installed(event: BinaryInstalledEvent) -> None:
            installed_events.append(event)

        bus.on(BinaryInstalledEvent, on_installed)
        try:
            await bus.emit(
                BinaryEvent(
                    name="python3",
                    plugin_name="demo",
                    hook_name="on_Crawl__00_demo_install",
                    output_dir=".",
                    binproviders="env",
                ),
            )
            return installed_events
        finally:
            await bus.stop()

    demo_events = [event for event in asyncio.run(run()) if event.name == "python3"]
    assert demo_events
    assert Path(str(demo_events[-1].abspath)).is_absolute()
    assert Path(str(demo_events[-1].abspath)).name.startswith("python")
    assert demo_events[-1].abspath != "python3"
    assert demo_events[-1].binprovider == "env"
    assert demo_events[-1].version


def test_binary_event_uses_cached_config_binary_before_provider_hooks(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"
    cached_binary = tmp_path / "config-bin" / "demo-tool"
    cached_binary.parent.mkdir(parents=True, exist_ok=True)
    cached_binary.write_text("demo")
    cached_binary.chmod(0o755)

    _write(
        plugins_root / "producer" / "on_Crawl__00_emit.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "Binary", "name": "demo-tool", "binproviders": "provider,env"}))',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "env" / "on_Binary__00_env_discover.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "Binary", "name": "demo-tool", "abspath": "/tmp/env/demo-tool", "version": "0.1.0", "binprovider": "env"}))',
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
                "from pathlib import Path",
                "parser = argparse.ArgumentParser()",
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                'parser.add_argument("--overrides", default=None)',
                "args = parser.parse_args()",
                'Path.cwd().joinpath("provider-ran.txt").write_text(args.name)',
                'bin_path = Path.cwd() / "bin" / args.name',
                "bin_path.parent.mkdir(parents=True, exist_ok=True)",
                'bin_path.write_text("demo")',
                'print(json.dumps({"type": "Binary", "name": args.name, "abspath": str(bin_path), "version": "1.2.3", "sha256": "abc123", "binprovider": "provider"}))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    bus = create_bus(total_timeout=60.0)
    machine = MachineService(bus, initial_config={"DEMO_TOOL_BINARY": str(cached_binary)})
    BinaryService(bus, machine=machine, plugins=plugins, auto_install=True)
    installed_events: list[BinaryInstalledEvent] = []

    async def on_installed(event: BinaryInstalledEvent) -> None:
        installed_events.append(event)

    bus.on(BinaryInstalledEvent, on_installed)
    try:
        _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True, bus=bus)
    finally:
        asyncio.run(bus.stop())

    demo_events = [event for event in installed_events if event.name == "demo-tool"]
    assert demo_events
    assert demo_events[-1].binprovider == "env"
    assert demo_events[-1].abspath == str(cached_binary)
    assert not (tmp_path / "run" / "provider" / "provider-ran.txt").exists()


def test_binary_event_uses_lib_bin_binary_before_provider_hooks(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"
    lib_bin_dir = tmp_path / "lib-bin"
    cached_binary = lib_bin_dir / "demo-tool"
    lib_bin_dir.mkdir(parents=True, exist_ok=True)
    cached_binary.write_text("demo")
    cached_binary.chmod(0o755)

    _write(
        plugins_root / "producer" / "on_Crawl__00_emit.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "Binary", "name": "demo-tool", "binproviders": "provider"}))',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "provider" / "on_Binary__10_provider_install.py",
        "\n".join(
            [
                "from pathlib import Path",
                'Path.cwd().joinpath("provider-ran.txt").write_text("demo-tool")',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    bus = create_bus(total_timeout=60.0)
    installed_events: list[BinaryInstalledEvent] = []

    async def on_installed(event: BinaryInstalledEvent) -> None:
        installed_events.append(event)

    bus.on(BinaryInstalledEvent, on_installed)
    try:
        _run_download(
            "https://example.com",
            plugins,
            tmp_path / "run",
            auto_install=True,
            bus=bus,
            config_overrides={"LIB_BIN_DIR": str(lib_bin_dir)},
        )
    finally:
        asyncio.run(bus.stop())

    demo_events = [event for event in installed_events if event.name == "demo-tool"]
    assert demo_events
    assert demo_events[-1].binprovider == "env"
    assert demo_events[-1].abspath == str(cached_binary)
    assert not (tmp_path / "run" / "provider" / "provider-ran.txt").exists()


def test_binary_installed_event_updates_lib_bin_symlink(tmp_path: Path) -> None:
    async def run() -> None:
        bus = create_bus(total_timeout=10.0)
        machine = MachineService(bus, initial_config={"LIB_BIN_DIR": str(tmp_path / "lib-bin")})
        BinaryService(bus, machine=machine, plugins={}, auto_install=True)

        first_target = tmp_path / "targets" / "first-demo"
        second_target = tmp_path / "targets" / "second-demo"
        first_target.parent.mkdir(parents=True, exist_ok=True)
        first_target.write_text("first")
        second_target.write_text("second")

        try:
            await bus.emit(
                BinaryInstalledEvent(
                    name="demo",
                    plugin_name="demo",
                    hook_name="install",
                    abspath=str(first_target),
                ),
            )
            link_path = tmp_path / "lib-bin" / "demo"
            assert link_path.is_symlink()
            assert link_path.resolve() == first_target.resolve()

            await bus.emit(
                BinaryInstalledEvent(
                    name="demo",
                    plugin_name="demo",
                    hook_name="install",
                    abspath=str(second_target),
                ),
            )
            assert link_path.is_symlink()
            assert link_path.resolve() == second_target.resolve()
        finally:
            await bus.stop()

    asyncio.run(run())


def test_download_creates_default_persona_dir(monkeypatch, tmp_path: Path) -> None:
    import abx_dl.config as config_mod

    monkeypatch.setattr(config_mod, "PERSONAS_DIR", tmp_path / "personas")
    assert not (tmp_path / "personas" / "Default").exists()

    _run_download("https://example.com", {}, tmp_path / "run", auto_install=True)

    assert (tmp_path / "personas" / "Default").is_dir()


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


def test_snapshot_finite_bg_hook_finishes_before_cleanup(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"
    artifact = tmp_path / "run" / "bgdemo" / "artifact.txt"

    _write(
        plugins_root / "bgdemo" / "on_Snapshot__05_write.finite.bg.py",
        "\n".join(
            [
                "import json",
                "import time",
                "from pathlib import Path",
                "",
                "time.sleep(0.3)",
                "output = Path('artifact.txt')",
                "output.write_text('finished')",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "artifact.txt"}), flush=True)',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    result = next(r for r in results if r.plugin == "bgdemo")
    assert result.status == "succeeded"
    assert result.output_str == "artifact.txt"
    assert artifact.read_text() == "finished"


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


def test_failed_hook_logs_survive_later_successful_retry(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"
    hook_path = plugins_root / "broken" / "on_Snapshot__00_fail.py"
    run_dir = tmp_path / "run"

    _write(
        hook_path,
        "\n".join(
            [
                "import sys",
                'print("first failure", file=sys.stderr)',
                "raise SystemExit(1)",
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    first_results = _run_download("https://example.com", plugins, run_dir, auto_install=True)
    first_failure = next(result for result in first_results if result.plugin == "broken")
    assert first_failure.status == "failed"

    stderr_log = run_dir / "broken" / "on_Snapshot__00_fail.stderr.log"
    assert stderr_log.exists()
    assert "first failure" in stderr_log.read_text()

    _write(
        hook_path,
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "ok"}))',
            ],
        )
        + "\n",
    )

    second_results = _run_download("https://example.com", plugins, run_dir, auto_install=True)
    second_result = next(result for result in second_results if result.plugin == "broken")
    assert second_result.status == "succeeded"

    archived_logs = sorted((run_dir / "broken").glob("on_Snapshot__00_fail.stderr.*.log"))
    assert archived_logs, "failed stderr log should be rotated aside before retry overwrites it"
    assert any("first failure" in path.read_text() for path in archived_logs)


def test_binary_provider_runs_keep_separate_stderr_logs(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "producer" / "on_Crawl__00_emit.py",
        "\n".join(
            [
                "import json",
                'print(json.dumps({"type": "Binary", "name": "alpha", "binproviders": "provider"}), flush=True)',
                'print(json.dumps({"type": "Binary", "name": "beta", "binproviders": "provider"}), flush=True)',
            ],
        )
        + "\n",
    )

    _write(
        plugins_root / "provider" / "on_Binary__10_provider_install.py",
        "\n".join(
            [
                "import argparse",
                "import sys",
                "import time",
                "parser = argparse.ArgumentParser()",
                'parser.add_argument("--name", required=True)',
                'parser.add_argument("--binproviders", default="*")',
                "args = parser.parse_args()",
                "time.sleep(0.2)",
                'print(f"provider failure for {args.name}", file=sys.stderr, flush=True)',
                "raise SystemExit(1)",
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    _run_download("https://example.com", plugins, tmp_path / "run", auto_install=True)

    stderr_logs = sorted((tmp_path / "run" / "provider").glob("*.stderr.log"))
    assert len(stderr_logs) == 2
    stderr_contents = sorted(path.read_text().strip() for path in stderr_logs)
    assert stderr_contents == [
        "provider failure for alpha",
        "provider failure for beta",
    ]

    index_lines = [json.loads(line) for line in (tmp_path / "run" / "index.jsonl").read_text().splitlines()]
    provider_processes = [
        line
        for line in index_lines
        if line.get("type") == "Process" and "on_Binary__10_provider_install.py" in " ".join(line.get("cmd", []))
    ]
    assert len(provider_processes) == 2
    assert sorted(process["stderr"].strip() for process in provider_processes) == stderr_contents


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


def test_output_files_include_path_extension_mimetype_and_size(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "demo" / "on_Snapshot__10_emit.py",
        "\n".join(
            [
                "import json",
                "from pathlib import Path",
                'output = Path.cwd() / "artifact.json"',
                'payload = "{\\"ok\\": true}\\n"',
                "output.write_text(payload)",
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "wrote artifact"}))',
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
    assert results[0].output_files == [
        OutputFile(path="artifact.json", extension="json", mimetype="application/json", size=13),
    ]

    index_lines = [json.loads(line) for line in (tmp_path / "run" / "index.jsonl").read_text().splitlines()]
    archive_result = next(line for line in index_lines if line.get("type") == "ArchiveResult" and line.get("plugin") == "demo")
    assert archive_result["output_files"] == [
        {"path": "artifact.json", "extension": "json", "mimetype": "application/json", "size": 13},
    ]


def test_output_files_detect_warc_gz_as_application_warc(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "demo" / "on_Snapshot__10_emit.py",
        "\n".join(
            [
                "import json",
                "from pathlib import Path",
                'output = Path.cwd() / "warc" / "capture.warc.gz"',
                "output.parent.mkdir(parents=True, exist_ok=True)",
                'output.write_bytes(b"warc-bytes")',
                'print(json.dumps({"type": "ArchiveResult", "status": "succeeded", "output_str": "wrote warc"}))',
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
    assert results[0].output_files == [
        OutputFile(path="warc/capture.warc.gz", extension="gz", mimetype="application/warc", size=10),
    ]

    index_lines = [json.loads(line) for line in (tmp_path / "run" / "index.jsonl").read_text().splitlines()]
    archive_result = next(line for line in index_lines if line.get("type") == "ArchiveResult" and line.get("plugin") == "demo")
    assert archive_result["output_files"] == [
        {"path": "warc/capture.warc.gz", "extension": "gz", "mimetype": "application/warc", "size": 10},
    ]


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


def test_download_dry_run_sets_env_and_skips_snapshot_processes(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "crawlprobe" / "on_Crawl__00_probe.py",
        "\n".join(
            [
                "import os",
                "from pathlib import Path",
                'Path.cwd().joinpath("crawl_marker.txt").write_text("crawl")',
                'Path.cwd().joinpath("dry_run.txt").write_text(os.environ.get("DRY_RUN", ""))',
            ],
        )
        + "\n",
    )
    _write(
        plugins_root / "snapprobe" / "on_Snapshot__01_probe.py",
        "\n".join(
            [
                "from pathlib import Path",
                'Path.cwd().joinpath("snapshot_marker.txt").write_text("snapshot")',
                'print("{\\"type\\": \\"ArchiveResult\\", \\"status\\": \\"succeeded\\", \\"output_str\\": \\"snapshot-ran\\"}")',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    results = _run_download("https://example.com", plugins, tmp_path / "run", dry_run=True, auto_install=True)

    assert (tmp_path / "run" / "crawlprobe" / "crawl_marker.txt").read_text() == "crawl"
    assert (tmp_path / "run" / "crawlprobe" / "dry_run.txt").read_text() == "True"
    assert not (tmp_path / "run" / "snapprobe" / "snapshot_marker.txt").exists()
    assert results == []


def test_install_plugins_dry_run_sets_env_for_install_hooks(tmp_path: Path) -> None:
    plugins_root = tmp_path / "plugins"

    _write(
        plugins_root / "installer" / "on_Crawl__00_install_probe.py",
        "\n".join(
            [
                "import os",
                "from pathlib import Path",
                'Path.cwd().joinpath("dry_run_install.txt").write_text(os.environ.get("DRY_RUN", ""))',
            ],
        )
        + "\n",
    )

    plugins = discover_plugins(plugins_root)
    asyncio.run(install_plugins(plugins=plugins, output_dir=tmp_path / "install-run", dry_run=True))

    assert (tmp_path / "install-run" / "installer" / "dry_run_install.txt").read_text() == "True"
