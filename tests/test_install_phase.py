import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from abxpkg.binary_service import BinaryEvent as AbxPkgBinaryEvent
from abxpkg.binary_service import BinaryRequestEvent as AbxPkgBinaryRequestEvent

from abx_dl.config import get_initial_env
from abx_dl.events import InstallEvent, MachineEvent, ProcessCompletedEvent
from abx_dl.models import Hook, Plugin, PluginConfig, RequiredBinary, Snapshot, discover_plugins
from abx_dl.orchestrator import create_bus, download
from abx_dl.services.binary_service import PluginBinariesService as BinaryService

if TYPE_CHECKING:

    class BinaryRequestEvent(AbxPkgBinaryRequestEvent):
        plugin_name: str = ""

    class BinaryEvent(AbxPkgBinaryEvent):
        plugin_name: str = ""
else:
    BinaryEvent = AbxPkgBinaryEvent
    BinaryRequestEvent = AbxPkgBinaryRequestEvent


def test_install_event_does_not_skip_stale_cached_binary_requests(tmp_path: Path) -> None:
    plugins = discover_plugins()
    plugin = plugins["ytdlp"]
    snapshot = Snapshot(url="")
    run_dir = tmp_path / "run"
    managed_lib_dir = tmp_path / "lib"
    stale_binary = managed_lib_dir / "pip" / "venv" / "bin" / "yt-dlp"
    bus = create_bus(total_timeout=60.0, name=f"install_phase_stale_cache_{tmp_path.name}")
    BinaryService(
        bus,
        plugins={"ytdlp": plugin},
        auto_install=False,
        install_plugins=[plugin],
        output_dir=run_dir,
        snapshot=snapshot,
    )
    request_events: list[BinaryRequestEvent] = []
    machine_events: list[MachineEvent] = []

    async def on_BinaryRequestEvent(event: BinaryRequestEvent) -> None:
        request_events.append(event)

    async def on_MachineEvent(event: MachineEvent) -> None:
        machine_events.append(event)

    bus.on(BinaryRequestEvent, on_BinaryRequestEvent)
    bus.on(MachineEvent, on_MachineEvent)

    async def run() -> None:
        await bus.emit(
            MachineEvent(
                config={
                    **get_initial_env(),
                    "LIB_DIR": str(managed_lib_dir),
                },
                config_type="user",
            ),
        ).now()
        await bus.emit(
            MachineEvent(
                config={
                    "ABX_INSTALL_CACHE": {
                        "yt-dlp": datetime.now(timezone.utc).isoformat(),
                    },
                    "YTDLP_BINARY": str(stale_binary),
                },
                config_type="derived",
            ),
        ).now()
        await bus.emit(
            InstallEvent(
                url="",
                snapshot_id=snapshot.id,
                output_dir=str(run_dir),
            ),
        ).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    assert any(event.plugin_name == "ytdlp" and event.name == "yt-dlp" for event in request_events)
    assert any(
        event.config_type == "derived" and event.method == "unset" and event.key == "config/YTDLP_BINARY" for event in machine_events
    )
    cache_update = next(
        event
        for event in reversed(machine_events)
        if event.config_type == "derived"
        and event.method == "update"
        and event.key == "config/ABX_INSTALL_CACHE"
        and isinstance(event.value, dict)
    )
    assert "yt-dlp" not in cache_update.value


def test_install_event_emits_cached_binary_requests_for_persistence(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "myplugin"
    plugin_dir.mkdir()
    (plugin_dir / "config.json").write_text(
        """
        {
          "properties": {
            "MYTOOL_BINARY": {"type": "string", "default": "mytool"}
          },
          "required_binaries": [
            {"name": "mytool", "binproviders": "env"}
          ]
        }
        """,
    )
    plugin = Plugin(
        name="myplugin",
        path=plugin_dir,
        config=PluginConfig(
            properties={"MYTOOL_BINARY": {"type": "string", "default": "mytool"}},
            required_binaries=[RequiredBinary(name="mytool", binproviders="env")],
        ),
    )
    snapshot = Snapshot(url="")
    run_dir = tmp_path / "run"
    managed_lib_dir = tmp_path / "lib"
    cached_binary = managed_lib_dir / "env" / "bin" / "mytool"
    cached_binary.parent.mkdir(parents=True)
    cached_binary.write_text("#!/bin/sh\n")
    cached_binary.chmod(0o755)
    bus = create_bus(total_timeout=60.0, name=f"install_phase_cached_persist_{tmp_path.name}")
    BinaryService(
        bus,
        plugins={"myplugin": plugin},
        auto_install=False,
        install_plugins=[plugin],
        output_dir=run_dir,
        snapshot=snapshot,
    )
    request_events: list[BinaryRequestEvent] = []
    binary_events: list[BinaryEvent] = []

    async def on_BinaryRequestEvent(event: BinaryRequestEvent) -> None:
        request_events.append(event)

    async def on_BinaryEvent(event: BinaryEvent) -> None:
        binary_events.append(event)

    bus.on(BinaryRequestEvent, on_BinaryRequestEvent)
    bus.on(BinaryEvent, on_BinaryEvent)

    async def run() -> None:
        await bus.emit(
            MachineEvent(
                config={
                    **get_initial_env(),
                    "LIB_DIR": str(managed_lib_dir),
                },
                config_type="user",
            ),
        ).now()
        await bus.emit(
            MachineEvent(
                config={
                    "ABX_INSTALL_CACHE": {
                        "mytool": datetime.now(timezone.utc).isoformat(),
                    },
                    "MYTOOL_BINARY": str(cached_binary),
                },
                config_type="derived",
            ),
        ).now()
        await bus.emit(
            InstallEvent(
                url="",
                snapshot_id=snapshot.id,
                output_dir=str(run_dir),
            ),
        ).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    assert any(event.plugin_name == "myplugin" and event.name == "mytool" for event in request_events)
    assert any(event.name == "mytool" and event.abspath == str(cached_binary) for event in binary_events)


def test_download_waits_for_install_preflight_before_snapshot_hooks(tmp_path: Path) -> None:
    provider_dir = tmp_path / "fakeprovider"
    consumer_dir = tmp_path / "needsfake"
    provider_dir.mkdir()
    consumer_dir.mkdir()
    (provider_dir / "config.json").write_text("{}")
    (consumer_dir / "config.json").write_text(
        """
        {
          "properties": {
            "FAKE_BINARY": {"type": "string", "default": "fake-tool"}
          },
          "required_binaries": [
            {"name": "{FAKE_BINARY}", "binproviders": "fakeprovider"}
          ]
        }
        """,
    )

    provider_hook = provider_dir / "on_BinaryRequest__10_fakeprovider.py"
    provider_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json",
                "import time",
                "from pathlib import Path",
                "",
                "time.sleep(0.2)",
                "binary = Path.cwd() / 'fake-tool'",
                "binary.write_text('#!/bin/sh\\n')",
                "binary.chmod(0o755)",
                "print(json.dumps({",
                "    'type': 'Binary',",
                "    'name': 'fake-tool',",
                "    'abspath': str(binary),",
                "    'version': '1.0.0',",
                "    'binprovider': 'fakeprovider',",
                "}), flush=True)",
            ],
        ),
    )
    provider_hook.chmod(0o755)

    snapshot_hook = consumer_dir / "on_Snapshot__10_needsfake.py"
    snapshot_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import os",
                "import sys",
                "from pathlib import Path",
                "",
                "binary = os.environ.get('FAKE_BINARY', '')",
                "if not binary or not Path(binary).is_file():",
                "    print(f'FAKE_BINARY not installed before snapshot hook: {binary}', file=sys.stderr)",
                "    raise SystemExit(1)",
                "print('fake binary ready')",
            ],
        ),
    )
    snapshot_hook.chmod(0o755)

    provider = Plugin(
        name="fakeprovider",
        path=provider_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=provider_hook.stem,
                event="BinaryRequest",
                plugin_name="fakeprovider",
                path=provider_hook,
                order=10,
                is_background=False,
            ),
        ],
    )
    consumer = Plugin(
        name="needsfake",
        path=consumer_dir,
        config=PluginConfig(
            properties={"FAKE_BINARY": {"type": "string", "default": "fake-tool"}},
            required_binaries=[RequiredBinary(name="{FAKE_BINARY}", binproviders="fakeprovider")],
        ),
        hooks=[
            Hook(
                name=snapshot_hook.stem,
                event="Snapshot",
                plugin_name="needsfake",
                path=snapshot_hook,
                order=10,
                is_background=False,
            ),
        ],
    )

    run_dir = tmp_path / "run"
    bus = create_bus(total_timeout=30.0, name=f"install_phase_boundary_{tmp_path.name}")

    async def run() -> list[ProcessCompletedEvent]:
        await download(
            "https://example.com",
            {"fakeprovider": provider, "needsfake": consumer},
            run_dir,
            ["needsfake"],
            {"TIMEOUT": 5},
            bus=bus,
            emit_jsonl=False,
            interactive_tty=False,
            MachineService=None,
        )
        await bus.wait_until_idle()
        return await bus.filter(ProcessCompletedEvent, plugin_name="needsfake", past=True, future=False)

    completed = asyncio.run(run())

    assert len(completed) == 1
    assert completed[0].status == "succeeded"
