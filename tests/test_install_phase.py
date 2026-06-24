import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from abxpkg.binary_service import BinaryCacheService, BinaryEvent, BinaryRequestEvent

from abx_dl.config import get_initial_env
from abx_dl.events import InstallEvent, MachineEvent
from abx_dl.models import Plugin, PluginConfig, RequiredBinary, Snapshot, discover_plugins
from abx_dl.orchestrator import create_bus
from abx_dl.services.binary_service import AbxDlEnvConfigFileBinaryCacheBackend, PluginBinariesService


def test_install_event_does_not_skip_stale_cached_binary_requests(tmp_path: Path) -> None:
    plugins = discover_plugins()
    plugin = plugins["ytdlp"]
    snapshot = Snapshot(url="")
    run_dir = tmp_path / "run"
    managed_lib_dir = tmp_path / "lib"
    stale_binary = managed_lib_dir / "pip" / "venv" / "bin" / "yt-dlp"
    bus = create_bus(total_timeout=60.0, name=f"install_phase_stale_cache_{tmp_path.name}")
    PluginBinariesService(
        bus,
        plugins={"ytdlp": plugin},
        auto_install=False,
        install_plugins=[plugin],
        output_dir=run_dir,
        snapshot=snapshot,
    )
    BinaryCacheService(bus, backend=AbxDlEnvConfigFileBinaryCacheBackend(bus, plugins={"ytdlp": plugin}))
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
                    "ABXPKG_LIB_DIR": str(managed_lib_dir),
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

    assert any(event.extra_context.get("plugin_name") == "ytdlp" and event.name == "yt-dlp" for event in request_events)
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


def test_install_event_preserves_chrome_abxbus_binary_overrides(tmp_path: Path) -> None:
    plugins = discover_plugins()
    plugin = plugins["chrome"]
    snapshot = Snapshot(url="")
    run_dir = tmp_path / "run"
    managed_lib_dir = tmp_path / "lib"

    async def collect_requests(*, no_cache: bool) -> list[BinaryRequestEvent]:
        bus = create_bus(total_timeout=60.0, name=f"install_phase_chrome_abxbus_{tmp_path.name}_{no_cache}")
        PluginBinariesService(
            bus,
            plugins={"chrome": plugin},
            auto_install=False,
            install_plugins=[plugin],
            output_dir=run_dir,
            snapshot=snapshot,
        )
        request_events: list[BinaryRequestEvent] = []

        async def on_BinaryRequestEvent(event: BinaryRequestEvent) -> None:
            request_events.append(event)

        bus.on(BinaryRequestEvent, on_BinaryRequestEvent)
        config = {
            **get_initial_env(),
            "ABXPKG_LIB_DIR": str(managed_lib_dir),
        }
        if no_cache:
            config["ABXPKG_NO_CACHE"] = "1"
        await bus.emit(
            MachineEvent(
                config=config,
                config_type="user",
            ),
        ).now()
        await bus.emit(
            MachineEvent(
                config={},
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
        return request_events

    request_events = asyncio.run(collect_requests(no_cache=False))
    abxbus_request = next(event for event in request_events if event.name == "abxbus")
    playwright_request = next(event for event in request_events if event.name == "playwright")
    chromium_index = next(i for i, event in enumerate(request_events) if event.name == "chromium")

    assert request_events.index(playwright_request) < chromium_index
    assert playwright_request.binproviders == "pnpm"
    assert playwright_request.postinstall_scripts is True
    assert playwright_request.overrides == {
        "pnpm": {
            "install_root": str(managed_lib_dir / "pnpm" / "packages" / "playwright"),
            "install_args": ["playwright@next"],
        },
    }
    assert abxbus_request.no_cache is None
    assert abxbus_request.binproviders == "pnpm"
    assert abxbus_request.min_version == "2.5.9"
    assert abxbus_request.min_release_age == 0
    assert abxbus_request.overrides == {
        "pnpm": {
            "install_root": str(managed_lib_dir / "pnpm" / "packages" / "abxbus"),
            "install_args": ["abxbus@2.5.9"],
            "abspath": str(managed_lib_dir / "pnpm" / "packages" / "abxbus" / "node_modules" / "abxbus" / "dist" / "cjs" / "index.js"),
            "version": "2.5.9",
        },
    }
    no_cache_request = next(event for event in asyncio.run(collect_requests(no_cache=True)) if event.name == "abxbus")
    assert no_cache_request.no_cache is True


def test_install_event_includes_opencode_when_route_is_disabled(tmp_path: Path) -> None:
    plugins = discover_plugins(runtime="archivebox")
    plugin = plugins["opencode"]
    snapshot = Snapshot(url="")
    run_dir = tmp_path / "run"
    managed_lib_dir = tmp_path / "lib"
    bus = create_bus(total_timeout=60.0, name=f"install_phase_opencode_disabled_{tmp_path.name}")
    PluginBinariesService(
        bus,
        plugins={"opencode": plugin},
        auto_install=False,
        install_plugins=[plugin],
        output_dir=run_dir,
        snapshot=snapshot,
    )
    request_events: list[BinaryRequestEvent] = []

    async def on_BinaryRequestEvent(event: BinaryRequestEvent) -> None:
        request_events.append(event)

    bus.on(BinaryRequestEvent, on_BinaryRequestEvent)

    async def run() -> None:
        await bus.emit(
            MachineEvent(
                config={
                    **get_initial_env(),
                    "ABXPKG_LIB_DIR": str(managed_lib_dir),
                    "OPENCODE_ENABLED": False,
                },
                config_type="user",
            ),
        ).now()
        await bus.emit(
            MachineEvent(
                config={},
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

    opencode_request = next(
        event for event in request_events if event.extra_context.get("plugin_name") == "opencode" and event.name == "opencode"
    )
    assert opencode_request.binproviders == "env,pnpm"
    assert opencode_request.overrides is not None
    assert opencode_request.overrides["pnpm"]["install_root"] == str(managed_lib_dir / "pnpm" / "packages" / "opencode")


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
    PluginBinariesService(
        bus,
        plugins={"myplugin": plugin},
        auto_install=False,
        install_plugins=[plugin],
        output_dir=run_dir,
        snapshot=snapshot,
    )
    BinaryCacheService(bus, backend=AbxDlEnvConfigFileBinaryCacheBackend(bus, plugins={"myplugin": plugin}))
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
                    "ABXPKG_LIB_DIR": str(managed_lib_dir),
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

    assert any(event.extra_context.get("plugin_name") == "myplugin" and event.name == "mytool" for event in request_events)
    assert any(event.name == "mytool" and event.abspath == str(cached_binary) for event in binary_events)


def test_install_event_moves_plugin_override_metadata_to_extra_context(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "feedplugin"
    plugin_dir.mkdir()
    raw_overrides = {
        "pip": {
            "install_args": ["feedparser"],
            "module_name": "feedparser",
        },
    }
    plugin = Plugin(
        name="feedplugin",
        path=plugin_dir,
        config=PluginConfig.model_validate(
            {
                "properties": {},
                "required_binaries": [
                    {
                        "name": "feedparser",
                        "binproviders": "pip",
                        "overrides": raw_overrides,
                    },
                ],
            },
        ),
    )
    snapshot = Snapshot(url="")
    run_dir = tmp_path / "run"
    managed_lib_dir = tmp_path / "lib"
    bus = create_bus(total_timeout=60.0, name=f"install_phase_override_metadata_{tmp_path.name}")
    PluginBinariesService(
        bus,
        plugins={"feedplugin": plugin},
        auto_install=False,
        install_plugins=[plugin],
        output_dir=run_dir,
        snapshot=snapshot,
    )
    request_events: list[BinaryRequestEvent] = []

    async def on_BinaryRequestEvent(event: BinaryRequestEvent) -> None:
        request_events.append(event)

    bus.on(BinaryRequestEvent, on_BinaryRequestEvent)

    async def run() -> None:
        await bus.emit(
            MachineEvent(
                config={
                    **get_initial_env(),
                    "ABXPKG_LIB_DIR": str(managed_lib_dir),
                },
                config_type="user",
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

    request = next(event for event in request_events if event.name == "feedparser")
    assert request.overrides == {"pip": {"install_args": ["feedparser"]}}
    assert request.extra_context["provider_metadata"] == {"pip": {"module_name": "feedparser"}}
    assert request.extra_context["raw_overrides"] == raw_overrides


def test_chromewebstore_install_preflight_uses_shared_cache_without_persona_duplicate(tmp_path: Path) -> None:
    plugins = discover_plugins()
    plugin = plugins["archivewebpage"]
    snapshot = Snapshot(url="")
    run_dir = tmp_path / "run"
    managed_lib_dir = tmp_path / "lib"
    personas_dir = tmp_path / "personas"
    extensions_dir = managed_lib_dir / "chromewebstore" / "extensions"
    unpacked_dir = extensions_dir / "fpeoodllldobpkbkabpblcfaogecpndd__archivewebpage"
    unpacked_dir.mkdir(parents=True)
    manifest_path = unpacked_dir / "manifest.json"
    manifest_path.write_text(
        """
        {
          "manifest_version": 3,
          "name": "archivewebpage",
          "version": "1.0.0",
          "background": {"service_worker": "service_worker.js"}
        }
        """,
    )
    (unpacked_dir / "service_worker.js").write_text("chrome.runtime.onInstalled.addListener(() => {});\n")
    (extensions_dir / "archivewebpage.extension.json").write_text(
        f"""
        {{
          "name": "archivewebpage",
          "webstore_id": "fpeoodllldobpkbkabpblcfaogecpndd",
          "unpacked_path": {json.dumps(str(unpacked_dir))},
          "version": "1.0.0"
        }}
        """,
    )

    bus = create_bus(total_timeout=60.0, name=f"chromewebstore_shared_cache_{tmp_path.name}")
    PluginBinariesService(
        bus,
        plugins={"archivewebpage": plugin},
        auto_install=False,
        install_plugins=[plugin],
        output_dir=run_dir,
        snapshot=snapshot,
    )
    BinaryCacheService(bus, backend=AbxDlEnvConfigFileBinaryCacheBackend(bus, plugins={"archivewebpage": plugin}))
    from abxpkg.binary_service import BinaryService

    BinaryService(bus)
    binary_events: list[BinaryEvent] = []

    async def on_BinaryEvent(event: BinaryEvent) -> None:
        binary_events.append(event)

    bus.on(BinaryEvent, on_BinaryEvent)

    async def run() -> None:
        await bus.emit(
            MachineEvent(
                config={
                    **get_initial_env(),
                    "ABXPKG_LIB_DIR": str(managed_lib_dir),
                    "PERSONAS_DIR": str(personas_dir),
                    "ARCHIVEWEBPAGE_ENABLED": "true",
                },
                config_type="user",
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

    cache_record_path = extensions_dir / "archivewebpage.extension.json"
    archivewebpage_events = [event for event in binary_events if event.name == "archivewebpage"]
    assert archivewebpage_events
    assert archivewebpage_events[-1].abspath == str(cache_record_path)
    assert archivewebpage_events[-1].env["CHROMEWEBSTORE_EXTENSIONS_DIR"] == str(extensions_dir)
    assert manifest_path.exists()
    assert not (personas_dir / "Default" / "chrome_extensions").exists()
