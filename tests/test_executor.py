import asyncio
import json
import os
import signal
import sys
import threading
from pathlib import Path
from uuid import uuid4

from abxpkg.binary_service import BinaryCacheService, BinaryEvent, BinaryRequestEvent, BinaryService
from pytest_httpserver import HTTPServer
from werkzeug import Response

from abx_dl.config import GlobalConfig, RuntimeConfig, get_initial_env
from abx_dl.config import get_required_binary_requests
from abx_dl.events import (
    ArchiveResultEvent,
    CrawlAbortEvent,
    CrawlCleanupEvent,
    CrawlCompletedEvent,
    CrawlEvent,
    CrawlSetupEvent,
    CrawlStartEvent,
    MachineEvent,
    ProcessCompletedEvent,
    ProcessEvent,
    ProcessKillEvent,
    ProcessStartedEvent,
    ProcessStdoutEvent,
    SnapshotCompletedEvent,
    SnapshotCleanupEvent,
    SnapshotEvent,
)
from abx_dl.limits import CrawlLimitState
from abx_dl.models import Snapshot, discover_plugins
from abx_dl.orchestrator import create_bus, download, setup_services
from abx_dl.services.archive_result_service import ArchiveResultService
from abx_dl.services.binary_service import AbxDlEnvConfigFileBinaryCacheBackend
from abx_dl.services.crawl_service import CrawlService
from abx_dl.services.machine_service import MachineService
from abx_dl.services.process_service import ProcessService
from abx_dl.services.snapshot_service import SnapshotService


def _binary_extra_context(
    *,
    plugin_name: str,
    hook_name: str = "",
    output_dir: str = "",
    binary_id: str = "",
    machine_id: str = "",
) -> dict[str, str]:
    return {
        "plugin_name": plugin_name,
        "hook_name": hook_name,
        "output_dir": output_dir,
        "binary_id": binary_id,
        "machine_id": machine_id,
    }


def _run_download(*args, **kwargs):
    bus = kwargs.get("bus")
    if bus is None:
        bus = create_bus(total_timeout=120.0, name=f"test_executor_download_{uuid4().hex[:8]}")
        kwargs["bus"] = bus
    results = []

    async def on_ArchiveResultEvent(event: ArchiveResultEvent) -> None:
        if event.snapshot_id:
            results.append(event)

    bus.on(ArchiveResultEvent, on_ArchiveResultEvent)

    async def run() -> None:
        try:
            await download(*args, **kwargs)
        finally:
            await bus.wait_until_idle()

    asyncio.run(run())
    return results


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _resolve_real_wget_binary(tmp_path: Path) -> BinaryEvent:
    plugins = discover_plugins()
    selected = {"wget": plugins["wget"]}
    bus = create_bus(total_timeout=30.0, name=f"resolve_real_wget_binary_{tmp_path.name}")
    setup_services(bus, plugins=selected, auto_install=True, emit_jsonl=False, persist_derived=False)
    installed_events: list[BinaryEvent] = []

    async def on_BinaryEvent(event: BinaryEvent) -> None:
        installed_events.append(event)

    bus.on(BinaryEvent, on_BinaryEvent)

    async def run() -> None:
        await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
        await bus.emit(
            BinaryRequestEvent(
                name="wget",
                binproviders="env,apt,brew",
                extra_context=_binary_extra_context(plugin_name="wget", output_dir=str(tmp_path / "resolve-wget")),
            ),
        ).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    return next(event for event in reversed(installed_events) if event.name == "wget")


def _streaming_http_response(httpserver: HTTPServer, path: str) -> tuple[str, threading.Event, threading.Event]:
    response_started = threading.Event()
    release_response = threading.Event()

    def stream_response(_request) -> Response:
        def body():
            response_started.set()
            yield b"ready\n"
            release_response.wait()
            yield b"complete\n"

        return Response(body(), content_type="text/plain")

    httpserver.expect_request(path).respond_with_handler(stream_response)
    return httpserver.url_for(path), response_started, release_response


def _real_hook_path(plugin_name: str, hook_name: str) -> str:
    plugin = discover_plugins()[plugin_name]
    hook = next(hook for hook in plugin.hooks if hook.name == hook_name)
    assert hook.path.is_file()
    return str(hook.path)


def _runtime_config(**user_config) -> RuntimeConfig:
    return RuntimeConfig(user=GlobalConfig(**user_config), derived={})


def test_binary_installed_event_preserves_child_provider_metadata(tmp_path: Path) -> None:
    plugins = discover_plugins()
    selected = {"wget": plugins["wget"]}
    bus = create_bus(total_timeout=30.0, name=f"binary_installed_metadata_{tmp_path.name}")
    setup_services(bus, plugins=selected, auto_install=True, emit_jsonl=False, persist_derived=False)
    installed_events: list[BinaryEvent] = []

    async def on_BinaryEvent(event: BinaryEvent) -> None:
        installed_events.append(event)

    bus.on(BinaryEvent, on_BinaryEvent)

    async def run() -> None:
        await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
        await bus.emit(
            BinaryRequestEvent(
                name="wget",
                binproviders="env,apt,brew",
                extra_context=_binary_extra_context(plugin_name="wget", output_dir=str(tmp_path / "run")),
            ),
        ).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    wget_events = [event for event in installed_events if event.name == "wget"]
    assert wget_events
    assert all(event.extra_context.get("plugin_name") == "wget" for event in wget_events)
    assert all(event.binproviders == "env,apt,brew" for event in wget_events)
    assert all(event.binprovider == "env" for event in wget_events)
    assert all("binary_id" in event.extra_context for event in wget_events)


def test_binary_installed_event_uses_machine_config_seeded_from_persistent_config(tmp_path: Path) -> None:
    from abx_dl.config import set_user_config

    plugins = discover_plugins()
    resolved_binary = _resolve_real_wget_binary(tmp_path)
    set_user_config({name: plugin.config.properties for name, plugin in plugins.items()}, WGET_BINARY=resolved_binary.abspath)

    async def run() -> list[BinaryEvent]:
        bus = create_bus(total_timeout=10.0, name=f"machine_config_seeded_{tmp_path.name}")
        MachineService(bus)
        BinaryCacheService(
            bus,
            backend=AbxDlEnvConfigFileBinaryCacheBackend(bus, plugins={"wget": plugins["wget"]}),
        )
        BinaryService(bus, auto_install=True)
        installed_events: list[BinaryEvent] = []

        async def on_BinaryEvent(event: BinaryEvent) -> None:
            installed_events.append(event)

        bus.on(BinaryEvent, on_BinaryEvent)
        try:
            await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
            await bus.emit(
                BinaryRequestEvent(
                    name=resolved_binary.abspath,
                    binproviders="env,apt,brew",
                    extra_context=_binary_extra_context(plugin_name="wget", output_dir="."),
                ),
            ).now()
            await bus.wait_until_idle()
            return installed_events
        finally:
            await bus.wait_until_idle()

    wget_events = [event for event in asyncio.run(run()) if event.name == resolved_binary.abspath]
    assert wget_events
    assert wget_events[-1].abspath == resolved_binary.abspath
    assert wget_events[-1].binproviders == "env,apt,brew"


def test_binary_installed_event_resolves_config_backed_command_name(tmp_path: Path) -> None:
    resolved_binary = _resolve_real_wget_binary(tmp_path)
    plugins = discover_plugins()

    async def run() -> list[BinaryEvent]:
        bus = create_bus(total_timeout=10.0, name=f"config_backed_command_{tmp_path.name}")
        MachineService(bus)
        BinaryCacheService(
            bus,
            backend=AbxDlEnvConfigFileBinaryCacheBackend(bus, plugins={"wget": plugins["wget"]}),
        )
        BinaryService(bus, auto_install=True)
        installed_events: list[BinaryEvent] = []

        async def on_BinaryEvent(event: BinaryEvent) -> None:
            installed_events.append(event)

        bus.on(BinaryEvent, on_BinaryEvent)
        try:
            await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
            await bus.emit(MachineEvent(config={"WGET_BINARY": resolved_binary.abspath}, config_type="derived")).now()
            await bus.emit(
                BinaryRequestEvent(
                    name="wget",
                    binproviders="env,apt,brew",
                    extra_context=_binary_extra_context(plugin_name="wget", output_dir="."),
                ),
            ).now()
            await bus.wait_until_idle()
            return installed_events
        finally:
            await bus.wait_until_idle()

    wget_events = [event for event in asyncio.run(run()) if event.name == "wget"]
    assert wget_events
    assert wget_events[-1].abspath == resolved_binary.abspath
    assert wget_events[-1].version
    assert wget_events[-1].binprovider == "env"


def test_binary_installed_event_uses_user_absolute_path_for_real_plugin(tmp_path: Path) -> None:
    resolved_binary = _resolve_real_wget_binary(tmp_path)
    plugins = discover_plugins()

    async def run() -> list[BinaryEvent]:
        bus = create_bus(total_timeout=10.0, name=f"user_absolute_path_{tmp_path.name}")
        MachineService(bus)
        BinaryCacheService(
            bus,
            backend=AbxDlEnvConfigFileBinaryCacheBackend(bus, plugins={"wget": plugins["wget"]}),
        )
        BinaryService(bus, auto_install=True)
        installed_events: list[BinaryEvent] = []

        async def on_BinaryEvent(event: BinaryEvent) -> None:
            installed_events.append(event)

        bus.on(BinaryEvent, on_BinaryEvent)
        try:
            await bus.emit(MachineEvent(config={"WGET_BINARY": resolved_binary.abspath}, config_type="user")).now()
            await bus.emit(
                BinaryRequestEvent(
                    name=resolved_binary.abspath,
                    binproviders="env,apt,brew",
                    extra_context=_binary_extra_context(plugin_name="wget", output_dir="."),
                ),
            ).now()
            await bus.wait_until_idle()
            return installed_events
        finally:
            await bus.wait_until_idle()

    wget_events = [event for event in asyncio.run(run()) if event.name == resolved_binary.abspath]
    assert wget_events
    assert wget_events[-1].abspath == resolved_binary.abspath
    assert wget_events[-1].version
    assert wget_events[-1].binprovider == "env"


def test_binary_installed_event_validates_real_plugin_derived_paths_through_abxpkg(tmp_path: Path) -> None:
    resolved_binary = _resolve_real_wget_binary(tmp_path)
    plugins = discover_plugins()

    async def run() -> list[BinaryEvent]:
        bus = create_bus(total_timeout=10.0, name=f"reuses_cached_paths_{tmp_path.name}")
        MachineService(bus)
        BinaryCacheService(
            bus,
            backend=AbxDlEnvConfigFileBinaryCacheBackend(bus, plugins={"wget": plugins["wget"]}),
        )
        BinaryService(bus, auto_install=True)
        installed_events: list[BinaryEvent] = []

        async def on_BinaryEvent(event: BinaryEvent) -> None:
            installed_events.append(event)

        bus.on(BinaryEvent, on_BinaryEvent)
        try:
            await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
            await bus.emit(MachineEvent(config={"WGET_BINARY": resolved_binary.abspath}, config_type="derived")).now()
            await bus.emit(
                BinaryRequestEvent(
                    name="wget",
                    binproviders="env,apt,brew",
                    extra_context=_binary_extra_context(plugin_name="wget", output_dir="."),
                ),
            ).now()
            await bus.wait_until_idle()
            return installed_events
        finally:
            await bus.wait_until_idle()

    wget_events = [event for event in asyncio.run(run()) if event.name == "wget"]
    assert wget_events
    assert wget_events[-1].abspath == resolved_binary.abspath
    assert wget_events[-1].version
    assert wget_events[-1].binprovider == "env"


def test_binary_event_validates_derived_config_binary_through_abxpkg_resolution(tmp_path: Path) -> None:
    resolved_binary = _resolve_real_wget_binary(tmp_path)
    plugins = discover_plugins()
    selected = {"wget": plugins["wget"]}
    bus = create_bus(total_timeout=60.0, name=f"cached_binary_before_provider_{tmp_path.name}")
    setup_services(
        bus,
        plugins=selected,
        auto_install=True,
        emit_jsonl=False,
        persist_derived=False,
    )
    installed_events: list[BinaryEvent] = []
    process_events: list[ProcessEvent] = []

    async def on_BinaryEvent(event: BinaryEvent) -> None:
        installed_events.append(event)

    async def on_ProcessEvent(event: ProcessEvent) -> None:
        process_events.append(event)

    bus.on(BinaryEvent, on_BinaryEvent)
    bus.on(ProcessEvent, on_ProcessEvent)

    async def run() -> None:
        await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
        await bus.emit(MachineEvent(config={"WGET_BINARY": resolved_binary.abspath}, config_type="derived")).now()
        await bus.emit(
            BinaryRequestEvent(
                name="wget",
                binproviders="env,apt,brew",
                extra_context=_binary_extra_context(plugin_name="wget", output_dir=str(tmp_path / "run")),
            ),
        ).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    wget_events = [event for event in installed_events if event.name == "wget"]
    assert wget_events
    assert wget_events[-1].abspath == resolved_binary.abspath
    assert wget_events[-1].version
    assert wget_events[-1].binprovider == "env"


def test_required_binary_requests_preserve_extra_config_fields() -> None:
    plugins = discover_plugins()
    plugin = plugins["papersdl"]

    requests = get_required_binary_requests(
        plugin,
        plugin.config.required_binaries,
        overrides=get_initial_env(),
        derived_overrides={},
        run_output_dir=Path.cwd(),
    )

    papersdl_request = next(request for request in requests if request["name"] == "papers-dl")
    assert papersdl_request["postinstall_scripts"] is True
    install_args = papersdl_request["overrides"]["uv"]["install_args"]
    assert install_args[:3] == [
        "--no-deps",
        "--only-binary=PyMuPDF",
        "papers-dl==0.0.25",
    ]
    assert "aiohttp>=3.13.2" in install_args
    assert "/lib/" in papersdl_request["overrides"]["uv"]["install_root"]
    assert papersdl_request["overrides"]["uv"]["install_root"].endswith("/uv/packages/papers-dl")


def test_setup_services_accepts_runtime_config_overrides_and_seeds_machine_events(tmp_path: Path) -> None:
    wget_binary = _resolve_real_wget_binary(tmp_path)

    async def run() -> list[MachineEvent]:
        bus = create_bus(total_timeout=5.0, name=f"setup_services_runtime_config_{uuid4().hex[:8]}")
        observed: list[MachineEvent] = []

        async def on_MachineEvent(event: MachineEvent) -> None:
            observed.append(event)

        bus.on(MachineEvent, on_MachineEvent)
        try:
            setup_services(
                bus,
                plugins={},
                config_overrides={"TIMEOUT": 123, "DRY_RUN": True},
                derived_config_overrides={"WGET_BINARY": str(wget_binary.abspath)},
                MachineService=MachineService,
                PluginBinariesService=None,
                ProcessService=None,
                ArchiveResultService=None,
                TagService=None,
                CrawlService=None,
                SnapshotService=None,
            )
            await bus.wait_until_idle(timeout=2.0)
            return observed
        finally:
            await bus.wait_until_idle()

    events = asyncio.run(run())
    user_event = next(event for event in events if event.config_type == "user")
    derived_event = next(event for event in events if event.config_type == "derived")
    assert user_event.config is not None
    assert user_event.config["TIMEOUT"] == 123
    assert user_event.config["DRY_RUN"] is True
    assert derived_event.config == {"WGET_BINARY": str(wget_binary.abspath)}


def test_binary_service_honors_declared_provider_order() -> None:
    bus = create_bus(total_timeout=10.0, name="binary_provider_order")
    service = BinaryService(bus, auto_install=True)

    assert service._provider_names("env,apt,brew") == ["env", "apt", "brew"]


def test_binary_service_stops_after_successful_provider_result(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=30.0, name=f"binary_provider_result_{tmp_path.name}")
    setup_services(
        bus,
        plugins={},
        auto_install=True,
        emit_jsonl=False,
        persist_derived=False,
    )
    process_events: list[ProcessEvent] = []
    binary_events: list[BinaryEvent] = []

    async def on_ProcessEvent(event: ProcessEvent) -> None:
        process_events.append(event)

    async def on_BinaryEvent(event: BinaryEvent) -> None:
        binary_events.append(event)

    bus.on(ProcessEvent, on_ProcessEvent)
    bus.on(BinaryEvent, on_BinaryEvent)

    async def run() -> None:
        await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
        request = bus.emit(
            BinaryRequestEvent(
                name="python3",
                binproviders="env,apt,brew",
                extra_context=_binary_extra_context(plugin_name="python", output_dir=str(tmp_path / "run")),
            ),
        )
        await request.now(first_result=True)
        assert await request.event_result() is not None
        await bus.wait_until_idle()

    asyncio.run(run())

    assert process_events == []
    python_events = [event for event in binary_events if event.name == "python3"]
    assert python_events
    assert python_events[-1].binprovider == "env"
    assert Path(python_events[-1].abspath).name == "python3"


def test_binary_service_concurrent_real_requests_preserve_env_projection(tmp_path: Path) -> None:
    async def run() -> list[BinaryEvent]:
        bus = create_bus(total_timeout=10.0, name=f"binary_install_lock_{tmp_path.name}")
        MachineService(bus)
        BinaryService(bus, auto_install=True)
        binary_events: list[BinaryEvent] = []

        async def on_BinaryEvent(event: BinaryEvent) -> None:
            binary_events.append(event)

        bus.on(BinaryEvent, on_BinaryEvent)
        lib_dir = tmp_path / "lib"
        await bus.emit(MachineEvent(config={**get_initial_env(), "ABXPKG_LIB_DIR": str(lib_dir)}, config_type="user")).now()
        await asyncio.gather(
            bus.emit(
                BinaryRequestEvent(
                    name="wget",
                    binproviders="env",
                    lib_dir=lib_dir,
                    extra_context=_binary_extra_context(plugin_name="wget", output_dir=str(tmp_path / "wget")),
                ),
            ).now(),
            bus.emit(
                BinaryRequestEvent(
                    name="node",
                    binproviders="env",
                    lib_dir=lib_dir,
                    extra_context=_binary_extra_context(plugin_name="node", output_dir=str(tmp_path / "node")),
                ),
            ).now(),
        )
        await bus.wait_until_idle()
        return binary_events

    binary_events = asyncio.run(run())
    resolved = {event.name: event for event in binary_events if event.name in {"wget", "node"}}
    assert set(resolved) == {"wget", "node"}
    for name, event in resolved.items():
        binary_path = Path(event.abspath)
        assert event.binprovider == "env"
        assert event.version
        assert binary_path == tmp_path / "lib" / "env" / "bin" / name
        assert binary_path.is_symlink()


def test_binary_event_ignores_unknown_request_plugin_when_persisting_config(tmp_path: Path) -> None:
    async def run() -> list[BinaryEvent]:
        bus = create_bus(total_timeout=10.0, name=f"unknown_binary_request_plugin_{tmp_path.name}")
        MachineService(bus)
        BinaryCacheService(
            bus,
            backend=AbxDlEnvConfigFileBinaryCacheBackend(bus, plugins={}),
        )
        BinaryService(bus, auto_install=True)
        installed_events: list[BinaryEvent] = []

        async def on_BinaryEvent(event: BinaryEvent) -> None:
            installed_events.append(event)

        bus.on(BinaryEvent, on_BinaryEvent)
        try:
            await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
            binary_id = str(uuid4())
            machine_id = str(uuid4())
            await bus.emit(
                BinaryRequestEvent(
                    name=sys.executable,
                    binproviders="env",
                    extra_context=_binary_extra_context(
                        plugin_name="archivebox",
                        hook_name="install:archivebox",
                        output_dir=str(tmp_path / "run"),
                        binary_id=binary_id,
                        machine_id=machine_id,
                    ),
                ),
            ).now()
            await bus.emit(
                BinaryEvent(
                    name=sys.executable,
                    abspath=sys.executable,
                    binproviders="env",
                    binprovider="env",
                    extra_context=_binary_extra_context(
                        plugin_name="archivebox",
                        hook_name="install:archivebox",
                        binary_id=binary_id,
                        machine_id=machine_id,
                    ),
                ),
            ).now()
            await bus.wait_until_idle()
            return installed_events
        finally:
            await bus.wait_until_idle()

    installed_events = asyncio.run(run())
    assert installed_events
    assert installed_events[-1].abspath == sys.executable
    assert installed_events[-1].extra_context.get("plugin_name") == "archivebox"


def test_binary_event_delegates_stale_cached_config_binary_to_abxpkg_resolution(tmp_path: Path) -> None:
    managed_lib_dir = tmp_path / "lib"
    stale_binary = managed_lib_dir / "pip" / "venv" / "bin" / "wget"
    plugins = discover_plugins()
    selected = {"wget": plugins["wget"]}
    bus = create_bus(total_timeout=60.0, name=f"stale_cached_binary_{tmp_path.name}")
    setup_services(
        bus,
        plugins=selected,
        auto_install=True,
        emit_jsonl=False,
        persist_derived=False,
    )
    installed_events: list[BinaryEvent] = []
    process_events: list[ProcessEvent] = []

    async def on_BinaryEvent(event: BinaryEvent) -> None:
        installed_events.append(event)

    async def on_ProcessEvent(event: ProcessEvent) -> None:
        process_events.append(event)

    bus.on(BinaryEvent, on_BinaryEvent)
    bus.on(ProcessEvent, on_ProcessEvent)

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
        await bus.emit(MachineEvent(config={"WGET_BINARY": str(stale_binary)}, config_type="derived")).now()
        await bus.emit(
            BinaryRequestEvent(
                name="wget",
                binproviders="env,apt,brew",
                extra_context=_binary_extra_context(plugin_name="wget", output_dir=str(tmp_path / "run")),
            ),
        ).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    wget_events = [event for event in installed_events if event.name == "wget"]
    assert wget_events
    assert wget_events[-1].binprovider == "env"
    assert Path(wget_events[-1].abspath).name == "wget"
    assert process_events == []
    derived_update = next(
        event
        for event in reversed(asyncio.run(bus.filter(MachineEvent, past=True)))
        if event.config_type == "derived" and event.key == "config/WGET_BINARY" and event.method == "update" and event.value
    )
    assert Path(str(derived_update.value)).name == "wget"


def test_binary_event_delegates_missing_user_binary_abspath_override_to_abxpkg(tmp_path: Path) -> None:
    broken_binary = tmp_path / "broken" / "wget"
    plugins = discover_plugins()
    selected = {"wget": plugins["wget"]}
    bus = create_bus(total_timeout=60.0, name=f"user_abspath_override_{tmp_path.name}")
    setup_services(
        bus,
        plugins=selected,
        auto_install=True,
        emit_jsonl=False,
        persist_derived=False,
    )
    installed_events: list[BinaryEvent] = []
    process_events: list[ProcessEvent] = []

    async def on_BinaryEvent(event: BinaryEvent) -> None:
        installed_events.append(event)

    async def on_ProcessEvent(event: ProcessEvent) -> None:
        process_events.append(event)

    bus.on(BinaryEvent, on_BinaryEvent)
    bus.on(ProcessEvent, on_ProcessEvent)

    async def run() -> None:
        await bus.emit(MachineEvent(config={"WGET_BINARY": str(broken_binary)}, config_type="user")).now()
        request = bus.emit(
            BinaryRequestEvent(
                name=str(broken_binary),
                binproviders="env,apt,brew",
                extra_context=_binary_extra_context(plugin_name="wget", output_dir=str(tmp_path / "run")),
            ),
        )
        await request.now(first_result=True)
        assert await request.event_result() is not None
        await bus.wait_until_idle()

    asyncio.run(run())

    assert installed_events
    assert installed_events[-1].name == str(broken_binary)
    assert installed_events[-1].binprovider == "env"
    assert process_events == []


def test_download_creates_default_persona_dir(tmp_path: Path) -> None:
    personas_dir = Path(os.environ["PERSONAS_DIR"])

    assert not (personas_dir / "Default").exists()

    _run_download("https://example.com", {}, tmp_path / "run", auto_install=True)

    assert (personas_dir / "Default").is_dir()


def test_download_sets_plugin_specific_binary_env_from_binary_default(tmp_path: Path) -> None:
    plugins = discover_plugins()
    selected = {"wget": plugins["wget"]}
    bus = create_bus(total_timeout=120.0, name=f"real_wget_binary_env_{tmp_path.name}")
    snapshot_processes: list[ProcessEvent] = []

    async def on_ProcessEvent(event: ProcessEvent) -> None:
        if event.plugin_name == "wget" and event.hook_name == "on_Snapshot__06_wget.finite.bg":
            snapshot_processes.append(event)

    bus.on(ProcessEvent, on_ProcessEvent)
    _run_download(
        "https://example.com",
        selected,
        tmp_path / "run",
        auto_install=True,
        bus=bus,
    )

    assert snapshot_processes
    assert Path(snapshot_processes[-1].env["WGET_BINARY"]).name == "wget"


def test_snapshot_background_only_hook_finishes_before_cleanup_without_filename_special_case(tmp_path: Path) -> None:
    plugins = discover_plugins()
    selected = {"wget": plugins["wget"]}
    bus = create_bus(total_timeout=120.0, name=f"real_wget_background_cleanup_{tmp_path.name}")
    completed_processes: list[ProcessCompletedEvent] = []

    async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
        if event.plugin_name == "wget" and event.hook_name == "on_Snapshot__06_wget.finite.bg":
            completed_processes.append(event)

    bus.on(ProcessCompletedEvent, on_ProcessCompletedEvent)
    results = _run_download(
        "https://example.com",
        selected,
        tmp_path / "run",
        auto_install=True,
        emit_jsonl=False,
        bus=bus,
    )

    result = next(r for r in results if r.plugin == "wget")
    assert result.hook_name == "on_Snapshot__06_wget.finite.bg"
    assert result.status == "succeeded"
    assert result.output_str == "wget/example.com/index.html"
    assert completed_processes
    assert completed_processes[-1].status == "succeeded"
    assert completed_processes[-1].exit_code == 0
    assert "ArchiveResult" in completed_processes[-1].stdout
    assert completed_processes[-1].stderr == ""
    assert (tmp_path / "run" / "wget" / "example.com" / "index.html").exists()
    assert not list((tmp_path / "run" / "wget").glob("*.pid"))


def test_real_js_snapshot_hook_replays_early_sigterm_to_late_cleanup_handler(tmp_path: Path) -> None:
    plugin = discover_plugins()["staticfile"]
    chrome = discover_plugins()["chrome"]
    selected = {chrome.name: chrome, plugin.name: plugin}
    hook = plugin.hooks[0]
    bus = create_bus(total_timeout=120.0, name=f"real_js_early_sigterm_{tmp_path.name}")
    output_dir = tmp_path / "run"

    async def run() -> ProcessCompletedEvent | None:
        download_task = asyncio.create_task(
            download(
                "https://example.com",
                selected,
                output_dir,
                selected_plugins=list(selected),
                auto_install=True,
                emit_jsonl=False,
                interactive_tty=False,
                bus=bus,
            ),
        )
        ready = await bus.find(
            ProcessStdoutEvent,
            past=True,
            future=60.0,
            plugin_name=plugin.name,
            hook_name=hook.name,
            where=lambda event: event.line == "waiting for initial response...",
        )
        assert isinstance(ready, ProcessStdoutEvent)
        snapshot_event = await bus.find(SnapshotEvent, past=True, future=False)
        assert isinstance(snapshot_event, SnapshotEvent)
        crawl = await bus.find(CrawlEvent, past=True, future=False)
        assert isinstance(crawl, CrawlEvent)
        await bus.emit(
            SnapshotCleanupEvent(
                url=snapshot_event.url,
                snapshot_id=snapshot_event.snapshot_id,
                output_dir=snapshot_event.output_dir,
                event_parent_id=snapshot_event.event_id,
            ),
        ).now()
        await bus.emit(CrawlAbortEvent(event_parent_id=crawl.event_id)).now()
        await download_task
        completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=False,
            plugin_name=plugin.name,
            hook_name=hook.name,
        )
        await bus.wait_until_idle()
        return completed if isinstance(completed, ProcessCompletedEvent) else None

    completed = asyncio.run(run())
    assert isinstance(completed, ProcessCompletedEvent)
    assert completed.status == "succeeded", (completed.exit_code, completed.stdout, completed.stderr)
    assert completed.exit_code == 0
    assert '"type":"ArchiveResult"' in completed.stdout
    assert '"status":"failed"' in completed.stdout
    assert '"output_str":"No main response captured"' in completed.stdout
    assert "Received SIGTERM, emitting final results" in completed.stderr
    assert not list((tmp_path / "run" / plugin.name).glob(f"{hook.name}.*.pid"))


def test_snapshot_service_emits_background_process_without_extra_wait(tmp_path: Path) -> None:
    plugin = discover_plugins()["wget"]
    bus = create_bus(total_timeout=120.0, name=f"make_hook_handler_background_{tmp_path.name}")
    started_processes: list[ProcessStartedEvent] = []
    process_events: list[ProcessEvent] = []

    async def on_ProcessEvent(event: ProcessEvent) -> None:
        if event.plugin_name == "wget":
            process_events.append(event)

    async def on_ProcessStartedEvent(event: ProcessStartedEvent) -> None:
        if event.plugin_name == "wget":
            started_processes.append(event)

    bus.on(ProcessEvent, on_ProcessEvent)
    bus.on(ProcessStartedEvent, on_ProcessStartedEvent)
    results = _run_download(
        "https://example.com",
        {plugin.name: plugin},
        tmp_path / "run",
        auto_install=True,
        emit_jsonl=False,
        bus=bus,
    )

    result = next(result for result in results if result.plugin == "wget")
    assert result.status == "succeeded"
    assert len(started_processes) == 1
    assert len(process_events) == 1
    assert process_events[0].is_background is True
    assert process_events[0].event_blocks_parent_completion is False
    assert process_events[0].event_timeout is None
    assert process_events[0].event_handler_timeout is None


def test_snapshot_service_selected_hooks_by_plugin_runs_only_named_hooks(tmp_path: Path) -> None:
    plugin = discover_plugins()["chrome"]
    selected_hook = next(hook for hook in plugin.hooks if hook.name == "on_Snapshot__11_chrome_wait")
    bus = create_bus(total_timeout=20.0, name=f"selected_snapshot_hooks_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    snapshot = Snapshot(url="https://example.com", id="snap-123")
    SnapshotService(
        bus,
        url="https://example.com",
        snapshot=snapshot,
        output_dir=tmp_path / "run",
        plugins={plugin.name: plugin},
        config=_runtime_config(CHROME_TIMEOUT=5),
        snapshot_phase_timeout=5.0,
        selected_hooks_by_plugin={plugin.name: {selected_hook.name}},
    )

    async def run() -> list[ProcessEvent]:
        await bus.emit(MachineEvent(config={"CHROME_TIMEOUT": 5}, config_type="user")).now()
        crawl_start_event = CrawlStartEvent(url=snapshot.url, snapshot_id=snapshot.id, output_dir=str(tmp_path / "run"))
        root_event = SnapshotEvent(
            url=snapshot.url,
            snapshot_id=snapshot.id,
            output_dir=str(tmp_path / "run"),
            event_parent_id=crawl_start_event.event_id,
        )
        await bus.emit(crawl_start_event).now()
        await bus.emit(root_event).now()
        await bus.wait_until_idle()
        return await bus.filter(ProcessEvent, child_of=root_event, past=True, future=False)

    process_events = asyncio.run(run())

    assert [event.hook_name for event in process_events] == [selected_hook.name]
    assert {event.hook_name for event in process_events}.isdisjoint(hook.name for hook in plugin.hooks if hook != selected_hook)


def test_snapshot_service_repins_snapshot_persona_after_global_config_merge(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=30.0, name=f"snapshot_chrome_env_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    output_dir = tmp_path / "archive" / "users" / "system" / "snapshots" / "20260603" / "example.com" / "current"
    stale_dir = tmp_path / "archive" / "users" / "system" / "snapshots" / "20260603" / "example.com" / "stale"
    plugin = discover_plugins()["chrome"]
    real_hook = next(hook for hook in plugin.hooks if hook.name == "on_Snapshot__11_chrome_wait")
    snapshot = Snapshot(url="https://example.com", id="snap-current")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    SnapshotService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        config=_runtime_config(
            ABX_RUNTIME="archivebox",
            CHROME_ISOLATION="snapshot",
            CHROME_TIMEOUT=10,
            ACTIVE_PERSONA="Default",
            CHROME_USER_DATA_DIR=stale_dir / ".persona" / "Default" / "chrome_profile",
            CHROME_DOWNLOADS_DIR=stale_dir / ".persona" / "Default" / "chrome_downloads",
        ),
        snapshot_phase_timeout=10.0,
        selected_hooks_by_plugin={plugin.name: {real_hook.name}},
    )

    async def run() -> ProcessEvent | None:
        await bus.emit(
            MachineEvent(
                config={
                    "ABX_RUNTIME": "archivebox",
                    "CHROME_ISOLATION": "snapshot",
                    "CHROME_TIMEOUT": "10",
                    "ACTIVE_PERSONA": "Default",
                    "CHROME_USER_DATA_DIR": str(stale_dir / ".persona" / "Default" / "chrome_profile"),
                    "CHROME_DOWNLOADS_DIR": str(stale_dir / ".persona" / "Default" / "chrome_downloads"),
                },
                config_type="user",
            ),
        ).now()
        crawl_start_event = CrawlStartEvent(
            url=snapshot.url,
            snapshot_id=snapshot.id,
            output_dir=str(output_dir),
        )
        root_event = SnapshotEvent(
            url=snapshot.url,
            snapshot_id=snapshot.id,
            output_dir=str(output_dir),
            event_parent_id=crawl_start_event.event_id,
        )
        await bus.emit(crawl_start_event).now()
        await bus.emit(root_event).now()
        process_event = await bus.find(
            ProcessEvent,
            past=True,
            future=False,
            child_of=root_event,
            plugin_name=plugin.name,
            hook_name=real_hook.name,
        )
        return process_event if isinstance(process_event, ProcessEvent) else None

    try:
        emitted = asyncio.run(run())
    finally:
        asyncio.run(bus.wait_until_idle())

    assert emitted is not None
    assert emitted.env["SNAP_DIR"] == str(output_dir)
    assert emitted.env["PERSONAS_DIR"] == str(output_dir / ".persona")
    assert emitted.env["ACTIVE_PERSONA"] == "Default"
    assert "CHROME_USER_DATA_DIR" not in emitted.env
    assert "CHROME_DOWNLOADS_DIR" not in emitted.env


def test_concurrent_snapshot_services_use_their_injected_runtime_config(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=30.0, name=f"snapshot_config_isolation_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    plugin = discover_plugins()["parse_txt_urls"]
    snapshots = [
        Snapshot(url="https://example.com/first", id="snap-config-first"),
        Snapshot(url="https://example.com/second", id="snap-config-second"),
    ]
    output_dirs = [tmp_path / "first", tmp_path / "second"]
    crawl_dir = tmp_path / "crawl"

    for snapshot, output_dir in zip(snapshots, output_dirs, strict=True):
        SnapshotService(
            bus,
            url=snapshot.url,
            snapshot=snapshot,
            output_dir=output_dir,
            plugins={plugin.name: plugin},
            config=_runtime_config(
                CRAWL_DIR=crawl_dir,
                EXTRA_CONTEXT=json.dumps({"snapshot_url": snapshot.url}),
                TIMEOUT=10,
            ),
            snapshot_phase_timeout=10.0,
        )

    async def run() -> list[ProcessEvent]:
        # Shared-bus history deliberately ends with a conflicting snapshot URL,
        # matching the state that raced in concurrent ArchiveBox runs.
        await bus.emit(
            MachineEvent(
                config={
                    "CRAWL_DIR": str(crawl_dir),
                    "EXTRA_CONTEXT": json.dumps({"snapshot_url": "https://example.com/conflicting"}),
                    "TIMEOUT": 10,
                },
                config_type="user",
            ),
        ).now()
        crawl_start_events = [
            CrawlStartEvent(url=snapshot.url, snapshot_id=snapshot.id, output_dir=str(output_dir))
            for snapshot, output_dir in zip(snapshots, output_dirs, strict=True)
        ]
        for crawl_start_event in crawl_start_events:
            await bus.emit(crawl_start_event).now()
        snapshot_events = [
            SnapshotEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_parent_id=crawl_start_event.event_id,
            )
            for snapshot, output_dir, crawl_start_event in zip(
                snapshots,
                output_dirs,
                crawl_start_events,
                strict=True,
            )
        ]
        await asyncio.gather(*(bus.emit(event).now() for event in snapshot_events))
        await bus.wait_until_idle()
        process_events = []
        for snapshot_event in snapshot_events:
            process_event = await bus.find(
                ProcessEvent,
                child_of=snapshot_event,
                past=True,
                future=False,
                plugin_name=plugin.name,
            )
            assert isinstance(process_event, ProcessEvent)
            process_events.append(process_event)
        return process_events

    process_events = asyncio.run(run())

    assert [event.env["CRAWL_DIR"] for event in process_events] == [str(crawl_dir), str(crawl_dir)]
    assert [json.loads(event.env["EXTRA_CONTEXT"])["snapshot_url"] for event in process_events] == [snapshot.url for snapshot in snapshots]


def test_snapshot_limit_admission_uses_stable_snapshot_id_across_retries(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    snapshot = Snapshot(url="https://example.com", id="snap-limit-retry")
    limit_state = CrawlLimitState(crawl_dir=output_dir, crawl_max_urls=1)
    assert limit_state.admit_snapshot(snapshot.id).allowed is True

    bus = create_bus(total_timeout=10.0, name=f"snapshot_limit_stable_id_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    SnapshotService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={},
        config=_runtime_config(
            CRAWL_DIR=output_dir,
            CRAWL_MAX_URLS=1,
            CRAWL_MAX_SIZE=0,
            SNAPSHOT_MAX_SIZE=0,
        ),
        snapshot_phase_timeout=2.0,
        snapshot_cleanup_phase_timeout=2.0,
    )

    async def run() -> SnapshotCompletedEvent | None:
        await bus.emit(
            MachineEvent(
                config={
                    "CRAWL_DIR": str(output_dir),
                    "CRAWL_MAX_URLS": 1,
                    "CRAWL_MAX_SIZE": 0,
                    "SNAPSHOT_MAX_SIZE": 0,
                },
                config_type="user",
            ),
        ).now()
        crawl_start_event = CrawlStartEvent(url=snapshot.url, snapshot_id=snapshot.id, output_dir=str(output_dir))
        root_event = SnapshotEvent(
            url=snapshot.url,
            snapshot_id=snapshot.id,
            output_dir=str(output_dir),
            event_parent_id=crawl_start_event.event_id,
        )
        await bus.emit(crawl_start_event).now()
        await bus.emit(root_event).now(timeout=2.0)
        completed = await bus.find(SnapshotCompletedEvent, child_of=root_event, past=True, future=1.0)
        await bus.wait_until_idle()
        return completed if isinstance(completed, SnapshotCompletedEvent) else None

    completed = asyncio.run(run())

    assert completed is not None
    assert CrawlLimitState(crawl_dir=output_dir, crawl_max_urls=1).admit_snapshot(snapshot.id).allowed is True


def test_snapshot_hook_binary_event_env_replay_applies_newest_last(tmp_path: Path) -> None:
    plugin = discover_plugins()["parse_txt_urls"]
    snapshot = Snapshot(url="https://example.com", id="snap-env-check")
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=30.0, name=f"snapshot_binary_env_order_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    SnapshotService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        config=_runtime_config(CRAWL_DIR=output_dir),
        snapshot_phase_timeout=10.0,
    )
    wget_binary = _resolve_real_wget_binary(tmp_path)

    async def run() -> ProcessEvent:
        await bus.emit(
            BinaryEvent(
                name="env-check-tool",
                abspath=str(wget_binary.abspath),
                env={"ABX_TEST_BINARY_MARKER": "old"},
                extra_context=_binary_extra_context(plugin_name=plugin.name),
            ),
        ).now()
        await bus.emit(
            BinaryEvent(
                name="env-check-tool",
                abspath=str(wget_binary.abspath),
                env={"ABX_TEST_BINARY_MARKER": "new"},
                extra_context=_binary_extra_context(plugin_name=plugin.name),
            ),
        ).now()
        crawl_start_event = CrawlStartEvent(url=snapshot.url, snapshot_id=snapshot.id, output_dir=str(output_dir))
        snapshot_event = SnapshotEvent(
            url=snapshot.url,
            snapshot_id=snapshot.id,
            output_dir=str(output_dir),
            event_parent_id=crawl_start_event.event_id,
        )
        await bus.emit(crawl_start_event).now()
        await bus.emit(snapshot_event).now()
        process_event = await bus.find(ProcessEvent, child_of=snapshot_event, past=True, future=False, plugin_name=plugin.name)
        assert isinstance(process_event, ProcessEvent)
        return process_event

    try:
        process_event = asyncio.run(run())
    finally:
        asyncio.run(bus.wait_until_idle())

    assert process_event.env["ABX_TEST_BINARY_MARKER"] == "new"


def test_crawl_setup_hook_binary_event_env_replay_applies_newest_last(tmp_path: Path) -> None:
    plugin = discover_plugins()["twocaptcha"]
    snapshot = Snapshot(url="https://example.com", id="snap-setup-env-check")
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=30.0, name=f"crawl_setup_binary_env_order_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    CrawlService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        crawl_event_enabled=False,
        crawl_start_enabled=False,
        crawl_cleanup_enabled=False,
        crawl_completed_enabled=False,
        crawl_setup_phase_timeout=10.0,
    )
    wget_binary = _resolve_real_wget_binary(tmp_path)

    async def run() -> ProcessEvent:
        await bus.emit(
            BinaryEvent(
                name="setup-env-check-tool",
                abspath=str(wget_binary.abspath),
                env={"ABX_TEST_BINARY_MARKER": "old"},
                extra_context=_binary_extra_context(plugin_name=plugin.name),
            ),
        ).now()
        await bus.emit(
            BinaryEvent(
                name="setup-env-check-tool",
                abspath=str(wget_binary.abspath),
                env={"ABX_TEST_BINARY_MARKER": "new"},
                extra_context=_binary_extra_context(plugin_name=plugin.name),
            ),
        ).now()
        crawl_event = CrawlEvent(url=snapshot.url, snapshot_id=snapshot.id, output_dir=str(output_dir))
        setup_event = CrawlSetupEvent(
            url=snapshot.url,
            snapshot_id=snapshot.id,
            output_dir=str(output_dir),
            event_parent_id=crawl_event.event_id,
        )
        await bus.emit(crawl_event).now()
        await bus.emit(setup_event).now()
        process_event = await bus.find(ProcessEvent, child_of=setup_event, past=True, future=False, plugin_name=plugin.name)
        assert isinstance(process_event, ProcessEvent)
        return process_event

    try:
        process_event = asyncio.run(run())
    finally:
        asyncio.run(bus.wait_until_idle())

    assert process_event.env["ABX_TEST_BINARY_MARKER"] == "new"


def test_snapshot_background_daemon_stays_alive_until_cleanup(tmp_path: Path) -> None:
    plugin = discover_plugins()["wget"]
    bus = create_bus(total_timeout=120.0, name=f"snapshot_real_background_lifecycle_{tmp_path.name}")
    started: list[ProcessStartedEvent] = []
    completed: list[ProcessCompletedEvent] = []

    async def on_ProcessStartedEvent(event: ProcessStartedEvent) -> None:
        if event.plugin_name == plugin.name:
            started.append(event)

    async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
        if event.plugin_name == plugin.name:
            completed.append(event)

    bus.on(ProcessStartedEvent, on_ProcessStartedEvent)
    bus.on(ProcessCompletedEvent, on_ProcessCompletedEvent)
    results = _run_download("https://example.com", {plugin.name: plugin}, tmp_path / "run", auto_install=True, emit_jsonl=False, bus=bus)
    assert [result.status for result in results if result.plugin == plugin.name] == ["succeeded"]
    assert len(started) == 1 and started[0].is_background is True
    assert len(completed) == 1 and completed[0].status == "succeeded" and completed[0].exit_code == 0
    assert not _pid_is_alive(started[0].pid)


def test_snapshot_abort_stops_scheduling_later_hooks(tmp_path: Path, httpserver: HTTPServer) -> None:
    stream_url, response_started, release_response = _streaming_http_response(httpserver, "/snapshot-abort")
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("chrome", "title")}
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=300.0, name=f"snapshot_abort_{tmp_path.name}")

    async def run() -> tuple[ProcessCompletedEvent | None, SnapshotCompletedEvent | None, ProcessStartedEvent | None]:
        task = asyncio.create_task(
            download(
                stream_url,
                plugins=selected,
                output_dir=output_dir,
                selected_plugins=list(selected),
                auto_install=True,
                emit_jsonl=False,
                interactive_tty=False,
                bus=bus,
            ),
        )
        navigate_started = await bus.find(
            ProcessStartedEvent,
            past=True,
            future=180.0,
            hook_name="on_Snapshot__30_chrome_navigate",
        )
        assert isinstance(navigate_started, ProcessStartedEvent)
        assert await asyncio.to_thread(response_started.wait, 60.0)
        crawl = await bus.find(CrawlEvent, past=True, future=False)
        assert isinstance(crawl, CrawlEvent)
        await bus.emit(CrawlAbortEvent(event_parent_id=crawl.event_id)).now()
        await task
        first_completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=False,
            hook_name="on_Snapshot__30_chrome_navigate",
        )
        snapshot_completed = await bus.find(
            SnapshotCompletedEvent,
            past=True,
            future=False,
        )
        second_started = await bus.find(ProcessStartedEvent, past=True, future=False, hook_name="on_Snapshot__54_title")
        await bus.wait_until_idle()
        return (
            first_completed if isinstance(first_completed, ProcessCompletedEvent) else None,
            snapshot_completed if isinstance(snapshot_completed, SnapshotCompletedEvent) else None,
            second_started if isinstance(second_started, ProcessStartedEvent) else None,
        )

    try:
        first_completed, snapshot_completed, second_started = asyncio.run(run())
    finally:
        release_response.set()

    assert first_completed is not None
    assert first_completed.status == "skipped"
    assert snapshot_completed is not None
    assert second_started is None


def test_snapshot_completed_waits_for_cleanup_process_listeners(tmp_path: Path) -> None:
    plugin = discover_plugins()["wget"]
    daemon_hook_name = plugin.hooks[0].name

    output_dir = tmp_path / "run"
    side_effect = output_dir / "process-completed-listener.txt"
    bus = create_bus(total_timeout=20.0, name=f"snapshot_cleanup_wait_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    SnapshotService(
        bus,
        url="https://example.com",
        snapshot=Snapshot(url="https://example.com", id="snap-cleanup-wait"),
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        config=_runtime_config(CRAWL_DIR=output_dir),
        snapshot_phase_timeout=10.0,
        snapshot_cleanup_phase_timeout=5.0,
    )
    completed_saw_side_effect: list[bool] = []
    listener_started = asyncio.Event()
    release_listener = asyncio.Event()

    async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
        if event.hook_name == daemon_hook_name:
            listener_started.set()
            await release_listener.wait()
            side_effect.parent.mkdir(parents=True, exist_ok=True)
            side_effect.write_text("done")

    async def on_SnapshotCompletedEvent(event: SnapshotCompletedEvent) -> None:
        completed_saw_side_effect.append(side_effect.exists())

    bus.on(ProcessCompletedEvent, on_ProcessCompletedEvent)
    bus.on(SnapshotCompletedEvent, on_SnapshotCompletedEvent)

    async def run() -> None:
        crawl_start_event = CrawlStartEvent(
            url="https://example.com",
            snapshot_id="snap-cleanup-wait",
            output_dir=str(output_dir),
        )
        root_event = SnapshotEvent(
            url="https://example.com",
            snapshot_id="snap-cleanup-wait",
            output_dir=str(output_dir),
            event_parent_id=crawl_start_event.event_id,
        )
        await bus.emit(crawl_start_event).now()
        snapshot_task = asyncio.create_task(bus.emit(root_event).now())
        await asyncio.wait_for(listener_started.wait(), timeout=5.0)
        assert await bus.find(SnapshotCompletedEvent, past=True, future=False) is None
        release_listener.set()
        await snapshot_task
        await bus.wait_until_idle()

    asyncio.run(run())

    assert side_effect.read_text() == "done"
    assert completed_saw_side_effect == [True]


def test_crawl_setup_background_daemon_survives_until_explicit_cleanup(tmp_path: Path) -> None:
    plugin = discover_plugins()["chrome"]
    daemon_hook_name = "on_CrawlSetup__90_chrome_launch.daemon.bg"
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=300.0, name=f"crawl_bg_lifetime_{tmp_path.name}")

    async def run() -> ProcessCompletedEvent | None:
        await download(
            "https://example.com",
            plugins={plugin.name: plugin},
            output_dir=output_dir,
            selected_plugins=[plugin.name],
            auto_install=True,
            emit_jsonl=False,
            interactive_tty=False,
            crawl_start_enabled=False,
            crawl_cleanup_enabled=False,
            bus=bus,
        )
        started = await bus.find(ProcessStartedEvent, past=True, future=False, hook_name=daemon_hook_name)
        assert isinstance(started, ProcessStartedEvent)
        os.kill(started.pid, 0)
        assert started.stdout_file.is_file()
        started.stdout_file.unlink()
        crawl_event = await bus.find(CrawlEvent, past=True, future=False)
        assert isinstance(crawl_event, CrawlEvent)
        await bus.emit(
            CrawlCleanupEvent(
                url=crawl_event.url,
                snapshot_id=crawl_event.snapshot_id,
                output_dir=str(output_dir),
                event_parent_id=crawl_event.event_id,
            ),
        ).now()
        completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=60.0,
            hook_name=daemon_hook_name,
        )
        await bus.wait_until_idle()
        return completed if isinstance(completed, ProcessCompletedEvent) else None

    daemon_completed = asyncio.run(run())

    assert daemon_completed is not None
    assert daemon_completed.status == "succeeded"
    assert daemon_completed.exit_code == 0
    cleanup_records = []
    for line in daemon_completed.stdout.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            cleanup_records.append(record)
    assert {"succeeded": True, "skipped": False} in cleanup_records


def test_crawl_completed_waits_for_cleanup_process_listeners(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    side_effect = output_dir / "process-completed-listener.txt"
    plugin = discover_plugins()["chrome"]
    hook_name = "on_CrawlSetup__90_chrome_launch.daemon.bg"
    bus = create_bus(total_timeout=300.0, name=f"crawl_cleanup_wait_{tmp_path.name}")
    completed_saw_side_effect: list[bool] = []
    listener_started = asyncio.Event()
    release_listener = asyncio.Event()

    async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
        if event.hook_name == hook_name:
            listener_started.set()
            await release_listener.wait()
            side_effect.parent.mkdir(parents=True, exist_ok=True)
            side_effect.write_text("done")

    async def on_CrawlCompletedEvent(event: CrawlCompletedEvent) -> None:
        completed_saw_side_effect.append(side_effect.exists())

    bus.on(ProcessCompletedEvent, on_ProcessCompletedEvent)
    bus.on(CrawlCompletedEvent, on_CrawlCompletedEvent)

    async def run() -> None:
        download_task = asyncio.create_task(
            download(
                "https://example.com",
                plugins={plugin.name: plugin},
                output_dir=output_dir,
                selected_plugins=[plugin.name],
                auto_install=True,
                emit_jsonl=False,
                interactive_tty=False,
                crawl_start_enabled=False,
                bus=bus,
            ),
        )
        await asyncio.wait_for(listener_started.wait(), timeout=240.0)
        assert await bus.find(CrawlCompletedEvent, past=True, future=False) is None
        release_listener.set()
        await download_task
        await bus.wait_until_idle()

    asyncio.run(run())

    assert side_effect.read_text() == "done"
    assert completed_saw_side_effect == [True]


def test_crawl_abort_during_setup_cleans_background_daemon(tmp_path: Path) -> None:
    plugin = discover_plugins()["chrome"]
    daemon_hook_name = "on_CrawlSetup__90_chrome_launch.daemon.bg"
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=300.0, name=f"crawl_setup_abort_{tmp_path.name}")

    async def run() -> ProcessCompletedEvent | None:
        task = asyncio.create_task(
            download(
                "https://example.com",
                plugins={plugin.name: plugin},
                output_dir=output_dir,
                selected_plugins=[plugin.name],
                auto_install=True,
                emit_jsonl=False,
                interactive_tty=False,
                crawl_start_enabled=False,
                bus=bus,
            ),
        )
        started = await bus.find(ProcessStartedEvent, past=True, future=180.0, hook_name=daemon_hook_name)
        assert isinstance(started, ProcessStartedEvent)
        crawl = await bus.find(CrawlEvent, past=True, future=False)
        assert isinstance(crawl, CrawlEvent)
        await bus.emit(CrawlAbortEvent(event_parent_id=crawl.event_id)).now()
        await task
        completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=False,
            hook_name=daemon_hook_name,
        )
        await bus.wait_until_idle()
        return completed if isinstance(completed, ProcessCompletedEvent) else None

    daemon_completed = asyncio.run(run())

    assert daemon_completed is not None
    assert daemon_completed.status == "succeeded"


def test_crawl_abort_during_foreground_setup_interrupts_hook_and_stops_later_setup(
    tmp_path: Path,
) -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("chrome", "claudechrome")}
    daemon_hook_name = "on_CrawlSetup__90_chrome_launch.daemon.bg"
    foreground_hook_name = "on_CrawlSetup__91_chrome_wait"
    later_hook_name = "on_CrawlSetup__96_claudechrome_config"
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=300.0, name=f"crawl_setup_abort_foreground_{tmp_path.name}")

    async def run() -> tuple[ProcessCompletedEvent | None, ProcessCompletedEvent | None, list[ProcessStartedEvent]]:
        crawl_task = asyncio.create_task(
            download(
                "https://example.com",
                plugins=selected,
                output_dir=output_dir,
                selected_plugins=list(selected),
                config_overrides={"CLAUDECHROME_ENABLED": True},
                auto_install=True,
                emit_jsonl=False,
                interactive_tty=False,
                crawl_start_enabled=False,
                bus=bus,
            ),
        )
        daemon_started = await bus.find(ProcessStartedEvent, past=True, future=180.0, hook_name=daemon_hook_name)
        foreground_started = await bus.find(ProcessStartedEvent, past=True, future=180.0, hook_name=foreground_hook_name)
        assert isinstance(daemon_started, ProcessStartedEvent)
        assert isinstance(foreground_started, ProcessStartedEvent)
        crawl_event = await bus.find(CrawlEvent, past=True, future=False)
        assert isinstance(crawl_event, CrawlEvent)
        await bus.emit(CrawlAbortEvent(event_parent_id=crawl_event.event_id)).now()
        await crawl_task
        daemon_completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=False,
            hook_name=daemon_hook_name,
        )
        foreground_completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=False,
            hook_name=foreground_hook_name,
        )
        later_started = await bus.filter(
            ProcessStartedEvent,
            past=True,
            hook_name=later_hook_name,
        )
        await bus.wait_until_idle()
        return (
            daemon_completed if isinstance(daemon_completed, ProcessCompletedEvent) else None,
            foreground_completed if isinstance(foreground_completed, ProcessCompletedEvent) else None,
            later_started,
        )

    daemon_completed, foreground_completed, later_started = asyncio.run(run())

    assert daemon_completed is not None
    assert daemon_completed.status == "succeeded"
    assert foreground_completed is not None
    assert foreground_completed.status == "skipped"
    assert "Hook interrupted by user" in foreground_completed.stderr
    assert later_started == []


def test_crawl_abort_cleans_real_chrome_process_tree_and_foreground_hook(
    tmp_path: Path,
    httpserver: HTTPServer,
) -> None:
    plugin = discover_plugins()["chrome"]
    stream_url, response_started, release_response = _streaming_http_response(httpserver, "/chrome-abort")
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=300.0, name=f"real_chrome_abort_{tmp_path.name}")

    async def run() -> tuple[int, int, int, list[ProcessCompletedEvent], list[ProcessKillEvent]]:
        download_task = asyncio.create_task(
            download(
                stream_url,
                plugins={plugin.name: plugin},
                output_dir=output_dir,
                selected_plugins=[plugin.name],
                auto_install=True,
                emit_jsonl=False,
                interactive_tty=False,
                bus=bus,
            ),
        )
        navigate_started = await bus.find(
            ProcessStartedEvent,
            past=True,
            future=180.0,
            hook_name="on_Snapshot__30_chrome_navigate",
        )
        assert isinstance(navigate_started, ProcessStartedEvent)
        assert await asyncio.to_thread(response_started.wait, 60.0)

        launch_started = await bus.find(
            ProcessStartedEvent,
            past=True,
            future=False,
            hook_name="on_CrawlSetup__90_chrome_launch.daemon.bg",
        )
        tab_started = await bus.find(
            ProcessStartedEvent,
            past=True,
            future=False,
            hook_name="on_Snapshot__10_chrome_tab.daemon.bg",
        )
        assert isinstance(launch_started, ProcessStartedEvent)
        assert isinstance(tab_started, ProcessStartedEvent)
        chrome_pid = int((output_dir / "chrome" / "chrome.pid").read_text().strip())
        assert all(_pid_is_alive(pid) for pid in (launch_started.pid, chrome_pid, navigate_started.pid))

        crawl = await bus.find(CrawlEvent, past=True, future=False)
        assert isinstance(crawl, CrawlEvent)
        await bus.emit(CrawlAbortEvent(event_parent_id=crawl.event_id)).now()
        await download_task
        await bus.wait_until_idle()
        completed = await bus.filter(ProcessCompletedEvent, past=True, future=False)
        kills = await bus.filter(ProcessKillEvent, past=True, future=False)
        return launch_started.pid, chrome_pid, navigate_started.pid, completed, kills

    try:
        launch_pid, chrome_pid, navigate_pid, completed, kills = asyncio.run(run())
    finally:
        release_response.set()

    by_hook = {event.hook_name: event for event in completed}
    launch_completed = by_hook["on_CrawlSetup__90_chrome_launch.daemon.bg"]
    navigate_completed = by_hook["on_Snapshot__30_chrome_navigate"]
    assert launch_completed.status == "succeeded"
    assert "shutting down" in launch_completed.stdout
    assert "exited successfully" in launch_completed.stdout
    assert navigate_completed.status == "skipped"
    assert navigate_completed.stderr == "Hook interrupted by user"
    assert {event.hook_name for event in kills} >= {
        "on_CrawlSetup__90_chrome_launch.daemon.bg",
        "on_Snapshot__10_chrome_tab.daemon.bg",
        "on_Snapshot__30_chrome_navigate",
    }
    assert not any(_pid_is_alive(pid) for pid in (launch_pid, chrome_pid, navigate_pid))
    assert not (output_dir / "chrome" / "chrome.pid").exists()


def test_real_chrome_hook_completes_while_child_survives_then_lifecycle_cleans_it(tmp_path: Path) -> None:
    plugin = discover_plugins()["chrome"]
    output_dir = tmp_path / "run"

    async def run() -> tuple[ProcessCompletedEvent, int, ProcessCompletedEvent]:
        keepalive_bus = create_bus(total_timeout=300.0, name=f"chrome_keepalive_parent_{tmp_path.name}")
        await download(
            "https://example.com",
            plugins={plugin.name: plugin},
            output_dir=output_dir,
            selected_plugins=[plugin.name],
            config_overrides={"CHROME_KEEPALIVE": True},
            auto_install=True,
            emit_jsonl=False,
            interactive_tty=False,
            crawl_start_enabled=False,
            bus=keepalive_bus,
        )
        await keepalive_bus.wait_until_idle()
        first_completed = await keepalive_bus.find(
            ProcessCompletedEvent,
            past=True,
            future=False,
            hook_name="on_CrawlSetup__90_chrome_launch.daemon.bg",
        )
        assert isinstance(first_completed, ProcessCompletedEvent)
        chrome_pid = int((output_dir / "chrome" / "chrome.pid").read_text().strip())
        assert _pid_is_alive(chrome_pid)

        cleanup_bus = create_bus(total_timeout=300.0, name=f"chrome_adopt_cleanup_{tmp_path.name}")
        await download(
            "https://example.com",
            plugins={plugin.name: plugin},
            output_dir=output_dir,
            selected_plugins=[plugin.name],
            config_overrides={"CHROME_KEEPALIVE": False},
            auto_install=True,
            emit_jsonl=False,
            interactive_tty=False,
            crawl_start_enabled=False,
            bus=cleanup_bus,
        )
        await cleanup_bus.wait_until_idle()
        second_completed = await cleanup_bus.find(
            ProcessCompletedEvent,
            past=True,
            future=False,
            hook_name="on_CrawlSetup__90_chrome_launch.daemon.bg",
        )
        assert isinstance(second_completed, ProcessCompletedEvent)
        return first_completed, chrome_pid, second_completed

    first_completed, chrome_pid, second_completed = asyncio.run(run())

    assert first_completed.status == "succeeded"
    assert first_completed.exit_code == 0
    assert first_completed.stdout == ""
    assert "session started" in first_completed.stderr
    assert second_completed.status == "succeeded"
    assert "shutting down" in second_completed.stdout
    assert "exited successfully" in second_completed.stdout
    assert not _pid_is_alive(chrome_pid)
    assert not (output_dir / "chrome" / "chrome.pid").exists()


def test_crawl_abort_from_crawl_event_interrupts_active_setup_hook(tmp_path: Path) -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("chrome", "twocaptcha")}
    foreground_hook_name = "on_CrawlSetup__91_chrome_wait"
    later_hook_name = "on_CrawlSetup__95_twocaptcha_config"
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=300.0, name=f"crawl_setup_abort_from_root_{tmp_path.name}")

    async def run() -> tuple[ProcessCompletedEvent | None, list[ProcessStartedEvent]]:
        task = asyncio.create_task(
            download(
                "https://example.com",
                plugins=selected,
                output_dir=output_dir,
                selected_plugins=list(selected),
                auto_install=True,
                emit_jsonl=False,
                interactive_tty=False,
                crawl_start_enabled=False,
                bus=bus,
            ),
        )
        started = await bus.find(ProcessStartedEvent, past=True, future=180.0, hook_name=foreground_hook_name)
        assert isinstance(started, ProcessStartedEvent)
        crawl = await bus.find(CrawlEvent, past=True, future=False)
        assert isinstance(crawl, CrawlEvent)
        await bus.emit(CrawlAbortEvent(event_parent_id=crawl.event_id)).now()
        await task
        foreground_completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=False,
            hook_name=foreground_hook_name,
        )
        later_started = await bus.filter(
            ProcessStartedEvent,
            past=True,
            hook_name=later_hook_name,
        )
        await bus.wait_until_idle()
        return foreground_completed if isinstance(foreground_completed, ProcessCompletedEvent) else None, later_started

    foreground_completed, later_started = asyncio.run(run())

    assert foreground_completed is not None
    assert foreground_completed.status == "skipped"
    assert "Hook interrupted by user" in foreground_completed.stderr
    assert later_started == []


def test_crawl_runs_real_background_wget_through_cleanup(tmp_path: Path) -> None:
    plugin = discover_plugins()["wget"]
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=120.0, name=f"crawl_real_background_lifecycle_{tmp_path.name}")
    started: list[ProcessStartedEvent] = []
    completed: list[ProcessCompletedEvent] = []

    async def on_ProcessStartedEvent(event: ProcessStartedEvent) -> None:
        if event.plugin_name == plugin.name:
            started.append(event)

    async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
        if event.plugin_name == plugin.name:
            completed.append(event)

    bus.on(ProcessStartedEvent, on_ProcessStartedEvent)
    bus.on(ProcessCompletedEvent, on_ProcessCompletedEvent)
    _run_download("https://example.com", {plugin.name: plugin}, output_dir, auto_install=True, emit_jsonl=False, bus=bus)
    assert len(started) == 1 and started[0].is_background is True
    assert len(completed) == 1 and completed[0].status == "succeeded"
    assert (output_dir / "wget" / "example.com" / "index.html").is_file() and not _pid_is_alive(started[0].pid)


def test_process_kill_uses_live_subprocess_handle_when_pid_file_validation_fails(
    tmp_path: Path,
    httpserver: HTTPServer,
) -> None:
    plugin = discover_plugins()["wget"]
    stream_url, response_started, release_response = _streaming_http_response(httpserver, "/live-handle")
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=60.0, name=f"process_kill_live_handle_{tmp_path.name}")

    async def run() -> ProcessCompletedEvent:
        download_task = asyncio.create_task(
            download(
                stream_url,
                plugins={plugin.name: plugin},
                output_dir=output_dir,
                selected_plugins=[plugin.name],
                auto_install=True,
                emit_jsonl=False,
                interactive_tty=False,
                bus=bus,
            ),
        )
        process_event = await bus.find(
            ProcessEvent,
            past=True,
            future=30.0,
            plugin_name=plugin.name,
            hook_name=plugin.hooks[0].name,
        )
        assert isinstance(process_event, ProcessEvent)
        started_process = await bus.find(
            ProcessStartedEvent,
            child_of=process_event,
            past=True,
            future=30.0,
        )
        assert isinstance(started_process, ProcessStartedEvent)
        assert await asyncio.to_thread(response_started.wait, 30.0)

        os.utime(started_process.pid_file, (1, 1))
        try:
            await bus.emit(
                ProcessKillEvent(
                    plugin_name=started_process.plugin_name,
                    hook_name=started_process.hook_name,
                    pid=started_process.pid,
                    grace_period=1.0,
                    event_timeout=10.0,
                    event_parent_id=started_process.event_id,
                ),
            ).now()
        finally:
            release_response.set()

        await download_task
        completed_process = await bus.find(
            ProcessCompletedEvent,
            child_of=process_event,
            past=True,
            future=10.0,
        )
        assert isinstance(completed_process, ProcessCompletedEvent)
        await bus.wait_until_idle()
        return completed_process

    completed = asyncio.run(run())

    assert completed.status == "failed"
    assert completed.exit_code == -signal.SIGKILL
    assert not list(output_dir.rglob(f"{plugin.hooks[0].name}.*.pid"))


def test_download_cleanup_records_real_background_process_without_failure(tmp_path: Path) -> None:
    plugin = discover_plugins()["wget"]
    output_dir = tmp_path / "run"
    _run_download(
        "https://example.com",
        plugins={plugin.name: plugin},
        output_dir=output_dir,
        selected_plugins=[plugin.name],
        auto_install=True,
        emit_jsonl=False,
        interactive_tty=False,
    )
    records = [json.loads(line) for line in (output_dir / "index.jsonl").read_text().splitlines() if line.startswith("{")]
    processes = [r for r in records if r.get("type") == "Process" and r.get("hook_name") == plugin.hooks[0].name]
    results = [r for r in records if r.get("type") == "ArchiveResult" and r.get("plugin") == plugin.name]
    assert processes[-1]["status"] == "succeeded" and processes[-1]["exit_code"] == 0 and results[-1]["status"] == "succeeded"
    assert not [r for r in records if r.get("status") == "failed"]


def test_background_process_event_returns_after_real_hook_start(tmp_path: Path, httpserver: HTTPServer) -> None:
    plugin = discover_plugins()["wget"]
    stream_url, response_started, release_response = _streaming_http_response(httpserver, "/background-process")
    output_dir = tmp_path / "run"
    bus = create_bus(total_timeout=60.0, name=f"background_returns_after_start_{tmp_path.name}")

    async def run() -> ProcessCompletedEvent:
        download_task = asyncio.create_task(
            download(
                stream_url,
                plugins={plugin.name: plugin},
                output_dir=output_dir,
                selected_plugins=[plugin.name],
                auto_install=True,
                emit_jsonl=False,
                interactive_tty=False,
                bus=bus,
            ),
        )
        process_event = await bus.find(
            ProcessEvent,
            past=True,
            future=30.0,
            plugin_name=plugin.name,
            hook_name=plugin.hooks[0].name,
        )
        assert isinstance(process_event, ProcessEvent)
        started_process = await bus.find(
            ProcessStartedEvent,
            child_of=process_event,
            past=True,
            future=30.0,
        )
        assert isinstance(started_process, ProcessStartedEvent)
        assert await asyncio.to_thread(response_started.wait, 30.0)
        assert await asyncio.wait_for(process_event.event_result(), timeout=5.0) is not None
        assert _pid_is_alive(started_process.pid)

        release_response.set()
        await download_task
        completed_process = await bus.find(
            ProcessCompletedEvent,
            child_of=process_event,
            past=True,
            future=10.0,
        )
        assert isinstance(completed_process, ProcessCompletedEvent)
        await bus.wait_until_idle()
        return completed_process

    try:
        completed = asyncio.run(run())
    finally:
        release_response.set()

    assert completed.status == "succeeded"
    assert completed.exit_code == 0
    assert not list(output_dir.rglob(f"{plugin.hooks[0].name}.*.pid"))


def test_process_event_subprocess_starts_once_when_event_is_awaited_twice(tmp_path: Path) -> None:
    real_hook = discover_plugins()["parse_txt_urls"].hooks[0]

    output_dir = tmp_path / "run" / "start_once"
    bus = create_bus(total_timeout=10.0, name=f"process_event_start_once_{tmp_path.name}")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)

    async def run() -> tuple[list[ProcessStartedEvent], ProcessCompletedEvent | None]:
        process_event = bus.emit(
            ProcessEvent(
                plugin_name="parse_txt_urls",
                hook_name=real_hook.name,
                hook_path=str(real_hook.path),
                hook_args=["--url=https://example.com"],
                is_background=False,
                output_dir=str(output_dir),
                env=os.environ.copy(),
                timeout=5,
                event_timeout=10.0,
                event_handler_timeout=10.0,
            ),
        )
        await asyncio.gather(process_event.now(), process_event.now())
        completed = await bus.find(
            ProcessCompletedEvent,
            child_of=process_event,
            past=True,
            future=5.0,
        )
        await bus.wait_until_idle()
        started = await bus.filter(ProcessStartedEvent, child_of=process_event, past=True)
        return started, completed if isinstance(completed, ProcessCompletedEvent) else None

    started_events, completed_event = asyncio.run(run())

    assert len(started_events) == 1
    assert completed_event is not None
    assert completed_event.status == "succeeded"


def test_concurrent_process_events_for_same_hook_keep_distinct_artifacts(tmp_path: Path) -> None:
    real_hook = discover_plugins()["parse_txt_urls"].hooks[0]

    output_dir = tmp_path / "run" / "same_hook"
    bus = create_bus(total_timeout=10.0, name=f"process_event_same_hook_{tmp_path.name}")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)

    async def run() -> list[ProcessCompletedEvent]:
        events = [
            bus.emit(
                ProcessEvent(
                    plugin_name="parse_txt_urls",
                    hook_name=real_hook.name,
                    hook_path=str(real_hook.path),
                    hook_args=["--url=https://example.com"],
                    is_background=False,
                    output_dir=str(output_dir),
                    env=os.environ.copy(),
                    timeout=5,
                    event_timeout=10.0,
                    event_handler_timeout=10.0,
                ),
            )
            for _ in range(2)
        ]
        await asyncio.gather(*(event.now() for event in events))
        await bus.wait_until_idle()
        completed = []
        for event in events:
            found = await bus.find(ProcessCompletedEvent, child_of=event, past=True)
            assert isinstance(found, ProcessCompletedEvent)
            completed.append(found)
        return completed

    completed_events = asyncio.run(run())

    assert [event.status for event in completed_events] == ["succeeded", "succeeded"]
    assert len(list(output_dir.glob(f"{real_hook.name}.*.sh"))) == 2


def test_process_completion_waits_for_stdout_consumers(tmp_path: Path) -> None:
    real_hook = discover_plugins()["parse_txt_urls"].hooks[0]

    output_dir = tmp_path / "run" / "stdout_order"
    bus = create_bus(total_timeout=10.0, name=f"process_stdout_order_{tmp_path.name}")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    consumed_stdout = asyncio.Event()
    stdout_listener_started = asyncio.Event()
    release_stdout_listener = asyncio.Event()
    event_order: list[str] = []

    async def on_ProcessStdoutEvent(event: ProcessStdoutEvent) -> None:
        if event.plugin_name != "parse_txt_urls":
            return
        stdout_listener_started.set()
        await release_stdout_listener.wait()
        event_order.append("stdout")
        consumed_stdout.set()

    async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
        if event.plugin_name != "parse_txt_urls":
            return
        event_order.append("completed")

    bus.on(ProcessStdoutEvent, on_ProcessStdoutEvent)
    bus.on(ProcessCompletedEvent, on_ProcessCompletedEvent)

    async def run() -> None:
        async def on_SnapshotEvent(snapshot_event: SnapshotEvent) -> None:
            process_event = snapshot_event.emit(
                ProcessEvent(
                    plugin_name="parse_txt_urls",
                    hook_name=real_hook.name,
                    hook_path=str(real_hook.path),
                    hook_args=["--url=https://example.com"],
                    is_background=False,
                    output_dir=str(output_dir),
                    env=os.environ.copy(),
                    timeout=5,
                    event_timeout=10.0,
                    event_handler_timeout=10.0,
                ),
            )
            await (await process_event.now()).event_results_list()
            started = await bus.find(
                ProcessStartedEvent,
                child_of=process_event,
                past=True,
                future=2.0,
            )
            assert isinstance(started, ProcessStartedEvent)
            stdout = await bus.find(
                ProcessStdoutEvent,
                child_of=process_event,
                past=True,
                future=2.0,
            )
            assert isinstance(stdout, ProcessStdoutEvent)
            completed = await bus.find(
                ProcessCompletedEvent,
                child_of=process_event,
                past=True,
                future=2.0,
            )
            assert isinstance(completed, ProcessCompletedEvent)

        bus.on(SnapshotEvent, on_SnapshotEvent)
        snapshot_event = bus.emit(
            SnapshotEvent(
                url="https://example.com",
                snapshot_id="stdout-order",
                output_dir=str(tmp_path / "run"),
            ),
        )
        snapshot_task = asyncio.create_task(snapshot_event.now())
        await asyncio.wait_for(stdout_listener_started.wait(), timeout=5.0)
        assert await bus.find(ProcessCompletedEvent, past=True, future=False, plugin_name="parse_txt_urls") is None
        release_stdout_listener.set()
        await (await snapshot_task).event_results_list()
        await bus.wait_until_idle()

    asyncio.run(run())

    assert consumed_stdout.is_set()
    assert event_order[-1] == "completed"
    assert event_order[:-1]
    assert set(event_order[:-1]) == {"stdout"}


def test_download_can_suppress_jsonl_stdout(tmp_path: Path, capsys) -> None:
    plugins = discover_plugins()
    selected = {"wget": plugins["wget"]}
    results = _run_download(
        "https://example.com",
        selected,
        tmp_path / "run",
        auto_install=True,
        emit_jsonl=False,
    )
    captured = capsys.readouterr()

    assert next(result for result in results if result.plugin == "wget").status == "succeeded"
    assert captured.out == ""


def test_nested_snapshot_events_are_emitted_but_ignored_by_snapshot_hooks(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=60.0, name=f"nested_snapshot_events_{tmp_path.name}")
    seen_snapshot_events: list[tuple[int, str]] = []

    async def on_SnapshotEvent(event: SnapshotEvent) -> None:
        seen_snapshot_events.append((event.depth, event.url))

    bus.on(SnapshotEvent, on_SnapshotEvent)
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    snapshot = Snapshot(url="https://example.com", depth=0, id="root-depth-0")
    SnapshotService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=tmp_path / "run",
        plugins={},
        config=_runtime_config(CRAWL_DIR=tmp_path / "run"),
        snapshot_phase_timeout=60.0,
        snapshot_cleanup_phase_timeout=60.0,
    )

    crawl_start_event = CrawlStartEvent(
        url=snapshot.url,
        snapshot_id=snapshot.id,
        output_dir=str(tmp_path / "run"),
    )
    root_event = SnapshotEvent(
        url=snapshot.url,
        snapshot_id=snapshot.id,
        output_dir=str(tmp_path / "run"),
        depth=snapshot.depth,
        event_parent_id=crawl_start_event.event_id,
    )
    process_event = ProcessEvent(
        plugin_name="producer",
        hook_name="on_Snapshot__10_emit_nested",
        hook_path=_real_hook_path("parse_html_urls", "on_Snapshot__70_parse_html_urls"),
        hook_args=[],
        is_background=False,
        output_dir=str(tmp_path / "run" / "producer"),
        env={},
        timeout=60,
        event_parent_id=root_event.event_id,
    )
    stdout_event = ProcessStdoutEvent(
        line='{"type":"Snapshot","id":"child-depth-1","url":"https://example.com/child","depth":999}',
        plugin_name="producer",
        hook_name="on_Snapshot__10_emit_nested",
        output_dir=str(tmp_path / "run" / "producer"),
        event_parent_id=process_event.event_id,
    )

    async def run() -> None:
        await bus.emit(MachineEvent(config={"CRAWL_DIR": str(tmp_path / "run")}, config_type="user")).now()
        await bus.emit(crawl_start_event).now()
        await bus.emit(root_event).now()
        await bus.emit(process_event).now()
        await bus.emit(stdout_event).now()
        await bus.wait_until_idle()

    asyncio.run(run())
    assert seen_snapshot_events == [(0, "https://example.com"), (1, "https://example.com/child")]


def test_discovered_snapshot_depth_increments_from_parent_snapshot(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=60.0, name=f"discovered_snapshot_depth_{tmp_path.name}")
    seen_snapshot_events: list[tuple[int, str]] = []

    async def on_SnapshotEvent(event: SnapshotEvent) -> None:
        seen_snapshot_events.append((event.depth, event.url))

    bus.on(SnapshotEvent, on_SnapshotEvent)
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    snapshot = Snapshot(url="https://example.com/parent", depth=2, id="parent-depth-2")
    SnapshotService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=tmp_path / "run",
        plugins={},
        config=_runtime_config(CRAWL_DIR=tmp_path / "run"),
        snapshot_phase_timeout=60.0,
        snapshot_cleanup_phase_timeout=60.0,
    )

    crawl_start_event = CrawlStartEvent(
        url=snapshot.url,
        snapshot_id=snapshot.id,
        output_dir=str(tmp_path / "run"),
    )
    root_event = SnapshotEvent(
        url=snapshot.url,
        snapshot_id=snapshot.id,
        output_dir=str(tmp_path / "run"),
        depth=snapshot.depth,
        event_parent_id=crawl_start_event.event_id,
    )
    process_event = ProcessEvent(
        plugin_name="producer",
        hook_name="on_Snapshot__10_emit_nested",
        hook_path=_real_hook_path("parse_html_urls", "on_Snapshot__70_parse_html_urls"),
        hook_args=[],
        is_background=False,
        output_dir=str(tmp_path / "run" / "producer"),
        env={},
        timeout=60,
        event_parent_id=root_event.event_id,
    )
    stdout_event = ProcessStdoutEvent(
        line='{"type":"Snapshot","id":"child-depth-3","url":"https://example.com/grandchild","depth":999}',
        plugin_name="producer",
        hook_name="on_Snapshot__10_emit_nested",
        output_dir=str(tmp_path / "run" / "producer"),
        event_parent_id=process_event.event_id,
    )

    async def run() -> None:
        await bus.emit(MachineEvent(config={"CRAWL_DIR": str(tmp_path / "run")}, config_type="user")).now()
        await bus.emit(crawl_start_event).now()
        await bus.emit(root_event).now()
        await bus.emit(process_event).now()
        await bus.emit(stdout_event).now()
        await bus.wait_until_idle()

    asyncio.run(run())
    assert seen_snapshot_events == [(2, "https://example.com/parent"), (3, "https://example.com/grandchild")]


def test_inline_archive_result_collects_current_output_files(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=60.0, name=f"inline_archive_result_output_files_{tmp_path.name}")
    seen_archive_results: list[ArchiveResultEvent] = []

    async def on_ArchiveResultEvent(event: ArchiveResultEvent) -> None:
        seen_archive_results.append(event)

    bus.on(ArchiveResultEvent, on_ArchiveResultEvent)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    ArchiveResultService(bus, emit_jsonl=False)

    wget_hook = discover_plugins()["wget"].hooks[0]
    wget_binary = _resolve_real_wget_binary(tmp_path)
    run_dir = tmp_path / "run"
    process_env = {
        **os.environ,
        **{str(key): str(value) for key, value in wget_binary.env.items()},
        "SNAP_DIR": str(run_dir),
        "WGET_BINARY": str(wget_binary.abspath),
        "WGET_WARC_ENABLED": "false",
    }

    crawl_start_event = CrawlStartEvent(
        url="https://example.com",
        snapshot_id="snap-inline-output-files",
        output_dir=str(tmp_path / "run"),
    )
    snapshot_event = SnapshotEvent(
        url="https://example.com",
        snapshot_id="snap-inline-output-files",
        output_dir=str(tmp_path / "run"),
        event_parent_id=crawl_start_event.event_id,
    )
    process_event = ProcessEvent(
        plugin_name="wget",
        hook_name=wget_hook.name,
        hook_path=str(wget_hook.path),
        hook_args=["--url=https://example.com"],
        is_background=False,
        output_dir=str(run_dir / "wget"),
        env=process_env,
        timeout=60,
        event_parent_id=snapshot_event.event_id,
    )

    async def run() -> None:
        await bus.emit(crawl_start_event).now()
        await bus.emit(snapshot_event).now()
        await bus.emit(process_event).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    assert len(seen_archive_results) == 1
    assert seen_archive_results[0].status == "succeeded"
    assert "example.com/index.html" in [output_file.path for output_file in seen_archive_results[0].output_files]
    assert (run_dir / "wget" / "example.com" / "index.html").is_file()
