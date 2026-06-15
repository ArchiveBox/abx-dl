import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Self
from uuid import uuid4

from abxpkg import Binary as AbxBinary
from abxpkg.base_types import BinProviderName
from abxpkg.binary_service import BinaryCacheService, BinaryEvent, BinaryRequestEvent, BinaryService

from abx_dl.config import get_initial_env
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
    SnapshotEvent,
)
from abx_dl.limits import CrawlLimitState
from abx_dl.models import Hook, Plugin, PluginConfig, Snapshot, discover_plugins
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


def _wait_for_pid_exit(pid: int, *, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_is_alive(pid):
            return
        time.sleep(0.05)
    raise AssertionError(f"PID {pid} is still alive")


def _cleanup_test_process_group(group_pid: int | None, *child_pids: int | None) -> None:
    if group_pid and _pid_is_alive(group_pid):
        try:
            os.killpg(group_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError:
            try:
                os.kill(group_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    for pid in child_pids:
        if pid and _pid_is_alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


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
    assert wget_events[-1].binprovider == ""


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


def test_binary_installed_event_reuses_real_plugin_cached_paths_without_provider_inference(tmp_path: Path) -> None:
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
    assert wget_events[-1].version == ""
    assert wget_events[-1].binprovider == ""


def test_binary_event_uses_cached_config_binary_before_abxpkg_resolution(tmp_path: Path) -> None:
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
    assert wget_events[-1].binprovider == ""


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


def test_setup_services_accepts_runtime_config_overrides_and_seeds_machine_events() -> None:
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
                derived_config_overrides={"WGET_BINARY": "/tmp/fake-wget"},
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
    assert derived_event.config == {"WGET_BINARY": "/tmp/fake-wget"}


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


def test_binary_service_serializes_provider_installs(tmp_path: Path) -> None:
    async def run() -> int:
        bus = create_bus(total_timeout=10.0, name=f"binary_install_lock_{tmp_path.name}")
        MachineService(bus)
        active_installs = 0
        max_active_installs = 0

        class InstrumentedBinary(AbxBinary):
            def install(
                self,
                binproviders: list[BinProviderName] | None = None,
                no_cache: bool = False,
                dry_run: bool | None = None,
                postinstall_scripts: bool | None = None,
                min_release_age: float | None = None,
                **extra_overrides: Any,
            ) -> Self:
                nonlocal active_installs, max_active_installs
                active_installs += 1
                max_active_installs = max(max_active_installs, active_installs)
                time.sleep(0.05)
                active_installs -= 1
                return type(self).model_validate(
                    {
                        "name": self.name,
                        "loaded_abspath": f"/tmp/{self.name}",
                    },
                )

        class LockedBinaryService(BinaryService):
            def _load(self, event: BinaryRequestEvent) -> AbxBinary:
                return AbxBinary.model_validate({"name": event.name})

            def _binary_for_event(self, event: BinaryRequestEvent) -> AbxBinary:
                return InstrumentedBinary(name=event.name)

        LockedBinaryService(bus, auto_install=True)
        await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
        await asyncio.gather(
            bus.emit(
                BinaryRequestEvent(
                    name="first-test-binary",
                    binproviders="env",
                    extra_context=_binary_extra_context(plugin_name="env", output_dir=str(tmp_path / "first")),
                ),
            ).now(),
            bus.emit(
                BinaryRequestEvent(
                    name="second-test-binary",
                    binproviders="env",
                    extra_context=_binary_extra_context(plugin_name="env", output_dir=str(tmp_path / "second")),
                ),
            ).now(),
        )
        await bus.wait_until_idle()
        return max_active_installs

    assert asyncio.run(run()) == 1


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


def test_binary_event_falls_back_from_stale_cached_config_binary_to_abxpkg_resolution(tmp_path: Path) -> None:
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


def test_binary_event_falls_back_from_missing_user_binary_abspath_override_via_abxpkg(tmp_path: Path) -> None:
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


def test_binary_installed_event_updates_lib_bin_command(tmp_path: Path) -> None:
    async def run() -> None:
        bus = create_bus(total_timeout=10.0, name=f"updates_lib_bin_command_{tmp_path.name}")
        MachineService(bus)
        BinaryCacheService(
            bus,
            backend=AbxDlEnvConfigFileBinaryCacheBackend(bus, plugins={}),
        )

        first_target = tmp_path / "targets" / "first-demo"
        second_target = tmp_path / "targets" / "second-demo"
        first_target.parent.mkdir(parents=True, exist_ok=True)
        first_target.write_text("#!/bin/sh\nprintf first\n")
        first_target.chmod(0o755)
        second_target.write_text("#!/bin/sh\nprintf second\n")
        second_target.chmod(0o755)

        try:
            await bus.emit(MachineEvent(config={"ABXPKG_LIB_DIR": str(tmp_path / "lib")}, config_type="user")).now()
            await bus.emit(
                BinaryEvent(
                    name="demo",
                    abspath=str(first_target),
                    extra_context=_binary_extra_context(plugin_name="demo", hook_name="install"),
                ),
            ).now()
            link_path = tmp_path / "lib" / "bin" / "demo"
            assert subprocess.run([str(link_path)], check=True, capture_output=True, text=True).stdout == "first"

            await bus.emit(
                BinaryEvent(
                    name="demo",
                    abspath=str(second_target),
                    extra_context=_binary_extra_context(plugin_name="demo", hook_name="install"),
                ),
            ).now()
            assert subprocess.run([str(link_path)], check=True, capture_output=True, text=True).stdout == "second"
        finally:
            await bus.wait_until_idle()

    asyncio.run(run())


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


def test_snapshot_service_emits_background_process_without_extra_wait(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "background_start"
    plugin_dir.mkdir(parents=True)
    hook_path = plugin_dir / "on_Snapshot__10_background.daemon.bg.sh"
    hook_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "trap 'echo cleaned; exit 0' TERM",
                "set -euo pipefail",
                "echo ready",
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    hook_path.chmod(0o755)

    plugin = Plugin(
        name="background_start",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=hook_path.name,
                event="Snapshot",
                plugin_name="background_start",
                path=hook_path,
                order=10,
                is_background=True,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"make_hook_handler_background_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    snapshot = Snapshot(url="https://example.com", id="snap-123")
    SnapshotService(
        bus,
        url="https://example.com",
        snapshot=snapshot,
        output_dir=tmp_path / "run",
        plugins={plugin.name: plugin},
        snapshot_phase_timeout=1.0,
        snapshot_cleanup_phase_timeout=5.0,
    )

    try:

        async def run() -> tuple[ProcessEvent | None, ProcessStartedEvent | None, ProcessStdoutEvent | None, ProcessCompletedEvent | None]:
            crawl_start_event = CrawlStartEvent(
                url="https://example.com",
                snapshot_id=snapshot.id,
                output_dir=str(tmp_path / "run"),
            )
            root_event = SnapshotEvent(
                url="https://example.com",
                snapshot_id=snapshot.id,
                output_dir=str(tmp_path / "run"),
                event_parent_id=crawl_start_event.event_id,
            )
            await bus.emit(crawl_start_event).now()
            await bus.emit(
                root_event,
            ).now()
            process_event = await bus.find(
                ProcessEvent,
                past=True,
                future=1.0,
                child_of=root_event,
                plugin_name=plugin.name,
                hook_name=hook_path.name,
            )
            assert isinstance(process_event, ProcessEvent)
            started_process = await bus.find(
                ProcessStartedEvent,
                child_of=process_event,
                past=True,
                future=5.0,
            )
            ready_line = await bus.find(
                ProcessStdoutEvent,
                child_of=started_process,
                past=True,
                future=5.0,
                where=lambda candidate: candidate.line == "ready",
            )
            completed_process = await bus.find(
                ProcessCompletedEvent,
                child_of=process_event,
                past=True,
                future=5.0,
            )
            return (
                process_event,
                started_process if isinstance(started_process, ProcessStartedEvent) else None,
                ready_line if isinstance(ready_line, ProcessStdoutEvent) else None,
                completed_process if isinstance(completed_process, ProcessCompletedEvent) else None,
            )

        emitted, started, ready, completed = asyncio.run(run())
    finally:
        asyncio.run(bus.wait_until_idle())

    assert emitted is not None
    assert emitted.is_background is True
    assert emitted.event_blocks_parent_completion is False
    assert emitted.timeout and emitted.timeout > 0
    assert emitted.event_timeout is None
    assert emitted.event_handler_timeout is None
    assert emitted.hook_name == hook_path.name
    assert started is not None
    assert ready is not None
    assert completed is not None
    assert completed.status == "succeeded"
    assert completed.exit_code == 0
    assert "ready" in completed.stdout
    assert "cleaned" in completed.stdout
    assert not list((tmp_path / "run" / "background_start").glob("*.pid"))


def test_snapshot_background_js_hook_replays_early_sigterm_to_real_cleanup(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "background_js_relay"
    plugin_dir.mkdir(parents=True)
    hook_path = plugin_dir / "on_Snapshot__10_background.daemon.bg.js"
    hook_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env node",
                "let __abxEarlyShutdownSignal = null;",
                "function __abxRememberEarlyShutdown(signal) {",
                "  if (__abxEarlyShutdownSignal === null) {",
                "    __abxEarlyShutdownSignal = signal;",
                "  }",
                "}",
                "function __abxInstallShutdownHandler(handler) {",
                "  process.removeAllListeners('SIGTERM');",
                "  process.removeAllListeners('SIGINT');",
                "  process.on('SIGTERM', () => handler('SIGTERM'));",
                "  process.on('SIGINT', () => handler('SIGINT'));",
                "  if (__abxEarlyShutdownSignal !== null) {",
                "    const signal = __abxEarlyShutdownSignal;",
                "    __abxEarlyShutdownSignal = null;",
                "    setImmediate(() => handler(signal));",
                "  }",
                "}",
                "process.on('SIGTERM', () => __abxRememberEarlyShutdown('SIGTERM'));",
                "process.on('SIGINT', () => __abxRememberEarlyShutdown('SIGINT'));",
                "console.log('ready');",
                "setTimeout(() => {",
                "  __abxInstallShutdownHandler((signal) => {",
                "    console.log(`cleaned ${signal}`);",
                "    process.exit(0);",
                "  });",
                "}, 150);",
                "setInterval(() => {}, 1000);",
                "",
            ],
        ),
    )
    hook_path.chmod(0o755)

    plugin = Plugin(
        name="background_js_relay",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=hook_path.name,
                event="Snapshot",
                plugin_name="background_js_relay",
                path=hook_path,
                order=10,
                is_background=True,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"js_background_relay_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    snapshot = Snapshot(url="https://example.com", id="snap-js-relay")
    SnapshotService(
        bus,
        url="https://example.com",
        snapshot=snapshot,
        output_dir=tmp_path / "run",
        plugins={plugin.name: plugin},
        snapshot_phase_timeout=1.0,
        snapshot_cleanup_phase_timeout=5.0,
    )

    try:

        async def run() -> ProcessCompletedEvent:
            crawl_start_event = CrawlStartEvent(
                url="https://example.com",
                snapshot_id=snapshot.id,
                output_dir=str(tmp_path / "run"),
            )
            root_event = SnapshotEvent(
                url="https://example.com",
                snapshot_id=snapshot.id,
                output_dir=str(tmp_path / "run"),
                event_parent_id=crawl_start_event.event_id,
            )
            await bus.emit(crawl_start_event).now()
            await bus.emit(root_event).now()
            process_event = await bus.find(
                ProcessEvent,
                past=True,
                future=1.0,
                child_of=root_event,
                plugin_name=plugin.name,
                hook_name=hook_path.name,
            )
            assert isinstance(process_event, ProcessEvent)
            ready_line = await bus.find(
                ProcessStdoutEvent,
                child_of=process_event,
                past=True,
                future=5.0,
                where=lambda candidate: candidate.line == "ready",
            )
            assert isinstance(ready_line, ProcessStdoutEvent)
            completed_process = await bus.find(
                ProcessCompletedEvent,
                child_of=process_event,
                past=True,
                future=5.0,
            )
            assert isinstance(completed_process, ProcessCompletedEvent)
            return completed_process

        completed = asyncio.run(run())
    finally:
        asyncio.run(bus.wait_until_idle())

    assert completed.status == "succeeded"
    assert completed.exit_code == 0
    assert "ready" in completed.stdout
    assert "cleaned SIGTERM" in completed.stdout
    assert completed.stderr == ""
    assert not list((tmp_path / "run" / "background_js_relay").glob("*.pid"))


def test_snapshot_service_selected_hooks_by_plugin_runs_only_named_hooks(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "selected_hooks"
    plugin_dir.mkdir(parents=True)
    first_hook = plugin_dir / "on_Snapshot__10_first.sh"
    second_hook = plugin_dir / "on_Snapshot__20_second.sh"
    marker_dir = tmp_path / "markers"
    for hook_path, marker_name in ((first_hook, "first"), (second_hook, "second")):
        hook_path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env bash",
                    "set -euo pipefail",
                    f"mkdir -p {str(marker_dir)!r}",
                    f"touch {str(marker_dir / marker_name)!r}",
                    "",
                ],
            ),
        )
        hook_path.chmod(0o755)

    plugin = Plugin(
        name="selected_hooks",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(name=first_hook.name, event="Snapshot", plugin_name="selected_hooks", path=first_hook, order=10, is_background=False),
            Hook(name=second_hook.name, event="Snapshot", plugin_name="selected_hooks", path=second_hook, order=20, is_background=False),
        ],
    )
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
        snapshot_phase_timeout=5.0,
        selected_hooks_by_plugin={plugin.name: {second_hook.stem}},
    )

    async def run() -> list[ProcessEvent]:
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

    assert [event.hook_name for event in process_events] == [second_hook.name]
    assert not (marker_dir / "first").exists()
    assert (marker_dir / "second").exists()


def test_snapshot_service_repins_snapshot_chrome_dirs_after_global_config_merge(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=30.0, name=f"snapshot_chrome_env_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    output_dir = tmp_path / "archive" / "users" / "system" / "snapshots" / "20260603" / "example.com" / "current"
    stale_dir = tmp_path / "archive" / "users" / "system" / "snapshots" / "20260603" / "example.com" / "stale"
    plugin_dir = tmp_path / "plugins" / "chrome"
    plugin_dir.mkdir(parents=True)
    hook_path = plugin_dir / "on_Snapshot__09_chrome_launch.sh"
    hook_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'printf \'{"type":"ArchiveResult","status":"succeeded","output_str":"ok"}\\n\'',
                "",
            ],
        ),
    )
    hook_path.chmod(0o755)
    real_chrome_plugin = discover_plugins()["chrome"]
    plugin = Plugin(
        name="chrome",
        path=real_chrome_plugin.path,
        config=real_chrome_plugin.config,
        hooks=[
            Hook(
                name=hook_path.name,
                event="Snapshot",
                plugin_name="chrome",
                path=hook_path,
                order=9,
                is_background=False,
            ),
        ],
    )
    snapshot = Snapshot(url="https://example.com", id="snap-current")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    SnapshotService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        snapshot_phase_timeout=10.0,
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
            hook_name=hook_path.name,
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
    assert emitted.env["CHROME_USER_DATA_DIR"] == str(stale_dir / ".persona" / "Default" / "chrome_profile")
    assert emitted.env["CHROME_DOWNLOADS_DIR"] == str(stale_dir / ".persona" / "Default" / "chrome_downloads")


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
    hook_script = tmp_path / "on_Snapshot__10_env_check.sh"
    hook_script.write_text("#!/usr/bin/env bash\nexit 0\n")
    hook_script.chmod(0o755)
    plugin = Plugin(
        name="env_check",
        path=tmp_path,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=hook_script.name,
                event="Snapshot",
                plugin_name="env_check",
                path=hook_script,
                order=10,
                is_background=False,
            ),
        ],
    )
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
        snapshot_phase_timeout=10.0,
    )

    async def run() -> ProcessEvent:
        await bus.emit(
            BinaryEvent(
                name="env-check-tool",
                abspath="/bin/echo",
                env={"ABX_TEST_BINARY_MARKER": "old"},
                extra_context=_binary_extra_context(plugin_name=plugin.name),
            ),
        ).now()
        await bus.emit(
            BinaryEvent(
                name="env-check-tool",
                abspath="/bin/echo",
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
    hook_script = tmp_path / "on_CrawlSetup__10_env_check.sh"
    hook_script.write_text("#!/usr/bin/env bash\nexit 0\n")
    hook_script.chmod(0o755)
    plugin = Plugin(
        name="setup_env_check",
        path=tmp_path,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=hook_script.name,
                event="CrawlSetup",
                plugin_name="setup_env_check",
                path=hook_script,
                order=10,
                is_background=False,
            ),
        ],
    )
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

    async def run() -> ProcessEvent:
        await bus.emit(
            BinaryEvent(
                name="setup-env-check-tool",
                abspath="/bin/echo",
                env={"ABX_TEST_BINARY_MARKER": "old"},
                extra_context=_binary_extra_context(plugin_name=plugin.name),
            ),
        ).now()
        await bus.emit(
            BinaryEvent(
                name="setup-env-check-tool",
                abspath="/bin/echo",
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
    plugin_dir = tmp_path / "plugins" / "daemon_check"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_Snapshot__10_daemon.daemon.bg.sh"
    foreground_hook = plugin_dir / "on_Snapshot__20_check.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "trap 'echo cleaned; exit 0' TERM",
                "set -euo pipefail",
                'echo $$ > "$SNAP_DIR/daemon.pid"',
                'echo ready > "$SNAP_DIR/daemon.ready"',
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    foreground_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "for _ in $(seq 1 50); do",
                '    test -s "$SNAP_DIR/daemon.pid" && break',
                "    sleep 0.1",
                "done",
                'pid=$(cat "$SNAP_DIR/daemon.pid")',
                "sleep 2",
                'kill -0 "$pid"',
                'printf \'{"type":"ArchiveResult","status":"succeeded","output_str":"daemon alive"}\\n\'',
                "",
            ],
        ),
    )
    daemon_hook.chmod(0o755)
    foreground_hook.chmod(0o755)

    output_dir = tmp_path / "run"
    plugin = Plugin(
        name="daemon_check",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=daemon_hook.name,
                event="Snapshot",
                plugin_name="daemon_check",
                path=daemon_hook,
                order=10,
                is_background=True,
            ),
            Hook(
                name=foreground_hook.name,
                event="Snapshot",
                plugin_name="daemon_check",
                path=foreground_hook,
                order=20,
                is_background=False,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"snapshot_bg_lifetime_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    ArchiveResultService(bus, emit_jsonl=False)
    snapshot = Snapshot(url="https://example.com", id="snap-daemon")
    SnapshotService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        snapshot_phase_timeout=10.0,
        snapshot_cleanup_phase_timeout=5.0,
    )

    async def run() -> tuple[ProcessStartedEvent | None, ProcessStartedEvent | None, ProcessCompletedEvent | None]:
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
        daemon_started = await bus.find(
            ProcessStartedEvent,
            past=True,
            future=False,
            hook_name=daemon_hook.name,
        )
        foreground_started = await bus.find(
            ProcessStartedEvent,
            past=True,
            future=False,
            hook_name=foreground_hook.name,
        )
        daemon_completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=5.0,
            hook_name=daemon_hook.name,
        )
        await bus.wait_until_idle()
        return (
            daemon_started if isinstance(daemon_started, ProcessStartedEvent) else None,
            foreground_started if isinstance(foreground_started, ProcessStartedEvent) else None,
            daemon_completed if isinstance(daemon_completed, ProcessCompletedEvent) else None,
        )

    daemon_started, foreground_started, daemon_completed = asyncio.run(run())

    assert daemon_started is not None
    assert foreground_started is not None
    assert (output_dir / "index.jsonl").read_text().count('"output_str": "daemon alive"') == 1
    assert daemon_completed is not None
    assert daemon_completed.status == "succeeded"
    assert "cleaned" in daemon_completed.stdout


def test_snapshot_abort_stops_scheduling_later_hooks(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "snapshot_abort"
    plugin_dir.mkdir(parents=True)
    first_hook = plugin_dir / "on_Snapshot__10_first.sh"
    second_hook = plugin_dir / "on_Snapshot__20_second.sh"
    first_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'echo first-started > "$SNAP_DIR/first.txt"',
                "sleep 30",
                "",
            ],
        ),
    )
    second_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'echo second-ran > "$SNAP_DIR/second.txt"',
                "",
            ],
        ),
    )
    first_hook.chmod(0o755)
    second_hook.chmod(0o755)

    output_dir = tmp_path / "run"
    plugin = Plugin(
        name="snapshot_abort",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=first_hook.name,
                event="Snapshot",
                plugin_name="snapshot_abort",
                path=first_hook,
                order=10,
                is_background=False,
            ),
            Hook(
                name=second_hook.name,
                event="Snapshot",
                plugin_name="snapshot_abort",
                path=second_hook,
                order=20,
                is_background=False,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"snapshot_abort_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    snapshot = Snapshot(url="https://example.com", id="snap-abort")
    SnapshotService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        snapshot_phase_timeout=10.0,
        snapshot_cleanup_phase_timeout=5.0,
    )

    async def run() -> tuple[ProcessCompletedEvent | None, SnapshotCompletedEvent | None, ProcessStartedEvent | None]:
        async def abort_from_first_hook_context(event: ProcessStartedEvent) -> None:
            if event.hook_name == first_hook.name:
                for _ in range(50):
                    if (output_dir / "first.txt").exists():
                        break
                    await asyncio.sleep(0.1)
                assert (output_dir / "first.txt").exists()
                await event.emit(CrawlAbortEvent()).now()

        bus.on(ProcessStartedEvent, abort_from_first_hook_context)
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
        first_completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=5.0,
            hook_name=first_hook.name,
        )
        snapshot_completed = await bus.find(
            SnapshotCompletedEvent,
            past=True,
            future=5.0,
            child_of=root_event,
        )
        second_started = await bus.find(ProcessStartedEvent, past=True, future=False, hook_name=second_hook.name)
        await bus.wait_until_idle()
        return (
            first_completed if isinstance(first_completed, ProcessCompletedEvent) else None,
            snapshot_completed if isinstance(snapshot_completed, SnapshotCompletedEvent) else None,
            second_started if isinstance(second_started, ProcessStartedEvent) else None,
        )

    first_completed, snapshot_completed, second_started = asyncio.run(run())

    assert (output_dir / "first.txt").read_text() == "first-started\n"
    assert first_completed is not None
    assert first_completed.status == "skipped"
    assert snapshot_completed is not None
    assert not (output_dir / "second.txt").exists()
    assert second_started is None


def test_snapshot_completed_waits_for_cleanup_process_listeners(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "snapshot_cleanup_wait"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_Snapshot__10_daemon.daemon.bg.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "trap 'echo cleaned; exit 0' TERM",
                "set -euo pipefail",
                'echo $$ > "$SNAP_DIR/daemon.pid"',
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    daemon_hook.chmod(0o755)

    output_dir = tmp_path / "run"
    side_effect = output_dir / "process-completed-listener.txt"
    plugin = Plugin(
        name="snapshot_cleanup_wait",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=daemon_hook.name,
                event="Snapshot",
                plugin_name="snapshot_cleanup_wait",
                path=daemon_hook,
                order=10,
                is_background=True,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"snapshot_cleanup_wait_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    SnapshotService(
        bus,
        url="https://example.com",
        snapshot=Snapshot(url="https://example.com", id="snap-cleanup-wait"),
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        snapshot_phase_timeout=10.0,
        snapshot_cleanup_phase_timeout=5.0,
    )
    completed_saw_side_effect: list[bool] = []

    async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
        if event.hook_name == daemon_hook.name:
            await asyncio.sleep(0.2)
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
        await bus.emit(root_event).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    assert side_effect.read_text() == "done"
    assert completed_saw_side_effect == [True]


def test_crawl_setup_background_daemon_survives_until_explicit_cleanup(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "crawl_daemon_check"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_CrawlSetup__10_daemon.daemon.bg.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "trap 'echo cleaned; exit 0' TERM",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo $$ > "$crawl_dir/daemon.pid"',
                'echo ready > "$crawl_dir/daemon.ready"',
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    daemon_hook.chmod(0o755)

    output_dir = tmp_path / "run"
    snapshot = Snapshot(url="https://example.com", id="snap-crawl-daemon")
    plugin = Plugin(
        name="crawl_daemon_check",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=daemon_hook.name,
                event="CrawlSetup",
                plugin_name="crawl_daemon_check",
                path=daemon_hook,
                order=10,
                is_background=True,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"crawl_bg_lifetime_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    ArchiveResultService(bus, emit_jsonl=False)
    CrawlService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        crawl_start_enabled=False,
        crawl_cleanup_enabled=False,
        crawl_setup_phase_timeout=10.0,
        snapshot_phase_timeout=10.0,
        snapshot_cleanup_phase_timeout=5.0,
        crawl_cleanup_phase_timeout=5.0,
    )

    async def run() -> ProcessCompletedEvent | None:
        crawl_event = bus.emit(
            CrawlEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_timeout=20.0,
            ),
        )
        await crawl_event.now()
        pid_file = output_dir / "daemon.pid"
        for _ in range(50):
            if pid_file.exists():
                break
            await asyncio.sleep(0.1)
        pid = int(pid_file.read_text().strip())
        os.kill(pid, 0)
        await bus.emit(
            CrawlCleanupEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_parent_id=crawl_event.event_id,
                event_timeout=5.0,
            ),
        ).now()
        completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=5.0,
            hook_name=daemon_hook.name,
        )
        await bus.wait_until_idle()
        return completed if isinstance(completed, ProcessCompletedEvent) else None

    daemon_completed = asyncio.run(run())

    assert daemon_completed is not None
    assert daemon_completed.status == "succeeded"
    assert "cleaned" in daemon_completed.stdout


def test_crawl_completed_waits_for_cleanup_process_listeners(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "crawl_cleanup_wait"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_CrawlSetup__10_daemon.daemon.bg.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "trap 'echo cleaned; exit 0' TERM",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo $$ > "$crawl_dir/daemon.pid"',
                'echo ready > "$crawl_dir/daemon.ready"',
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    daemon_hook.chmod(0o755)

    output_dir = tmp_path / "run"
    side_effect = output_dir / "process-completed-listener.txt"
    snapshot = Snapshot(url="https://example.com", id="crawl-cleanup-wait")
    plugin = Plugin(
        name="crawl_cleanup_wait",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=daemon_hook.name,
                event="CrawlSetup",
                plugin_name="crawl_cleanup_wait",
                path=daemon_hook,
                order=10,
                is_background=True,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"crawl_cleanup_wait_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    CrawlService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        crawl_start_enabled=False,
        crawl_cleanup_enabled=True,
        crawl_setup_phase_timeout=10.0,
        crawl_cleanup_phase_timeout=5.0,
    )
    completed_saw_side_effect: list[bool] = []

    async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
        if event.hook_name == daemon_hook.name:
            await asyncio.sleep(0.2)
            side_effect.parent.mkdir(parents=True, exist_ok=True)
            side_effect.write_text("done")

    async def on_CrawlCompletedEvent(event: CrawlCompletedEvent) -> None:
        completed_saw_side_effect.append(side_effect.exists())

    bus.on(ProcessCompletedEvent, on_ProcessCompletedEvent)
    bus.on(CrawlCompletedEvent, on_CrawlCompletedEvent)

    async def run() -> None:
        await bus.emit(
            CrawlEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_timeout=20.0,
            ),
        ).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    assert side_effect.read_text() == "done"
    assert completed_saw_side_effect == [True]


def test_crawl_abort_during_setup_cleans_background_daemon(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "crawl_setup_abort"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_CrawlSetup__10_daemon.daemon.bg.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "trap 'echo cleaned; exit 0' TERM",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo $$ > "$crawl_dir/daemon.pid"',
                'echo ready > "$crawl_dir/daemon.ready"',
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    daemon_hook.chmod(0o755)

    output_dir = tmp_path / "run"
    snapshot = Snapshot(url="https://example.com", id="crawl-setup-abort")
    plugin = Plugin(
        name="crawl_setup_abort",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=daemon_hook.name,
                event="CrawlSetup",
                plugin_name="crawl_setup_abort",
                path=daemon_hook,
                order=10,
                is_background=True,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"crawl_setup_abort_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    CrawlService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        crawl_start_enabled=False,
        crawl_cleanup_enabled=True,
        crawl_setup_phase_timeout=10.0,
        crawl_cleanup_phase_timeout=5.0,
    )

    async def run() -> ProcessCompletedEvent | None:
        async def abort_after_daemon_starts(event: CrawlSetupEvent) -> None:
            started = await bus.find(ProcessStartedEvent, past=True, future=5.0, hook_name=daemon_hook.name)
            assert isinstance(started, ProcessStartedEvent)
            ready_file = output_dir / "daemon.ready"
            for _ in range(50):
                if ready_file.exists():
                    break
                await asyncio.sleep(0.1)
            assert ready_file.read_text().strip() == "ready"
            await event.emit(CrawlAbortEvent()).now()

        bus.on(CrawlSetupEvent, abort_after_daemon_starts)
        await bus.emit(
            CrawlEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_timeout=20.0,
            ),
        ).now()
        completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=5.0,
            hook_name=daemon_hook.name,
        )
        await bus.wait_until_idle()
        return completed if isinstance(completed, ProcessCompletedEvent) else None

    daemon_completed = asyncio.run(run())

    assert daemon_completed is not None
    assert daemon_completed.status == "succeeded"
    assert "cleaned" in daemon_completed.stdout


def test_download_cancellation_kills_background_setup_process_group(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "cancel_group"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_CrawlSetup__10_daemon.daemon.bg.sh"
    foreground_hook = plugin_dir / "on_CrawlSetup__20_foreground.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                "sleep 600 &",
                'echo $$ > "$crawl_dir/daemon.pid"',
                'echo $! > "$crawl_dir/daemon-child.pid"',
                'echo ready > "$crawl_dir/daemon.ready"',
                "trap 'echo cleaned > \"$crawl_dir/daemon.cleaned\"; exit 0' TERM INT",
                "wait",
                "",
            ],
        ),
    )
    foreground_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo $$ > "$crawl_dir/foreground.pid"',
                'echo ready > "$crawl_dir/foreground.ready"',
                "trap 'echo cleaned > \"$crawl_dir/foreground.cleaned\"; exit 0' TERM INT",
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    daemon_hook.chmod(0o755)
    foreground_hook.chmod(0o755)

    output_dir = tmp_path / "run"
    plugin = Plugin(
        name="cancel_group",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=daemon_hook.stem,
                event="CrawlSetup",
                plugin_name="cancel_group",
                path=daemon_hook,
                order=10,
                is_background=True,
            ),
            Hook(
                name=foreground_hook.stem,
                event="CrawlSetup",
                plugin_name="cancel_group",
                path=foreground_hook,
                order=20,
                is_background=False,
            ),
        ],
    )
    daemon_pid: int | None = None
    daemon_child_pid: int | None = None
    foreground_pid: int | None = None

    async def run_and_cancel() -> tuple[int, int, int]:
        bus = create_bus(total_timeout=30.0, name=f"download_cancel_group_{tmp_path.name}")
        download_task = asyncio.create_task(
            download(
                "https://example.com",
                plugins={plugin.name: plugin},
                output_dir=output_dir,
                selected_plugins=[plugin.name],
                config_overrides={"TIMEOUT": 5},
                auto_install=False,
                emit_jsonl=False,
                interactive_tty=False,
                bus=bus,
            ),
        )

        for _ in range(100):
            if (output_dir / "daemon.ready").exists() and (output_dir / "foreground.ready").exists():
                break
            await asyncio.sleep(0.05)
        assert (output_dir / "daemon.ready").exists()
        assert (output_dir / "foreground.ready").exists()

        pids = (
            int((output_dir / "daemon.pid").read_text().strip()),
            int((output_dir / "daemon-child.pid").read_text().strip()),
            int((output_dir / "foreground.pid").read_text().strip()),
        )
        assert all(_pid_is_alive(pid) for pid in pids)
        download_task.cancel()
        try:
            await download_task
        except asyncio.CancelledError:
            pass
        return pids

    try:
        daemon_pid, daemon_child_pid, foreground_pid = asyncio.run(run_and_cancel())
        _wait_for_pid_exit(daemon_pid)
        _wait_for_pid_exit(daemon_child_pid)
        _wait_for_pid_exit(foreground_pid)
        assert (output_dir / "daemon.cleaned").read_text().strip() == "cleaned"
        assert (output_dir / "foreground.cleaned").read_text().strip() == "cleaned"
    finally:
        _cleanup_test_process_group(daemon_pid, daemon_child_pid)
        _cleanup_test_process_group(foreground_pid)


def test_crawl_abort_during_foreground_setup_interrupts_hook_and_stops_later_setup(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "crawl_setup_abort_foreground"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_CrawlSetup__10_daemon.daemon.bg.sh"
    foreground_hook = plugin_dir / "on_CrawlSetup__20_wait.sh"
    later_hook = plugin_dir / "on_CrawlSetup__30_later.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo $$ > "$crawl_dir/daemon.pid"',
                "trap 'echo daemon-cleaned; exit 0' TERM",
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    foreground_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo $$ > "$crawl_dir/foreground.pid"',
                "trap 'echo foreground-cleaned; exit 0' TERM",
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    later_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo ran > "$crawl_dir/later.ran"',
                "",
            ],
        ),
    )
    for hook_path in (daemon_hook, foreground_hook, later_hook):
        hook_path.chmod(0o755)

    output_dir = tmp_path / "run"
    snapshot = Snapshot(url="https://example.com", id="crawl-setup-abort-foreground")
    plugin = Plugin(
        name="crawl_setup_abort_foreground",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=daemon_hook.name,
                event="CrawlSetup",
                plugin_name="crawl_setup_abort_foreground",
                path=daemon_hook,
                order=10,
                is_background=True,
            ),
            Hook(
                name=foreground_hook.name,
                event="CrawlSetup",
                plugin_name="crawl_setup_abort_foreground",
                path=foreground_hook,
                order=20,
                is_background=False,
            ),
            Hook(
                name=later_hook.name,
                event="CrawlSetup",
                plugin_name="crawl_setup_abort_foreground",
                path=later_hook,
                order=30,
                is_background=False,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"crawl_setup_abort_foreground_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    CrawlService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        crawl_start_enabled=False,
        crawl_cleanup_enabled=True,
        crawl_setup_phase_timeout=10.0,
        crawl_cleanup_phase_timeout=5.0,
    )
    abort_emitted = False

    async def abort_after_foreground_starts(event: ProcessStartedEvent) -> None:
        nonlocal abort_emitted
        if event.hook_name != foreground_hook.name or abort_emitted:
            return
        abort_emitted = True
        await event.emit(CrawlAbortEvent()).now()

    bus.on(ProcessStartedEvent, abort_after_foreground_starts)

    async def run() -> tuple[ProcessCompletedEvent | None, ProcessCompletedEvent | None, list[ProcessStartedEvent]]:
        await bus.emit(
            CrawlEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_timeout=20.0,
            ),
        ).now()
        daemon_completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=5.0,
            hook_name=daemon_hook.name,
        )
        foreground_completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=5.0,
            hook_name=foreground_hook.name,
        )
        later_started = await bus.filter(
            ProcessStartedEvent,
            past=True,
            hook_name=later_hook.name,
        )
        await bus.wait_until_idle()
        return (
            daemon_completed if isinstance(daemon_completed, ProcessCompletedEvent) else None,
            foreground_completed if isinstance(foreground_completed, ProcessCompletedEvent) else None,
            later_started,
        )

    daemon_completed, foreground_completed, later_started = asyncio.run(run())

    assert abort_emitted
    assert daemon_completed is not None
    assert daemon_completed.status == "succeeded"
    assert "daemon-cleaned" in daemon_completed.stdout
    assert foreground_completed is not None
    assert foreground_completed.status == "skipped"
    assert "Hook interrupted by user" in foreground_completed.stderr
    assert later_started == []
    assert not (output_dir / "later.ran").exists()


def test_crawl_abort_from_crawl_event_interrupts_active_setup_hook(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "crawl_setup_abort_from_root"
    plugin_dir.mkdir(parents=True)
    foreground_hook = plugin_dir / "on_CrawlSetup__20_wait.sh"
    later_hook = plugin_dir / "on_CrawlSetup__30_later.sh"
    foreground_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo $$ > "$crawl_dir/foreground.pid"',
                "trap 'echo foreground-cleaned; exit 0' TERM",
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    later_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo ran > "$crawl_dir/later.ran"',
                "",
            ],
        ),
    )
    for hook_path in (foreground_hook, later_hook):
        hook_path.chmod(0o755)

    output_dir = tmp_path / "run"
    snapshot = Snapshot(url="https://example.com", id="crawl-setup-abort-from-root")
    plugin = Plugin(
        name="crawl_setup_abort_from_root",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=foreground_hook.name,
                event="CrawlSetup",
                plugin_name="crawl_setup_abort_from_root",
                path=foreground_hook,
                order=20,
                is_background=False,
            ),
            Hook(
                name=later_hook.name,
                event="CrawlSetup",
                plugin_name="crawl_setup_abort_from_root",
                path=later_hook,
                order=30,
                is_background=False,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"crawl_setup_abort_from_root_{tmp_path.name}")
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
        crawl_cleanup_enabled=True,
        crawl_setup_phase_timeout=10.0,
        crawl_cleanup_phase_timeout=5.0,
    )

    async def run() -> tuple[float, ProcessCompletedEvent | None, list[ProcessStartedEvent]]:
        abort_task_holder: list[asyncio.Task[None]] = []

        async def drive_archivebox_style_crawl(event: CrawlEvent) -> None:
            async def abort_from_crawl_event_after_foreground_starts() -> None:
                started = await bus.find(ProcessStartedEvent, past=True, future=5.0, hook_name=foreground_hook.name)
                assert isinstance(started, ProcessStartedEvent)
                await event.emit(CrawlAbortEvent()).now()

            abort_task = asyncio.create_task(abort_from_crawl_event_after_foreground_starts())
            abort_task_holder.append(abort_task)
            try:
                await event.emit(
                    CrawlSetupEvent(
                        url=snapshot.url,
                        snapshot_id=snapshot.id,
                        output_dir=str(output_dir),
                        event_timeout=10.0,
                    ),
                ).now()
            finally:
                await event.emit(
                    CrawlCleanupEvent(
                        url=snapshot.url,
                        snapshot_id=snapshot.id,
                        output_dir=str(output_dir),
                        event_timeout=5.0,
                    ),
                ).now()
                await abort_task

        bus.on(CrawlEvent, drive_archivebox_style_crawl)
        start = asyncio.get_running_loop().time()
        await bus.emit(
            CrawlEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_timeout=20.0,
            ),
        ).now()
        elapsed = asyncio.get_running_loop().time() - start
        foreground_completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=5.0,
            hook_name=foreground_hook.name,
        )
        later_started = await bus.filter(
            ProcessStartedEvent,
            past=True,
            hook_name=later_hook.name,
        )
        await bus.wait_until_idle()
        return (
            elapsed,
            foreground_completed if isinstance(foreground_completed, ProcessCompletedEvent) else None,
            later_started,
        )

    elapsed, foreground_completed, later_started = asyncio.run(run())

    assert elapsed < 5.0
    assert foreground_completed is not None
    assert foreground_completed.status == "skipped"
    assert "Hook interrupted by user" in foreground_completed.stderr
    assert later_started == []
    assert not (output_dir / "later.ran").exists()


def test_crawl_background_daemon_stays_alive_through_snapshot_until_cleanup(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "crawl_daemon_check"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_CrawlSetup__10_daemon.daemon.bg.sh"
    snapshot_hook = plugin_dir / "on_Snapshot__20_check.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "trap 'echo cleaned; exit 0' TERM",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo $$ > "$crawl_dir/daemon.pid"',
                'echo ready > "$crawl_dir/daemon.ready"',
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    snapshot_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                "for _ in $(seq 1 50); do",
                '    test -s "$crawl_dir/daemon.pid" && break',
                "    sleep 0.1",
                "done",
                'pid="$(cat "$crawl_dir/daemon.pid")"',
                "sleep 2",
                'kill -0 "$pid"',
                'printf \'{"type":"ArchiveResult","status":"succeeded","output_str":"crawl daemon alive"}\\n\'',
                "",
            ],
        ),
    )
    daemon_hook.chmod(0o755)
    snapshot_hook.chmod(0o755)

    output_dir = tmp_path / "run"
    snapshot = Snapshot(url="https://example.com", id="snap-crawl-daemon")
    plugin = Plugin(
        name="crawl_daemon_check",
        path=plugin_dir,
        config=PluginConfig(),
        hooks=[
            Hook(
                name=daemon_hook.name,
                event="CrawlSetup",
                plugin_name="crawl_daemon_check",
                path=daemon_hook,
                order=10,
                is_background=True,
            ),
            Hook(
                name=snapshot_hook.name,
                event="Snapshot",
                plugin_name="crawl_daemon_check",
                path=snapshot_hook,
                order=20,
                is_background=False,
            ),
        ],
    )
    bus = create_bus(total_timeout=20.0, name=f"manual_crawl_bg_lifetime_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    ArchiveResultService(bus, emit_jsonl=False)
    CrawlService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        crawl_start_enabled=False,
        crawl_cleanup_enabled=False,
        crawl_setup_phase_timeout=10.0,
        snapshot_phase_timeout=10.0,
        snapshot_cleanup_phase_timeout=5.0,
        crawl_cleanup_phase_timeout=5.0,
    )
    SnapshotService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        plugins={plugin.name: plugin},
        snapshot_phase_timeout=10.0,
        snapshot_cleanup_phase_timeout=5.0,
    )

    async def run() -> tuple[ArchiveResultEvent | None, ProcessCompletedEvent | None]:
        crawl_event = bus.emit(
            CrawlEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_timeout=20.0,
            ),
        )
        await crawl_event.now()
        crawl_start_event = await bus.emit(
            CrawlStartEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_parent_id=crawl_event.event_id,
                event_timeout=10.0,
            ),
        ).now()
        snapshot_event = await bus.emit(
            SnapshotEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_parent_id=crawl_start_event.event_id,
                event_timeout=10.0,
            ),
        ).now()
        result = await bus.find(
            ArchiveResultEvent,
            child_of=snapshot_event,
            past=True,
            future=10.0,
            plugin="crawl_daemon_check",
            hook_name=snapshot_hook.name,
        )
        await bus.emit(
            CrawlCleanupEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_parent_id=crawl_event.event_id,
                event_timeout=5.0,
            ),
        ).now()
        daemon_completed = await bus.find(
            ProcessCompletedEvent,
            past=True,
            future=5.0,
            hook_name=daemon_hook.name,
        )
        await bus.wait_until_idle()
        return (
            result if isinstance(result, ArchiveResultEvent) else None,
            daemon_completed if isinstance(daemon_completed, ProcessCompletedEvent) else None,
        )

    result, daemon_completed = asyncio.run(run())

    assert result is not None
    assert result.status == "succeeded"
    assert result.output_str == "crawl daemon alive"
    assert daemon_completed is not None
    assert daemon_completed.status == "succeeded"
    assert "cleaned" in daemon_completed.stdout


def test_process_kill_uses_live_subprocess_handle_when_pid_file_validation_fails(tmp_path: Path) -> None:
    script = tmp_path / "background-hook.sh"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "trap 'echo cleaned; exit 0' TERM",
                "echo ready",
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    script.chmod(0o755)

    output_dir = tmp_path / "run" / "background"
    bus = create_bus(total_timeout=10.0, name=f"process_kill_live_handle_{tmp_path.name}")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)

    async def run() -> ProcessCompletedEvent:
        process_event = bus.emit(
            ProcessEvent(
                plugin_name="background",
                hook_name="on_Snapshot__10_background.daemon.bg",
                hook_path=str(script),
                hook_args=[],
                is_background=True,
                output_dir=str(output_dir),
                env={},
                timeout=5,
                event_timeout=10.0,
                event_handler_timeout=10.0,
            ),
        )
        process_task = asyncio.create_task(process_event.now())
        started_process = await bus.find(
            ProcessStartedEvent,
            child_of=process_event,
            past=True,
            future=5.0,
        )
        assert isinstance(started_process, ProcessStartedEvent)
        ready_line = await bus.find(
            ProcessStdoutEvent,
            child_of=started_process,
            past=True,
            future=5.0,
            where=lambda candidate: candidate.line == "ready",
        )
        assert isinstance(ready_line, ProcessStdoutEvent)

        os.utime(started_process.pid_file, (1, 1))
        await bus.emit(
            ProcessKillEvent(
                plugin_name=started_process.plugin_name,
                hook_name=started_process.hook_name,
                pid=started_process.pid,
                grace_period=1.0,
                event_timeout=5.0,
                event_parent_id=started_process.event_id,
            ),
        ).now()
        completed_process = await bus.find(
            ProcessCompletedEvent,
            child_of=process_event,
            past=True,
            future=5.0,
        )
        assert isinstance(completed_process, ProcessCompletedEvent)
        await process_task
        await bus.wait_until_idle()
        return completed_process

    completed = asyncio.run(run())

    assert completed.status == "succeeded"
    assert "cleaned" in completed.stdout
    assert not list(output_dir.glob("on_Snapshot__10_background.daemon.bg.*.pid"))


def test_background_process_event_returns_after_start(tmp_path: Path) -> None:
    script = tmp_path / "background-hook.sh"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "trap 'echo cleaned; exit 0' TERM",
                "echo ready",
                "while true; do sleep 1; done",
                "",
            ],
        ),
    )
    script.chmod(0o755)

    output_dir = tmp_path / "run" / "background"
    bus = create_bus(total_timeout=10.0, name=f"background_returns_after_start_{tmp_path.name}")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)

    async def run() -> ProcessCompletedEvent:
        process_event = bus.emit(
            ProcessEvent(
                plugin_name="background",
                hook_name="on_Snapshot__10_background.daemon.bg",
                hook_path=str(script),
                hook_args=[],
                is_background=True,
                output_dir=str(output_dir),
                env={},
                timeout=5,
                event_timeout=10.0,
                event_handler_timeout=10.0,
            ),
        )
        process_task = asyncio.create_task(process_event.now())
        started_process = await bus.find(
            ProcessStartedEvent,
            child_of=process_event,
            past=True,
            future=5.0,
        )
        assert isinstance(started_process, ProcessStartedEvent)
        await asyncio.wait_for(process_task, timeout=1.0)
        ready_line = await bus.find(
            ProcessStdoutEvent,
            child_of=started_process,
            past=True,
            future=5.0,
            where=lambda candidate: candidate.line == "ready",
        )
        assert isinstance(ready_line, ProcessStdoutEvent)

        await bus.emit(
            ProcessKillEvent(
                plugin_name=started_process.plugin_name,
                hook_name=started_process.hook_name,
                pid=started_process.pid,
                grace_period=1.0,
                event_timeout=5.0,
                event_parent_id=started_process.event_id,
            ),
        ).now()
        completed_process = await bus.find(
            ProcessCompletedEvent,
            child_of=process_event,
            past=True,
            future=5.0,
        )
        assert isinstance(completed_process, ProcessCompletedEvent)
        await bus.wait_until_idle()
        return completed_process

    completed = asyncio.run(run())

    assert completed.status == "succeeded"
    assert "cleaned" in completed.stdout
    assert not list(output_dir.glob("on_Snapshot__10_background.daemon.bg.*.pid"))


def test_process_event_subprocess_starts_once_when_event_is_awaited_twice(tmp_path: Path) -> None:
    script = tmp_path / "start-once-hook.sh"
    launch_count = tmp_path / "launch-count.txt"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f"printf 'launch\\n' >> {launch_count}",
                "echo ready",
                "sleep 0.2",
                "",
            ],
        ),
    )
    script.chmod(0o755)

    output_dir = tmp_path / "run" / "start_once"
    bus = create_bus(total_timeout=10.0, name=f"process_event_start_once_{tmp_path.name}")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)

    async def run() -> tuple[list[ProcessStartedEvent], ProcessCompletedEvent | None]:
        process_event = bus.emit(
            ProcessEvent(
                plugin_name="start_once",
                hook_name="on_Snapshot__10_start_once.sh",
                hook_path=str(script),
                hook_args=[],
                is_background=False,
                output_dir=str(output_dir),
                env={},
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

    assert launch_count.read_text().splitlines() == ["launch"]
    assert len(started_events) == 1
    assert completed_event is not None
    assert completed_event.status == "succeeded"


def test_concurrent_process_events_for_same_hook_keep_distinct_artifacts(tmp_path: Path) -> None:
    script = tmp_path / "same-hook.sh"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "echo ready",
                "echo warning >&2",
                "sleep 0.2",
                "",
            ],
        ),
    )
    script.chmod(0o755)

    output_dir = tmp_path / "run" / "same_hook"
    bus = create_bus(total_timeout=10.0, name=f"process_event_same_hook_{tmp_path.name}")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)

    async def run() -> list[ProcessCompletedEvent]:
        events = [
            bus.emit(
                ProcessEvent(
                    plugin_name="same_hook",
                    hook_name="on_Snapshot__10_same_hook.sh",
                    hook_path=str(script),
                    hook_args=[],
                    is_background=False,
                    output_dir=str(output_dir),
                    env={},
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
    assert len(list(output_dir.glob("on_Snapshot__10_same_hook.sh.*.sh"))) == 2


def test_process_event_does_not_wait_for_stdout_inherited_by_child_process(tmp_path: Path) -> None:
    child_pid_file = tmp_path / "child.pid"
    script = tmp_path / "stdout-inherited-hook.py"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import subprocess",
                "import sys",
                "print('parent-started', flush=True)",
                "child = subprocess.Popen(['sleep', '30'], stdout=sys.stdout, stderr=sys.stderr)",
                f"open({str(child_pid_file)!r}, 'w').write(str(child.pid))",
                "print('parent-done', flush=True)",
                "",
            ],
        ),
    )
    script.chmod(0o755)

    output_dir = tmp_path / "run" / "stdout_inherited"
    bus = create_bus(total_timeout=10.0, name=f"process_stdout_inherited_{tmp_path.name}")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)

    async def run() -> tuple[float, ProcessCompletedEvent | None]:
        process_event = bus.emit(
            ProcessEvent(
                plugin_name="stdout_inherited",
                hook_name="on_Snapshot__10_stdout_inherited.sh",
                hook_path=str(script),
                hook_args=[],
                is_background=False,
                output_dir=str(output_dir),
                env={},
                timeout=5,
                event_timeout=10.0,
                event_handler_timeout=10.0,
            ),
        )
        start = asyncio.get_running_loop().time()
        await (await process_event.now()).event_results_list()
        elapsed = asyncio.get_running_loop().time() - start
        completed = await bus.find(
            ProcessCompletedEvent,
            child_of=process_event,
            past=True,
            future=2.0,
        )
        await bus.wait_until_idle()
        return elapsed, completed if isinstance(completed, ProcessCompletedEvent) else None

    try:
        elapsed, completed_event = asyncio.run(run())
    finally:
        if child_pid_file.exists():
            try:
                os.kill(int(child_pid_file.read_text().strip()), 9)
            except (ProcessLookupError, ValueError):
                pass

    assert elapsed < 3.0
    assert completed_event is not None
    assert completed_event.status == "succeeded"
    assert "parent-started" in completed_event.stdout
    assert "parent-done" in completed_event.stdout


def test_crawl_abort_interrupts_noninteractive_foreground_process(tmp_path: Path) -> None:
    script = tmp_path / "long-running-hook.sh"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "echo started",
                "sleep 30",
                "",
            ],
        ),
    )
    script.chmod(0o755)

    output_dir = tmp_path / "run" / "abort_process"
    bus = create_bus(total_timeout=10.0, name=f"process_abort_{tmp_path.name}")
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)

    async def run() -> tuple[ProcessStartedEvent, ProcessCompletedEvent | None]:
        process_event_holder: dict[str, ProcessEvent] = {}

        async def on_CrawlEvent(event: CrawlEvent) -> None:
            process_event = event.emit(
                ProcessEvent(
                    plugin_name="abort_process",
                    hook_name="on_Snapshot__10_abort_process.sh",
                    hook_path=str(script),
                    hook_args=[],
                    is_background=False,
                    output_dir=str(output_dir),
                    env={},
                    timeout=30,
                    event_timeout=60.0,
                    event_handler_timeout=60.0,
                ),
            )
            process_event_holder["event"] = process_event
            process_task = asyncio.create_task(process_event.now())
            started = await bus.find(ProcessStartedEvent, child_of=process_event, past=False, future=5.0)
            assert isinstance(started, ProcessStartedEvent)
            await event.emit(CrawlAbortEvent()).now()
            await process_task

        bus.on(CrawlEvent, on_CrawlEvent)
        crawl_event = bus.emit(
            CrawlEvent(
                url="https://example.com",
                snapshot_id="snapshot-1",
                output_dir=str(tmp_path / "run"),
            ),
        )
        await crawl_event.now()
        process_event = process_event_holder["event"]
        started = await bus.find(ProcessStartedEvent, child_of=process_event, past=True, future=5.0)
        assert isinstance(started, ProcessStartedEvent)
        completed = await bus.find(ProcessCompletedEvent, child_of=process_event, past=True, future=5.0)
        await bus.wait_until_idle()
        return started, completed if isinstance(completed, ProcessCompletedEvent) else None

    started_event, completed_event = asyncio.run(run())

    assert started_event.pid > 0
    assert completed_event is not None
    assert completed_event.status == "skipped"
    assert completed_event.stderr == "Hook interrupted by user"


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
        hook_path="/bin/echo",
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
        hook_path="/bin/echo",
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
    bus = create_bus(total_timeout=10.0, name=f"inline_archive_result_output_files_{tmp_path.name}")
    seen_archive_results: list[ArchiveResultEvent] = []

    async def on_ArchiveResultEvent(event: ArchiveResultEvent) -> None:
        seen_archive_results.append(event)

    bus.on(ArchiveResultEvent, on_ArchiveResultEvent)
    ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    ArchiveResultService(bus, emit_jsonl=False)

    script_path = tmp_path / "emit_archive_result.sh"
    script_path.write_text(
        "#!/bin/sh\n"
        "mkdir -p headers\n"
        "printf '{}' > headers/headers.json\n"
        'echo \'{"type":"ArchiveResult","status":"succeeded","output_str":"headers/headers.json"}\'\n',
    )
    script_path.chmod(0o755)

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
        plugin_name="headers",
        hook_name="on_Snapshot__27_headers",
        hook_path=str(script_path),
        hook_args=[],
        is_background=False,
        output_dir=str(tmp_path / "run" / "headers"),
        env={},
        timeout=10,
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
    assert [output_file.path for output_file in seen_archive_results[0].output_files] == ["headers/headers.json"]
