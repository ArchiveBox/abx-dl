import asyncio
import os
import sys
from pathlib import Path
from uuid import uuid4


from abx_dl.config import get_initial_env
from abx_dl.config import get_required_binary_requests
from abx_dl.events import (
    ArchiveResultEvent,
    BinaryEvent,
    CrawlCleanupEvent,
    BinaryRequestEvent,
    CrawlEvent,
    CrawlStartEvent,
    MachineEvent,
    ProcessCompletedEvent,
    ProcessEvent,
    ProcessKillEvent,
    ProcessStartedEvent,
    ProcessStdoutEvent,
    SnapshotEvent,
)
from abx_dl.models import Hook, Plugin, PluginConfig, Snapshot, discover_plugins
from abx_dl.orchestrator import create_bus, download, setup_services
from abx_dl.services.archive_result_service import ArchiveResultService
from abx_dl.services.binary_service import BinaryService
from abx_dl.services.crawl_service import CrawlService
from abx_dl.services.machine_service import MachineService
from abx_dl.services.process_service import ProcessService
from abx_dl.services.snapshot_service import SnapshotService


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


def _resolve_real_wget_binary(tmp_path: Path) -> BinaryEvent:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("wget", "env", "apt", "brew")}
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
                plugin_name="wget",
                output_dir=str(tmp_path / "resolve-wget"),
                binproviders="env,apt,brew",
            ),
        ).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    return next(event for event in reversed(installed_events) if event.name == "wget")


def test_binary_installed_event_preserves_child_provider_metadata(tmp_path: Path) -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("wget", "env", "apt", "brew")}
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
                plugin_name="wget",
                output_dir=str(tmp_path / "run"),
                binproviders="env,apt,brew",
            ),
        ).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    wget_events = [event for event in installed_events if event.name == "wget"]
    assert wget_events
    assert all(event.plugin_name == "wget" for event in wget_events)
    assert all(event.binproviders == "env,apt,brew" for event in wget_events)
    assert all(event.binprovider == "env" for event in wget_events)
    assert all(event.binary_id for event in wget_events)


def test_binary_installed_event_uses_machine_config_seeded_from_persistent_config(tmp_path: Path) -> None:
    from abx_dl.config import set_user_config

    plugins = discover_plugins()
    resolved_binary = _resolve_real_wget_binary(tmp_path)
    set_user_config({name: plugin.config.properties for name, plugin in plugins.items()}, WGET_BINARY=resolved_binary.abspath)

    async def run() -> list[BinaryEvent]:
        bus = create_bus(total_timeout=10.0, name=f"machine_config_seeded_{tmp_path.name}")
        MachineService(bus)
        BinaryService(
            bus,
            plugins={name: plugins[name] for name in ("wget", "env", "apt", "brew")},
            auto_install=True,
        )
        installed_events: list[BinaryEvent] = []

        async def on_BinaryEvent(event: BinaryEvent) -> None:
            installed_events.append(event)

        bus.on(BinaryEvent, on_BinaryEvent)
        try:
            await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
            await bus.emit(
                BinaryRequestEvent(
                    name=resolved_binary.abspath,
                    plugin_name="wget",
                    output_dir=".",
                    binproviders="env,apt,brew",
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
        BinaryService(
            bus,
            plugins={name: plugins[name] for name in ("wget", "env", "apt", "brew")},
            auto_install=True,
        )
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
                    plugin_name="wget",
                    output_dir=".",
                    binproviders="env,apt,brew",
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
        BinaryService(
            bus,
            plugins={name: plugins[name] for name in ("wget", "env", "apt", "brew")},
            auto_install=True,
        )
        installed_events: list[BinaryEvent] = []

        async def on_BinaryEvent(event: BinaryEvent) -> None:
            installed_events.append(event)

        bus.on(BinaryEvent, on_BinaryEvent)
        try:
            await bus.emit(MachineEvent(config={"WGET_BINARY": resolved_binary.abspath}, config_type="user")).now()
            await bus.emit(
                BinaryRequestEvent(
                    name=resolved_binary.abspath,
                    plugin_name="wget",
                    output_dir=".",
                    binproviders="env,apt,brew",
                ),
            ).now()
            await bus.wait_until_idle()
            return installed_events
        finally:
            await bus.wait_until_idle()

    wget_events = [event for event in asyncio.run(run()) if event.name == resolved_binary.abspath]
    assert wget_events
    assert wget_events[-1].abspath == resolved_binary.abspath
    assert wget_events[-1].version == ""
    assert wget_events[-1].binprovider == ""


def test_binary_installed_event_reuses_real_plugin_cached_paths_without_provider_inference(tmp_path: Path) -> None:
    resolved_binary = _resolve_real_wget_binary(tmp_path)
    plugins = discover_plugins()

    async def run() -> list[BinaryEvent]:
        bus = create_bus(total_timeout=10.0, name=f"reuses_cached_paths_{tmp_path.name}")
        MachineService(bus)
        BinaryService(
            bus,
            plugins={name: plugins[name] for name in ("wget", "env", "apt", "brew")},
            auto_install=True,
        )
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
                    plugin_name="wget",
                    output_dir=".",
                    binproviders="env,apt,brew",
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


def test_binary_event_uses_cached_config_binary_before_provider_hooks(tmp_path: Path) -> None:
    resolved_binary = _resolve_real_wget_binary(tmp_path)
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("wget", "env", "apt", "brew")}
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
                plugin_name="wget",
                output_dir=str(tmp_path / "run"),
                binproviders="env,apt,brew",
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
                BinaryService=None,
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


def test_binary_service_passes_extra_binary_kwargs_to_provider_hooks(
    tmp_path: Path,
) -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("papersdl", "pip")}

    async def run() -> list[ProcessEvent]:
        bus = create_bus(total_timeout=10.0, name=f"binary_kwargs_{tmp_path.name}")
        MachineService(bus)
        BinaryService(bus, plugins=selected, auto_install=True)
        process_events: list[ProcessEvent] = []

        async def on_ProcessEvent(event: ProcessEvent) -> None:
            process_events.append(event)

        bus.on(ProcessEvent, on_ProcessEvent)
        try:
            await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
            await bus.emit(
                BinaryRequestEvent(
                    name="papers-dl",
                    plugin_name="papersdl",
                    output_dir=str(tmp_path / "papersdl"),
                    binproviders="pip",
                    postinstall_scripts=True,
                ),
            ).now()
            await bus.wait_until_idle()
            return process_events
        finally:
            await bus.wait_until_idle()

    process_events = asyncio.run(run())
    pip_event = next(event for event in process_events if event.hook_name.endswith("on_BinaryRequest__11_pip"))

    assert "--name=papers-dl" in pip_event.hook_args
    assert "--binproviders=pip" in pip_event.hook_args
    assert "--postinstall-scripts=true" in pip_event.hook_args


def test_binary_service_honors_declared_provider_order() -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("env", "apt", "brew")}
    bus = create_bus(total_timeout=10.0, name="binary_provider_order")
    service = BinaryService(bus, plugins=selected, auto_install=True)

    hook_names = [
        hook.name
        for _provider_name, _plugin, hook in service._provider_hook_sequence(
            ["env", "apt", "brew"],
        )
    ]

    assert hook_names == [
        "on_BinaryRequest__00_env",
        "on_BinaryRequest__13_apt",
        "on_BinaryRequest__12_brew",
    ]


def test_binary_service_stops_after_successful_provider_result(tmp_path: Path) -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("env", "apt", "brew")}
    bus = create_bus(total_timeout=30.0, name=f"binary_provider_result_{tmp_path.name}")
    setup_services(
        bus,
        plugins=selected,
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
                plugin_name="python",
                output_dir=str(tmp_path / "run"),
                binproviders="env,apt,brew",
            ),
        )
        await request.now(first_result=True)
        assert await request.event_result() is not None
        await bus.wait_until_idle()

    asyncio.run(run())

    assert [event.plugin_name for event in process_events] == ["env"]
    python_events = [event for event in binary_events if event.name == "python3"]
    assert python_events
    assert python_events[-1].binprovider == "env"
    assert Path(python_events[-1].abspath).name == "python3"


def test_binary_event_ignores_unknown_request_plugin_when_persisting_config(tmp_path: Path) -> None:
    plugins = discover_plugins()

    async def run() -> list[BinaryEvent]:
        bus = create_bus(total_timeout=10.0, name=f"unknown_binary_request_plugin_{tmp_path.name}")
        MachineService(bus)
        BinaryService(
            bus,
            plugins={name: plugins[name] for name in ("env", "apt", "brew")},
            auto_install=True,
        )
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
                    plugin_name="archivebox",
                    hook_name="on_BinaryRequest__archivebox_run",
                    output_dir=str(tmp_path / "run"),
                    binproviders="env",
                    binary_id=binary_id,
                    machine_id=machine_id,
                ),
            ).now()
            await bus.emit(
                BinaryEvent(
                    name=sys.executable,
                    plugin_name="archivebox",
                    hook_name="on_BinaryRequest__archivebox_run",
                    abspath=sys.executable,
                    binproviders="env",
                    binprovider="env",
                    binary_id=binary_id,
                    machine_id=machine_id,
                ),
            ).now()
            await bus.wait_until_idle()
            return installed_events
        finally:
            await bus.wait_until_idle()

    installed_events = asyncio.run(run())
    assert installed_events
    assert installed_events[-1].abspath == sys.executable
    assert installed_events[-1].plugin_name == "archivebox"


def test_binary_event_falls_back_from_stale_cached_config_binary_to_provider_hooks(tmp_path: Path) -> None:
    managed_lib_dir = tmp_path / "lib"
    stale_binary = managed_lib_dir / "pip" / "venv" / "bin" / "wget"
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("wget", "env", "apt", "brew")}
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
                    "LIB_DIR": str(managed_lib_dir),
                },
                config_type="user",
            ),
        ).now()
        await bus.emit(MachineEvent(config={"WGET_BINARY": str(stale_binary)}, config_type="derived")).now()
        await bus.emit(
            BinaryRequestEvent(
                name="wget",
                plugin_name="wget",
                output_dir=str(tmp_path / "run"),
                binproviders="env,apt,brew",
            ),
        ).now()
        await bus.wait_until_idle()

    asyncio.run(run())

    wget_events = [event for event in installed_events if event.name == "wget"]
    assert wget_events
    assert wget_events[-1].binprovider == "env"
    assert Path(wget_events[-1].abspath).name == "wget"
    assert any(event.plugin_name == "env" for event in process_events)
    derived_update = next(
        event
        for event in reversed(list(bus.event_history.values()))
        if isinstance(event, MachineEvent)
        and event.config_type == "derived"
        and event.key == "config/WGET_BINARY"
        and event.method == "update"
        and event.value
    )
    assert Path(str(derived_update.value)).name == "wget"


def test_binary_event_does_not_fallback_from_user_binary_abspath_override(tmp_path: Path) -> None:
    broken_binary = tmp_path / "broken" / "wget"
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("wget", "env", "apt", "brew")}
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
                plugin_name="wget",
                output_dir=str(tmp_path / "run"),
                binproviders="env,apt,brew",
            ),
        )
        await request.now()
        try:
            await request.event_results_list()
        except FileNotFoundError as error:
            assert str(broken_binary) in str(error)
        else:
            raise AssertionError("missing user absolute path should fail without provider fallback")
        await bus.wait_until_idle()

    asyncio.run(run())

    assert installed_events == []
    assert process_events == []


def test_binary_installed_event_updates_lib_bin_symlink(tmp_path: Path) -> None:
    async def run() -> None:
        bus = create_bus(total_timeout=10.0, name=f"updates_lib_bin_symlink_{tmp_path.name}")
        MachineService(bus)
        BinaryService(
            bus,
            plugins={},
            auto_install=True,
        )

        first_target = tmp_path / "targets" / "first-demo"
        second_target = tmp_path / "targets" / "second-demo"
        first_target.parent.mkdir(parents=True, exist_ok=True)
        first_target.write_text("first")
        second_target.write_text("second")

        try:
            await bus.emit(MachineEvent(config={"LIB_BIN_DIR": str(tmp_path / "lib-bin")}, config_type="user")).now()
            await bus.emit(
                BinaryEvent(
                    name="demo",
                    plugin_name="demo",
                    hook_name="install",
                    abspath=str(first_target),
                ),
            ).now()
            link_path = tmp_path / "lib-bin" / "demo"
            assert link_path.is_symlink()
            assert link_path.resolve() == first_target.resolve()

            await bus.emit(
                BinaryEvent(
                    name="demo",
                    plugin_name="demo",
                    hook_name="install",
                    abspath=str(second_target),
                ),
            ).now()
            assert link_path.is_symlink()
            assert link_path.resolve() == second_target.resolve()
        finally:
            await bus.wait_until_idle()

    asyncio.run(run())


def test_download_creates_default_persona_dir(monkeypatch, tmp_path: Path) -> None:
    import abx_dl.config as config_mod

    monkeypatch.setattr(config_mod, "PERSONAS_DIR", tmp_path / "personas")
    assert not (tmp_path / "personas" / "Default").exists()

    _run_download("https://example.com", {}, tmp_path / "run", auto_install=True)

    assert (tmp_path / "personas" / "Default").is_dir()


def test_download_sets_plugin_specific_binary_env_from_binary_default(tmp_path: Path) -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("wget", "env", "apt", "brew")}
    bus = create_bus(total_timeout=120.0, name=f"real_wget_binary_env_{tmp_path.name}")
    snapshot_processes: list[ProcessEvent] = []

    async def on_ProcessEvent(event: ProcessEvent) -> None:
        if event.plugin_name == "wget" and event.hook_name == "on_Snapshot__06_wget.finite.bg":
            snapshot_processes.append(event)

    bus.on(ProcessEvent, on_ProcessEvent)
    results = _run_download("https://example.com", selected, tmp_path / "run", auto_install=True, bus=bus)

    wget_result = next(result for result in results if result.plugin == "wget")
    assert wget_result.status == "succeeded"
    assert snapshot_processes
    assert Path(snapshot_processes[-1].env["WGET_BINARY"]).name == "wget"


def test_snapshot_finite_bg_hook_finishes_before_cleanup(tmp_path: Path) -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("wget", "env", "apt", "brew")}
    results = _run_download("https://example.com", selected, tmp_path / "run", auto_install=True, emit_jsonl=False)

    result = next(r for r in results if r.plugin == "wget")
    assert result.hook_name == "on_Snapshot__06_wget.finite.bg"
    assert result.status == "succeeded"
    assert result.output_str == "wget/example.com/index.html"
    assert (tmp_path / "run" / "wget" / "example.com" / "index.html").exists()


def test_snapshot_service_emits_background_process_without_extra_wait(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=30.0, name=f"make_hook_handler_background_{tmp_path.name}")
    MachineService(bus, persist_derived=False)
    snapshot = Snapshot(url="https://example.com", id="snap-123")
    plugin = discover_plugins()["responses"]
    hook = next(h for h in plugin.hooks if h.name == "on_Snapshot__24_responses.daemon.bg")
    SnapshotService(
        bus,
        url="https://example.com",
        snapshot=snapshot,
        output_dir=tmp_path / "run",
        plugins={plugin.name: plugin},
        snapshot_phase_timeout=300.0,
    )

    try:

        async def run() -> ProcessEvent | None:
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
                hook_name=hook.name,
            )
            return process_event if isinstance(process_event, ProcessEvent) else None

        emitted = asyncio.run(run())
    finally:
        asyncio.run(bus.wait_until_idle())

    assert emitted is not None
    assert emitted.is_background is True
    assert emitted.event_blocks_parent_completion is False
    assert emitted.hook_name == "on_Snapshot__24_responses.daemon.bg"


def test_snapshot_background_daemon_stays_alive_until_cleanup(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "daemon_check"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_Snapshot__10_daemon.daemon.bg.sh"
    foreground_hook = plugin_dir / "on_Snapshot__20_check.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'echo $$ > "$SNAP_DIR/daemon.pid"',
                'echo ready > "$SNAP_DIR/daemon.ready"',
                "trap 'echo cleaned; exit 0' TERM",
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


def test_crawl_setup_background_daemon_survives_until_explicit_cleanup(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "crawl_daemon_check"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_CrawlSetup__10_daemon.daemon.bg.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo $$ > "$crawl_dir/daemon.pid"',
                'echo ready > "$crawl_dir/daemon.ready"',
                "trap 'echo cleaned; exit 0' TERM",
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


def test_crawl_background_daemon_stays_alive_through_snapshot_until_cleanup(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "crawl_daemon_check"
    plugin_dir.mkdir(parents=True)
    daemon_hook = plugin_dir / "on_CrawlSetup__10_daemon.daemon.bg.sh"
    snapshot_hook = plugin_dir / "on_Snapshot__20_check.sh"
    daemon_hook.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'crawl_dir="${CRAWL_DIR:-$(dirname "$PWD")}"',
                'echo $$ > "$crawl_dir/daemon.pid"',
                'echo ready > "$crawl_dir/daemon.ready"',
                "trap 'echo cleaned; exit 0' TERM",
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
            child_of=started_process,
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
    assert not (output_dir / "on_Snapshot__10_background.daemon.bg.pid").exists()


def test_download_can_suppress_jsonl_stdout(tmp_path: Path, capsys) -> None:
    plugins = discover_plugins()
    selected = {name: plugins[name] for name in ("wget", "env", "apt", "brew")}
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
