import asyncio
from pathlib import Path
from uuid import uuid4


from abx_dl.config import get_initial_env
from abx_dl.config import get_required_binary_requests
from abx_dl.events import (
    ArchiveResultEvent,
    BinaryEvent,
    BinaryRequestEvent,
    CrawlStartEvent,
    MachineEvent,
    ProcessEvent,
    ProcessStdoutEvent,
    SnapshotEvent,
)
from abx_dl.models import Snapshot, discover_plugins
from abx_dl.orchestrator import create_bus, download, setup_services
from abx_dl.services.archive_result_service import ArchiveResultService
from abx_dl.services.binary_service import BinaryService
from abx_dl.services.machine_service import MachineService
from abx_dl.services.process_service import ProcessService
from abx_dl.services.snapshot_service import SnapshotService


def _run_download(*args, **kwargs):
    bus = kwargs.get("bus")
    owns_bus = bus is None
    if bus is None:
        bus = create_bus(total_timeout=120.0, name=f"test_executor_download_{uuid4().hex[:8]}")
        kwargs["bus"] = bus
    results = []

    async def on_ArchiveResultEvent(event: ArchiveResultEvent) -> None:
        if event.snapshot_id:
            results.append(event)

    bus.on(ArchiveResultEvent, on_ArchiveResultEvent)
    try:
        asyncio.run(download(*args, **kwargs))
    finally:
        if owns_bus:
            asyncio.run(bus.stop())
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
        await bus.emit(MachineEvent(config=get_initial_env(), config_type="user"))
        await bus.emit(
            BinaryRequestEvent(
                name="wget",
                plugin_name="wget",
                output_dir=str(tmp_path / "resolve-wget"),
                binproviders="env,apt,brew",
            ),
        )

    try:
        asyncio.run(run())
    finally:
        asyncio.run(bus.stop())

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
        await bus.emit(MachineEvent(config=get_initial_env(), config_type="user"))
        await bus.emit(
            BinaryRequestEvent(
                name="wget",
                plugin_name="wget",
                output_dir=str(tmp_path / "run"),
                binproviders="env,apt,brew",
            ),
        )

    try:
        asyncio.run(run())
    finally:
        asyncio.run(bus.stop())

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
            await bus.emit(MachineEvent(config=get_initial_env(), config_type="user"))
            await bus.emit(
                BinaryRequestEvent(
                    name=resolved_binary.abspath,
                    plugin_name="wget",
                    output_dir=".",
                    binproviders="env,apt,brew",
                ),
            )
            return installed_events
        finally:
            await bus.stop()

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
            await bus.emit(MachineEvent(config=get_initial_env(), config_type="user"))
            await bus.emit(MachineEvent(config={"WGET_BINARY": resolved_binary.abspath}, config_type="derived"))
            await bus.emit(
                BinaryRequestEvent(
                    name="wget",
                    plugin_name="wget",
                    output_dir=".",
                    binproviders="env,apt,brew",
                ),
            )
            return installed_events
        finally:
            await bus.stop()

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
            await bus.emit(MachineEvent(config={"WGET_BINARY": resolved_binary.abspath}, config_type="user"))
            await bus.emit(
                BinaryRequestEvent(
                    name=resolved_binary.abspath,
                    plugin_name="wget",
                    output_dir=".",
                    binproviders="env,apt,brew",
                ),
            )
            return installed_events
        finally:
            await bus.stop()

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
            await bus.emit(MachineEvent(config=get_initial_env(), config_type="user"))
            await bus.emit(MachineEvent(config={"WGET_BINARY": resolved_binary.abspath}, config_type="derived"))
            await bus.emit(
                BinaryRequestEvent(
                    name="wget",
                    plugin_name="wget",
                    output_dir=".",
                    binproviders="env,apt,brew",
                ),
            )
            return installed_events
        finally:
            await bus.stop()

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
        await bus.emit(MachineEvent(config=get_initial_env(), config_type="user"))
        await bus.emit(MachineEvent(config={"WGET_BINARY": resolved_binary.abspath}, config_type="derived"))
        await bus.emit(
            BinaryRequestEvent(
                name="wget",
                plugin_name="wget",
                output_dir=str(tmp_path / "run"),
                binproviders="env,apt,brew",
            ),
        )

    try:
        asyncio.run(run())
    finally:
        asyncio.run(bus.stop())

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
            await bus.emit(MachineEvent(config=get_initial_env(), config_type="user"))
            await bus.emit(
                BinaryRequestEvent(
                    name="papers-dl",
                    plugin_name="papersdl",
                    output_dir=str(tmp_path / "papersdl"),
                    binproviders="pip",
                    postinstall_scripts=True,
                ),
            )
            return process_events
        finally:
            await bus.stop()

    process_events = asyncio.run(run())
    pip_event = next(event for event in process_events if event.hook_name.endswith("on_BinaryRequest__11_pip"))

    assert "--name=papers-dl" in pip_event.hook_args
    assert "--binproviders=pip" in pip_event.hook_args
    assert "--postinstall-scripts=true" in pip_event.hook_args


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
        )
        await bus.emit(MachineEvent(config={"WGET_BINARY": str(stale_binary)}, config_type="derived"))
        await bus.emit(
            BinaryRequestEvent(
                name="wget",
                plugin_name="wget",
                output_dir=str(tmp_path / "run"),
                binproviders="env,apt,brew",
            ),
        )

    try:
        asyncio.run(run())
    finally:
        asyncio.run(bus.stop())

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
        await bus.emit(MachineEvent(config={"WGET_BINARY": str(broken_binary)}, config_type="user"))
        request = await bus.emit(
            BinaryRequestEvent(
                name=str(broken_binary),
                plugin_name="wget",
                output_dir=str(tmp_path / "run"),
                binproviders="env,apt,brew",
            ),
        )
        errors = [result.error for result in request.event_results.values() if result.error is not None]
        assert errors
        assert all(isinstance(error, FileNotFoundError) for error in errors)

    try:
        asyncio.run(run())
    finally:
        asyncio.run(bus.stop())

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
            await bus.emit(MachineEvent(config={"LIB_BIN_DIR": str(tmp_path / "lib-bin")}, config_type="user"))
            await bus.emit(
                BinaryEvent(
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
                BinaryEvent(
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
            await bus.emit(crawl_start_event)
            await bus.emit(
                root_event,
            )
            return await bus.find(
                ProcessEvent,
                past=True,
                future=1.0,
                child_of=root_event,
                plugin_name=plugin.name,
                hook_name=hook.name,
            )

        emitted = asyncio.run(run())
    finally:
        asyncio.run(bus.stop())

    assert emitted is not None
    assert emitted.is_background is True
    assert emitted.hook_name == "on_Snapshot__24_responses.daemon.bg"


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
        await bus.emit(MachineEvent(config={"CRAWL_DIR": str(tmp_path / "run")}, config_type="user"))
        await bus.emit(crawl_start_event)
        await bus.emit(root_event)
        await bus.emit(process_event)
        await bus.emit(stdout_event)
        await bus.stop()

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
        await bus.emit(MachineEvent(config={"CRAWL_DIR": str(tmp_path / "run")}, config_type="user"))
        await bus.emit(crawl_start_event)
        await bus.emit(root_event)
        await bus.emit(process_event)
        await bus.emit(stdout_event)
        await bus.stop()

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
        await bus.emit(crawl_start_event)
        await bus.emit(snapshot_event)
        await bus.emit(process_event)
        await bus.stop()

    asyncio.run(run())

    assert len(seen_archive_results) == 1
    assert seen_archive_results[0].status == "succeeded"
    assert [output_file.path for output_file in seen_archive_results[0].output_files] == ["headers/headers.json"]
