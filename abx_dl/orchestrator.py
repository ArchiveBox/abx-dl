"""
Event-driven orchestrator for abx-dl.

This is the main entry point for downloading a URL. It wires up all services
on a caller-provided abxbus EventBus and emits a single CrawlEvent to kick off
the entire lifecycle. Everything else is driven by services reacting to events.

Full event tree for a typical run::

    CrawlEvent                                  # emitted here by download()
    │
    ├── CrawlSetupEvent                         # on_Crawl hooks run here
    │   ├── ProcessEvent  (bg: wget_install)
    │   ├── ProcessEvent  (bg: chrome_install)
    │   ├── ProcessEvent  (bg: chrome_launch)
    │   ├── ProcessEvent  (FG: chrome_wait)
    │   │   ├── ProcessStdoutEvent
    │   │   │   │  (side-effect chain from chrome_install):
    │   │   │   │  BinaryEvent → provider hooks → BinaryInstalledEvent
    │   │   └── ProcessCompletedEvent
    │   └── ...
    │
    ├── CrawlStartEvent                # triggers snapshot phase
    │   └── SnapshotEvent (depth=0)
    │       ├── ProcessEvent  (on_Snapshot hooks)
    │       │   ├── ProcessStdoutEvent
    │       │   │   ├── SnapshotEvent (depth>0, ignored by abx-dl)
    │       │   │   └── ArchiveResultEvent (from hook JSONL)
    │       │   └── ProcessCompletedEvent
    │       │       └── ArchiveResultEvent (synthetic, only if hook didn't report one)
    │       ├── SnapshotCleanupEvent
    │       │   └── ProcessKillEvent × N
    │       └── SnapshotCompletedEvent
    │
    ├── CrawlCleanupEvent                       # SIGTERMs bg crawl daemons
    │   └── ProcessKillEvent × N
    │
    └── CrawlCompletedEvent                     # informational

Result collection:
- ArchiveResultEvents are only emitted during the snapshot phase (under
  CrawlStartEvent → SnapshotEvent). CrawlSetupEvent only produces Binary,
  Machine, and Process events — never ArchiveResults or Snapshots.
- ArchiveResultService emits ArchiveResultEvents in two cases: directly from
  hook JSONL output (inline), or as a synthetic fallback on ProcessCompletedEvent
  when the hook didn't report one itself (failed, or succeeded with output files).
  The orchestrator collects all of them.
- Callers subscribe to events on the bus before calling download():
  ``bus.on(ProcessCompletedEvent, handler)`` for process records,
  ``bus.on(ArchiveResultEvent, handler)`` for archive results.

Key abxbus concepts used:

- **event_concurrency='parallel'**: child events of a parent can process
  concurrently. This is what lets bg ProcessEvents (fire-and-forget) run
  alongside the parent CrawlEvent's serial handler chain.

- **Serial handler execution** (default): handlers on the same event run one
  at a time in registration order. This preserves hook ordering — fg hooks
  see config updates from all prior hooks.

- **Queue-jump** (``await bus.emit(...)``): the emitted event and ALL its
  descendants complete synchronously before the await returns. This is how
  config propagation works: hook outputs Binary JSONL → ProcessService emits
  ProcessStdoutEvent → BinaryService emits BinaryEvent → provider
  hooks install it → MachineEvent updates config → all before the next
  stdout line is read.

- **Fire-and-forget** (``bus.emit(...)`` without await): the event becomes a
  concurrent child of the current event. It runs in the background and is
  subject to the parent event's timeout.

- **max_history_drop=True**: when the event history buffer fills up, old
  entries are dropped instead of rejecting new events. A full plugin run
  generates hundreds of events, easily exceeding the default 100.
"""

import sys
from collections.abc import Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from abxbus import EventBus, EventBusMiddleware, EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode

from .config import ensure_default_persona_dir
from .events import (
    ArchiveResultEvent,
    CrawlCleanupEvent,
    CrawlCompletedEvent,
    CrawlSetupEvent,
    CrawlStartEvent,
    SnapshotEvent,
    slow_warning_timeout,
)
from .models import INSTALL_URL, ArchiveResult, Snapshot, write_jsonl
from .models import Hook, Plugin, discover_plugins, filter_plugins


class _BusServices:
    def __init__(self, *, machine, binary, process, archive_result, tag):
        self.machine = machine
        self.binary = binary
        self.process = process
        self.archive_result = archive_result
        self.tag = tag


def _get_or_create_bus_services(
    bus: EventBus,
    *,
    plugins: dict[str, Plugin],
    config_overrides: dict[str, Any] | None,
    auto_install: bool,
    emit_jsonl: bool,
    stderr_is_tty: bool,
) -> _BusServices:
    state = getattr(bus, "_abx_dl_services", None)
    if state is None:
        from .services import ArchiveResultService, BinaryService, MachineService, ProcessService, TagService

        machine = MachineService(bus, initial_config=config_overrides)
        state = _BusServices(
            machine=machine,
            binary=BinaryService(
                bus,
                machine=machine,
                plugins=plugins,
                auto_install=auto_install,
            ),
            process=ProcessService(
                bus,
                emit_jsonl=emit_jsonl,
                stderr_is_tty=stderr_is_tty,
            ),
            archive_result=ArchiveResultService(
                bus,
                emit_jsonl=emit_jsonl,
            ),
            tag=TagService(bus),
        )
        setattr(bus, "_abx_dl_services", state)
    elif config_overrides:
        state.machine.shared_config.update(config_overrides)
    return state


def setup_services(
    bus: EventBus,
    *,
    plugins: dict[str, Plugin],
    config_overrides: dict[str, Any] | None = None,
    auto_install: bool = True,
    emit_jsonl: bool = True,
    stderr_is_tty: bool | None = None,
) -> _BusServices:
    """Attach the shared abx-dl services to an existing bus.

    This is the public entrypoint for embedding abx-dl as an event-driven
    runtime without immediately starting a crawl via ``download()``.
    """
    if stderr_is_tty is None:
        stderr_is_tty = getattr(sys.stderr, "isatty", lambda: False)()
    return _get_or_create_bus_services(
        bus,
        plugins=plugins,
        config_overrides=config_overrides,
        auto_install=auto_install,
        emit_jsonl=emit_jsonl,
        stderr_is_tty=bool(stderr_is_tty),
    )


def prepare_install_plugins(
    plugins: dict[str, Plugin],
    plugin_names: Sequence[str] | None = None,
) -> dict[str, Plugin]:
    """Filter plugins down to the install-only hooks used by `abx-dl install`."""
    if plugin_names:
        selected = filter_plugins(plugins, list(plugin_names), include_providers=True)
        visible_plugins = set(filter_plugins(plugins, list(plugin_names), include_providers=False))
    else:
        selected = plugins
        visible_plugins = set(selected)

    return {
        name: plugin.model_copy(
            update={
                "hooks": [
                    hook
                    for hook in plugin.hooks
                    if "Binary" in hook.name
                    or (name.lower() in visible_plugins and "Crawl" in hook.name and "install" in hook.name.lower())
                ],
            },
        )
        for name, plugin in selected.items()
    }


async def install_plugins(
    plugin_names: Sequence[str] | None = None,
    *,
    plugins: dict[str, Plugin] | None = None,
    output_dir: Path | None = None,
    config_overrides: dict[str, Any] | None = None,
    auto_install: bool = True,
    emit_jsonl: bool = False,
    bus: EventBus | None = None,
) -> list[ArchiveResult]:
    """Run the abx-dl install flow on an existing bus or a temporary one."""
    all_plugins = plugins or discover_plugins()
    selected = prepare_install_plugins(all_plugins, plugin_names=plugin_names)
    if not selected:
        return []

    if output_dir is not None:
        return await download(
            INSTALL_URL,
            selected,
            output_dir,
            auto_install=auto_install,
            emit_jsonl=emit_jsonl,
            config_overrides=config_overrides,
            bus=bus,
        )

    with TemporaryDirectory(prefix="abx-dl-install-") as temp_dir:
        return await download(
            INSTALL_URL,
            selected,
            Path(temp_dir),
            auto_install=auto_install,
            emit_jsonl=emit_jsonl,
            config_overrides=config_overrides,
            bus=bus,
        )


def get_plugin_timeout(plugin: Plugin, config: dict[str, Any] | None = None) -> int:
    """Resolve a plugin's timeout from config overrides and schema defaults.

    Checks (in priority order):
    1. ``{PLUGIN_NAME}_TIMEOUT`` in *config*
    2. ``TIMEOUT`` in *config*
    3. ``{PLUGIN_NAME}_TIMEOUT`` default from the plugin's config_schema
    4. Global default (60s)
    """
    name_upper = plugin.name.upper()
    cfg = config or {}
    # Check config overrides first
    if f"{name_upper}_TIMEOUT" in cfg:
        return int(cfg[f"{name_upper}_TIMEOUT"])
    if "TIMEOUT" in cfg:
        return int(cfg["TIMEOUT"])
    # Check plugin schema defaults
    schema_def = plugin.config_schema.get(f"{name_upper}_TIMEOUT", {})
    if isinstance(schema_def, dict) and "default" in schema_def:
        return int(schema_def["default"])
    return 60


def compute_phase_timeout(hooks: list[tuple[Plugin, Hook]], config: dict[str, Any] | None = None) -> float:
    """Sum per-plugin timeouts across all hooks in a phase.

    Each hook contributes its plugin's timeout (from ``get_plugin_timeout``).
    This gives an accurate ceiling: slow plugins (e.g. YTDLP_TIMEOUT=120)
    contribute more than fast ones (e.g. TITLE_TIMEOUT=10).

    Returns at least 60.0 (minimum phase timeout).
    """
    total = sum(get_plugin_timeout(plugin, config) for plugin, _hook in hooks)
    return max(float(total), 60.0)


def create_bus(
    *,
    total_timeout: float = 60.0,
    name: str | None = "AbxDl",
    event_concurrency: EventConcurrencyMode | str | None = EventConcurrencyMode.PARALLEL,
    event_handler_concurrency: EventHandlerConcurrencyMode | str = EventHandlerConcurrencyMode.SERIAL,
    event_handler_completion: EventHandlerCompletionMode | str = EventHandlerCompletionMode.ALL,
    max_history_size: int | None = 1000,
    max_history_drop: bool = True,
    event_timeout: float | None = None,
    event_slow_timeout: float | None = None,
    event_handler_slow_timeout: float | None = 60.0,
    event_handler_detect_file_paths: bool = True,
    warn_on_duplicate_handler_names: bool = False,
    max_handler_recursion_depth: int = 6,
    middlewares: Sequence[EventBusMiddleware | type[EventBusMiddleware]] | None = None,
    id: str | None = None,
) -> EventBus:
    """Create a configured EventBus for a download run.

    Callers should subscribe to events on the bus before passing it to
    download(). Common subscriptions::

        bus.on(ProcessCompletedEvent, my_process_handler)
        bus.on(ArchiveResultEvent, my_result_handler)

    Args:
        total_timeout: Total timeout for the entire run (sum of all phase
            timeouts). Computed by ``compute_phase_timeout`` in download().
        name: Optional EventBus instance name.
        middlewares: Optional EventBus middlewares.
    """
    return EventBus(
        name=name,
        # parallel event concurrency lets bg ProcessEvents (fire-and-forget
        # children) process concurrently with the parent event's serial handlers
        event_concurrency=event_concurrency,
        event_handler_concurrency=event_handler_concurrency,
        event_handler_completion=event_handler_completion,
        # A full plugin run generates hundreds of events; drop old history
        # entries instead of rejecting new events when the buffer fills
        max_history_size=max_history_size,
        max_history_drop=max_history_drop,
        # Total timeout covers both crawl and snapshot phases.
        # Individual hooks set their own timeouts via event_handler_timeout
        # on their ProcessEvent.
        event_timeout=total_timeout if event_timeout is None else event_timeout,
        event_slow_timeout=(total_timeout * 0.8) if event_slow_timeout is None else event_slow_timeout,
        event_handler_slow_timeout=event_handler_slow_timeout,
        event_handler_detect_file_paths=event_handler_detect_file_paths,
        warn_on_duplicate_handler_names=warn_on_duplicate_handler_names,
        # Normal abx-dl processing legitimately nests several queue-jumped
        # handler chains (stdout -> typed event -> follow-up process/event work).
        max_handler_recursion_depth=max_handler_recursion_depth,
        middlewares=middlewares,
        id=id,
    )


async def download(
    url: str,
    plugins: dict[str, Plugin],
    output_dir: Path,
    selected_plugins: list[str] | None = None,
    config_overrides: dict[str, Any] | None = None,
    auto_install: bool = True,
    crawl_only: bool = False,
    *,
    bus: EventBus | None = None,
    emit_jsonl: bool | None = None,
    snapshot: Snapshot | None = None,
    skip_crawl_setup: bool = False,
    skip_crawl_cleanup: bool = False,
    crawl_setup_only: bool = False,
    crawl_cleanup_only: bool = False,
) -> list[ArchiveResult]:
    """Download a URL using plugins, coordinated through a abxbus EventBus.

    This is the only public function in the orchestrator. It:
    1. Discovers and sorts hooks from selected plugins
    2. Wires up all services on the bus
    3. Registers a bus handler for ArchiveResultEvents to collect results
    4. Emits CrawlEvent (which cascades through the entire lifecycle)
    5. Returns all ArchiveResult records collected from ArchiveResultEvents

    Args:
        url: The URL to download/archive.
        plugins: All discovered plugins (from discover_plugins()).
        output_dir: Where to write output files and index.jsonl.
        selected_plugins: If set, only use these plugins (with dependency resolution).
        config_overrides: Extra config values (e.g. TIMEOUT) merged into shared_config.
        auto_install: Whether to auto-install missing binaries.
        crawl_only: If True, skip the snapshot phase (only run on_Crawl hooks).
        bus: Pre-configured EventBus. Callers subscribe to events (e.g.
            ProcessCompletedEvent, ArchiveResultEvent) before calling download().
            If None, a default bus is created via create_bus().
        emit_jsonl: Whether to print JSONL to stdout. Defaults to True if not a TTY.

    Returns:
        List of ArchiveResult records produced (collected from
        ArchiveResultEvents on the bus).
    """

    if crawl_setup_only and crawl_cleanup_only:
        raise ValueError("crawl_setup_only and crawl_cleanup_only are mutually exclusive")
    ensure_default_persona_dir()
    output_dir = output_dir or Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "index.jsonl"
    stdout_is_tty = sys.stdout.isatty()
    stderr_is_tty = sys.stderr.isatty()
    if emit_jsonl is None:
        emit_jsonl = not stdout_is_tty

    # Filter plugins (no binary pre-check — on_Crawl hooks handle installation)
    if selected_plugins:
        plugins = filter_plugins(plugins, selected_plugins)

    # Create snapshot record and write it as the first line of index.jsonl
    snapshot = snapshot or Snapshot(url=url)
    write_jsonl(index_path, snapshot, also_print=emit_jsonl)

    # Collect and sort hooks by (order, name) so execution order matches
    # the numeric prefix in hook filenames (e.g. __10, __41, __70, __90, __91)
    crawl_hooks: list[tuple[Plugin, Hook]] = []
    snapshot_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in plugins.values():
        for hook in plugin.get_crawl_hooks():
            crawl_hooks.append((plugin, hook))
        for hook in plugin.get_snapshot_hooks():
            snapshot_hooks.append((plugin, hook))
    crawl_hooks.sort(key=lambda x: x[1].sort_key)
    snapshot_hooks.sort(key=lambda x: x[1].sort_key)

    # Compute per-phase timeouts from plugin-specific settings
    crawl_phase_timeout = compute_phase_timeout(crawl_hooks, config_overrides)
    snapshot_phase_timeout = compute_phase_timeout(snapshot_hooks, config_overrides)
    total_timeout = crawl_phase_timeout + snapshot_phase_timeout

    # Create bus if caller didn't provide one
    owns_bus = bus is None
    if bus is None:
        bus = create_bus(total_timeout=total_timeout)
    assert bus is not None
    active_bus = bus

    # Collect ArchiveResult records from the bus
    results: list[ArchiveResult] = []

    tracked_events: list[CrawlStartEvent | SnapshotEvent] = []

    async def on_ArchiveResultEvent(event: ArchiveResultEvent) -> None:
        """Collects all ArchiveResultEvents."""
        if tracked_events and not any(
            event.event_id == tracked.event_id or active_bus.event_is_child_of(event, tracked) for tracked in tracked_events
        ):
            return
        results.append(
            ArchiveResult(
                snapshot_id=event.snapshot_id,
                plugin=event.plugin,
                id=event.id,
                hook_name=event.hook_name,
                status=event.status,
                process_id=event.process_id or None,
                output_str=event.output_str,
                output_json=event.output_json,
                output_files=event.output_files,
                start_ts=event.start_ts or None,
                end_ts=event.end_ts or None,
                error=event.error or None,
            ),
        )

    collector = active_bus.on(ArchiveResultEvent, on_ArchiveResultEvent)

    # --- Wire up services ---
    from .services import CrawlService, SnapshotService

    shared = _get_or_create_bus_services(
        active_bus,
        plugins=plugins,
        config_overrides=config_overrides,
        auto_install=auto_install,
        emit_jsonl=emit_jsonl,
        stderr_is_tty=stderr_is_tty,
    )

    run_services = []
    if not skip_crawl_setup or not skip_crawl_cleanup or crawl_setup_only or crawl_cleanup_only:
        run_services.append(
            CrawlService(
                active_bus,
                url=url,
                snapshot=snapshot,
                output_dir=output_dir,
                machine=shared.machine,
                hooks=crawl_hooks,
                crawl_only=crawl_only,
                phase_timeout=crawl_phase_timeout,
            ),
        )
    if not crawl_setup_only and not crawl_cleanup_only and not crawl_only:
        run_services.append(
            SnapshotService(
                active_bus,
                url=url,
                snapshot=snapshot,
                output_dir=output_dir,
                machine=shared.machine,
                hooks=snapshot_hooks,
                phase_timeout=snapshot_phase_timeout,
            ),
        )

    try:
        if crawl_setup_only:
            await active_bus.emit(
                CrawlSetupEvent(
                    url=url,
                    snapshot_id=snapshot.id,
                    output_dir=str(output_dir),
                    event_timeout=crawl_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(crawl_phase_timeout),
                ),
            )
        elif crawl_cleanup_only:
            await active_bus.emit(
                CrawlCleanupEvent(
                    url=url,
                    snapshot_id=snapshot.id,
                    output_dir=str(output_dir),
                    event_timeout=crawl_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(crawl_phase_timeout),
                ),
            )
            await active_bus.emit(CrawlCompletedEvent(url=url, snapshot_id=snapshot.id, output_dir=str(output_dir)))
        else:
            if not skip_crawl_setup:
                await active_bus.emit(
                    CrawlSetupEvent(
                        url=url,
                        snapshot_id=snapshot.id,
                        output_dir=str(output_dir),
                        event_timeout=crawl_phase_timeout,
                        event_handler_slow_timeout=slow_warning_timeout(crawl_phase_timeout),
                    ),
                )
            if not crawl_only:
                crawl_start = CrawlStartEvent(
                    url=url,
                    snapshot_id=snapshot.id,
                    output_dir=str(output_dir),
                    event_timeout=snapshot_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(snapshot_phase_timeout),
                )
                tracked_events.append(crawl_start)
                await active_bus.emit(crawl_start)
                snapshot_event = SnapshotEvent(
                    url=url,
                    snapshot_id=snapshot.id,
                    output_dir=str(output_dir),
                    depth=snapshot.depth,
                    parent_snapshot_id=snapshot.parent_snapshot_id or "",
                    event_timeout=snapshot_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(snapshot_phase_timeout),
                )
                tracked_events.append(snapshot_event)
                await active_bus.emit(snapshot_event)
            if not skip_crawl_cleanup:
                await active_bus.emit(
                    CrawlCleanupEvent(
                        url=url,
                        snapshot_id=snapshot.id,
                        output_dir=str(output_dir),
                        event_timeout=crawl_phase_timeout,
                        event_handler_slow_timeout=slow_warning_timeout(crawl_phase_timeout),
                    ),
                )
                await active_bus.emit(CrawlCompletedEvent(url=url, snapshot_id=snapshot.id, output_dir=str(output_dir)))
    finally:
        active_bus.off(ArchiveResultEvent, collector)
        while run_services:
            run_services.pop().close()
        if owns_bus:
            await active_bus.stop()

    return results
