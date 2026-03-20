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
from pathlib import Path
from typing import Any

from abxbus import EventBus

from .events import ArchiveResultEvent, CrawlEvent, slow_warning_timeout
from .models import ArchiveResult, Snapshot, write_jsonl
from .models import Hook, Plugin, filter_plugins


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
    if f'{name_upper}_TIMEOUT' in cfg:
        return int(cfg[f'{name_upper}_TIMEOUT'])
    if 'TIMEOUT' in cfg:
        return int(cfg['TIMEOUT'])
    # Check plugin schema defaults
    schema_def = plugin.config_schema.get(f'{name_upper}_TIMEOUT', {})
    if isinstance(schema_def, dict) and 'default' in schema_def:
        return int(schema_def['default'])
    return 60


def compute_phase_timeout(hooks: list[tuple[Plugin, Hook]], config: dict[str, Any] | None = None) -> float:
    """Sum per-plugin timeouts across all hooks in a phase.

    Each hook contributes its plugin's timeout (from ``get_plugin_timeout``).
    This gives an accurate ceiling: slow plugins (e.g. YTDLP_TIMEOUT=120)
    contribute more than fast ones (e.g. TITLE_TIMEOUT=10).

    Returns at least 60.0 (minimum phase timeout).
    """
    total = sum(
        get_plugin_timeout(plugin, config)
        for plugin, _hook in hooks
    )
    return max(float(total), 60.0)


def create_bus(*, total_timeout: float = 60.0, **kwargs) -> EventBus:
    """Create a configured EventBus for a download run.

    Callers should subscribe to events on the bus before passing it to
    download(). Common subscriptions::

        bus.on(ProcessCompletedEvent, my_process_handler)
        bus.on(ArchiveResultEvent, my_result_handler)

    Args:
        total_timeout: Total timeout for the entire run (sum of all phase
            timeouts). Computed by ``compute_phase_timeout`` in download().
        **kwargs: Extra keyword arguments passed through to the ``EventBus``
            constructor (e.g. ``name``, ``middlewares``).
    """
    defaults = dict(
        name='AbxDl',
        # parallel event concurrency lets bg ProcessEvents (fire-and-forget
        # children) process concurrently with the parent event's serial handlers
        event_concurrency='parallel',
        # A full plugin run generates hundreds of events; drop old history
        # entries instead of rejecting new events when the buffer fills
        max_history_size=1000,
        max_history_drop=True,
        # Normal abx-dl processing legitimately nests several queue-jumped
        # handler chains (stdout -> typed event -> follow-up process/event work).
        max_handler_recursion_depth=6,
        # Total timeout covers both crawl and snapshot phases.
        # Individual hooks set their own timeouts via event_handler_timeout
        # on their ProcessEvent.
        event_timeout=total_timeout,
        event_slow_timeout=total_timeout * 0.8,
        event_handler_slow_timeout=60.0,
    )
    defaults.update(kwargs)
    return EventBus(**defaults)


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

    output_dir = output_dir or Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / 'index.jsonl'
    stdout_is_tty = sys.stdout.isatty()
    stderr_is_tty = sys.stderr.isatty()
    if emit_jsonl is None:
        emit_jsonl = not stdout_is_tty

    # Filter plugins (no binary pre-check — on_Crawl hooks handle installation)
    if selected_plugins:
        plugins = filter_plugins(plugins, selected_plugins)

    # Create snapshot record and write it as the first line of index.jsonl
    snapshot = Snapshot(url=url)
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
    if bus is None:
        bus = create_bus(total_timeout=total_timeout)

    # Collect ArchiveResult records from the bus
    results: list[ArchiveResult] = []

    async def on_ArchiveResultEvent(event: ArchiveResultEvent) -> None:
        """Collects all ArchiveResultEvents."""
        results.append(ArchiveResult(
            snapshot_id=event.snapshot_id, plugin=event.plugin,
            id=event.id, hook_name=event.hook_name, status=event.status,
            process_id=event.process_id or None,
            output_str=event.output_str,
            output_files=event.output_files,
            start_ts=event.start_ts or None, end_ts=event.end_ts or None,
            error=event.error or None,
        ))

    bus.on(ArchiveResultEvent, on_ArchiveResultEvent)

    # --- Wire up services ---
    # Import here to avoid circular imports (services import events/models)
    from .services import MachineService, BinaryService, ProcessService, ArchiveResultService, CrawlService, SnapshotService

    machine_svc = MachineService(bus, initial_config=config_overrides)
    BinaryService(
        bus, machine=machine_svc, plugins=plugins, auto_install=auto_install,
        output_dir=output_dir,
    )
    ProcessService(
        bus, index_path=index_path, output_dir=output_dir, emit_jsonl=emit_jsonl,
        stderr_is_tty=stderr_is_tty,
    )
    ArchiveResultService(bus, index_path=index_path, emit_jsonl=emit_jsonl)
    CrawlService(
        bus, url=url, snapshot=snapshot, output_dir=output_dir,
        machine=machine_svc, hooks=crawl_hooks, crawl_only=crawl_only,
        phase_timeout=crawl_phase_timeout,
    )
    SnapshotService(
        bus, url=url, snapshot=snapshot, output_dir=output_dir,
        machine=machine_svc, hooks=snapshot_hooks,
        phase_timeout=snapshot_phase_timeout,
    )

    # --- Drive the lifecycle ---
    # Emitting CrawlEvent triggers the entire cascade:
    # CrawlSetupEvent → CrawlStartEvent → SnapshotEvent →
    # SnapshotCleanupEvent → SnapshotCompletedEvent → CrawlCleanupEvent →
    # CrawlCompletedEvent. The await returns after the full tree completes.
    try:
        await bus.emit(CrawlEvent(
            url=url, snapshot_id=snapshot.id, output_dir=str(output_dir),
            event_timeout=total_timeout,
            event_handler_slow_timeout=slow_warning_timeout(total_timeout),
        ))
    finally:
        await bus.stop()

    return results
