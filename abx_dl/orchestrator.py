"""
Event-driven orchestrator for abx-dl.

This is the main entry point for downloading a URL. It creates a bubus EventBus,
wires up all services, and emits a single CrawlEvent to kick off the entire
lifecycle. Everything else is driven by services reacting to events.

Full event tree for a typical run::

    CrawlEvent                                  # emitted here by download()
    │
    ├── CrawlSetupEvent                         # on_Crawl hooks run here
    │   ├── ProcessEvent  (bg: wget_install)
    │   ├── ProcessEvent  (bg: chrome_install)
    │   ├── ProcessEvent  (bg: chrome_launch)
    │   ├── ProcessEvent  (FG: chrome_wait)
    │   │   ├── ProcessRecordOutputtedEvent
    │   │   │   │  (side-effect chain from chrome_install):
    │   │   │   │  BinaryEvent → provider hooks → BinaryInstalledEvent
    │   │   │   │      → BinaryEvent(abspath) → MachineEvent → BinaryLoadedEvent
    │   │   │   └── ArchiveResultEvent (inline, via ArchiveResultService)
    │   │   └── ProcessCompletedEvent
    │   │       └── ArchiveResultEvent (enriched with process metadata)
    │   └── ...
    │
    ├── CrawlStartEvent                # triggers snapshot phase
    │   └── SnapshotEvent (depth=1)
    │       ├── ProcessEvent  (on_Snapshot hooks)
    │       │   ├── ProcessRecordOutputtedEvent
    │       │   │   ├── SnapshotEvent (depth>1, ignored by abx-dl)
    │       │   │   └── ArchiveResultEvent (inline)
    │       │   └── ProcessCompletedEvent
    │       │       └── ArchiveResultEvent (enriched)
    │       ├── SnapshotCleanupEvent
    │       │   └── ProcessKillEvent × N
    │       └── SnapshotCompletedEvent
    │
    ├── CrawlCleanupEvent                       # SIGTERMs bg crawl daemons
    │   └── ProcessKillEvent × N
    │
    └── CrawlCompletedEvent                     # informational

Result collection:
- ArchiveResultService emits ArchiveResultEvent in two contexts: inline
  (from hook stdout, no process_id) and enriched (on ProcessCompletedEvent,
  with process_id set). The orchestrator collects the enriched ones.
- Process records flow through the on_process callback from ProcessService
  to the on_result callback (for TUI display).

Key bubus concepts used:

- **event_concurrency='parallel'**: child events of a parent can process
  concurrently. This is what lets bg ProcessEvents (fire-and-forget) run
  alongside the parent CrawlEvent's serial handler chain.

- **Serial handler execution** (default): handlers on the same event run one
  at a time in registration order. This preserves hook ordering — fg hooks
  see config updates from all prior hooks.

- **Queue-jump** (``await bus.emit(...)``): the emitted event and ALL its
  descendants complete synchronously before the await returns. This is how
  config propagation works: hook outputs Binary JSONL → ProcessService emits
  ProcessRecordOutputtedEvent → BinaryService emits BinaryEvent → provider
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
from typing import Any, Callable

from bubus import EventBus

from .events import ArchiveResultEvent, CrawlEvent
from .models import ArchiveResult, Process, Snapshot, write_jsonl
from .models import Hook, Plugin, VisibleRecord, filter_plugins


async def download(
    url: str,
    plugins: dict[str, Plugin],
    output_dir: Path,
    selected_plugins: list[str] | None = None,
    config_overrides: dict[str, Any] | None = None,
    auto_install: bool = True,
    crawl_only: bool = False,
    *,
    emit_jsonl: bool | None = None,
    on_result: Callable[[VisibleRecord], None] | None = None,
) -> list[ArchiveResult]:
    """Download a URL using plugins, coordinated through a bubus EventBus.

    This is the only public function in the orchestrator. It:
    1. Discovers and sorts hooks from selected plugins
    2. Creates the EventBus and wires up all services
    3. Registers a bus handler for enriched ArchiveResultEvents to collect results
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
        emit_jsonl: Whether to print JSONL to stdout. Defaults to True if not a TTY.
        on_result: Callback invoked for each Process and ArchiveResult record.
            Process records come from ProcessService's on_process callback.
            ArchiveResult records come from the ArchiveResultEvent bus handler.

    Returns:
        List of ArchiveResult records produced (collected from enriched
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

    # Shared mutable results list — only ArchiveResults go here (public return value).
    # ArchiveResults flow through ArchiveResultEvent on the bus.
    # Process records flow through the on_process callback.
    results: list[ArchiveResult] = []

    def on_process(proc: Process) -> None:
        """Callback for Process records — passed to ProcessService."""
        if on_result:
            on_result(proc)

    async def _on_archive_result_event(event: ArchiveResultEvent) -> None:
        """Bus handler for ArchiveResultEvent — collects enriched results.

        Only collects events with process_id set (enriched by
        ArchiveResultService on ProcessCompletedEvent). Inline events
        (no process_id) are informational only.
        """
        if not event.process_id:
            return
        ar = ArchiveResult(
            snapshot_id=event.snapshot_id, plugin=event.plugin,
            id=event.id, hook_name=event.hook_name, status=event.status,
            process_id=event.process_id or None,
            output_str=event.output_str, output_files=event.output_files,
            start_ts=event.start_ts or None, end_ts=event.end_ts or None,
            error=event.error or None,
        )
        results.append(ar)
        if on_result:
            on_result(ar)

    # --- Create event bus ---
    timeout = int((config_overrides or {}).get('TIMEOUT', 60))
    # The total crawl timeout is proportional to the number of snapshot hooks
    # (each one may take up to TIMEOUT seconds). Crawl/binary hooks fit in the
    # margin since they rarely approach the full per-hook timeout.
    total_timeout = max(float(len(snapshot_hooks) * timeout), float(timeout))
    bus = EventBus(
        name='AbxDl',
        # parallel event concurrency lets bg ProcessEvents (fire-and-forget
        # children) process concurrently with the parent event's serial handlers
        event_concurrency='parallel',
        # A full plugin run generates hundreds of events; drop old history
        # entries instead of rejecting new events when the buffer fills
        max_history_size=1000,
        max_history_drop=True,
        # Timeouts scale with the number of hooks × per-hook TIMEOUT.
        # Individual hooks set their own timeouts via event_handler_timeout
        # on their ProcessEvent.
        event_timeout=total_timeout,
        event_slow_timeout=total_timeout * 0.8,
        event_handler_slow_timeout=60.0,
    )

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
        stderr_is_tty=stderr_is_tty, on_process=on_process,
    )
    ArchiveResultService(bus, index_path=index_path, emit_jsonl=emit_jsonl)
    # Collect enriched ArchiveResult records from the bus
    bus.on(ArchiveResultEvent, _on_archive_result_event)
    CrawlService(
        bus, url=url, snapshot=snapshot, output_dir=output_dir,
        machine=machine_svc, hooks=crawl_hooks, crawl_only=crawl_only,
    )
    SnapshotService(
        bus, url=url, snapshot=snapshot, output_dir=output_dir,
        machine=machine_svc, hooks=snapshot_hooks,
    )

    # --- Drive the lifecycle ---
    # Emitting CrawlEvent triggers the entire cascade:
    # CrawlSetupEvent → CrawlStartEvent → SnapshotEvent →
    # SnapshotCleanupEvent → SnapshotCompletedEvent → CrawlCleanupEvent →
    # CrawlCompletedEvent. The await returns after the full tree completes.
    try:
        await bus.emit(CrawlEvent(
            url=url, snapshot_id=snapshot.id, output_dir=str(output_dir),
        ))
    finally:
        await bus.stop()

    return results
