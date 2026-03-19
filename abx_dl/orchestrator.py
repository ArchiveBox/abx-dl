"""
Event-driven orchestrator for abx-dl.

This is the main entry point for downloading a URL. It creates a bubus EventBus,
wires up all services, and emits a single CrawlEvent to kick off the entire
lifecycle. Everything else is driven by services reacting to events.

Full event tree for a typical run::

    CrawlEvent                              # emitted here by download()
    │
    │  ── Crawl hooks (sorted by order) ──
    │
    ├── ProcessEvent  (bg: wget_install)          ╮
    ├── ProcessEvent  (bg: trafilatura_install)    │ bg hooks fire concurrently
    ├── ProcessEvent  (bg: chrome_install)         │ as children of CrawlEvent
    ├── ProcessEvent  (bg: chrome_launch daemon)   ╯
    ├── ProcessEvent  (FG: chrome_wait)       ← blocks until Chrome is ready
    │   │
    │   │  (side-effect chain from chrome_install, happening concurrently):
    │   │  ProcessEvent → BinaryEvent → ProcessEvent (puppeteer install)
    │   │      → BinaryEvent(abspath=...) → MachineEvent(CHROME_BINARY=...)
    │   │
    ├── SnapshotEvent                         ← emitted by CrawlService
    │   ├── ProcessEvent  (bg: wget download)
    │   ├── ProcessEvent  (bg: chrome_launch)
    │   ├── ProcessEvent  (bg: chrome_tab)
    │   ├── ProcessEvent  (FG: chrome_wait)   ← blocks until tab ready
    │   ├── ProcessEvent  (FG: chrome_navigate)
    │   ├── ProcessEvent  (FG: title)
    │   ├── ProcessEvent  (FG: htmltotext)
    │   ├── ProcessEvent  (FG: trafilatura)
    │   ├── ProcessEvent  (FG: hashes)
    │   └── ProcessKillEvent × N              ← snapshot bg cleanup
    │
    ├── ProcessKillEvent × N                  ← crawl bg cleanup
    │
    CrawlCompleted                            ← informational, after everything

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
  BinaryEvent → provider hooks install it → MachineEvent updates config →
  all before the next stdout line is read.

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

from .events import CrawlEvent, CrawlCompleted
from .models import ArchiveResult, Snapshot, VisibleRecord, write_jsonl
from .models import INSTALL_URL, Hook, Plugin, filter_plugins


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
) -> list[VisibleRecord]:
    """Download a URL using plugins, coordinated through a bubus EventBus.

    This is the only public function in the orchestrator. It:
    1. Discovers and sorts hooks from selected plugins
    2. Creates the EventBus and wires up all services
    3. Emits CrawlEvent (which cascades through the entire lifecycle)
    4. Returns all ArchiveResult/Process/Snapshot records produced

    Args:
        url: The URL to download/archive.
        plugins: All discovered plugins (from discover_plugins()).
        output_dir: Where to write output files and index.jsonl.
        selected_plugins: If set, only use these plugins (with dependency resolution).
        config_overrides: Extra config values (e.g. TIMEOUT) merged into shared_config.
        auto_install: Whether to auto-install missing binaries.
        crawl_only: If True, skip the snapshot phase (only run on_Crawl hooks).
        emit_jsonl: Whether to print JSONL to stdout. Defaults to True if not a TTY.
        on_result: Callback invoked for each ArchiveResult as it's produced.

    Returns:
        List of all VisibleRecord objects (Snapshot, Process, ArchiveResult) produced.
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
    # The on_result callback receives both Process and ArchiveResult for TUI rendering.
    results: list[VisibleRecord] = []

    def emit_result(record: VisibleRecord) -> None:
        if isinstance(record, ArchiveResult):
            results.append(record)
        if on_result:
            on_result(record)

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
    from .services import MachineService, BinaryService, ProcessService, CrawlService, SnapshotService

    machine_svc = MachineService(bus, initial_config=config_overrides)
    BinaryService(
        bus, machine=machine_svc, plugins=plugins, auto_install=auto_install,
        output_dir=output_dir, emit_result=emit_result,
    )
    ProcessService(
        bus, index_path=index_path, output_dir=output_dir, emit_jsonl=emit_jsonl,
        stderr_is_tty=stderr_is_tty, emit_result=emit_result,
    )
    CrawlService(
        bus, url=url, snapshot=snapshot, output_dir=output_dir,
        machine=machine_svc, hooks=crawl_hooks, crawl_only=crawl_only,
    )
    SnapshotService(
        bus, url=url, snapshot=snapshot, output_dir=output_dir,
        machine=machine_svc, hooks=snapshot_hooks,
    )

    # --- Drive the lifecycle ---
    # Emitting CrawlEvent triggers the entire cascade: crawl hooks → snapshot
    # hooks → cleanup. The await returns only after the full tree completes.
    event_kwargs = dict(url=url, snapshot_id=snapshot.id, output_dir=str(output_dir))
    try:
        await bus.emit(CrawlEvent(**event_kwargs))
        # CrawlCompleted is informational — no handlers react to it, but it
        # appears in the event tree and can be used for logging/monitoring.
        await bus.emit(CrawlCompleted(**event_kwargs))

    finally:
        await bus.stop()

    return results
