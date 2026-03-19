"""
Event-driven orchestrator for abx-dl using bubus.

The orchestrator emits a single CrawlEvent — everything else is driven by
services reacting to events on the bus, forming a proper parent/child tree:

    CrawlEvent
      ├── ProcessEvent (crawl hooks: install, bg daemons)
      ├── SnapshotEvent (emitted by CrawlService as child of CrawlEvent)
      │   ├── ProcessEvent (snapshot hooks)
      │   └── cleanup: kill snapshot bg daemons
      └── cleanup: kill crawl bg daemons
    CrawlCompleted (informational, after everything)

Parallel event concurrency lets bg ProcessEvents (fire-and-forget children)
process concurrently with their parent event, while handler execution stays
serial within each event, preserving hook ordering for foreground hooks.

Side-effect cascades (Binary→Machine→config) chain through ``await bus.emit()``
queue-jumps: when a handler awaits an emitted event, bubus processes it and all
its children synchronously before returning.
"""

import sys
from pathlib import Path
from typing import Any, Callable

from bubus import EventBus

from .events import CrawlEvent, CrawlCompleted
from .models import Snapshot, VisibleRecord, write_jsonl
from .plugins import Hook, Plugin, filter_plugins


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
    """Download a URL using plugins, coordinated through a bubus EventBus."""

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

    # Create snapshot
    snapshot = Snapshot(url=url)
    write_jsonl(index_path, snapshot, also_print=emit_jsonl)

    # Collect and sort hooks
    crawl_hooks: list[tuple[Plugin, Hook]] = []
    snapshot_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in plugins.values():
        for hook in plugin.get_crawl_hooks():
            crawl_hooks.append((plugin, hook))
        for hook in plugin.get_snapshot_hooks():
            snapshot_hooks.append((plugin, hook))
    crawl_hooks.sort(key=lambda x: x[1].sort_key)
    snapshot_hooks.sort(key=lambda x: x[1].sort_key)

    # Shared mutable state
    results: list[VisibleRecord] = []

    def emit_result(record: VisibleRecord) -> None:
        results.append(record)
        if on_result:
            on_result(record)

    # --- Create event bus ---
    # Parallel event concurrency lets bg ProcessEvents (children of the phase
    # event) process concurrently with the phase event's foreground handlers.
    timeout = int((config_overrides or {}).get('TIMEOUT', 60))
    bus = EventBus(
        name='AbxDl',
        event_concurrency='parallel',
        max_history_size=1000,
        max_history_drop=True,
        event_timeout=float(timeout) + 120.0,          # hard timeout for any single event
        event_slow_timeout=float(timeout) + 60.0,      # slow warning threshold
        event_handler_slow_timeout=60.0,                # slow handler warning
    )

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

    # --- Drive the lifecycle through the bus ---
    event_kwargs = dict(url=url, snapshot_id=snapshot.id, output_dir=str(output_dir))
    try:
        # CrawlEvent is the root — CrawlService emits SnapshotEvent as a
        # child after crawl hooks finish (unless crawl_only), then cleans up
        # crawl bg daemons. The whole tree completes before CrawlEvent returns.
        await bus.emit(CrawlEvent(**event_kwargs))
        await bus.emit(CrawlCompleted(**event_kwargs))

    finally:
        await bus.stop()

    return results
