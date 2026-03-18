"""
Event-driven orchestrator for abx-dl using bubus.

Each plugin hook is registered as its own handler on the EventBus, keyed by
CrawlEvent or SnapshotEvent. The bus's default serial handler execution ensures
hooks run in registration order (sorted by step/priority).

Events follow command/completion pairs:
  - ProcessEvent (command) → handler runs subprocess
  - ProcessCompleted (notification) → handlers parse JSONL, emit Binary/Machine

Side-effect cascades (Binary→Machine→config) chain through ``await bus.emit()``
queue-jumps: when a handler awaits an emitted event, bubus processes it and all
its children synchronously before returning.
"""

import sys
from pathlib import Path
from typing import Any, Callable

from bubus import EventBus

from .events import CrawlEvent, SnapshotEvent
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
    known_background_meta_files: set[Path] = set()

    def emit_result(record: VisibleRecord) -> None:
        results.append(record)
        if on_result:
            on_result(record)

    # --- Create event bus and register services ---
    bus = EventBus(name='AbxDl')

    from .services import MachineService, BinaryService, ProcessService, CrawlService, SnapshotService

    machine_svc = MachineService(bus, initial_config=config_overrides)
    BinaryService(
        bus, machine=machine_svc, plugins=plugins, auto_install=auto_install,
        output_dir=output_dir, emit_result=emit_result,
    )
    process_svc = ProcessService(
        bus, index_path=index_path, output_dir=output_dir, emit_jsonl=emit_jsonl,
        stderr_is_tty=stderr_is_tty, emit_result=emit_result,
        known_background_meta_files=known_background_meta_files,
    )
    CrawlService(
        bus, url=url, snapshot=snapshot, output_dir=output_dir,
        machine=machine_svc, hooks=crawl_hooks,
    )
    SnapshotService(
        bus, url=url, snapshot=snapshot, output_dir=output_dir,
        machine=machine_svc, hooks=snapshot_hooks,
    )

    # --- Drive the lifecycle through the bus ---
    try:
        await bus.emit(CrawlEvent(url=url, snapshot_id=snapshot.id, output_dir=str(output_dir)))

        if crawl_only and known_background_meta_files:
            await process_svc.wait_for_background_hooks()
        elif not crawl_only:
            await bus.emit(SnapshotEvent(url=url, snapshot_id=snapshot.id, output_dir=str(output_dir)))

    finally:
        for ar in process_svc.cleanup_background_hooks():
            emit_result(ar)
        await bus.stop()

    return results
