"""SnapshotService — orchestrates the snapshot extraction phase."""

import asyncio

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

import json

from ..events import (
    ProcessEvent,
    ProcessKillEvent,
    ProcessStdoutEvent,
    SnapshotCleanupEvent,
    SnapshotCompletedEvent,
    SnapshotEvent,
)
from ..models import Snapshot
from ..models import Hook, Plugin
from .base import BaseService, make_hook_handler
from .machine_service import MachineService


class SnapshotService(BaseService):
    """Orchestrates the snapshot phase: extraction hooks, cleanup, completion.

    The SnapshotEvent is emitted by CrawlService.on_CrawlStartEvent::

        CrawlEvent
        ├── CrawlSetupEvent (crawl hooks)
        ├── CrawlStartEvent
        │   └── SnapshotEvent (depth=0)                    # triggers this service
        │       │
        │       │  ── Snapshot hook handlers run serially ──
        │       │  (each wrapped with depth>0 guard)
        │       │
        │       ├── on_Snapshot__06_wget.finite.bg
        │       │   └── ProcessEvent
        │       │       ├── ProcessStdoutEvent
        │       │       │   ├── SnapshotEvent (depth>0, discovered URL — ignored)
        │       │       │   └── ArchiveResultEvent (inline)
        │       │       └── ProcessCompletedEvent
        │       │           └── ArchiveResultEvent (enriched)
        │       ├── on_Snapshot__09_chrome_launch.daemon.bg
        │       ├── on_Snapshot__54_title
        │       ├── on_Snapshot__93_hashes
        │       │
        │       │  ── After all hook handlers ──
        │       │
        │       ├── SnapshotCleanupEvent
        │       │   └── ProcessKillEvent × N
        │       └── SnapshotCompletedEvent
        │
        ├── CrawlCleanupEvent
        └── CrawlCompletedEvent

    Depth guard: SnapshotEvents with depth > 0 are silently ignored by
    abx-dl. These represent discovered URLs from hook output (recursive
    crawling). ArchiveBox handles them by queueing new snapshots.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        ProcessStdoutEvent, SnapshotEvent, SnapshotCleanupEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        SnapshotEvent, ProcessEvent, ProcessKillEvent, SnapshotCleanupEvent, SnapshotCompletedEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        url: str,
        snapshot: Snapshot,
        output_dir: Path,
        machine: MachineService,
        hooks: list[tuple[Plugin, Hook]],
        phase_timeout: float = 300.0,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.machine = machine
        self.hooks = hooks
        self.phase_timeout = phase_timeout
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register handlers in correct order.

        1. ProcessStdoutEvent → SnapshotEvent routing
        2. Per-hook handlers on SnapshotEvent (each wrapped with depth>0 guard)
        3. on_SnapshotEvent last (cleanup + completion)
        """
        self.bus.on(ProcessStdoutEvent, self.on_ProcessStdoutEvent)

        for plugin, hook in self.hooks:
            inner = make_hook_handler(
                self, plugin, hook,
                url=self.url, snapshot=self.snapshot,
                output_dir=self.output_dir, machine=self.machine,
                phase_timeout=self.phase_timeout,
            )

            async def guarded(event: SnapshotEvent, _inner=inner) -> None:
                if event.depth > 0:
                    return
                await _inner(event)

            guarded.__name__ = hook.name
            guarded.__qualname__ = hook.name
            self.bus.on(SnapshotEvent, guarded)

        self.bus.on(SnapshotEvent, self.on_SnapshotEvent)
        self.bus.on(SnapshotCleanupEvent, self.on_SnapshotCleanupEvent)

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Route type=Snapshot records to SnapshotEvent.

        Discovered URLs default to depth=1 since any hook-discovered URL
        is at least one level deep from the root snapshot.
        """
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict) or record.get('type') != 'Snapshot':
            return
        await self.bus.emit(SnapshotEvent(
            url=record.get('url', ''),
            snapshot_id=record.get('id', record.get('snapshot_id', '')),
            output_dir=event.output_dir,
            depth=int(record.get('depth', 1)),
        ))

    async def on_SnapshotEvent(self, event: SnapshotEvent) -> None:
        """Emit cleanup and completion after all snapshot hooks have run.

        Ignores SnapshotEvents with depth > 0 — abx-dl does not support
        recursive crawling. ArchiveBox overrides this to process all depths.
        """
        if event.depth > 0:
            return
        url = self.url
        snapshot_id = self.snapshot.id
        output_dir = str(self.output_dir)
        await self.bus.emit(SnapshotCleanupEvent(url=url, snapshot_id=snapshot_id, output_dir=output_dir, event_timeout=self.phase_timeout))
        await self.bus.emit(SnapshotCompletedEvent(url=url, snapshot_id=snapshot_id, output_dir=output_dir))

    async def on_SnapshotCleanupEvent(self, event: SnapshotCleanupEvent) -> None:
        """SIGTERM all background snapshot daemons so they can flush and exit.

        Each daemon gets its plugin's timeout (PLUGINNAME_TIMEOUT) as the
        grace period before SIGKILL.
        """
        pending_kills = []
        for plugin, hook in self.hooks:
            if hook.is_background:
                env = self.machine.get_env_for_plugin(plugin, run_output_dir=self.output_dir)
                grace = float(env.get(f"{plugin.name.upper()}_TIMEOUT", env.get('TIMEOUT', '60')))
                plugin_output_dir = self.output_dir / plugin.name
                pending_kills.append(self.bus.emit(ProcessKillEvent(
                    plugin_name=plugin.name,
                    hook_name=hook.name,
                    output_dir=str(plugin_output_dir),
                    grace_period=grace,
                    event_timeout=grace + 10.0,
                )))
        if pending_kills:
            await asyncio.gather(*pending_kills)
