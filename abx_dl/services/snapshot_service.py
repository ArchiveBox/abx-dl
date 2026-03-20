"""SnapshotService — orchestrates the snapshot extraction phase."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import (
    ProcessEvent,
    ProcessKillEvent,
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
        │   └── SnapshotEvent                              # triggers this service
        │       │
        │       │  ── Snapshot hook handlers run serially ──
        │       │
        │       ├── on_Snapshot__06_wget.finite.bg
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
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        SnapshotEvent, SnapshotCleanupEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        ProcessEvent, ProcessKillEvent, SnapshotCleanupEvent, SnapshotCompletedEvent,
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
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.machine = machine
        self.hooks = hooks
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register snapshot hook handlers and lifecycle handlers.

        Each hook handler is wrapped with a depth guard — abx-dl does not
        support recursive crawling, so SnapshotEvents with depth > 1
        (discovered URLs from hook output) are silently ignored.
        """
        for plugin, hook in self.hooks:
            inner = make_hook_handler(
                self, plugin, hook,
                url=self.url, snapshot=self.snapshot,
                output_dir=self.output_dir, machine=self.machine,
            )

            async def guarded(event: SnapshotEvent, _inner=inner) -> None:
                if event.depth > 1:
                    return
                await _inner(event)

            guarded.__name__ = hook.name
            guarded.__qualname__ = hook.name
            self.bus.on(SnapshotEvent, guarded)

        self.bus.on(SnapshotEvent, self.on_SnapshotEvent)
        self.bus.on(SnapshotCleanupEvent, self.on_SnapshotCleanupEvent)

    async def on_SnapshotEvent(self, event: SnapshotEvent) -> None:
        """Emit cleanup and completion after all snapshot hooks have run.

        Ignores SnapshotEvents with depth > 1 — abx-dl does not support
        recursive crawling. ArchiveBox overrides this to process all depths.
        """
        if event.depth > 1:
            return
        snapshot_kwargs = dict(url=self.url, snapshot_id=self.snapshot.id, output_dir=str(self.output_dir))
        await self.bus.emit(SnapshotCleanupEvent(**snapshot_kwargs))
        await self.bus.emit(SnapshotCompletedEvent(**snapshot_kwargs))

    async def on_SnapshotCleanupEvent(self, event: SnapshotCleanupEvent) -> None:
        """SIGTERM all background snapshot daemons so they can flush and exit."""
        for plugin, hook in self.hooks:
            if hook.is_background:
                plugin_output_dir = self.output_dir / plugin.name
                await self.bus.emit(ProcessKillEvent(
                    plugin_name=plugin.name,
                    hook_name=hook.name,
                    output_dir=str(plugin_output_dir),
                ))
