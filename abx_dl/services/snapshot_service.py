"""SnapshotService — orchestrates the snapshot extraction phase."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import ProcessEvent, ProcessKillEvent, SnapshotCleanupEvent, SnapshotEvent
from ..models import Snapshot
from ..models import Hook, Plugin
from .base import BaseService, make_hook_handler
from .machine_service import MachineService


class SnapshotService(BaseService):
    """Orchestrates the snapshot phase: extraction hooks, then daemon cleanup.

    The SnapshotEvent is emitted by CrawlService.on_CrawlEvent as a child of
    CrawlEvent, so the full snapshot phase sits inside the crawl event tree::

        CrawlEvent
        ├── ... (crawl hooks)
        ├── SnapshotEvent                              # emitted by CrawlService
        │   │
        │   │  ── Snapshot hook handlers run serially ──
        │   │
        │   ├── on_Snapshot__06_wget.finite.bg         # bg: fire-and-forget
        │   ├── on_Snapshot__09_chrome_launch.daemon.bg # bg: fire-and-forget
        │   ├── on_Snapshot__10_chrome_tab.daemon.bg   # bg: fire-and-forget
        │   ├── on_Snapshot__11_chrome_wait             # FG: blocks until Chrome tab ready
        │   ├── on_Snapshot__30_chrome_navigate         # FG: navigates to URL
        │   ├── on_Snapshot__54_title                   # FG: extracts page title
        │   ├── on_Snapshot__58_htmltotext              # FG: html → text
        │   ├── ...                                    # more extractors
        │   ├── on_Snapshot__93_hashes                  # FG: compute output hashes
        │   │
        │   │  ── After all hook handlers return ──
        │   │
        │   └── SnapshotCleanupEvent                   # SIGTERMs snapshot bg daemons
        │       ├── ProcessKillEvent (chrome_launch)
        │       ├── ProcessKillEvent (chrome_tab)
        │       └── ...
        │
        └── ... (crawl cleanup)

    Registration order matters — hooks must run before cleanup emission. This
    service registers all handlers explicitly via ``bus.on(SnapshotEvent, ...)``,
    overriding BaseService's auto-discovery.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [SnapshotEvent, SnapshotCleanupEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ProcessEvent, ProcessKillEvent, SnapshotCleanupEvent]

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
        """Register snapshot hook handlers, then cleanup emission, on SnapshotEvent.

        Overrides BaseService auto-discovery to control registration order.
        """
        for plugin, hook in self.hooks:
            handler = make_hook_handler(
                self, plugin, hook,
                url=self.url, snapshot=self.snapshot,
                output_dir=self.output_dir, machine=self.machine,
            )
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self.bus.on(SnapshotEvent, handler)

        self.bus.on(SnapshotEvent, self._emit_cleanup)
        self.bus.on(SnapshotCleanupEvent, self.on_SnapshotCleanupEvent)

    async def _emit_cleanup(self, event: BaseEvent) -> None:
        """Emit SnapshotCleanupEvent to SIGTERM all bg snapshot daemons."""
        await self.bus.emit(SnapshotCleanupEvent(
            snapshot_id=self.snapshot.id,
            output_dir=str(self.output_dir),
        ))

    async def on_SnapshotCleanupEvent(self, event: SnapshotCleanupEvent) -> None:
        """SIGTERM all background snapshot daemons so they can flush and exit.

        Sends ProcessKillEvent for each bg hook. Hooks that already exited
        (finite bg hooks) will have no PID file — the kill is a safe no-op.
        """
        for plugin, hook in self.hooks:
            if hook.is_background:
                plugin_output_dir = self.output_dir / plugin.name
                await self.bus.emit(ProcessKillEvent(
                    plugin_name=plugin.name,
                    hook_name=hook.name,
                    output_dir=str(plugin_output_dir),
                ))
