"""CrawlService — orchestrates the crawl phase by dispatching plugin hooks."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import (
    CrawlCleanupEvent,
    CrawlCompletedEvent,
    CrawlEvent,
    CrawlStartEvent,
    CrawlSetupEvent,
    ProcessEvent,
    ProcessKillEvent,
    SnapshotEvent,
)
from ..models import Snapshot
from ..models import INSTALL_URL, Hook, Plugin
from .base import BaseService, make_hook_handler
from .machine_service import MachineService


class CrawlService(BaseService):
    """Orchestrates the full crawl lifecycle via a chain of events.

    Lifecycle::

        CrawlEvent                                    # emitted by orchestrator
        │
        ├── CrawlSetupEvent                           # on_Crawl hooks run here
        │   ├── on_Crawl__10_wget_install.finite.bg
        │   ├── on_Crawl__70_chrome_install.finite.bg
        │   ├── on_Crawl__90_chrome_launch.daemon.bg
        │   └── on_Crawl__91_chrome_wait
        │
        ├── CrawlStartEvent                  # triggers snapshot phase
        │   └── SnapshotEvent (full snapshot lifecycle)
        │
        ├── CrawlCleanupEvent                         # SIGTERMs bg crawl daemons
        │   └── ProcessKillEvent × N
        │
        └── CrawlCompletedEvent                       # informational

    on_CrawlEvent drives the chain by emitting each phase event in sequence.
    Hook handlers are registered on CrawlSetupEvent (not CrawlEvent) so they
    only run during the setup phase.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        CrawlEvent, CrawlStartEvent, CrawlCleanupEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        CrawlSetupEvent, CrawlStartEvent, CrawlCleanupEvent,
        CrawlCompletedEvent, ProcessEvent, ProcessKillEvent, SnapshotEvent,
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
        crawl_only: bool = False,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.machine = machine
        self.hooks = hooks
        self.crawl_only = crawl_only or (url == INSTALL_URL)
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register all handlers for the crawl lifecycle.

        Per-hook handlers go on CrawlSetupEvent (not CrawlEvent).
        Lifecycle handlers go on their respective events.
        """
        # Per-hook handlers on CrawlSetupEvent (run during setup phase)
        for plugin, hook in self.hooks:
            handler = make_hook_handler(
                self, plugin, hook,
                url=self.url, snapshot=self.snapshot,
                output_dir=self.output_dir, machine=self.machine,
            )
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self.bus.on(CrawlSetupEvent, handler)

        # Lifecycle chain handlers
        self.bus.on(CrawlEvent, self.on_CrawlEvent)
        self.bus.on(CrawlStartEvent, self.on_CrawlStartEvent)
        self.bus.on(CrawlCleanupEvent, self.on_CrawlCleanupEvent)

    async def on_CrawlEvent(self, event: CrawlEvent) -> None:
        """Drive the full crawl lifecycle by emitting phase events in sequence.

        CrawlSetupEvent → CrawlStartEvent → CrawlCleanupEvent → CrawlCompletedEvent
        """
        event_kwargs = dict(url=self.url, snapshot_id=self.snapshot.id, output_dir=str(self.output_dir))
        await self.bus.emit(CrawlSetupEvent(**event_kwargs))
        await self.bus.emit(CrawlStartEvent(**event_kwargs))
        await self.bus.emit(CrawlCleanupEvent(snapshot_id=self.snapshot.id, output_dir=str(self.output_dir)))
        await self.bus.emit(CrawlCompletedEvent(**event_kwargs))

    async def on_CrawlStartEvent(self, event: CrawlStartEvent) -> None:
        """Start the snapshot phase after crawl setup completes.

        Skipped when ``crawl_only`` is set (e.g. ``abx install`` or explicit flag).
        """
        if self.crawl_only:
            return
        await self.bus.emit(SnapshotEvent(
            url=self.url,
            snapshot_id=self.snapshot.id,
            output_dir=str(self.output_dir),
        ))

    async def on_CrawlCleanupEvent(self, event: CrawlCleanupEvent) -> None:
        """SIGTERM all background crawl daemons so they can flush and exit."""
        for plugin, hook in self.hooks:
            if hook.is_background:
                plugin_output_dir = self.output_dir / plugin.name
                await self.bus.emit(ProcessKillEvent(
                    plugin_name=plugin.name,
                    hook_name=hook.name,
                    output_dir=str(plugin_output_dir),
                ))
