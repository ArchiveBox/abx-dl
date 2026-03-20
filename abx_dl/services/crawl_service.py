"""CrawlService — orchestrates the crawl phase by dispatching plugin hooks."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import CrawlCleanupEvent, CrawlEvent, ProcessEvent, ProcessKillEvent, SnapshotEvent
from ..models import Snapshot
from ..models import INSTALL_URL, Hook, Plugin
from .base import BaseService, make_hook_handler
from .machine_service import MachineService


class CrawlService(BaseService):
    """Orchestrates the crawl phase: installs, daemons, then snapshot extraction.

    Lifecycle (all within a single CrawlEvent)::

        CrawlEvent                                # emitted by orchestrator.download()
        │
        │  ── Crawl hook handlers run serially (sorted by hook.order) ──
        │
        ├── on_Crawl__10_wget_install.finite.bg   # bg: fire-and-forget ProcessEvent
        ├── on_Crawl__41_trafilatura_install...    # bg: fire-and-forget ProcessEvent
        ├── on_Crawl__70_chrome_install.finite.bg  # bg: fire-and-forget ProcessEvent
        ├── on_Crawl__90_chrome_launch.daemon.bg   # bg: fire-and-forget ProcessEvent
        ├── on_Crawl__91_chrome_wait               # FG: awaits ProcessEvent (blocks)
        │
        │  ── After all hook handlers return ──
        │
        ├── on_CrawlEvent                         # FG: emits SnapshotEvent (blocks)
        │   └── SnapshotEvent (full snapshot phase runs here)
        │
        └── CrawlCleanupEvent                     # FG: SIGTERMs all bg crawl daemons
            ├── ProcessKillEvent (chrome_launch)
            ├── ProcessKillEvent (wget_install)
            └── ...

    Registration order matters — hooks must run before snapshot emission, which
    must run before cleanup. This service registers all handlers explicitly via
    ``bus.on(CrawlEvent, ...)`` to control ordering, overriding BaseService's
    auto-discovery.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [CrawlEvent, CrawlCleanupEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ProcessEvent, ProcessKillEvent, SnapshotEvent, CrawlCleanupEvent]

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
        # Skip the snapshot phase if explicitly requested OR if the URL is the
        # special install sentinel. This lets `abx install` reuse the normal
        # crawl flow (install hooks, binary resolution, config propagation)
        # without triggering any snapshot extraction hooks.
        self.crawl_only = crawl_only or (url == INSTALL_URL)
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register crawl hook handlers, then snapshot emission, then cleanup.

        Order: per-hook handlers → on_CrawlEvent (snapshot) → cleanup emission.
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
            self.bus.on(CrawlEvent, handler)

        self.bus.on(CrawlEvent, self.on_CrawlEvent)
        self.bus.on(CrawlCleanupEvent, self.on_CrawlCleanupEvent)

    async def on_CrawlEvent(self, event: BaseEvent) -> None:
        """Run the snapshot phase (unless crawl_only), then emit cleanup.

        Runs after all per-hook handlers. Bg crawl hooks may still be running
        concurrently. Both SnapshotEvent and CrawlCleanupEvent are awaited,
        so everything completes before this handler returns.
        """
        if not self.crawl_only:
            await self.bus.emit(SnapshotEvent(
                url=self.url,
                snapshot_id=self.snapshot.id,
                output_dir=str(self.output_dir),
            ))
        await self.bus.emit(CrawlCleanupEvent(
            snapshot_id=self.snapshot.id,
            output_dir=str(self.output_dir),
        ))

    async def on_CrawlCleanupEvent(self, event: CrawlCleanupEvent) -> None:
        """SIGTERM all background crawl daemons so they can flush and exit.

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
