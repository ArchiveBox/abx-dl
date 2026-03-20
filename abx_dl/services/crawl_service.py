"""CrawlService — orchestrates the crawl phase by dispatching plugin hooks."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import CrawlCleanupEvent, CrawlEvent, ProcessEvent, ProcessKillEvent, SnapshotEvent
from ..models import Snapshot
from ..models import INSTALL_URL, Hook, Plugin
from .base import HookRunnerService
from .machine_service import MachineService


class CrawlService(HookRunnerService):
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
        ├── on_CrawlEvent                            # FG: emits SnapshotEvent (blocks)
        │   └── SnapshotEvent (full snapshot phase runs here)
        │
        └── CrawlCleanupEvent                     # FG: SIGTERMs all bg crawl daemons
            ├── ProcessKillEvent (chrome_launch)
            ├── ProcessKillEvent (wget_install)
            └── ...

    See HookRunnerService for the shared fg/bg execution model and config propagation.
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
        self._register_hook_handlers()
        self.bus.on(CrawlCleanupEvent, self.on_CrawlCleanupEvent)

    def _register_hook_handlers(self) -> None:
        """Register crawl hook handlers, then snapshot emission, then cleanup.

        Order: per-hook handlers → on_CrawlEvent (snapshot phase) → cleanup emission.
        """
        for plugin, hook in self.hooks:
            handler = self._make_hook_handler(plugin, hook)
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self.bus.on(CrawlEvent, handler)

        self.bus.on(CrawlEvent, self.on_CrawlEvent)
        self.bus.on(CrawlEvent, self._emit_cleanup)

    async def on_CrawlEvent(self, event: BaseEvent) -> None:
        """Start the snapshot extraction phase after all crawl hooks complete.

        Bg crawl hooks may still be running concurrently. The SnapshotEvent
        is awaited, so the entire snapshot phase completes before this returns.

        Skipped when ``crawl_only`` is set (e.g. ``abx install`` or explicit flag).
        """
        if self.crawl_only:
            return
        await self.bus.emit(SnapshotEvent(
            url=self.url,
            snapshot_id=self.snapshot.id,
            output_dir=str(self.output_dir),
        ))

    async def _emit_cleanup(self, event: BaseEvent) -> None:
        """Emit CrawlCleanupEvent to SIGTERM all bg crawl daemons."""
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
