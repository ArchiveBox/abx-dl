"""CrawlService — orchestrates the crawl phase by dispatching plugin hooks."""

import asyncio

from pathlib import Path
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import (
    CrawlCleanupEvent,
    CrawlCompletedEvent,
    CrawlEvent,
    CrawlStartEvent,
    CrawlSetupEvent,
    ProcessEvent,
    ProcessKillEvent,
    SnapshotEvent,
    slow_warning_timeout,
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
        CrawlEvent,
        CrawlStartEvent,
        CrawlCleanupEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        CrawlSetupEvent,
        CrawlStartEvent,
        CrawlCleanupEvent,
        CrawlCompletedEvent,
        ProcessEvent,
        ProcessKillEvent,
        SnapshotEvent,
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
        phase_timeout: float = 300.0,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.machine = machine
        self.hooks = hooks
        self.crawl_only = crawl_only or (url == INSTALL_URL)
        self.phase_timeout = phase_timeout
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register all handlers for the crawl lifecycle.

        Per-hook handlers go on CrawlSetupEvent (not CrawlEvent).
        Lifecycle handlers go on their respective events.
        """
        # Per-hook handlers on CrawlSetupEvent (run during setup phase)
        for plugin, hook in self.hooks:
            handler = make_hook_handler(
                self,
                plugin,
                hook,
                url=self.url,
                snapshot=self.snapshot,
                output_dir=self.output_dir,
                machine=self.machine,
                phase_timeout=self.phase_timeout,
            )
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self._register(CrawlSetupEvent, handler)

        # Lifecycle cleanup handler
        self._register(CrawlCleanupEvent, self.on_CrawlCleanupEvent)

    async def on_CrawlEvent(self, event: CrawlEvent) -> None:
        """Drive the full crawl lifecycle by emitting phase events in sequence.

        CrawlSetupEvent → CrawlStartEvent → CrawlCleanupEvent → CrawlCompletedEvent
        """
        if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
            return
        url = self.url
        snapshot_id = self.snapshot.id
        output_dir = str(self.output_dir)
        await self.bus.emit(
            CrawlSetupEvent(
                url=url,
                snapshot_id=snapshot_id,
                output_dir=output_dir,
                event_timeout=self.phase_timeout,
                event_handler_slow_timeout=slow_warning_timeout(self.phase_timeout),
            ),
        )
        await self.bus.emit(
            CrawlStartEvent(
                url=url,
                snapshot_id=snapshot_id,
                output_dir=output_dir,
                event_timeout=event.event_timeout,
                event_handler_slow_timeout=slow_warning_timeout(event.event_timeout),
            ),
        )
        await self.bus.emit(
            CrawlCleanupEvent(
                url=url,
                snapshot_id=snapshot_id,
                output_dir=output_dir,
                event_timeout=self.phase_timeout,
                event_handler_slow_timeout=slow_warning_timeout(self.phase_timeout),
            ),
        )
        await self.bus.emit(CrawlCompletedEvent(url=url, snapshot_id=snapshot_id, output_dir=output_dir))

    async def on_CrawlStartEvent(self, event: CrawlStartEvent) -> None:
        """Start the snapshot phase after crawl setup completes.

        Skipped when ``crawl_only`` is set (e.g. ``abx install`` or explicit flag).
        """
        if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
            return
        if self.crawl_only:
            return
        await self.bus.emit(
            SnapshotEvent(
                url=self.url,
                snapshot_id=self.snapshot.id,
                output_dir=str(self.output_dir),
                depth=0,
                event_timeout=event.event_timeout,
                event_handler_slow_timeout=slow_warning_timeout(event.event_timeout),
            ),
        )

    async def on_CrawlCleanupEvent(self, event: CrawlCleanupEvent) -> None:
        """SIGTERM all background crawl daemons so they can flush and exit.

        Each daemon gets its plugin's timeout (PLUGINNAME_TIMEOUT) as the
        grace period before SIGKILL.
        """
        if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
            return
        pending_kills = []
        for plugin, hook in self.hooks:
            if hook.is_background:
                env = self.machine.get_env_for_plugin(plugin, run_output_dir=self.output_dir)
                grace = float(env.get(f"{plugin.name.upper()}_TIMEOUT", env.get("TIMEOUT", "60")))
                plugin_output_dir = self.output_dir / plugin.name
                pending_kills.append(
                    self.bus.emit(
                        ProcessKillEvent(
                            plugin_name=plugin.name,
                            hook_name=hook.name,
                            output_dir=str(plugin_output_dir),
                            grace_period=grace,
                            event_timeout=grace + 10.0,
                        ),
                    ),
                )
        if pending_kills:
            await asyncio.gather(*pending_kills)
