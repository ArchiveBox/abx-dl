"""CrawlService — orchestrates the install + crawl lifecycle phases."""

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import (
    CrawlCleanupEvent,
    CrawlCompletedEvent,
    CrawlEvent,
    CrawlStartEvent,
    CrawlSetupEvent,
    InstallEvent,
    ProcessEvent,
    ProcessKillEvent,
    SnapshotEvent,
    slow_warning_timeout,
)
from ..models import Snapshot
from ..models import Hook, Plugin
from .base import BaseService, make_hook_handler
from .machine_service import MachineService

if TYPE_CHECKING:
    from .process_service import ProcessService


class CrawlService(BaseService):
    """Orchestrates the pre-run install phase and the crawl lifecycle.

    Lifecycle::

        CrawlEvent                                    # emitted by orchestrator
        │
        ├── InstallEvent                              # on_Install hooks run here
        │   ├── on_Install__10_wget.finite.bg
        │   ├── on_Install__70_chrome.finite.bg
        │   └── ...
        │
        ├── CrawlSetupEvent                           # on_CrawlSetup hooks run here
        │   ├── on_CrawlSetup__90_chrome_launch.daemon.bg
        │   └── on_CrawlSetup__91_chrome_wait
        │
        ├── CrawlStartEvent                  # triggers snapshot phase
        │   └── SnapshotEvent (full snapshot lifecycle)
        │
        ├── CrawlCleanupEvent                         # SIGTERMs bg crawl daemons
        │   └── ProcessKillEvent × N
        │
        └── CrawlCompletedEvent                       # informational

    InstallEvent and CrawlEvent are the two root phase drivers.
    Per-hook handlers are registered on InstallEvent / CrawlSetupEvent rather
    than the roots themselves so phase ordering stays explicit.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        InstallEvent,
        CrawlEvent,
        CrawlStartEvent,
        CrawlCleanupEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        InstallEvent,
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
        process: "ProcessService",
        install_hooks: list[tuple[Plugin, Hook]],
        crawl_setup_hooks: list[tuple[Plugin, Hook]],
        crawl_only: bool = False,
        install_phase_timeout: float = 300.0,
        crawl_setup_phase_timeout: float = 300.0,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.machine = machine
        self.process = process
        self.install_hooks = install_hooks
        self.crawl_setup_hooks = crawl_setup_hooks
        self.crawl_only = crawl_only
        self.install_phase_timeout = install_phase_timeout
        self.crawl_setup_phase_timeout = crawl_setup_phase_timeout
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register all handlers for the crawl lifecycle.

        Per-hook handlers go on InstallEvent / CrawlSetupEvent.
        Lifecycle handlers go on the root/phase events they advance.
        """
        for plugin, hook in self.install_hooks:
            handler = make_hook_handler(
                self,
                plugin,
                hook,
                url=self.url,
                snapshot=self.snapshot,
                output_dir=self.output_dir,
                machine=self.machine,
                phase_timeout=self.install_phase_timeout,
            )
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self._register(InstallEvent, handler)

        for plugin, hook in self.crawl_setup_hooks:
            handler = make_hook_handler(
                self,
                plugin,
                hook,
                url=self.url,
                snapshot=self.snapshot,
                output_dir=self.output_dir,
                machine=self.machine,
                phase_timeout=self.crawl_setup_phase_timeout,
            )
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self._register(CrawlSetupEvent, handler)

        # Lifecycle cleanup handler
        self._register(InstallEvent, self.on_InstallEvent)
        self._register(CrawlEvent, self.on_CrawlEvent)
        self._register(CrawlStartEvent, self.on_CrawlStartEvent)
        self._register(CrawlCleanupEvent, self.on_CrawlCleanupEvent)

    async def on_InstallEvent(self, event: InstallEvent) -> None:
        if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
            return

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
                event_timeout=self.crawl_setup_phase_timeout,
                event_handler_slow_timeout=slow_warning_timeout(self.crawl_setup_phase_timeout),
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
                event_timeout=self.crawl_setup_phase_timeout,
                event_handler_slow_timeout=slow_warning_timeout(self.crawl_setup_phase_timeout),
            ),
        )
        await self.bus.emit(CrawlCompletedEvent(url=url, snapshot_id=snapshot_id, output_dir=output_dir))

    async def on_CrawlStartEvent(self, event: CrawlStartEvent) -> None:
        """Start the snapshot phase after crawl setup completes.

        Skipped when ``crawl_only`` is set, meaning the caller only wants the
        pre-snapshot phases (InstallEvent + CrawlSetupEvent).
        """
        if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
            return
        if self.crawl_only:
            return
        if self.machine.shared_config.get("DRY_RUN"):
            self.process.suspend_process_events()
        try:
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
        finally:
            if self.machine.shared_config.get("DRY_RUN"):
                self.process.resume_process_events()

    async def on_CrawlCleanupEvent(self, event: CrawlCleanupEvent) -> None:
        """SIGTERM all background crawl daemons so they can flush and exit.

        Each daemon gets its plugin's timeout (PLUGINNAME_TIMEOUT) as the
        grace period before SIGKILL.
        """
        if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
            return
        pending_kills = []
        for plugin, hook in self.crawl_setup_hooks:
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
