"""CrawlService — orchestrates the install + crawl lifecycle phases."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..config import get_plugin_config, get_required_binary_requests
from ..events import (
    BinaryRequestEvent,
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
from ..models import Snapshot, uuid7
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
        ├── InstallEvent                              # emits required_binaries
        │   ├── BinaryRequestEvent
        │   ├── BinaryRequestEvent
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
    Crawl setup per-hook handlers are registered on CrawlSetupEvent so phase
    ordering stays explicit.
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
        install_plugins: list[Plugin],
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
        self.install_plugins = install_plugins
        self.crawl_setup_hooks = crawl_setup_hooks
        self.crawl_only = crawl_only
        self.install_phase_timeout = install_phase_timeout
        self.crawl_setup_phase_timeout = crawl_setup_phase_timeout
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register all handlers for the crawl lifecycle.

        Crawl setup per-hook handlers go on CrawlSetupEvent.
        Lifecycle handlers go on the root/phase events they advance.
        """
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

    def _plugin_enabled(self, plugin: Plugin) -> bool:
        plugin_config = get_plugin_config(
            plugin.name,
            plugin.config_schema,
            config_path=plugin.path / "config.json",
            base_config=self.machine.declared_config,
        )
        return bool(plugin_config.get(plugin.enabled_key, True))

    async def on_InstallEvent(self, event: InstallEvent) -> None:
        if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
            return
        install_cache = self.machine.derived_config.get("ABX_INSTALL_CACHE")
        if not isinstance(install_cache, dict):
            install_cache = {}
        now = datetime.now(timezone.utc)
        pruned_install_cache: dict[str, str] = {}
        for binary_name, cached_at in install_cache.items():
            try:
                cache_time = datetime.fromisoformat(str(cached_at))
            except ValueError:
                continue
            if cache_time.tzinfo is None:
                cache_time = cache_time.replace(tzinfo=timezone.utc)
            if now - cache_time < timedelta(hours=24):
                pruned_install_cache[str(binary_name)] = cache_time.isoformat()
        if pruned_install_cache != install_cache:
            self.machine.update_derived(ABX_INSTALL_CACHE=pruned_install_cache)
        seen: set[str] = set()
        for plugin in self.install_plugins:
            if not self._plugin_enabled(plugin):
                continue
            plugin_output_dir = self.output_dir / plugin.name
            for record in get_required_binary_requests(
                plugin.name,
                plugin.config_schema,
                plugin.binaries,
                overrides=self.machine.declared_config,
                config_path=plugin.path / "config.json",
                run_output_dir=self.output_dir,
            ):
                signature = json.dumps(record, sort_keys=True, default=str)
                if signature in seen:
                    continue
                seen.add(signature)
                binary_name = str(record.get("name") or "").strip()
                if binary_name and binary_name in pruned_install_cache:
                    continue
                binary_id = str(record.pop("binary_id", "") or uuid7())
                await self.bus.emit(
                    BinaryRequestEvent(
                        plugin_name=plugin.name,
                        output_dir=str(plugin_output_dir),
                        binary_id=binary_id,
                        **record,
                    ),
                )

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
