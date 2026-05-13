"""CrawlService — orchestrates the install + crawl lifecycle phases."""

import asyncio
from pathlib import Path
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..config import get_plugin_env
from ..events import (
    CrawlAbortEvent,
    CrawlCleanupEvent,
    CrawlCompletedEvent,
    CrawlEvent,
    CrawlStartEvent,
    CrawlSetupEvent,
    ProcessCompletedEvent,
    ProcessEvent,
    ProcessKillEvent,
    ProcessStartedEvent,
    SnapshotEvent,
    slow_warning_timeout,
)
from ..models import Snapshot
from ..models import Hook, Plugin
from .base import BaseService


class CrawlService(BaseService):
    """Orchestrates the crawl lifecycle after the install phase.

    Lifecycle::

        CrawlEvent                                    # emitted by orchestrator
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

    CrawlEvent is the root crawl-lifecycle driver. InstallEvent is handled by
    BinaryService before the crawl phase starts. Crawl setup per-hook handlers
    are registered on CrawlSetupEvent so phase ordering stays explicit.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        CrawlEvent,
        CrawlAbortEvent,
        CrawlSetupEvent,
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
        plugins: dict[str, Plugin],
        crawl_setup_enabled: bool = True,
        crawl_start_enabled: bool = True,
        crawl_cleanup_enabled: bool = True,
        crawl_setup_phase_timeout: float = 300.0,
        snapshot_phase_timeout: float = 300.0,
        snapshot_cleanup_phase_timeout: float = 300.0,
        crawl_cleanup_phase_timeout: float = 300.0,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.crawl_setup_hooks: list[tuple[Plugin, Hook]] = []
        for plugin in plugins.values():
            for hook in plugin.filter_hooks("CrawlSetup"):
                self.crawl_setup_hooks.append((plugin, hook))
        self.crawl_setup_hooks.sort(key=lambda item: item[1].sort_key)
        self.crawl_setup_enabled = crawl_setup_enabled
        self.crawl_start_enabled = crawl_start_enabled
        self.crawl_cleanup_enabled = crawl_cleanup_enabled
        self.crawl_setup_phase_timeout = crawl_setup_phase_timeout
        self.snapshot_phase_timeout = snapshot_phase_timeout
        self.snapshot_cleanup_phase_timeout = snapshot_cleanup_phase_timeout
        self.crawl_cleanup_phase_timeout = crawl_cleanup_phase_timeout
        self.abort_requested = False
        super().__init__(bus)
        self.bus.on(CrawlSetupEvent, self.on_CrawlSetupEvent)
        self.bus.on(CrawlEvent, self.on_CrawlEvent)
        self.bus.on(CrawlAbortEvent, self.on_CrawlAbortEvent)
        self.bus.on(CrawlStartEvent, self.on_CrawlStartEvent)
        self.bus.on(CrawlCleanupEvent, self.on_CrawlCleanupEvent)

    def on_CrawlSetupEvent__for_hook(self, plugin: Plugin, hook: Hook):
        """Create the concrete CrawlSetupEvent handler for one crawl hook."""

        async def on_CrawlSetupEvent__hook(event: CrawlSetupEvent) -> None:
            if event.output_dir != str(self.output_dir):
                return
            if self.abort_requested:
                return
            runtime = get_plugin_env(
                self.bus,
                plugin=plugin,
                run_output_dir=self.output_dir,
                extra_context={
                    "snapshot_id": self.snapshot.id,
                    "snapshot_depth": self.snapshot.depth,
                    "plugin": plugin.name,
                    "hook_name": hook.name,
                },
            )
            env = runtime.to_env()
            timeout_key = f"{plugin.name.upper()}_TIMEOUT"
            timeout = runtime[timeout_key] if timeout_key in plugin.config.properties else runtime.TIMEOUT
            plugin_output_dir = self.output_dir / plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)
            handler_timeout = (
                self.crawl_setup_phase_timeout
                + self.snapshot_phase_timeout
                + self.snapshot_cleanup_phase_timeout
                + self.crawl_cleanup_phase_timeout
                + 30.0
                if hook.is_background
                else timeout + 30.0
            )
            process_event = ProcessEvent(
                plugin_name=plugin.name,
                hook_name=hook.name,
                hook_path=str(hook.path),
                hook_args=[f"--url={self.url}"],
                is_background=hook.is_background,
                output_dir=str(plugin_output_dir),
                env=env,
                timeout=timeout,
                event_timeout=handler_timeout,
                event_handler_timeout=handler_timeout,
                event_handler_slow_timeout=slow_warning_timeout(handler_timeout),
            )
            if hook.is_background:
                background_process = event.emit(process_event)
                background_task = asyncio.create_task(background_process.now())
                background_task.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
                started_process = await self.bus.find(
                    ProcessStartedEvent,
                    child_of=background_process,
                    past=True,
                    future=min(5.0, handler_timeout),
                )
                if started_process is None:
                    raise RuntimeError(f"Background hook {hook.name} did not start")
            else:
                foreground_process = event.emit(process_event)
                await foreground_process.now()
                completed_process = await self.bus.find(
                    ProcessCompletedEvent,
                    child_of=foreground_process,
                    past=True,
                    future=handler_timeout,
                )
                if completed_process is None:
                    raise RuntimeError(f"Foreground hook {hook.name} did not complete")

        handler_name = f"on_CrawlSetupEvent__{plugin.name}__{hook.name.replace('.', '_')}"
        on_CrawlSetupEvent__hook.__name__ = handler_name
        on_CrawlSetupEvent__hook.__qualname__ = handler_name
        return on_CrawlSetupEvent__hook

    async def on_CrawlSetupEvent(self, event: CrawlSetupEvent) -> None:
        """Run crawl setup hooks in hook sort order."""
        if event.output_dir != str(self.output_dir):
            return
        if self.abort_requested:
            return
        for plugin, hook in self.crawl_setup_hooks:
            await self.on_CrawlSetupEvent__for_hook(plugin, hook)(event)

    async def on_CrawlEvent(self, event: CrawlEvent) -> None:
        """Drive the full crawl lifecycle by emitting phase events in sequence.

        CrawlSetupEvent → CrawlStartEvent → CrawlCleanupEvent → CrawlCompletedEvent
        """
        if event.output_dir != str(self.output_dir):
            return
        url = self.url
        snapshot_id = self.snapshot.id
        output_dir = str(self.output_dir)
        if self.crawl_setup_enabled:
            await event.emit(
                CrawlSetupEvent(
                    url=url,
                    snapshot_id=snapshot_id,
                    output_dir=output_dir,
                    event_timeout=self.crawl_setup_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(self.crawl_setup_phase_timeout),
                ),
            ).now()
        if self.abort_requested:
            if self.crawl_cleanup_enabled:
                await event.emit(
                    CrawlCleanupEvent(
                        url=url,
                        snapshot_id=snapshot_id,
                        output_dir=output_dir,
                        event_timeout=self.crawl_cleanup_phase_timeout,
                        event_handler_slow_timeout=slow_warning_timeout(self.crawl_cleanup_phase_timeout),
                    ),
                ).now()
            await event.emit(CrawlCompletedEvent(url=url, snapshot_id=snapshot_id, output_dir=output_dir)).now()
            return
        if self.crawl_start_enabled:
            await event.emit(
                CrawlStartEvent(
                    url=url,
                    snapshot_id=snapshot_id,
                    output_dir=output_dir,
                    event_timeout=self.snapshot_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(self.snapshot_phase_timeout),
                ),
            ).now()
        if self.crawl_cleanup_enabled:
            await event.emit(
                CrawlCleanupEvent(
                    url=url,
                    snapshot_id=snapshot_id,
                    output_dir=output_dir,
                    event_timeout=self.crawl_cleanup_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(self.crawl_cleanup_phase_timeout),
                ),
            ).now()
        await event.emit(CrawlCompletedEvent(url=url, snapshot_id=snapshot_id, output_dir=output_dir)).now()

    async def on_CrawlStartEvent(self, event: CrawlStartEvent) -> None:
        """Start the snapshot phase after crawl setup completes.

        Skipped when snapshot execution is disabled for this run.
        """
        if event.output_dir != str(self.output_dir):
            return
        if not self.crawl_start_enabled:
            return
        if self.abort_requested:
            return
        await event.emit(
            SnapshotEvent(
                url=self.url,
                snapshot_id=self.snapshot.id,
                output_dir=str(self.output_dir),
                depth=0,
                event_timeout=event.event_timeout,
                event_handler_slow_timeout=slow_warning_timeout(event.event_timeout),
            ),
        ).now()

    async def on_CrawlCleanupEvent(self, event: CrawlCleanupEvent) -> None:
        """SIGTERM all background crawl hooks so they can flush and exit.

        Each background hook gets its plugin's timeout (PLUGINNAME_TIMEOUT) as the
        grace period before SIGKILL. Background crawl daemons are resolved from
        the current CrawlEvent ancestry.
        """
        if event.output_dir != str(self.output_dir):
            return
        background_hooks = [(plugin, hook) for plugin, hook in self.crawl_setup_hooks if hook.is_background]
        crawl_event = await self.bus.find(
            CrawlEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_child_of(event, candidate),
        )
        assert crawl_event is not None
        background_hook_keys = {(plugin.name, hook.name) for plugin, hook in background_hooks}
        background_process_events: list[ProcessEvent] = []
        seen_process_event_ids: set[str] = set()
        while True:
            process_event = await self.bus.find(
                ProcessEvent,
                past=True,
                future=False,
                where=lambda candidate: (
                    self.bus.event_is_child_of(candidate, crawl_event)
                    and candidate.is_background
                    and (candidate.plugin_name, candidate.hook_name) in background_hook_keys
                    and candidate.event_id not in seen_process_event_ids
                ),
            )
            if process_event is None:
                break
            assert isinstance(process_event, ProcessEvent)
            seen_process_event_ids.add(process_event.event_id)
            background_process_events.append(process_event)
        grace_by_hook: dict[tuple[str, str], int] = {}
        for plugin, hook in background_hooks:
            plugin_config = get_plugin_env(
                self.bus,
                plugin=plugin,
                run_output_dir=self.output_dir,
            )
            timeout_key = f"{plugin.name.upper()}_TIMEOUT"
            grace_by_hook[(plugin.name, hook.name)] = (
                plugin_config[timeout_key] if timeout_key in plugin.config.properties else plugin_config.TIMEOUT
            )
        started_processes: list[ProcessStartedEvent] = []
        for process_event in background_process_events:
            completed_process = await self.bus.find(
                ProcessCompletedEvent,
                child_of=process_event,
                past=True,
                future=False,
            )
            if completed_process is not None:
                continue
            started_process = await self.bus.find(
                ProcessStartedEvent,
                child_of=process_event,
                past=True,
                future=min(5.0, event.event_timeout or 5.0),
            )
            if started_process is None:
                continue
            assert isinstance(started_process, ProcessStartedEvent)
            started_processes.append(started_process)
        pending_kills = [
            event.emit(
                ProcessKillEvent(
                    plugin_name=started_process.plugin_name,
                    hook_name=started_process.hook_name,
                    pid=started_process.pid,
                    grace_period=grace_by_hook[(started_process.plugin_name, started_process.hook_name)],
                    event_timeout=grace_by_hook[(started_process.plugin_name, started_process.hook_name)] + 10.0,
                ),
            )
            for started_process in started_processes
        ]

        # await the killing of any bg hooks that are still running
        if pending_kills:
            await asyncio.gather(*pending_kills)

        # await the final handling of any ProcessCompletedEvent listeners
        if started_processes:
            await asyncio.gather(
                *[
                    self.bus.find(
                        ProcessCompletedEvent,
                        child_of=started_process,
                        past=True,
                        future=event.event_timeout,
                    )
                    for started_process in started_processes
                ],
            )

    async def on_CrawlAbortEvent(self, event: CrawlAbortEvent) -> None:
        """Stop scheduling any further crawl work after a user abort."""
        self.abort_requested = True
