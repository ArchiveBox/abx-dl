"""CrawlService — orchestrates the install + crawl lifecycle phases."""

import asyncio
from inspect import isawaitable
from pathlib import Path
from typing import ClassVar
from collections.abc import Awaitable, Callable

from abxbus import BaseEvent, EventBus
from abxpkg import BinProvider

from ..config import get_config, get_plugin_env
from ..events import (
    BinaryEvent,
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
    SnapshotCompletedEvent,
    SnapshotEvent,
    slow_warning_timeout,
)
from ..models import Snapshot
from ..models import Hook, Plugin
from .base import BaseService, plugin_with_required_plugin_names


async def _wait_for_process_completed(event: ProcessCompletedEvent | None, timeout: float | None) -> ProcessCompletedEvent | None:
    if event is None:
        return None
    await event.wait(timeout=timeout)
    await event.event_results_list()
    return event


async def _run_event_now(event: BaseEvent, timeout: float | None = None) -> BaseEvent:
    await event.now(timeout=timeout)
    await event.wait(timeout=timeout)
    await event.event_results_list()
    return event


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
        crawl_completed_enabled: bool = True,
        crawl_event_enabled: bool = True,
        crawl_setup_phase_timeout: float = 300.0,
        snapshot_phase_timeout: float = 300.0,
        snapshot_cleanup_phase_timeout: float = 300.0,
        crawl_cleanup_phase_timeout: float = 300.0,
        abort_requested: Callable[[], bool | Awaitable[bool]] | None = None,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.plugins = plugins
        self.crawl_setup_hooks: list[tuple[Plugin, Hook]] = []
        for plugin in plugins.values():
            for hook in plugin.filter_hooks("CrawlSetup"):
                self.crawl_setup_hooks.append((plugin, hook))
        self.crawl_setup_hooks.sort(key=lambda item: item[1].sort_key)
        self.crawl_setup_enabled = crawl_setup_enabled
        self.crawl_start_enabled = crawl_start_enabled
        self.crawl_cleanup_enabled = crawl_cleanup_enabled
        self.crawl_completed_enabled = crawl_completed_enabled
        self.crawl_event_enabled = crawl_event_enabled
        self.crawl_setup_phase_timeout = crawl_setup_phase_timeout
        self.snapshot_phase_timeout = snapshot_phase_timeout
        self.snapshot_cleanup_phase_timeout = snapshot_cleanup_phase_timeout
        self.crawl_cleanup_phase_timeout = crawl_cleanup_phase_timeout
        self.abort_requested = False
        self.abort_requested_callback = abort_requested
        self._active_crawl_event_ids: set[str] = set()
        self._completed_crawl_event_ids: set[str] = set()
        super().__init__(bus)
        self.bus.on(CrawlSetupEvent, self.on_CrawlSetupEvent)
        if self.crawl_event_enabled:
            self.bus.on(CrawlEvent, self.on_CrawlEvent)
        self.bus.on(CrawlAbortEvent, self.on_CrawlAbortEvent)
        self.bus.on(CrawlStartEvent, self.on_CrawlStartEvent)
        self.bus.on(CrawlCleanupEvent, self.on_CrawlCleanupEvent)

    async def should_abort(self) -> bool:
        if self.abort_requested:
            return True
        if self.abort_requested_callback is None:
            return False
        callback_result = self.abort_requested_callback()
        if isawaitable(callback_result):
            callback_result = await callback_result
        if bool(callback_result):
            self.abort_requested = True
            return True
        return False

    def on_CrawlSetupEvent__for_hook(self, plugin: Plugin, hook: Hook):
        """Create the concrete CrawlSetupEvent handler for one crawl hook."""

        async def on_CrawlSetupEvent__hook(event: CrawlSetupEvent) -> None:
            if event.output_dir != str(self.output_dir):
                return
            if await self.should_abort():
                return
            config = await get_config(self.bus)
            runtime = await get_plugin_env(
                self.bus,
                plugin=plugin,
                run_output_dir=self.output_dir,
                config=config,
                extra_context={
                    "snapshot_id": self.snapshot.id,
                    "snapshot_depth": self.snapshot.depth,
                    "plugin": plugin.name,
                    "hook_name": hook.name,
                },
            )
            env = runtime.to_env()
            env_plugin_names = set(plugin_with_required_plugin_names(plugin, self.plugins))
            binary_events = await self.bus.filter(
                BinaryEvent,
                past=True,
                where=lambda candidate: candidate.plugin_name in env_plugin_names,
            )
            for binary_event in reversed(binary_events):
                if binary_event.env:
                    env = BinProvider.build_exec_env(
                        base_env=env,
                        extra_env=binary_event.env,
                    )
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
                event_blocks_parent_completion=not hook.is_background,
                event_timeout=0 if hook.is_background else handler_timeout,
                event_handler_timeout=0 if hook.is_background else handler_timeout,
                event_handler_slow_timeout=slow_warning_timeout(handler_timeout),
            )
            if hook.is_background:
                background_process = event.emit(process_event)
                started_process = await self.bus.find(
                    ProcessStartedEvent,
                    child_of=background_process,
                    past=True,
                    future=min(5.0, handler_timeout),
                )
                if await self.should_abort():
                    return
                if started_process is None:
                    raise RuntimeError(f"Background hook {hook.name} did not start")
            else:
                foreground_process = event.emit(process_event)
                await _run_event_now(foreground_process, handler_timeout)
                completed_process = await self.bus.find(
                    ProcessCompletedEvent,
                    child_of=foreground_process,
                    past=True,
                    future=handler_timeout,
                )
                if completed_process is None:
                    raise RuntimeError(f"Foreground hook {hook.name} did not complete")
                await _wait_for_process_completed(completed_process, handler_timeout)
                if await self.should_abort():
                    return

        handler_name = f"on_CrawlSetupEvent__{plugin.name}__{hook.name.replace('.', '_')}"
        on_CrawlSetupEvent__hook.__name__ = handler_name
        on_CrawlSetupEvent__hook.__qualname__ = handler_name
        return on_CrawlSetupEvent__hook

    async def on_CrawlSetupEvent(self, event: CrawlSetupEvent) -> None:
        """Run crawl setup hooks in hook sort order."""
        if event.output_dir != str(self.output_dir):
            return
        if await self.should_abort():
            return
        for plugin, hook in self.crawl_setup_hooks:
            await self.on_CrawlSetupEvent__for_hook(plugin, hook)(event)
            if await self.should_abort():
                return

    async def on_CrawlEvent(self, event: CrawlEvent) -> None:
        """Drive the full crawl lifecycle by emitting phase events in sequence.

        CrawlSetupEvent → CrawlStartEvent → CrawlCleanupEvent → CrawlCompletedEvent
        """
        if event.output_dir != str(self.output_dir):
            return
        if event.event_id in self._active_crawl_event_ids or event.event_id in self._completed_crawl_event_ids:
            return
        self._active_crawl_event_ids.add(event.event_id)
        try:
            await self._run_root_crawl_event(event)
        finally:
            self._active_crawl_event_ids.discard(event.event_id)
            self._completed_crawl_event_ids.add(event.event_id)

    async def _run_root_crawl_event(self, event: CrawlEvent) -> None:
        url = self.url
        snapshot_id = self.snapshot.id
        output_dir = str(self.output_dir)
        if self.crawl_setup_enabled:
            await _run_event_now(
                event.emit(
                    CrawlSetupEvent(
                        url=url,
                        snapshot_id=snapshot_id,
                        output_dir=output_dir,
                        event_timeout=self.crawl_setup_phase_timeout,
                        event_handler_slow_timeout=slow_warning_timeout(self.crawl_setup_phase_timeout),
                    ),
                ),
                self.crawl_setup_phase_timeout,
            )
        if await self.should_abort():
            if self.crawl_cleanup_enabled:
                await _run_event_now(
                    event.emit(
                        CrawlCleanupEvent(
                            url=url,
                            snapshot_id=snapshot_id,
                            output_dir=output_dir,
                            event_timeout=self.crawl_cleanup_phase_timeout,
                            event_handler_slow_timeout=slow_warning_timeout(self.crawl_cleanup_phase_timeout),
                        ),
                    ),
                    self.crawl_cleanup_phase_timeout,
                )
                return
            if self.crawl_completed_enabled:
                await _run_event_now(
                    event.emit(CrawlCompletedEvent(url=url, snapshot_id=snapshot_id, output_dir=output_dir)),
                    CrawlCompletedEvent.model_fields["event_timeout"].default,
                )
            return
        if self.crawl_start_enabled:
            await _run_event_now(
                event.emit(
                    CrawlStartEvent(
                        url=url,
                        snapshot_id=snapshot_id,
                        output_dir=output_dir,
                        event_timeout=self.snapshot_phase_timeout,
                        event_handler_slow_timeout=slow_warning_timeout(self.snapshot_phase_timeout),
                    ),
                ),
                self.snapshot_phase_timeout,
            )
        if self.crawl_cleanup_enabled:
            await _run_event_now(
                event.emit(
                    CrawlCleanupEvent(
                        url=url,
                        snapshot_id=snapshot_id,
                        output_dir=output_dir,
                        event_timeout=self.crawl_cleanup_phase_timeout,
                        event_handler_slow_timeout=slow_warning_timeout(self.crawl_cleanup_phase_timeout),
                    ),
                ),
                self.crawl_cleanup_phase_timeout,
            )
            return
        if self.crawl_completed_enabled:
            await _run_event_now(
                event.emit(CrawlCompletedEvent(url=url, snapshot_id=snapshot_id, output_dir=output_dir)),
                CrawlCompletedEvent.model_fields["event_timeout"].default,
            )

    async def on_CrawlStartEvent(self, event: CrawlStartEvent) -> None:
        """Start the snapshot phase after crawl setup completes.

        Skipped when snapshot execution is disabled for this run.
        """
        if event.output_dir != str(self.output_dir):
            return
        if not self.crawl_start_enabled:
            return
        if await self.should_abort():
            return
        snapshot_event = event.emit(
            SnapshotEvent(
                url=self.url,
                snapshot_id=self.snapshot.id,
                output_dir=str(self.output_dir),
                depth=0,
                event_timeout=event.event_timeout,
                event_handler_slow_timeout=slow_warning_timeout(event.event_timeout),
            ),
        )
        await _run_event_now(snapshot_event, event.event_timeout)
        completed_snapshot = await self.bus.find(
            SnapshotCompletedEvent,
            child_of=snapshot_event,
            past=True,
            future=event.event_timeout,
        )
        if completed_snapshot is None:
            raise RuntimeError(f"Snapshot {self.snapshot.id} did not complete")

    async def on_CrawlCleanupEvent(self, event: CrawlCleanupEvent) -> None:
        """SIGTERM any crawl setup hooks that should still be running."""
        if event.output_dir != str(self.output_dir):
            return
        aborting = await self.should_abort()
        setup_hook_keys = {(plugin.name, hook.name) for plugin, hook in self.crawl_setup_hooks}
        crawl_event = await self.bus.find(
            CrawlEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_child_of(event, candidate),
        )
        crawl_setup_event = await self.bus.find(
            CrawlSetupEvent,
            past=True,
            future=False,
            where=lambda candidate: (
                candidate.output_dir == event.output_dir
                and candidate.snapshot_id == event.snapshot_id
                and (
                    self.bus.event_is_parent_of(candidate, event)
                    or (crawl_event is not None and self.bus.event_is_child_of(candidate, crawl_event))
                )
            ),
        )
        setup_process_events = await self.bus.filter(
            ProcessEvent,
            past=True,
            future=False,
            where=lambda candidate: (
                (
                    (crawl_event is not None and self.bus.event_is_child_of(candidate, crawl_event))
                    or (crawl_setup_event is not None and self.bus.event_is_child_of(candidate, crawl_setup_event))
                )
                and (candidate.plugin_name, candidate.hook_name) in setup_hook_keys
            ),
        )
        grace_by_hook: dict[tuple[str, str], int] = {}
        config = await get_config(self.bus)
        for plugin, hook in self.crawl_setup_hooks:
            plugin_config = await get_plugin_env(
                self.bus,
                plugin=plugin,
                run_output_dir=self.output_dir,
                config=config,
            )
            timeout_key = f"{plugin.name.upper()}_TIMEOUT"
            grace_by_hook[(plugin.name, hook.name)] = (
                plugin_config[timeout_key] if timeout_key in plugin.config.properties else plugin_config.TIMEOUT
            )
        started_processes: list[tuple[ProcessEvent, ProcessStartedEvent]] = []
        for process_event in setup_process_events:
            started_process = await self.bus.find(
                ProcessStartedEvent,
                child_of=process_event,
                past=True,
                future=False,
            )
            if started_process is None:
                continue
            assert isinstance(started_process, ProcessStartedEvent)
            completed_process = await self.bus.find(
                ProcessCompletedEvent,
                child_of=process_event,
                past=True,
                future=False,
            )
            if completed_process is not None:
                await _wait_for_process_completed(completed_process, event.event_timeout)
                continue
            if not aborting and not process_event.is_background:
                await _wait_for_process_completed(
                    await self.bus.find(
                        ProcessCompletedEvent,
                        child_of=process_event,
                        past=True,
                        future=event.event_timeout,
                    ),
                    event.event_timeout,
                )
                continue
            started_processes.append((process_event, started_process))
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
            for _, started_process in started_processes
        ]

        # await the killing of any setup hooks that should still be running
        if pending_kills:
            await asyncio.gather(*(_run_event_now(pending_kill, pending_kill.event_timeout) for pending_kill in pending_kills))

        # await the final handling of any ProcessCompletedEvent listeners
        if started_processes:
            await asyncio.gather(
                *[
                    _wait_for_process_completed(
                        await self.bus.find(
                            ProcessCompletedEvent,
                            child_of=process_event,
                            past=True,
                            future=grace_by_hook[(process_event.plugin_name, process_event.hook_name)] + 10.0,
                        ),
                        grace_by_hook[(process_event.plugin_name, process_event.hook_name)] + 10.0,
                    )
                    for process_event, _ in started_processes
                ],
            )
        if self.crawl_completed_enabled:
            await _run_event_now(
                event.emit(
                    CrawlCompletedEvent(
                        url=event.url,
                        snapshot_id=event.snapshot_id,
                        output_dir=event.output_dir,
                    ),
                ),
                CrawlCompletedEvent.model_fields["event_timeout"].default,
            )

    async def on_CrawlAbortEvent(self, event: CrawlAbortEvent) -> None:
        """Stop scheduling any further crawl work after a user abort."""
        self.abort_requested = True
