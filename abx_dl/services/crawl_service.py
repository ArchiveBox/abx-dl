"""Plugin crawl-hook execution and cleanup listeners."""

import asyncio
from inspect import isawaitable
from pathlib import Path
from typing import ClassVar
from collections.abc import Awaitable, Callable

from abxbus import BaseEvent, EventBus

from ..catalog import PluginCatalog
from ..config import get_config, get_plugin_env
from ..events import (
    CrawlAbortEvent,
    CrawlCleanupEvent,
    CrawlEvent,
    CrawlSetupEvent,
    ProcessCompletedEvent,
    ProcessEvent,
    ProcessKillEvent,
    ProcessStartedEvent,
    slow_warning_timeout,
)
from ..models import Snapshot
from ..models import Hook, Plugin
from .base import BaseService, wait_for_process_ready
from .binary_service import build_plugin_process_env


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
    """Run plugin ``CrawlSetup`` hooks and clean up their processes.

    This service deliberately does not drive the crawl lifecycle. Embedders can
    attach it wherever plugin crawl hooks are wanted; standalone downloads also
    attach :class:`CrawlLifecycleService` to emit the phase events.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        CrawlAbortEvent,
        CrawlSetupEvent,
        CrawlCleanupEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        ProcessEvent,
        ProcessKillEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        url: str,
        snapshot: Snapshot,
        output_dir: Path,
        catalog: PluginCatalog,
        abort_requested: Callable[[], bool | Awaitable[bool]] | None = None,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.catalog = catalog
        self.crawl_setup_hooks: list[tuple[Plugin, Hook]] = []
        for plugin in catalog.values():
            for hook in plugin.filter_hooks("CrawlSetup"):
                self.crawl_setup_hooks.append((plugin, hook))
        self.crawl_setup_hooks.sort(key=lambda item: item[1].sort_key)
        self.abort_requested = False
        self.abort_requested_callback = abort_requested
        super().__init__(bus)
        self.bus.on(CrawlSetupEvent, self.on_CrawlSetupEvent)
        self.bus.on(CrawlAbortEvent, self.on_CrawlAbortEvent)
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
            if plugin.enabled_key in plugin.config.properties and not runtime[plugin.enabled_key]:
                return
            runtime_env = runtime.to_env()
            env = await build_plugin_process_env(
                self.bus,
                catalog=self.catalog,
                plugin=plugin,
                runtime_env=runtime_env,
            )
            timeout_key = f"{plugin.name.upper()}_TIMEOUT"
            timeout = runtime[timeout_key] if timeout_key in plugin.config.properties else runtime.TIMEOUT
            plugin_output_dir = self.output_dir / plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)
            # CrawlSetup background hooks own a crawl-scoped resource for
            # the *whole crawl* and are torn down by the explicit
            # ``CrawlCleanupEvent`` SIGTERM below — they must not have a
            # wall-clock handler_timeout.
            if hook.is_background:
                handler_timeout: float | None = None
                handler_slow_timeout: float | None = None
                started_wait_timeout = float(timeout or 0) + 30.0
            else:
                handler_timeout = float(timeout or 0) + 30.0
                handler_slow_timeout = slow_warning_timeout(handler_timeout)
                started_wait_timeout = handler_timeout
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
                event_timeout=handler_timeout,
                event_handler_timeout=handler_timeout,
                event_handler_slow_timeout=handler_slow_timeout,
            )
            if hook.is_background:
                background_process = event.emit(process_event)
                started_process = await self.bus.find(
                    ProcessStartedEvent,
                    child_of=background_process,
                    past=True,
                    future=started_wait_timeout,
                )
                if await self.should_abort():
                    return
                if started_process is None:
                    raise RuntimeError(f"Background hook {hook.name} did not start")
                await wait_for_process_ready(
                    started_process,
                    started_wait_timeout,
                    self.should_abort,
                )
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
                    grace_period=float(process_event.timeout),
                    event_timeout=float(process_event.timeout) + 10.0,
                ),
            )
            for process_event, started_process in started_processes
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
                            future=float(process_event.timeout) + 10.0,
                        ),
                        float(process_event.timeout) + 10.0,
                    )
                    for process_event, _ in started_processes
                ],
            )

    async def on_CrawlAbortEvent(self, event: CrawlAbortEvent) -> None:
        """Stop scheduling any further crawl work after a user abort."""
        self.abort_requested = True
