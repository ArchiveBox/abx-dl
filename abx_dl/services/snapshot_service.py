"""SnapshotService — orchestrates the snapshot extraction phase."""

import asyncio
import json
from inspect import isawaitable
from pathlib import Path
from typing import ClassVar
from collections.abc import Awaitable, Callable

from abxbus import BaseEvent, EventBus
from abxpkg import BinProvider
from abxpkg.binary_service import BinaryEvent
from pydantic import ValidationError

from ..config import RuntimeConfig, get_config, get_plugin_env
from ..events import (
    CrawlAbortEvent,
    CrawlStartEvent,
    ProcessEvent,
    ProcessCompletedEvent,
    ProcessKillEvent,
    ProcessStartedEvent,
    ProcessStdoutEvent,
    SnapshotCleanupEvent,
    SnapshotCompletedEvent,
    SnapshotEvent,
    slow_warning_timeout,
)
from ..limits import CrawlLimitState
from ..models import Snapshot
from ..models import Hook, Plugin, filter_plugins
from .base import BaseService, wait_for_background_ready


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


class SnapshotService(BaseService):
    """Orchestrates the snapshot phase: extraction hooks, cleanup, completion.

    The SnapshotEvent is emitted by CrawlService.on_CrawlStartEvent after the
    install and crawl-setup phases have already completed::

        InstallEvent
        CrawlEvent
        ├── CrawlSetupEvent (crawl-setup hooks)
        ├── CrawlStartEvent
        │   └── SnapshotEvent (depth=0)                    # triggers this service
        │       │
        │       │  ── Snapshot hook handlers run serially ──
        │       │  (only root SnapshotEvents emitted by CrawlStartEvent)
        │       │
        │       ├── on_Snapshot__06_wget.finite.bg
        │       │   └── ProcessEvent
        │       │       ├── ProcessStdoutEvent
        │       │       │   ├── SnapshotEvent (discovered URL)
        │       │       │   └── ArchiveResultEvent (inline)
        │       │       └── ProcessCompletedEvent
        │       │           └── ArchiveResultEvent (enriched)
        │       ├── on_Snapshot__09_chrome_launch.daemon.bg
        │       ├── on_Snapshot__54_title
        │       ├── on_Snapshot__93_hashes
        │       │
        │       │  ── After all hook handlers ──
        │       │
        │       ├── SnapshotCleanupEvent
        │       │   └── ProcessKillEvent × N
        │       └── SnapshotCompletedEvent
        │
        ├── CrawlCleanupEvent
        └── CrawlCompletedEvent

    Only the root SnapshotEvent emitted directly by CrawlStartEvent executes
    snapshot hooks. SnapshotEvents emitted from hook stdout are discovery
    records for higher-level consumers such as ArchiveBox.

    plugin_config state is kept on the bus or on disk, not in service-local toggles:
    - discovered snapshot flow is derived from ProcessStdoutEvent ancestry
    - background hook cleanup is driven by ProcessStartedEvent / ProcessCompletedEvent
    - crawl limit admission is persisted by CrawlLimitState in ``CRAWL_DIR/.abx-dl``
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        CrawlAbortEvent,
        ProcessStdoutEvent,
        SnapshotEvent,
        SnapshotCleanupEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        ProcessEvent,
        ProcessKillEvent,
        SnapshotEvent,
        SnapshotCleanupEvent,
        SnapshotCompletedEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        url: str,
        snapshot: Snapshot,
        output_dir: Path,
        plugins: dict[str, Plugin],
        snapshot_phase_timeout: float = 300.0,
        snapshot_cleanup_enabled: bool = True,
        snapshot_cleanup_phase_timeout: float = 300.0,
        abort_requested: Callable[[], bool | Awaitable[bool]] | None = None,
        selected_hooks_by_plugin: dict[str, set[str] | None] | None = None,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.hooks: list[tuple[Plugin, Hook]] = []
        self.plugins = plugins
        for plugin in plugins.values():
            if selected_hooks_by_plugin is not None and plugin.name not in selected_hooks_by_plugin:
                continue
            selected_hook_names = selected_hooks_by_plugin.get(plugin.name) if selected_hooks_by_plugin is not None else None
            for hook in plugin.filter_hooks("Snapshot"):
                if (
                    selected_hook_names is not None
                    and hook.name not in selected_hook_names
                    and Path(hook.name).stem not in selected_hook_names
                ):
                    continue
                self.hooks.append((plugin, hook))
        self.hooks.sort(key=lambda item: item[1].sort_key)
        self.snapshot_phase_timeout = snapshot_phase_timeout
        self.snapshot_cleanup_enabled = snapshot_cleanup_enabled
        self.snapshot_cleanup_phase_timeout = snapshot_cleanup_phase_timeout
        self.abort_requested = False
        self.abort_requested_callback = abort_requested
        self.limit_state: CrawlLimitState | None = None
        self._config: RuntimeConfig | None = None
        self._hook_timeouts: dict[tuple[str, str], int] = {}
        self._active_snapshot_event_ids: set[str] = set()
        self._completed_snapshot_event_ids: set[str] = set()
        super().__init__(bus)
        self._handler_registrations = [
            (CrawlAbortEvent, self.bus.on(CrawlAbortEvent, self.on_CrawlAbortEvent)),
            (ProcessStdoutEvent, self.bus.on(ProcessStdoutEvent, self.on_ProcessStdoutEvent)),
            (SnapshotEvent, self.bus.on(SnapshotEvent, self.on_SnapshotEvent)),
            (SnapshotCleanupEvent, self.bus.on(SnapshotCleanupEvent, self.on_SnapshotCleanupEvent)),
        ]

    def close(self) -> None:
        for event_pattern, handler in reversed(self._handler_registrations):
            self.bus.off(event_pattern, handler)
        self._handler_registrations.clear()

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

    def on_SnapshotEvent__for_hook(self, plugin: Plugin, hook: Hook):
        """Create the concrete SnapshotEvent handler for one snapshot hook."""

        async def on_SnapshotEvent__hook(event: SnapshotEvent) -> None:
            if event.output_dir != str(self.output_dir) or event.snapshot_id != self.snapshot.id:
                return
            parent_event = await self.bus.find(
                CrawlStartEvent,
                past=True,
                future=False,
                where=lambda candidate: self.bus.event_is_parent_of(candidate, event),
            )
            if not isinstance(parent_event, CrawlStartEvent):
                return
            if await self.should_abort():
                return
            assert self.limit_state is not None
            plugin_config = await get_plugin_env(
                self.bus,
                plugin=plugin,
                run_output_dir=self.output_dir,
                extra_context={
                    "snapshot_id": self.snapshot.id,
                    "snapshot_depth": self.snapshot.depth,
                    "plugin": plugin.name,
                    "hook_name": hook.name,
                },
                config=self._config,
            )
            if plugin_config.DRY_RUN:
                return
            env = plugin_config.to_env()
            env_plugin_names = set(filter_plugins(self.plugins, [plugin.name], include_providers=True))
            binary_events = await self.bus.filter(
                BinaryEvent,
                past=True,
                where=lambda candidate: str(candidate.extra_context.get("plugin_name") or "") in env_plugin_names,
            )
            for binary_event in reversed(binary_events):
                if binary_event.env:
                    env = BinProvider.build_exec_env(
                        base_env=env,
                        extra_env=binary_event.env,
                    )
            env["SNAP_DIR"] = str(self.output_dir)
            if str(env.get("CHROME_ISOLATION") or "").lower() == "snapshot":
                active_persona = str(env.get("ACTIVE_PERSONA") or "Default")
                env["PERSONAS_DIR"] = str(self.output_dir / ".persona")
                env["ACTIVE_PERSONA"] = active_persona
            timeout_key = f"{plugin.name.upper()}_TIMEOUT"
            timeout = plugin_config[timeout_key] if timeout_key in plugin.config.properties else plugin_config.TIMEOUT
            self._hook_timeouts[(plugin.name, hook.name)] = timeout
            plugin_output_dir = self.output_dir / plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)
            # Snapshot background hooks own resources that are explicitly
            # shut down by SnapshotCleanupEvent. Do not give abxbus a
            # wall-clock handler timeout for them; the foreground barrier
            # hooks enforce readiness and cleanup owns termination.
            if hook.is_background:
                handler_timeout: float | None = None
                handler_slow_timeout: float | None = None
                started_wait_timeout = 60.0
                ready_wait_timeout = float(timeout or 0) + 30.0
            else:
                handler_timeout = float(timeout or 0) + 30.0
                handler_slow_timeout = slow_warning_timeout(handler_timeout)
                started_wait_timeout = handler_timeout
                ready_wait_timeout = handler_timeout
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
                # First-run PEP-723 hooks need to resolve + install their inline
                # script deps via ``uv run --script``; on a cold cache that can
                # take 20–30s. Cap at min(60s, handler_timeout) to absorb that
                # cold-start latency without hanging indefinitely if the hook
                # truly fails to launch. ``handler_timeout`` already bounds the
                # hook's full lifetime, so this is at most an additive 60s on
                # the start phase.
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
                await wait_for_background_ready(self.bus, background_process, ready_wait_timeout)
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

        handler_name = f"on_SnapshotEvent__{plugin.name}__{hook.name.replace('.', '_')}__{self.snapshot.id.replace('-', '_')[-12:]}"
        on_SnapshotEvent__hook.__name__ = handler_name
        on_SnapshotEvent__hook.__qualname__ = handler_name
        return on_SnapshotEvent__hook

    async def on_SnapshotEvent__check_crawl_limits(self, event: SnapshotEvent) -> None:
        """Persist crawl-limit admission for the root snapshot before hook handlers run."""
        if event.output_dir != str(self.output_dir) or event.snapshot_id != self.snapshot.id:
            return
        if self.limit_state is None:
            self._config = self._config or await get_config(self.bus)
            self.limit_state = CrawlLimitState.from_config(self._config.user.model_dump(mode="json"))
        parent_event = await self.bus.find(
            CrawlStartEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_parent_of(candidate, event),
        )
        if not isinstance(parent_event, CrawlStartEvent):
            return
        if self.limit_state.has_limits():
            self.limit_state.admit_snapshot(event.snapshot_id)

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Route type=Snapshot records to SnapshotEvent.

        Discovered snapshots inherit their parent SnapshotEvent's depth and
        increment it by one.
        """
        if Path(event.output_dir).parent != self.output_dir:
            return
        self._config = self._config or await get_config(self.bus)
        if str(self._config.user.ABX_RUNTIME).lower() == "archivebox":
            return
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict):
            return
        if "type" not in record or record["type"] != "Snapshot":
            return
        try:
            discovered_snapshot = Snapshot(**record)
        except ValidationError:
            return
        if self.limit_state is None:
            self._config = self._config or await get_config(self.bus)
            self.limit_state = CrawlLimitState.from_config(self._config.user.model_dump(mode="json"))
        if not self.limit_state.should_emit_discovered_snapshots():
            return
        parent_snapshot = await self.bus.find(
            SnapshotEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_child_of(event, candidate),
        )
        if parent_snapshot is None:
            return
        assert isinstance(parent_snapshot, SnapshotEvent)
        await event.emit(
            SnapshotEvent(
                url=discovered_snapshot.url,
                snapshot_id=discovered_snapshot.id,
                output_dir=str(self.output_dir),
                depth=parent_snapshot.depth + 1,
                event_timeout=event.event_timeout,
                event_handler_slow_timeout=slow_warning_timeout(event.event_timeout),
            ),
        ).now()

    async def on_SnapshotEvent(self, event: SnapshotEvent) -> None:
        """Run snapshot hooks in sort order, then emit cleanup and completion.

        Only the root SnapshotEvent emitted directly by CrawlStartEvent drives
        hook execution and cleanup. Discovered SnapshotEvents emitted from hook
        stdout are left for higher-level consumers like ArchiveBox.
        """
        if event.output_dir != str(self.output_dir) or event.snapshot_id != self.snapshot.id:
            return
        parent_event = await self.bus.find(
            CrawlStartEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_parent_of(candidate, event),
        )
        if not isinstance(parent_event, CrawlStartEvent):
            return
        if event.event_id in self._active_snapshot_event_ids or event.event_id in self._completed_snapshot_event_ids:
            return
        self._active_snapshot_event_ids.add(event.event_id)
        try:
            await self._run_root_snapshot_event(event)
        finally:
            self._active_snapshot_event_ids.discard(event.event_id)
            self._completed_snapshot_event_ids.add(event.event_id)

    async def _run_root_snapshot_event(self, event: SnapshotEvent) -> None:
        completed_event = await self.bus.find(
            SnapshotCompletedEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_child_of(candidate, event),
            snapshot_id=self.snapshot.id,
            output_dir=str(self.output_dir),
        )
        if completed_event is not None:
            return
        self._config = self._config or await get_config(self.bus)
        if self.limit_state is None:
            self.limit_state = CrawlLimitState.from_config(self._config.user.model_dump(mode="json"))
        admission = self.limit_state.admit_snapshot(event.snapshot_id) if self.limit_state.has_limits() else None
        if admission is not None and not admission.allowed:
            raise RuntimeError(f"Snapshot {event.snapshot_id} denied by crawl limits: {admission.stop_reason}")
        url = self.url
        snapshot_id = self.snapshot.id
        output_dir = str(self.output_dir)
        try:
            for plugin, hook in self.hooks:
                if await self.should_abort():
                    break
                await self.on_SnapshotEvent__for_hook(plugin, hook)(event)
                if await self.should_abort():
                    break
                if self.limit_state.get_snapshot_stop_reason(event.snapshot_id) == "snapshot_max_size":
                    break
        finally:
            if self.snapshot_cleanup_enabled:
                cleanup_event = SnapshotCleanupEvent(
                    url=url,
                    snapshot_id=snapshot_id,
                    output_dir=output_dir,
                    event_timeout=self.snapshot_cleanup_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(self.snapshot_cleanup_phase_timeout),
                )
                event.emit(cleanup_event)
        if self.snapshot_cleanup_enabled:
            return

        completed_event = SnapshotCompletedEvent(
            url=url,
            snapshot_id=snapshot_id,
            output_dir=output_dir,
            event_timeout=self.snapshot_phase_timeout,
            event_handler_timeout=self.snapshot_phase_timeout,
            event_handler_slow_timeout=slow_warning_timeout(self.snapshot_phase_timeout),
        )
        event.emit(completed_event)

    async def on_SnapshotCleanupEvent(self, event: SnapshotCleanupEvent) -> None:
        """SIGTERM all background snapshot hooks so they can flush and exit.

        Each background hook gets its plugin's timeout (PLUGINNAME_TIMEOUT) as the
        grace period before SIGKILL. The processes to terminate are resolved
        from the current root SnapshotEvent ancestry.
        """
        if event.output_dir != str(self.output_dir) or event.snapshot_id != self.snapshot.id:
            return
        root_snapshot_event = await self.bus.find(
            SnapshotEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_child_of(event, candidate),
        )
        if root_snapshot_event is None:
            return
        background_hook_keys = {(plugin.name, hook.name) for plugin, hook in self.hooks if hook.is_background}
        background_process_events = await self.bus.filter(
            ProcessEvent,
            child_of=root_snapshot_event,
            past=True,
            future=False,
            where=lambda candidate: candidate.is_background and (candidate.plugin_name, candidate.hook_name) in background_hook_keys,
        )
        foreground_process = await self.bus.find(
            ProcessEvent,
            child_of=root_snapshot_event,
            past=True,
            future=False,
            where=lambda candidate: not candidate.is_background,
        )
        if foreground_process is None and background_process_events:
            # Background-only snapshots have no foreground hook to create a
            # natural gap between "the OS process exists" and cleanup's polite
            # SIGTERM. That gap matters because PEP-723 hooks first enter the
            # abxpkg shebang launcher before Python user code can install its
            # own signal disposition. Without this tiny grace, cleanup can send
            # SIGTERM in the same scheduler turn as spawn and convert valid
            # finite extraction work into a signal failure. Keep the budget
            # small and only spend it when there is no foreground work and no
            # background hook has already completed.
            pending_startup_processes: list[ProcessEvent] = []
            for process_event in background_process_events:
                completed_process = await self.bus.find(
                    ProcessCompletedEvent,
                    child_of=process_event,
                    past=True,
                    future=False,
                )
                if completed_process is None:
                    pending_startup_processes.append(process_event)
            if pending_startup_processes:
                await asyncio.gather(
                    *[
                        self.bus.find(
                            ProcessCompletedEvent,
                            child_of=process_event,
                            past=True,
                            future=0.1,
                        )
                        for process_event in pending_startup_processes
                    ],
                )
        grace_by_hook: dict[tuple[str, str], int] = {}
        for plugin, hook in self.hooks:
            if not hook.is_background:
                continue
            grace_by_hook[(plugin.name, hook.name)] = self._hook_timeouts.get((plugin.name, hook.name), 60)
        started_processes: list[tuple[ProcessEvent, ProcessStartedEvent]] = []
        for process_event in background_process_events:
            started_process = await self.bus.find(
                ProcessStartedEvent,
                child_of=process_event,
                past=True,
                future=min(5.0, event.event_timeout or 5.0),
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
        if pending_kills:
            await asyncio.gather(
                *(_run_event_now(pending_kill, pending_kill.event_timeout) for pending_kill in pending_kills),
            )
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
        completed_event = SnapshotCompletedEvent(
            url=event.url,
            snapshot_id=event.snapshot_id,
            output_dir=event.output_dir,
            event_timeout=event.event_timeout,
            event_handler_timeout=event.event_timeout,
            event_handler_slow_timeout=slow_warning_timeout(event.event_timeout),
        )
        root_snapshot_event.emit(completed_event)

    async def on_CrawlAbortEvent(self, event: CrawlAbortEvent) -> None:
        """Stop scheduling any further snapshot work after a user abort."""
        self.abort_requested = True
