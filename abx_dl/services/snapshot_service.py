"""SnapshotService — orchestrates the snapshot extraction phase."""

import asyncio
from datetime import UTC, datetime
import json
from inspect import isawaitable
from pathlib import Path
from typing import ClassVar
from collections.abc import Awaitable, Callable

from abxbus import BaseEvent, EventBus

from ..catalog import PluginCatalog
from pydantic import ValidationError

from ..config import RuntimeConfig, get_plugin_env
from ..events import (
    ArchiveResultEvent,
    CrawlAbortEvent,
    ProcessEvent,
    ProcessCompletedEvent,
    ProcessKillEvent,
    ProcessStartedEvent,
    ProcessStdoutEvent,
    SnapshotCleanupEvent,
    SnapshotCompletedEvent,
    SnapshotDiscoveredEvent,
    SnapshotEvent,
    slow_warning_timeout,
)
from ..limits import parse_filesize_to_bytes
from ..models import Snapshot
from ..models import Hook, Plugin
from .base import BaseService, wait_for_process_ready, wait_for_crawl_resume
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


class SnapshotService(BaseService):
    """Orchestrates the snapshot phase: extraction hooks, cleanup, completion.

    The SnapshotEvent is emitted by CrawlLifecycleService after the
    install and crawl-setup phases have already completed::

        InstallEvent
        CrawlEvent
        ├── CrawlSetupEvent (crawl-setup hooks)
        ├── CrawlStartEvent
        │   └── SnapshotEvent (depth=0)                    # triggers this service
        │       │
        │       │  ── Snapshot hook handlers run serially ──
        │       │  (the event matching this service's snapshot context)
        │       │
        │       ├── on_Snapshot__35_wget.finite.bg
        │       │   └── ProcessEvent
        │       │       ├── ProcessStdoutEvent
        │       │       │   ├── SnapshotDiscoveredEvent
        │       │       │   └── ArchiveResultEvent (inline)
        │       │       └── ProcessCompletedEvent
        │       │           └── ArchiveResultEvent (enriched)
        │       ├── on_Snapshot__00_chrome_launch.daemon.bg
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

    Each service instance handles only the exact SnapshotEvent matching its
    injected snapshot and output directory. Hook-emitted discovery records are
    routed to SnapshotDiscoveredEvent facts, never back into executable work.

    RuntimeConfig is injected for this snapshot so shared-bus MachineEvent
    history from another snapshot cannot change its hook environment or limits.
    Run-local bookkeeping follows the hook events:
    - discovered snapshot flow is derived from ProcessStdoutEvent ancestry
    - background hook cleanup is driven by ProcessStartedEvent / ProcessCompletedEvent
    - snapshot size is accounted in memory; the embedding app owns crawl budgets
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        ArchiveResultEvent,
        ProcessCompletedEvent,
        CrawlAbortEvent,
        ProcessStdoutEvent,
        SnapshotEvent,
        SnapshotCleanupEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        ProcessEvent,
        ProcessKillEvent,
        SnapshotDiscoveredEvent,
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
        catalog: PluginCatalog,
        config: RuntimeConfig,
        snapshot_phase_timeout: float = 300.0,
        snapshot_cleanup_phase_timeout: float = 300.0,
        abort_requested: Callable[[], bool | Awaitable[bool]] | None = None,
        selected_hooks_by_plugin: dict[str, set[str] | None] | None = None,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.hooks: list[tuple[Plugin, Hook]] = []
        self.catalog = catalog
        for plugin in catalog.values():
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
        self.snapshot_cleanup_phase_timeout = snapshot_cleanup_phase_timeout
        self.abort_requested = False
        self.abort_requested_callback = abort_requested
        self.snapshot_max_size = max(0, parse_filesize_to_bytes(config.user.model_dump(mode="json").get("SNAPSHOT_MAX_SIZE") or 0))
        # A retry must get a fresh budget, without a hidden ledger in the output
        # directory blocking it. Hook metadata already supplies file sizes;
        # count each plugin/path once even when several hooks report it.
        self._output_sizes: dict[tuple[str, str], int] = {}
        self.config: RuntimeConfig = config
        self._hook_timeouts: dict[tuple[str, str], int] = {}
        self._snapshot_by_process: dict[str, SnapshotEvent] = {}
        self._active_snapshot_event_ids: set[str] = set()
        self._completed_snapshot_event_ids: set[str] = set()
        self._failed_snapshot_event_ids: set[str] = set()
        super().__init__(bus)
        self._handler_registrations = [
            (ArchiveResultEvent, self.bus.on(ArchiveResultEvent, self.on_ArchiveResultEvent)),
            (ProcessCompletedEvent, self.bus.on(ProcessCompletedEvent, self.on_ProcessCompletedEvent)),
            (CrawlAbortEvent, self.bus.on(CrawlAbortEvent, self.on_CrawlAbortEvent)),
            (ProcessStdoutEvent, self.bus.on(ProcessStdoutEvent, self.on_ProcessStdoutEvent)),
            (SnapshotEvent, self.bus.on(SnapshotEvent, self.on_SnapshotEvent)),
            (SnapshotCleanupEvent, self.bus.on(SnapshotCleanupEvent, self.on_SnapshotCleanupEvent)),
        ]

    def close(self) -> None:
        for event_pattern, handler in reversed(self._handler_registrations):
            self.bus.off(event_pattern, handler)
        self._handler_registrations.clear()
        self._snapshot_by_process.clear()

    async def should_abort(self) -> bool:
        if self.abort_requested or await wait_for_crawl_resume(self.bus):
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

    async def wait_for_plugin_outputs(self, event: SnapshotEvent, plugin: Plugin) -> None:
        """Wait for already-started optional producer plugins before consuming their files."""
        dependency_names = set(plugin.config.wait_for_plugins)
        if not dependency_names:
            return
        process_events = await self.bus.filter(
            ProcessEvent,
            child_of=event,
            past=True,
            future=False,
            where=lambda candidate: candidate.plugin_name in dependency_names,
        )
        for process_event in process_events:
            timeout = float(self._hook_timeouts.get((process_event.plugin_name, process_event.hook_name), 60)) + 30.0
            completed_process = await self.bus.find(
                ProcessCompletedEvent,
                child_of=process_event,
                past=True,
                future=timeout,
            )
            if completed_process is None:
                raise RuntimeError(f"Plugin output dependency {process_event.plugin_name} did not complete")
            await _wait_for_process_completed(completed_process, timeout)

    def on_SnapshotEvent__for_hook(self, plugin: Plugin, hook: Hook):
        """Create the concrete SnapshotEvent handler for one snapshot hook."""

        async def on_SnapshotEvent__hook(event: SnapshotEvent) -> None:
            if event.output_dir != str(self.output_dir) or event.snapshot_id != self.snapshot.id:
                return
            if await self.should_abort():
                return
            await self.wait_for_plugin_outputs(event, plugin)
            if plugin.config.wait_for_background_cleanup:
                cleanup_event = SnapshotCleanupEvent(
                    url=event.url,
                    snapshot_id=event.snapshot_id,
                    output_dir=event.output_dir,
                    finalize_snapshot=False,
                    event_timeout=self.snapshot_cleanup_phase_timeout,
                )
                await _run_event_now(event.emit(cleanup_event), self.snapshot_cleanup_phase_timeout)
            if await self.should_abort():
                return
            plugin_config = await get_plugin_env(
                self.bus,
                plugin=plugin,
                run_output_dir=self.output_dir,
                extra_context={
                    "snapshot_id": self.snapshot.id,
                    "plugin": plugin.name,
                    "hook_name": hook.name,
                },
                config=self.config,
            )
            if plugin.enabled_key in plugin.config.properties and not plugin_config[plugin.enabled_key]:
                return
            if plugin_config.DRY_RUN:
                return
            runtime_env = plugin_config.to_env()
            env = await build_plugin_process_env(
                self.bus,
                catalog=self.catalog,
                plugin=plugin,
                runtime_env=runtime_env,
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
            # wall-clock handler timeout for them; cleanup owns termination.
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
                hook_args=[f"--url={self.url}", f"--snapshot-id={self.snapshot.id}", f"--depth={self.snapshot.depth}"],
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
                await background_process.now()
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
                # OS spawn alone is too early: short captures can reach cleanup
                # before this hook initializes. Stdout releases this scheduling
                # barrier; it says nothing about whether an output succeeded.
                await wait_for_process_ready(
                    started_process,
                    started_wait_timeout,
                    self.should_abort,
                )
            else:
                foreground_process = event.emit(process_event)
                await _run_event_now(foreground_process, handler_timeout)
                if await self.should_abort():
                    return
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

    async def on_ArchiveResultEvent(self, event: ArchiveResultEvent) -> None:
        if self.snapshot_max_size and event.snapshot_id == self.snapshot.id:
            for output_file in event.output_files:
                self._output_sizes[event.plugin, output_file.path] = output_file.size

    async def on_ProcessCompletedEvent(self, event: ProcessCompletedEvent) -> None:
        # Some hooks emit no result, or write more after their last stdout line.
        # Their bytes still count, regardless of the eventual result status.
        if self.snapshot_max_size and event.hook_name.startswith("on_Snapshot") and Path(event.output_dir).parent == self.output_dir:
            for output_file in event.output_files:
                self._output_sizes[event.plugin_name, output_file.path] = output_file.size

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Route type=Snapshot records to SnapshotDiscoveredEvent facts.

        Discovered snapshots inherit their parent SnapshotEvent's depth and
        increment it by one.
        """
        if Path(event.output_dir).parent != self.output_dir:
            return
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict):
            return
        if "type" not in record or record["type"] != "Snapshot":
            return
        snapshot_payload = {key: value for key, value in record.items() if key != "type"}
        if not snapshot_payload.get("id"):
            snapshot_payload.pop("id", None)
        try:
            discovered_snapshot = Snapshot(**snapshot_payload)
        except ValidationError:
            return
        # All records from one hook process have the same immutable ancestry.
        # Resolve it once, not once per URL: finding an older SnapshotEvent in
        # growing history for every line made bulk parser output quadratic
        # (thousands of URLs meant millions of comparisons). That bookkeeping
        # could even expire a hook deadline after its subprocess had succeeded.
        # Use the process-event identity, never plugin name: concurrent runs and
        # retries can share a plugin/output directory but must not share owners.
        process_id = event.event_parent_id
        parent_snapshot = self._snapshot_by_process.get(process_id) if process_id else None
        if parent_snapshot is None:
            parent_snapshot = await self.bus.find(
                SnapshotEvent,
                past=True,
                future=False,
                where=lambda candidate: self.bus.event_is_child_of(event, candidate),
            )
            if process_id and isinstance(parent_snapshot, SnapshotEvent):
                self._snapshot_by_process[process_id] = parent_snapshot
        if parent_snapshot is None:
            return
        assert isinstance(parent_snapshot, SnapshotEvent)
        discovered_snapshot = discovered_snapshot.model_copy(update={"depth": parent_snapshot.depth + 1})
        event.emit(
            SnapshotDiscoveredEvent(
                snapshot=discovered_snapshot,
                plugin_name=event.plugin_name,
                hook_name=event.hook_name,
            ),
        )

    async def on_SnapshotEvent(self, event: SnapshotEvent) -> None:
        """Run snapshot hooks in sort order, then emit cleanup and completion.

        The exact snapshot/output pair identifies this service's executable
        command. Discovery uses a separate event type and cannot re-enter it.
        """
        if event.output_dir != str(self.output_dir) or event.snapshot_id != self.snapshot.id:
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
        url = self.url
        snapshot_id = self.snapshot.id
        output_dir = str(self.output_dir)
        snapshot_failed = True
        try:
            # Results describe attempts, not a checklist of planned hooks.
            # Inventing failures for hooks we never launch would fill the
            # user's failure list with work that simply has not happened yet.
            for plugin, hook in self.hooks:
                if await self.should_abort():
                    break
                await self.on_SnapshotEvent__for_hook(plugin, hook)(event)
                if await self.should_abort():
                    break
                # This is a between-hooks budget, not a disk quota. Cleanup
                # must still run so background recorders can preserve output.
                if self.snapshot_max_size and sum(self._output_sizes.values()) >= self.snapshot_max_size:
                    break
            snapshot_failed = False
        finally:
            if snapshot_failed:
                self._failed_snapshot_event_ids.add(event.event_id)
            cleanup_event = SnapshotCleanupEvent(
                url=url,
                snapshot_id=snapshot_id,
                output_dir=output_dir,
                event_timeout=self.snapshot_cleanup_phase_timeout,
                event_handler_slow_timeout=slow_warning_timeout(self.snapshot_cleanup_phase_timeout),
            )
            try:
                await _run_event_now(event.emit(cleanup_event), self.snapshot_cleanup_phase_timeout)
            finally:
                self._failed_snapshot_event_ids.discard(event.event_id)

    async def on_SnapshotCleanupEvent(self, event: SnapshotCleanupEvent) -> None:
        """SIGTERM all background snapshot hooks so they can flush and exit.

        Each background hook gets its plugin's timeout (PLUGINNAME_TIMEOUT) as the
        grace period before SIGKILL. The processes to terminate are resolved
        from the current root SnapshotEvent ancestry.

        Cleanup is part of capture execution: recorders may write their real
        output and ArchiveResult only when asked to stop. Wait for completion
        and its result consumers before finishing the snapshot. Do not replace
        this with immediate termination or infer success from having started.
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
        grace_by_hook: dict[tuple[str, str], int] = {}
        for plugin, hook in self.hooks:
            if not hook.is_background:
                continue
            grace_by_hook[(plugin.name, hook.name)] = self._hook_timeouts.get((plugin.name, hook.name), 60)
        foreground_process = await self.bus.find(
            ProcessEvent,
            child_of=root_snapshot_event,
            past=True,
            future=False,
            where=lambda candidate: not candidate.is_background,
        )
        if foreground_process is None and background_process_events:
            # With no foreground barrier, keep the capture alive until its actual
            # background attempts finish. Re-read attempts after pause/retry: a
            # fixed gather of the original events treats the stopped attempt as
            # completion and abandons the replacement subprocess. Polling here
            # also lets an abort release this wait immediately instead of waiting
            # for a plugin's potentially hour-long capture timeout.
            while not await self.should_abort():
                attempts = await self.bus.filter(
                    ProcessStartedEvent,
                    child_of=root_snapshot_event,
                    past=True,
                    future=False,
                    where=lambda candidate: candidate.is_background,
                )
                pending = False
                for started in attempts:
                    if started.interruption_done is not None and not started.interruption_done.is_set():
                        pending = True
                        break
                    completed = await self.bus.find(
                        ProcessCompletedEvent,
                        past=True,
                        future=False,
                        pid=started.pid,
                    )
                    elapsed = (datetime.now(UTC) - datetime.fromisoformat(started.start_ts)).total_seconds()
                    if completed is None and elapsed < grace_by_hook.get((started.plugin_name, started.hook_name), 60):
                        pending = True
                        break
                if not pending:
                    break
                await asyncio.sleep(0.05)
        # A retry creates another real ProcessEvent. Cleanup must own that new
        # attempt too, even when the user retried while cleanup was waiting.
        background_process_events = await self.bus.filter(
            ProcessEvent,
            child_of=root_snapshot_event,
            past=True,
            future=False,
            where=lambda candidate: candidate.is_background and (candidate.plugin_name, candidate.hook_name) in background_hook_keys,
        )
        started_processes: list[tuple[ProcessEvent, ProcessStartedEvent]] = []
        for process_event in background_process_events:
            started_process = await self.bus.find(
                ProcessStartedEvent,
                event_parent_id=process_event.event_id,
                past=True,
                future=min(5.0, event.event_timeout or 5.0),
            )
            if started_process is None:
                continue
            assert isinstance(started_process, ProcessStartedEvent)
            completed_process = await self.bus.find(
                ProcessCompletedEvent,
                child_of=process_event,
                pid=started_process.pid,
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
        if not event.finalize_snapshot or root_snapshot_event.event_id in self._failed_snapshot_event_ids:
            return
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
