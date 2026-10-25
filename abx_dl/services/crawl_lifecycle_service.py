"""Shared crawl lifecycle event sequencing."""

from collections.abc import Awaitable, Callable
from inspect import isawaitable
from pathlib import Path
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import (
    CrawlAbortEvent,
    CrawlCleanupEvent,
    CrawlCompletedEvent,
    CrawlEvent,
    CrawlStartEvent,
    CrawlSetupEvent,
    ProcessCompletedEvent,
    SnapshotCompletedEvent,
    SnapshotEvent,
    slow_warning_timeout,
)
from ..models import Snapshot
from .base import BaseService


async def _run_event_now(event: BaseEvent, timeout: float | None = None) -> BaseEvent:
    await event.now(timeout=timeout)
    await event.wait(timeout=timeout)
    await event.event_results_list()
    return event


class SnapshotAttemptError(RuntimeError):
    """Capture attempts failed after independent work and cleanup finished."""


class CrawlLifecycleService(BaseService):
    """Own crawl phases; embedders may supply only the snapshot work scheduler."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        CrawlEvent,
        CrawlAbortEvent,
        CrawlStartEvent,
        ProcessCompletedEvent,
        SnapshotCompletedEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        CrawlSetupEvent,
        CrawlStartEvent,
        CrawlCleanupEvent,
        CrawlCompletedEvent,
        SnapshotEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        url: str,
        snapshot: Snapshot,
        output_dir: Path,
        crawl_setup_phase_timeout: float = 300.0,
        snapshot_phase_timeout: float = 300.0,
        crawl_cleanup_phase_timeout: float = 300.0,
        abort_requested: Callable[[], bool | Awaitable[bool]] | None = None,
        run_snapshots: Callable[[CrawlStartEvent], Awaitable[None]] | None = None,
    ) -> None:
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.crawl_setup_phase_timeout = crawl_setup_phase_timeout
        self.snapshot_phase_timeout = snapshot_phase_timeout
        self.crawl_cleanup_phase_timeout = crawl_cleanup_phase_timeout
        self.abort_requested = False
        self.abort_requested_callback = abort_requested
        self.run_snapshots = run_snapshots
        self._execution_interrupted = False
        self._active_crawl_event_ids: set[str] = set()
        self._completed_crawl_event_ids: set[str] = set()
        self._failed_snapshots: dict[str, dict[str, str]] = {}
        self._failed_setup_hooks: dict[str, set[str]] = {}
        super().__init__(bus)
        self.bus.on(CrawlEvent, self.on_CrawlEvent)
        self.bus.on(CrawlAbortEvent, self.on_CrawlAbortEvent)
        self.bus.on(CrawlStartEvent, self.on_CrawlStartEvent)
        self.bus.on(ProcessCompletedEvent, self.on_ProcessCompletedEvent)
        self.bus.on(SnapshotCompletedEvent, self.on_SnapshotCompletedEvent)

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

    async def on_CrawlEvent(self, event: CrawlEvent) -> None:
        if event.output_dir != str(self.output_dir) or event.snapshot_id != self.snapshot.id:
            return
        if event.event_id in self._active_crawl_event_ids or event.event_id in self._completed_crawl_event_ids:
            return
        self._active_crawl_event_ids.add(event.event_id)
        self._failed_snapshots[event.event_id] = {}
        self._failed_setup_hooks[event.event_id] = set()
        try:
            await self._run_root_crawl_event(event)
        finally:
            self._active_crawl_event_ids.discard(event.event_id)
            self._completed_crawl_event_ids.add(event.event_id)
            self._failed_snapshots.pop(event.event_id, None)
            self._failed_setup_hooks.pop(event.event_id, None)

    async def _run_root_crawl_event(self, event: CrawlEvent) -> None:
        try:
            if not await self.should_abort():
                await _run_event_now(
                    event.emit(
                        CrawlSetupEvent(
                            url=self.url,
                            snapshot_id=self.snapshot.id,
                            output_dir=str(self.output_dir),
                            event_timeout=self.crawl_setup_phase_timeout,
                            event_handler_slow_timeout=slow_warning_timeout(self.crawl_setup_phase_timeout),
                        ),
                    ),
                    self.crawl_setup_phase_timeout,
                )
            if not await self.should_abort():
                await _run_event_now(
                    event.emit(
                        CrawlStartEvent(
                            url=self.url,
                            snapshot_id=self.snapshot.id,
                            output_dir=str(self.output_dir),
                            event_timeout=self.snapshot_phase_timeout,
                            event_handler_slow_timeout=slow_warning_timeout(self.snapshot_phase_timeout),
                        ),
                    ),
                    self.snapshot_phase_timeout,
                )
        except BaseException as phase_error:
            try:
                await self._run_cleanup(event)
            except BaseException as cleanup_error:
                phase_error.add_note(f"Crawl cleanup also failed: {cleanup_error}")
            raise
        await self._run_cleanup(event)
        # A takeover relinquishes execution to another owner. Explicit user
        # abort still finishes this crawl's lifecycle after resources stop.
        if self._execution_interrupted:
            return
        await _run_event_now(
            event.emit(
                CrawlCompletedEvent(
                    url=self.url,
                    snapshot_id=self.snapshot.id,
                    output_dir=str(self.output_dir),
                ),
            ),
            CrawlCompletedEvent.model_fields["event_timeout"].default,
        )
        failed_snapshots = self._failed_snapshots[event.event_id]
        failure: RuntimeError | None = None
        if failed_snapshots:
            failure = SnapshotAttemptError(
                f"{len(failed_snapshots)} snapshot attempt(s) failed: {', '.join(sorted(failed_snapshots))}. "
                "See hook completion records for the original errors.",
            )
            for snapshot_id, error in failed_snapshots.items():
                failure.add_note(f"Snapshot {snapshot_id}: {error}")
        failed_setup_hooks = self._failed_setup_hooks[event.event_id]
        if failed_setup_hooks:
            setup_error = "; ".join(f"Crawl setup hook {hook} failed" for hook in sorted(failed_setup_hooks))
            if failure is None:
                failure = RuntimeError(setup_error)
            else:
                failure.add_note(setup_error)
        if failure is not None:
            raise failure

    async def _run_cleanup(self, event: CrawlEvent) -> None:
        await _run_event_now(
            event.emit(
                CrawlCleanupEvent(
                    url=self.url,
                    snapshot_id=self.snapshot.id,
                    output_dir=str(self.output_dir),
                    event_timeout=self.crawl_cleanup_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(self.crawl_cleanup_phase_timeout),
                ),
            ),
            self.crawl_cleanup_phase_timeout,
        )

    async def on_CrawlStartEvent(self, event: CrawlStartEvent) -> None:
        if event.output_dir != str(self.output_dir) or event.snapshot_id != self.snapshot.id or await self.should_abort():
            return
        if self.run_snapshots is not None:
            # Database-backed callers may admit many snapshots, but must await
            # and drain their admitted work (also on failure) before returning.
            # Setup, cleanup and terminal events remain owned by this service.
            await self.run_snapshots(event)
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
        await snapshot_event.now(timeout=event.event_timeout)
        await snapshot_event.wait(timeout=event.event_timeout)
        if self._execution_interrupted:
            return
        completed_snapshot = await self.bus.find(
            SnapshotCompletedEvent,
            child_of=snapshot_event,
            past=True,
            future=False,
        )
        if completed_snapshot is None:
            await snapshot_event.event_results_list()
            raise RuntimeError(f"Snapshot {self.snapshot.id} did not complete")
        await completed_snapshot.wait(timeout=event.event_timeout)
        await completed_snapshot.event_results_list()
        # Earlier preparation-handler exceptions are already represented by a
        # delivered terminal failure. Unhandled delivery errors still propagate.
        if not completed_snapshot.error:
            await snapshot_event.event_results_list()

    async def on_CrawlAbortEvent(self, event: CrawlAbortEvent) -> None:
        self.abort_requested = True
        self._execution_interrupted = self._execution_interrupted or not event.user_initiated

    async def on_ProcessCompletedEvent(self, event: ProcessCompletedEvent) -> None:
        if event.status != "failed" or event.cancelled:
            return
        crawl = await self.bus.find(
            CrawlEvent,
            past=True,
            future=False,
            where=lambda candidate: candidate.event_id in self._active_crawl_event_ids and self.bus.event_is_child_of(event, candidate),
        )
        if crawl is None:
            return
        setup = await self.bus.find(
            CrawlSetupEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_child_of(event, candidate),
        )
        if setup is not None:
            # Only hooks that actually ran can fail setup. A catalog scan would
            # wait for deliberately disabled hooks and cannot distinguish them
            # from missing work. Optional behavior belongs in the hook's exit.
            if not event.is_background:
                self._failed_setup_hooks[crawl.event_id].add(f"{event.plugin_name}:{event.hook_name}")
            return
        if event.pid:
            return
        snapshot = await self.bus.find(
            SnapshotEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_child_of(event, candidate),
        )
        if snapshot is not None:
            # Keep failure IDs and diagnostics while their completion facts are live:
            # database embedders may expire projected snapshot history. Report
            # preparation errors after all independent work, never ordinary
            # provider exits or user cancellations as orchestration failures.
            self._failed_snapshots[crawl.event_id].setdefault(snapshot.snapshot_id, event.stderr)

    async def on_SnapshotCompletedEvent(self, event: SnapshotCompletedEvent) -> None:
        if not event.error:
            return
        crawl = await self.bus.find(
            CrawlEvent,
            past=True,
            future=False,
            where=lambda candidate: candidate.event_id in self._active_crawl_event_ids and self.bus.event_is_child_of(event, candidate),
        )
        if crawl is not None:
            # Keep the terminal failure while its fact is live: embedders may
            # expire projected history before the queue finishes. Ordinary
            # failed hook results already have their own per-plugin semantics.
            self._failed_snapshots[crawl.event_id][event.snapshot_id] = event.error
