"""Standalone crawl lifecycle event sequencing."""

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


class CrawlLifecycleService(BaseService):
    """Expand a root ``CrawlEvent`` into the standalone lifecycle phases."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [CrawlEvent, CrawlAbortEvent, CrawlStartEvent]
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
    ) -> None:
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.crawl_setup_phase_timeout = crawl_setup_phase_timeout
        self.snapshot_phase_timeout = snapshot_phase_timeout
        self.crawl_cleanup_phase_timeout = crawl_cleanup_phase_timeout
        self.abort_requested = False
        self.abort_requested_callback = abort_requested
        self._active_crawl_event_ids: set[str] = set()
        self._completed_crawl_event_ids: set[str] = set()
        super().__init__(bus)
        self.bus.on(CrawlEvent, self.on_CrawlEvent)
        self.bus.on(CrawlAbortEvent, self.on_CrawlAbortEvent)
        self.bus.on(CrawlStartEvent, self.on_CrawlStartEvent)

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

    async def on_CrawlStartEvent(self, event: CrawlStartEvent) -> None:
        if event.output_dir != str(self.output_dir) or await self.should_abort():
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

    async def on_CrawlAbortEvent(self, event: CrawlAbortEvent) -> None:
        self.abort_requested = True
