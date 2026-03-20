"""ArchiveResultService — routes inline ArchiveResult records from hook stdout."""

from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import ArchiveResultEvent, ProcessRecordOutputtedEvent
from .base import BaseService


class ArchiveResultService(BaseService):
    """Routes type=ArchiveResult records from hook stdout to ArchiveResultEvent.

    When a hook outputs ``{"type": "ArchiveResult", ...}`` JSONL during
    execution, ProcessService emits ProcessRecordOutputtedEvent. This service
    picks up those records and emits ArchiveResultEvent(final=False) —
    informational events consumed by ArchiveBox to write DB rows for each
    extracted result.

    The **final** ArchiveResultEvent(final=True) at process completion is
    still emitted by ProcessService (it's tied to the process lifecycle and
    finalization logic, not JSONL routing).
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessRecordOutputtedEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ArchiveResultEvent]

    def __init__(self, bus: EventBus):
        super().__init__(bus)

    async def on_ProcessRecordOutputtedEvent(self, event: ProcessRecordOutputtedEvent) -> None:
        """Route type=ArchiveResult records to ArchiveResultEvent (inline)."""
        if event.record_type != 'ArchiveResult':
            return
        record = event.record
        await self.bus.emit(ArchiveResultEvent(
            snapshot_id=record.get('snapshot_id', event.snapshot_id),
            plugin=record.get('plugin', event.plugin_name),
            id=record.get('id', ''),
            hook_name=record.get('hook_name', event.hook_name),
            status=record.get('status', ''),
            output_str=record.get('output_str', ''),
            error=record.get('error', ''),
        ))
