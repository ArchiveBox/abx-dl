"""TagService — routes emitted tag records onto the bus."""

import json
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import ProcessStdoutEvent, TagEvent
from .base import BaseService


class TagService(BaseService):
    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessStdoutEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [TagEvent]

    def __init__(self, bus: EventBus):
        super().__init__(bus)

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict) or record.get("type") != "Tag":
            return
        name = str(record.get("name") or "").strip()
        if not name:
            return
        await self.bus.emit(
            TagEvent(
                name=name,
                snapshot_id=event.snapshot_id,
            ),
        )
