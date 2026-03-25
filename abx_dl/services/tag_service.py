"""TagService — routes emitted tag records onto the bus."""

import json
from typing import Any, ClassVar

from abxbus import BaseEvent, EventBus
from pydantic import ValidationError

from ..events import ProcessStdoutEvent, TagEvent
from ..models import Tag
from .base import BaseService


class TagService(BaseService):
    """Convert hook-emitted ``Tag`` JSONL lines into TagEvent records."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessStdoutEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [TagEvent]

    def __init__(self, bus: EventBus):
        super().__init__(bus)
        self.bus.on(ProcessStdoutEvent, self.on_ProcessStdoutEvent)

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Parse ``type=Tag`` stdout lines from snapshot hooks and emit TagEvent."""
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict):
            return
        tag_payload: dict[str, Any] = {str(key): value for key, value in record.items()}
        if "type" not in tag_payload or tag_payload["type"] != "Tag":
            return
        try:
            tag = Tag(**tag_payload)
        except ValidationError:
            return
        await self.bus.emit(TagEvent(**tag.model_dump(mode="json")))
