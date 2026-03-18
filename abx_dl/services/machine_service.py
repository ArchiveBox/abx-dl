"""MachineService — handles MachineEvent to update shared config."""

from typing import Any, ClassVar

from bubus import BaseEvent, EventBus

from ..config import set_config
from ..events import MachineEvent
from .base import BaseService


class MachineService(BaseService):
    """Updates shared_config when Machine JSONL records are emitted by hooks."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [MachineEvent]

    def __init__(self, bus: EventBus, *, shared_config: dict[str, Any]):
        self.shared_config = shared_config
        super().__init__(bus)

    async def on_Machine(self, event: MachineEvent) -> None:
        record = event.record
        config = record.get('config')
        if isinstance(config, dict):
            self.shared_config.update(config)
            try:
                set_config(**{k: v for k, v in config.items() if v is not None})
            except Exception:
                pass
            return
        if record.get('_method') != 'update':
            return
        key = record.get('key', '').replace('config/', '')
        if key:
            self.shared_config[key] = record.get('value', '')
            try:
                set_config(**{key: record.get('value', '')})
            except Exception:
                pass
