"""MachineService — projects MachineEvent updates into config.env/derived.env."""

from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..config import set_derived_config, unset_derived_config
from ..events import MachineEvent
from .base import BaseService


class MachineService(BaseService):
    """Projects MachineEvent updates into the standalone config files.

    Other services should not read this service's private state directly.
    Runtime readers reconstruct current config from the initial snapshots they
    were given plus MachineEvent history on the bus.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [MachineEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = []

    def __init__(
        self,
        bus: EventBus,
        *,
        persist_derived: bool = True,
    ):
        self.persist_derived = persist_derived
        super().__init__(bus)
        self.bus.on(MachineEvent, self.on_MachineEvent)

    async def on_MachineEvent(self, event: MachineEvent) -> None:
        """Persist derived config events; user config is seeded on the bus only."""
        if event.config_type != "derived" or not self.persist_derived:
            return
        if event.config is not None:
            set_derived_config(**event.config)
            return
        key = event.key.removeprefix("config/")
        if not key:
            return
        if event.method == "update":
            set_derived_config(**{key: event.value})
            return
        if event.method == "unset":
            unset_derived_config(key)
