"""Base service class for auto-registering event handlers on a bubus EventBus."""

from typing import ClassVar

from bubus import BaseEvent, EventBus


class BaseService:
    """Base class that auto-discovers on_* methods and registers them on the bus.

    Subclasses declare LISTENS_TO with event classes, then define async methods
    named on_<EventClassName> (or on_<EventClassName>__<suffix> for dynamic handlers).
    The __init__ auto-registers each matching method.

    No private state other than the bus — shared state is passed as constructor args.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = []
    EMITS: ClassVar[list[type[BaseEvent]]] = []

    def __init__(self, bus: EventBus):
        self.bus = bus
        self._attach_handlers()

    def _attach_handlers(self) -> None:
        """Discover on_* methods and register them on the bus."""
        for attr_name in dir(self):
            if not attr_name.startswith('on_'):
                continue
            # on_ProcessEvent -> "ProcessEvent"
            # on_CrawlEvent__plugin_hook -> "CrawlEvent"
            event_class_name = attr_name.split('on_', 1)[1].split('__')[0]
            for event_cls in self.LISTENS_TO:
                if event_cls.__name__ == event_class_name:
                    handler = getattr(self, attr_name)
                    self.bus.on(event_cls, handler)
                    break
