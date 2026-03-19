"""Base service class for auto-registering event handlers on a bubus EventBus."""

from typing import ClassVar

from bubus import BaseEvent, EventBus


class BaseService:
    """Base class that auto-discovers ``on_*`` methods and registers them on the bus.

    Subclasses declare ``LISTENS_TO`` with event classes they handle, then define
    async methods named ``on_<EventClassName>`` (e.g. ``on_ProcessEvent``). The
    ``__init__`` auto-registers each matching method as a handler on the bus.

    Handler name resolution:
    - ``on_ProcessEvent`` → matches ``ProcessEvent`` directly
    - ``on_ProcessCompleted`` → matches ``ProcessCompleted`` directly
    - ``on_Crawl__plugin_hook`` → tries "Crawl", then "CrawlEvent" (matches)

    This naming convention mirrors hook filenames (``on_Crawl__10_wget_install``)
    so the connection between hook files and their handler registration is clear.

    bubus detail: ``bus.on(EventClass, handler)`` registers the handler to be called
    whenever an event of that class is emitted on the bus. Handlers are called in
    registration order with serial concurrency (one at a time) by default.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = []
    EMITS: ClassVar[list[type[BaseEvent]]] = []

    def __init__(self, bus: EventBus):
        self.bus = bus
        self._attach_handlers()

    def _attach_handlers(self) -> None:
        """Discover ``on_*`` methods and register them on the bus.

        Iterates over all attributes starting with ``on_``, extracts the event
        class name from the method name, and registers it if it matches any
        class in ``LISTENS_TO``.

        The double-underscore split handles dynamic handler names like
        ``on_Crawl__10_wget_install`` — only the prefix before ``__`` is used
        for event class matching ("Crawl" → "CrawlEvent").
        """
        for attr_name in dir(self):
            if not attr_name.startswith('on_'):
                continue
            prefix = attr_name.split('on_', 1)[1].split('__')[0]
            candidates = [prefix] if prefix.endswith('Event') else [prefix, prefix + 'Event']
            for event_cls in self.LISTENS_TO:
                if event_cls.__name__ in candidates:
                    handler = getattr(self, attr_name)
                    self.bus.on(event_cls, handler)
                    break
