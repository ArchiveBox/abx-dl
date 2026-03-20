"""Base service class for auto-registering event handlers on a bubus EventBus."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from bubus import BaseEvent, EventBus

from ..events import ProcessEvent
from ..models import Hook, Plugin, Snapshot

if TYPE_CHECKING:
    from .machine_service import MachineService


class BaseService:
    """Base class that auto-discovers ``on_*`` methods and registers them on the bus.

    Subclasses declare ``LISTENS_TO`` with event classes they handle, then define
    async methods named ``on_<EventClassName>`` (e.g. ``on_ProcessEvent``). The
    ``__init__`` auto-registers each matching method as a handler on the bus.

    Handler name resolution:
    - ``on_ProcessEvent`` → matches ``ProcessEvent`` directly
    - ``on_ProcessCompletedEvent`` → matches ``ProcessCompletedEvent`` directly
    - ``on_CrawlSetup__plugin_hook`` → tries "CrawlSetup", then "CrawlSetupEvent" (matches)

    Note: CrawlService and SnapshotService override ``_attach_handlers()`` to
    control registration order explicitly. They register per-hook handlers on
    CrawlSetupEvent / SnapshotEvent directly via ``bus.on()``.

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
        ``on_CrawlSetup__10_wget_install`` — only the prefix before ``__`` is
        used for event class matching ("CrawlSetup" → "CrawlSetupEvent").
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


def make_hook_handler(
    service: BaseService,
    plugin: Plugin,
    hook: Hook,
    *,
    url: str,
    snapshot: Snapshot,
    output_dir: Path,
    machine: MachineService,
):
    """Create an async handler that emits a ProcessEvent for one hook.

    The handler captures ``plugin`` and ``hook`` via closure defaults so each
    handler is bound to its specific hook even though they share the same
    factory function.

    The env dict is built fresh each time from ``MachineService.shared_config``,
    so fg hooks pick up config updates (e.g. CHROME_BINARY path) from earlier hooks.
    """
    async def handler(event: BaseEvent, _plugin=plugin, _hook=hook) -> None:
        env = machine.get_env_for_plugin(_plugin, run_output_dir=output_dir)
        timeout = int(env.get(f"{_plugin.name.upper()}_TIMEOUT", env.get('TIMEOUT', '60')))
        plugin_output_dir = output_dir / _plugin.name
        plugin_output_dir.mkdir(parents=True, exist_ok=True)

        process_event = ProcessEvent(
            plugin_name=_plugin.name, hook_name=_hook.name,
            hook_path=str(_hook.path),
            hook_args=[f'--url={url}', f'--snapshot-id={snapshot.id}'],
            is_background=_hook.is_background,
            output_dir=str(plugin_output_dir), env=env,
            snapshot_id=snapshot.id, timeout=timeout,
            event_handler_timeout=timeout + 30.0,
        )
        if _hook.is_background:
            service.bus.emit(process_event)
        else:
            await service.bus.emit(process_event)

    return handler
