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


class HookRunnerService(BaseService):
    """Base class for services that run plugin hooks (CrawlService, SnapshotService).

    Provides shared logic for building and emitting ProcessEvents (fg/bg dispatch).
    Cleanup is handled via dedicated cleanup events (CrawlCleanupEvent,
    SnapshotCleanupEvent) — see each subclass for details.

    Unlike plain BaseService, hook runner services skip auto-discovery of ``on_*``
    methods. Registration order matters (hooks → post-hooks → cleanup emission),
    so subclasses control it explicitly via ``_register_hook_handlers()``, using
    explicit event classes (e.g. ``bus.on(CrawlEvent, handler)``).
    """

    url: str
    snapshot: Snapshot
    output_dir: Path
    machine: MachineService
    hooks: list[tuple[Plugin, Hook]]

    def _attach_handlers(self) -> None:
        """No-op — HookRunnerService subclasses register handlers explicitly
        via ``_register_hook_handlers()`` to control ordering."""

    def _make_hook_handler(self, plugin: Plugin, hook: Hook):
        """Create an async handler that emits a ProcessEvent for one hook.

        The handler captures ``plugin`` and ``hook`` via closure defaults so each
        handler is bound to its specific hook even though they share the same
        factory method.

        The env dict is built fresh each time from ``MachineService.shared_config``,
        so fg hooks pick up config updates (e.g. CHROME_BINARY path) from earlier hooks.
        """
        async def handler(event: BaseEvent, _plugin=plugin, _hook=hook) -> None:
            env = self.machine.get_env_for_plugin(_plugin, run_output_dir=self.output_dir)
            timeout = int(env.get(f"{_plugin.name.upper()}_TIMEOUT", env.get('TIMEOUT', '60')))
            plugin_output_dir = self.output_dir / _plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)

            process_event = ProcessEvent(
                plugin_name=_plugin.name, hook_name=_hook.name,
                hook_path=str(_hook.path),
                hook_args=[f'--url={self.url}', f'--snapshot-id={self.snapshot.id}'],
                is_background=_hook.is_background,
                output_dir=str(plugin_output_dir), env=env,
                snapshot_id=self.snapshot.id, timeout=timeout,
                event_handler_timeout=timeout + 30.0,
            )
            if _hook.is_background:
                # Fire-and-forget: concurrent child of the parent event.
                self.bus.emit(process_event)
            else:
                # Foreground: blocks until subprocess and all side-effect events complete.
                await self.bus.emit(process_event)

        return handler
