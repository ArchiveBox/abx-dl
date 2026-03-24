"""Base service class for auto-registering event handlers on a abxbus EventBus."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar
from collections.abc import Mapping

from abxbus import BaseEvent, EventBus, EventHandler

from ..events import ProcessEvent, ProcessStartedEvent, slow_warning_timeout
from ..limits import CrawlLimitState
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
    - ``on_BinaryRequest__provider`` → tries "BinaryRequest", then "BinaryRequestEvent" (matches)

    Note: CrawlService and SnapshotService override ``_attach_handlers()`` to
    control registration order explicitly. They register per-hook handlers on
    CrawlSetupEvent / SnapshotEvent directly via ``bus.on()``.

    abxbus detail: ``bus.on(EventClass, handler)`` registers the handler to be called
    whenever an event of that class is emitted on the bus. Handlers are called in
    registration order with serial concurrency (one at a time) by default.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = []
    EMITS: ClassVar[list[type[BaseEvent]]] = []

    def __init__(self, bus: EventBus):
        self.bus = bus
        self._registrations: list[tuple[type[BaseEvent] | str, EventHandler]] = []
        self._attach_handlers()

    def _register(self, event_pattern: type[BaseEvent] | str, handler) -> EventHandler:
        registration = self.bus.on(event_pattern, handler)
        self._registrations.append((event_pattern, registration))
        return registration

    def close(self) -> None:
        while self._registrations:
            event_pattern, registration = self._registrations.pop()
            self.bus.off(event_pattern, registration)

    def _attach_handlers(self) -> None:
        """Discover ``on_*`` methods and register them on the bus.

        Iterates over all attributes starting with ``on_``, extracts the event
        class name from the method name, and registers it if it matches any
        class in ``LISTENS_TO``.

        The double-underscore split handles dynamic handler names like
        ``on_CrawlSetup__90_chrome_launch`` — only the prefix before ``__`` is
        used for event class matching ("CrawlSetup" → "CrawlSetupEvent").
        """
        for attr_name in dir(self):
            if not attr_name.startswith("on_"):
                continue
            prefix = attr_name.split("on_", 1)[1].split("__")[0]
            candidates = [prefix] if prefix.endswith("Event") else [prefix, prefix + "Event"]
            for event_cls in self.LISTENS_TO:
                if event_cls.__name__ in candidates:
                    handler = getattr(self, attr_name)
                    self._register(event_cls, handler)
                    break


def add_extra_context(env: dict[str, str], extra_context: Mapping[str, Any]) -> dict[str, str]:
    """Merge hook passthrough metadata into EXTRA_CONTEXT without changing argv."""
    merged_env = dict(env)
    merged_context: dict[str, Any] = {}
    existing = merged_env.get("EXTRA_CONTEXT")
    if existing:
        try:
            parsed = json.loads(existing)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            merged_context.update(parsed)
    merged_context.update(extra_context)
    merged_env["EXTRA_CONTEXT"] = json.dumps(merged_context, separators=(",", ":"), sort_keys=True)
    return merged_env


def make_hook_handler(
    service: BaseService,
    plugin: Plugin,
    hook: Hook,
    *,
    url: str,
    snapshot: Snapshot,
    output_dir: Path,
    machine: MachineService,
    phase_timeout: float = 300.0,
):
    """Create an async handler that emits a ProcessEvent for one hook.

    The handler captures ``plugin`` and ``hook`` via closure defaults so each
    handler is bound to its specific hook even though they share the same
    factory function.

    The env dict is built fresh each time from ``MachineService.shared_config``,
    so later hooks pick up any runtime config updates emitted by earlier phases.

    For background daemons, ``phase_timeout`` is used as the process timeout
    ceiling instead of the per-hook timeout (daemons should survive until
    cleanup kills them, but not outlive the entire phase).
    """

    async def handler(event: BaseEvent, _plugin=plugin, _hook=hook) -> None:
        if getattr(event, "snapshot_id", snapshot.id) != snapshot.id:
            return
        if getattr(event, "output_dir", str(output_dir)) != str(output_dir):
            return
        if _hook.event == "Snapshot" and machine.shared_config.get("ABX_SKIP_SNAPSHOT_HOOKS"):
            return
        env = machine.get_env_for_plugin(_plugin, run_output_dir=output_dir)
        if _hook.event == "Snapshot":
            limit_state = CrawlLimitState.from_config(machine.shared_config)
            if limit_state.has_limits():
                admission = limit_state.admit_snapshot(snapshot.id)
                if not admission.allowed:
                    machine.shared_config["ABX_SKIP_SNAPSHOT_HOOKS"] = True
                    return
        env = add_extra_context(env, {"snapshot_id": snapshot.id})
        if snapshot.crawl_id:
            env["CRAWL_ID"] = snapshot.crawl_id
        if snapshot.parent_snapshot_id:
            env["PARENT_SNAPSHOT_ID"] = snapshot.parent_snapshot_id
        env["SOURCE_URL"] = url
        if _hook.event == "Snapshot":
            env["SNAP_DIR"] = str(output_dir)
            env["SNAPSHOT_ID"] = snapshot.id
            env["SNAPSHOT_DEPTH"] = str(snapshot.depth)
        timeout = int(env.get(f"{_plugin.name.upper()}_TIMEOUT", env.get("TIMEOUT", "60")))
        plugin_output_dir = output_dir / _plugin.name
        plugin_output_dir.mkdir(parents=True, exist_ok=True)

        run_in_background = _hook.is_background

        # BG daemons use the full phase timeout as their ceiling (they should
        # survive until cleanup, but not outlive the phase). FG hooks use
        # the per-plugin timeout.
        effective_timeout = int(phase_timeout) if run_in_background else timeout
        process_event = ProcessEvent(
            plugin_name=_plugin.name,
            hook_name=_hook.name,
            hook_path=str(_hook.path),
            hook_args=[f"--url={url}"],
            is_background=run_in_background,
            daemon=".daemon." in _hook.name,
            output_dir=str(plugin_output_dir),
            env=env,
            snapshot_id=snapshot.id,
            timeout=effective_timeout,
            event_handler_timeout=effective_timeout + 30.0,
            event_handler_slow_timeout=slow_warning_timeout(effective_timeout),
        )
        # Background hooks all follow the same orchestrator path. Awaiting the
        # ProcessEvent only waits for ProcessService to start and register the
        # process; the hook itself keeps running in the background.
        await service.bus.emit(process_event)
        if run_in_background and hasattr(service.bus, "find"):
            await service.bus.find(
                ProcessStartedEvent,
                child_of=process_event,
                past=False,
                future=10.0,
            )

    return handler
