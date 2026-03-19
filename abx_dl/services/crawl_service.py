"""CrawlService — registers per-hook handlers for CrawlEvent.

Foreground crawl hooks are awaited serially to preserve config/binary
propagation between ordered install hooks. Background crawl daemons
(e.g. Chrome) are fire-and-forget children of the CrawlEvent so bubus
tracks the full hierarchy and enforces the phase-level timeout.

A cleanup handler registered LAST on CrawlEvent sends ProcessKillEvent
to each bg daemon, giving them time to flush output and exit gracefully
before the CrawlEvent hard timeout.
"""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import CrawlCompleted, CrawlEvent, ProcessEvent, ProcessKillEvent
from ..models import Snapshot
from ..plugins import Hook, Plugin
from .base import BaseService
from .machine_service import MachineService


class CrawlService(BaseService):
    """Registers a handler per crawl hook that builds env and emits ProcessEvent."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [CrawlEvent, CrawlCompleted]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ProcessEvent, ProcessKillEvent]

    def __init__(
        self,
        bus: EventBus,
        *,
        url: str,
        snapshot: Snapshot,
        output_dir: Path,
        machine: MachineService,
        hooks: list[tuple[Plugin, Hook]],
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.machine = machine
        self.hooks = hooks
        super().__init__(bus)
        self._register_hook_handlers()

    def _register_hook_handlers(self) -> None:
        for plugin, hook in self.hooks:
            handler = self._make_hook_handler(plugin, hook)
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self.bus.on(CrawlEvent, handler)

        # Register cleanup handler LAST — runs after all hook handlers finish,
        # SIGTERMs bg daemons so they can exit before the phase-level timeout.
        self.bus.on(CrawlEvent, self._cleanup_bg_hooks)

    def _make_hook_handler(self, plugin: Plugin, hook: Hook):
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
                event_timeout=timeout + 30.0,
                event_handler_timeout=timeout + 30.0,
            )
            if _hook.is_background:
                self.bus.emit(process_event)   # fire-and-forget child of CrawlEvent
            else:
                await self.bus.emit(process_event)

        return handler

    async def _cleanup_bg_hooks(self, event: BaseEvent) -> None:
        """SIGTERM background crawl daemons so they can flush and exit gracefully."""
        for plugin, hook in self.hooks:
            if hook.is_background:
                plugin_output_dir = self.output_dir / plugin.name
                await self.bus.emit(ProcessKillEvent(
                    plugin_name=plugin.name,
                    hook_name=hook.name,
                    output_dir=str(plugin_output_dir),
                ))

    async def on_CrawlCompleted(self, event: CrawlCompleted) -> None:
        """CrawlCompleted is informational — cleanup already happened in CrawlEvent."""
        pass
