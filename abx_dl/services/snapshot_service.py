"""SnapshotService — registers per-hook handlers for SnapshotEvent.

SnapshotEvent is emitted by CrawlService as a child of CrawlEvent, so
snapshot hooks and their bg daemons sit inside the crawl event tree:

    CrawlEvent
      ├── ...
      ├── SnapshotEvent (this service handles it)
      │   ├── ProcessEvent (fg snapshot hooks, serial)
      │   ├── ProcessEvent (bg snapshot daemons, fire-and-forget)
      │   └── cleanup: ProcessKillEvent per bg daemon
      └── ...

A cleanup handler registered LAST on SnapshotEvent sends ProcessKillEvent
to each bg daemon, giving them time to flush output and exit gracefully
before the SnapshotEvent hard timeout.
"""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import ProcessEvent, ProcessKillEvent, SnapshotEvent
from ..models import Snapshot
from ..plugins import Hook, Plugin
from .base import BaseService
from .machine_service import MachineService


class SnapshotService(BaseService):
    """Registers snapshot hook handlers, cleans up bg daemons on completion.

        SnapshotEvent
          ├── ProcessEvent (fg snapshot hooks, serial)
          ├── ProcessEvent (bg snapshot daemons, fire-and-forget)
          └── cleanup: ProcessKillEvent per bg daemon
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [SnapshotEvent]
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
            self.bus.on(SnapshotEvent, handler)

        # Register cleanup handler LAST — runs after all hook handlers finish,
        # SIGTERMs bg daemons so they can exit before the phase-level timeout.
        self.bus.on(SnapshotEvent, self._cleanup_bg_hooks)

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
                event_handler_timeout=timeout + 30.0,
            )
            if _hook.is_background:
                self.bus.emit(process_event)   # fire-and-forget child of SnapshotEvent
            else:
                await self.bus.emit(process_event)

        return handler

    async def _cleanup_bg_hooks(self, event: BaseEvent) -> None:
        """SIGTERM background snapshot daemons so they can flush and exit gracefully."""
        for plugin, hook in self.hooks:
            if hook.is_background:
                plugin_output_dir = self.output_dir / plugin.name
                await self.bus.emit(ProcessKillEvent(
                    plugin_name=plugin.name,
                    hook_name=hook.name,
                    output_dir=str(plugin_output_dir),
                ))
