"""SnapshotService — registers per-hook handlers for SnapshotEvent."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import ProcessEvent, SnapshotEvent
from ..models import Snapshot
from ..plugins import Hook, Plugin
from .base import BaseService
from .machine_service import MachineService


class SnapshotService(BaseService):
    """Registers a handler per snapshot hook that builds env and emits ProcessEvent."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [SnapshotEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ProcessEvent]

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

    def _make_hook_handler(self, plugin: Plugin, hook: Hook):
        async def handler(event: BaseEvent, _plugin=plugin, _hook=hook) -> None:
            env = self.machine.get_env_for_plugin(_plugin, run_output_dir=self.output_dir)
            timeout = int(env.get(f"{_plugin.name.upper()}_TIMEOUT", env.get('TIMEOUT', '60')))
            plugin_output_dir = self.output_dir / _plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)

            await self.bus.emit(ProcessEvent(
                plugin_name=_plugin.name, hook_name=_hook.name,
                hook_path=str(_hook.path),
                hook_args=[f'--url={self.url}', f'--snapshot-id={self.snapshot.id}'],
                is_background=_hook.is_background,
                output_dir=str(plugin_output_dir), env=env,
                snapshot_id=self.snapshot.id, timeout=timeout,
            ))

        return handler
