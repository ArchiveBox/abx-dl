"""MachineService — single owner of shared config and env building."""

from pathlib import Path
from typing import Any, ClassVar

from bubus import BaseEvent, EventBus

from ..config import build_env_for_plugin, set_config
from ..events import MachineEvent
from ..plugins import Plugin
from .base import BaseService


class MachineService(BaseService):
    """Owns shared_config. All config reads and writes go through this service."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [MachineEvent]

    def __init__(self, bus: EventBus, *, initial_config: dict[str, Any] | None = None):
        self.shared_config: dict[str, Any] = dict(initial_config) if initial_config else {}
        super().__init__(bus)

    async def on_MachineEvent(self, event: MachineEvent) -> None:
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

    def get_env_for_plugin(self, plugin: Plugin, *, run_output_dir: Path) -> dict[str, str]:
        """Build env dict for a plugin using current shared_config state."""
        return build_env_for_plugin(
            plugin.name, plugin.config_schema, self.shared_config,
            run_output_dir=run_output_dir,
        )
