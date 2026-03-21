"""MachineService — single owner of shared config state."""

from pathlib import Path
from typing import Any, ClassVar

from abxbus import BaseEvent, EventBus

from ..config import build_env_for_plugin, set_config
import json

from ..events import MachineEvent, ProcessStdoutEvent
from ..models import Plugin
from .base import BaseService


class MachineService(BaseService):
    """Owns the shared_config dict — the single source of truth for runtime config.

    All config reads and writes go through this service. Hooks propagate config
    updates by outputting ``{"type": "Machine", "config": {"KEY": "value"}}``
    JSONL to stdout, which ProcessService parses and emits as MachineEvent.

    State:
    - ``shared_config``: mutable dict updated by MachineEvent handlers. Read by
      ``get_env_for_plugin()`` each time a hook needs an env dict. This is how
      config propagates between hooks: hook A sets CHROME_BINARY via MachineEvent,
      hook B's env dict picks it up when get_env_for_plugin() is called later.

    Config update flow::

        Hook stdout: {"type": "Machine", "config": {"CHROME_BINARY": "/path/to/chrome"}}
            → ProcessService emits ProcessStdoutEvent(record_type='Machine')
            → MachineService.on_ProcessStdoutEvent() emits MachineEvent
            → MachineService.on_MachineEvent() updates shared_config + persistent store

    Persistent store: ``~/.config/abx/config.env`` is updated via ``set_config()``
    so config persists across runs (e.g. cached binary paths). Persistent write
    failures are silently ignored — the in-memory shared_config is always authoritative.

    Two MachineEvent formats are supported (both emitted by hooks):
    1. Batch: ``{"type": "Machine", "config": {"KEY1": "val1", "KEY2": "val2"}}``
    2. Single: ``{"type": "Machine", "_method": "update", "key": "config/KEY", "value": "val"}``
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessStdoutEvent, MachineEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [MachineEvent]

    def __init__(self, bus: EventBus, *, initial_config: dict[str, Any] | None = None):
        self.shared_config: dict[str, Any] = dict(initial_config) if initial_config else {}
        super().__init__(bus)

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Route type=Machine records to MachineEvent."""
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict) or record.pop("type", "") != "Machine":
            return
        await self.bus.emit(MachineEvent(**record))

    async def on_MachineEvent(self, event: MachineEvent) -> None:
        """Apply a config update to shared_config and the persistent store.

        Handles both batch format (event.config dict) and single-key format
        (event.method='update', event.key='config/KEY', event.value='val').
        """
        if event.config is not None:
            self.shared_config.update(event.config)
            try:
                set_config(**{k: v for k, v in event.config.items() if v is not None})
            except Exception:
                pass  # persistent store write is best-effort
            return
        if event.method != "update":
            return
        key = event.key.replace("config/", "")
        if key:
            self.shared_config[key] = event.value
            try:
                set_config(None, **{key: event.value})
            except Exception:
                pass

    def get_env_for_plugin(self, plugin: Plugin, *, run_output_dir: Path) -> dict[str, str]:
        """Build a complete env dict for running a plugin hook.

        Merges (in priority order): os.environ < GLOBAL_DEFAULTS < plugin schema
        defaults < shared_config overrides < runtime path derivations.

        Called fresh before each hook execution, so it always reflects the latest
        shared_config state (including updates from hooks that ran earlier in the
        same CrawlEvent/SnapshotEvent).
        """
        return build_env_for_plugin(
            plugin.name,
            plugin.config_schema,
            self.shared_config,
            run_output_dir=run_output_dir,
        )
