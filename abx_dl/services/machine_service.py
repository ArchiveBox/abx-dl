"""MachineService — runtime config owner with optional derived-cache persistence."""

from pathlib import Path
from typing import Any, ClassVar

from abxbus import BaseEvent, EventBus

from ..config import (
    apply_runtime_path_defaults,
    build_env_for_plugin,
    get_config,
    get_derived_config,
    set_derived_config,
    unset_derived_config,
)
from ..events import MachineEvent
from ..models import Plugin
from .base import BaseService


def _is_path_like_binary_value(value: object) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    return raw.startswith(("~", ".", "/")) or "/" in raw or "\\" in raw


class MachineService(BaseService):
    """Owns the runtime config dict used to build hook environments.

    ``declared_config`` starts from ``get_config()`` (defaults + user config +
    current environment + caller overrides). Derived cache values are kept
    separate in ``derived_config`` and are only reused by the binary-resolution
    layer.

    Runtime updates are applied via internal ``MachineEvent`` records emitted by
    services like ``BinaryService``. Those updates affect the current run
    immediately via ``shared_config``. In standalone ``abx-dl`` runs they are
    also persisted to ``derived.env`` so future runs can reuse them. ArchiveBox
    passes its own derived cache in at startup and does not persist these events
    to ``Machine.config`` anymore.

    State:
    - ``declared_config``: stable config used for required_binaries template hydration
    - ``shared_config``: mutable runtime config used for hook env generation
    - ``derived_config``: cache entries loaded from ``derived.env`` or caller-provided overrides

    Config update flow::

        BinaryService emits MachineEvent(method="update", key="config/DEMO_FLAG", value="ready")
            → MachineService.on_MachineEvent() updates shared_config + derived cache

    Standalone persistent store: ``~/.config/abx/derived.env`` is updated via
    ``set_derived_config()`` when ``persist_derived=True``. ``config.env``
    remains user-owned and is only written by ``abx-dl config --set``.

    Two MachineEvent formats are supported:
    1. Batch: ``{"type": "Machine", "config": {"KEY1": "val1", "KEY2": "val2"}}``
    2. Single: ``{"type": "Machine", "_method": "update", "key": "config/KEY", "value": "val"}``
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [MachineEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = []

    def __init__(
        self,
        bus: EventBus,
        *,
        initial_config: dict[str, Any] | None = None,
        initial_derived_config: dict[str, Any] | None = None,
        persist_derived: bool = True,
    ):
        self.persist_derived = persist_derived
        self.declared_config: dict[str, Any] = get_config()
        if initial_config:
            self.declared_config.update(initial_config)
        self.declared_config = apply_runtime_path_defaults(self.declared_config)
        self.shared_config: dict[str, Any] = dict(self.declared_config)
        self.derived_config: dict[str, Any] = get_derived_config(self.shared_config) if persist_derived else {}
        if initial_derived_config:
            self.derived_config.update(initial_derived_config)
        super().__init__(bus)

    def update_derived(self, **kwargs: Any) -> None:
        updates = {key: value for key, value in kwargs.items() if value is not None}
        if not updates:
            return
        self.derived_config.update(updates)
        if not self.persist_derived:
            return
        try:
            set_derived_config(self.shared_config, **updates)
        except Exception:
            pass

    def unset_derived(self, *keys: str) -> None:
        removed = False
        for key in keys:
            if key in self.derived_config:
                self.derived_config.pop(key, None)
                removed = True
        if not removed or not self.persist_derived:
            return
        try:
            unset_derived_config(*keys, current_config=self.shared_config)
        except Exception:
            pass

    async def on_MachineEvent(self, event: MachineEvent) -> None:
        """Apply a config update to shared_config and the derived persistent store.

        Handles both batch format (event.config dict) and single-key format
        (event.method='update', event.key='config/KEY', event.value='val').
        """
        if event.config is not None:
            self.shared_config.update(event.config)
            self.shared_config = apply_runtime_path_defaults(self.shared_config)
            self.update_derived(**event.config)
            return
        if event.method != "update":
            return
        key = event.key.replace("config/", "")
        if key:
            self.shared_config[key] = event.value
            self.shared_config = apply_runtime_path_defaults(self.shared_config)
            self.update_derived(**{key: event.value})

    def get_env_for_plugin(self, plugin: Plugin, *, run_output_dir: Path) -> dict[str, str]:
        """Build a complete env dict for running a plugin hook.

        Merges (in priority order): os.environ < GLOBAL_DEFAULTS < plugin schema
        defaults < shared_config overrides < runtime path derivations.

        Called fresh before each hook execution, so it always reflects the latest
        shared_config state (including updates from hooks that ran earlier in the
        same InstallEvent / CrawlSetupEvent / SnapshotEvent chain).
        """
        effective_config = dict(self.shared_config)
        for key, value in self.derived_config.items():
            if value is None:
                continue
            if key.endswith("_BINARY"):
                current_value = str(effective_config.get(key) or self.declared_config.get(key) or "").strip()
                if _is_path_like_binary_value(current_value):
                    continue
                derived_value = str(value).strip()
                if not _is_path_like_binary_value(derived_value):
                    continue
                if current_value and Path(derived_value).expanduser().name != current_value:
                    continue
                effective_config[key] = derived_value
                continue
            effective_config[key] = value
        return build_env_for_plugin(
            plugin.name,
            plugin.config_schema,
            effective_config,
            config_path=plugin.path / "config.json",
            run_output_dir=run_output_dir,
        )

    def get_declared_env_for_plugin(self, plugin: Plugin, *, run_output_dir: Path) -> dict[str, str]:
        """Build env from stable user/default config for required_binaries hydration."""
        return build_env_for_plugin(
            plugin.name,
            plugin.config_schema,
            self.declared_config,
            config_path=plugin.path / "config.json",
            run_output_dir=run_output_dir,
        )
