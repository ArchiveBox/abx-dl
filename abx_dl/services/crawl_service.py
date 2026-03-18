"""CrawlService — registers per-hook handlers for CrawlEvent and SnapshotEvent."""

from pathlib import Path
from typing import Any, ClassVar

from bubus import BaseEvent, EventBus

from ..config import build_env_for_plugin
from ..events import CrawlEvent, ProcessEvent, SnapshotEvent
from ..models import Snapshot
from ..plugins import Hook, Plugin
from .base import BaseService


class CrawlService(BaseService):
    """Registers a handler per hook that builds env and emits ProcessEvent."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [CrawlEvent, SnapshotEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ProcessEvent]

    def __init__(
        self,
        bus: EventBus,
        *,
        url: str,
        snapshot: Snapshot,
        output_dir: Path,
        shared_config: dict[str, Any],
        crawl_hooks: list[tuple[Plugin, Hook]],
        snapshot_hooks: list[tuple[Plugin, Hook]],
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.shared_config = shared_config
        self.crawl_hooks = crawl_hooks
        self.snapshot_hooks = snapshot_hooks
        # Don't call super().__init__ yet — we register dynamic handlers manually
        self.bus = bus
        self._register_hook_handlers()

    def _register_hook_handlers(self) -> None:
        """Register a handler per plugin hook for CrawlEvent and SnapshotEvent."""
        for plugin, hook in self.crawl_hooks:
            handler = self._make_hook_handler(plugin, hook, CrawlEvent)
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self.bus.on(CrawlEvent, handler)

        for plugin, hook in self.snapshot_hooks:
            handler = self._make_hook_handler(plugin, hook, SnapshotEvent)
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self.bus.on(SnapshotEvent, handler)

    def _make_hook_handler(self, plugin: Plugin, hook: Hook, event_cls: type[BaseEvent]):
        """Create an async handler that builds env and emits ProcessEvent."""
        async def handler(event: BaseEvent, _plugin=plugin, _hook=hook) -> None:
            env = build_env_for_plugin(
                _plugin.name, _plugin.config_schema, self.shared_config,
                run_output_dir=self.output_dir,
            )
            timeout = int(env.get(f"{_plugin.name.upper()}_TIMEOUT", env.get('TIMEOUT', '60')))
            plugin_output_dir = self.output_dir / _plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)

            # Queue-jump: ProcessEvent handler runs the subprocess synchronously
            await self.bus.emit(ProcessEvent(
                url=self.url, snapshot_id=self.snapshot.id,
                plugin_name=_plugin.name, hook_name=_hook.name,
                hook_path=str(_hook.path), hook_language=_hook.language,
                is_background=_hook.is_background,
                output_dir=str(plugin_output_dir), env=env, timeout=timeout,
            ))

        return handler
