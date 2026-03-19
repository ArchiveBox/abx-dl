"""SnapshotService — orchestrates the snapshot extraction phase."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import ProcessEvent, ProcessKillEvent, SnapshotEvent
from ..models import Snapshot
from ..models import Hook, Plugin
from .base import BaseService
from .machine_service import MachineService


class SnapshotService(BaseService):
    """Orchestrates the snapshot phase: extraction hooks, then daemon cleanup.

    The SnapshotEvent is emitted by CrawlService as a child of CrawlEvent,
    so the full snapshot phase sits inside the crawl event tree::

        CrawlEvent
        ├── ... (crawl hooks)
        ├── SnapshotEvent                              # emitted by CrawlService
        │   │
        │   │  ── Snapshot hook handlers run serially ──
        │   │
        │   ├── on_Snapshot__06_wget.finite.bg         # bg: fire-and-forget
        │   ├── on_Snapshot__09_chrome_launch.daemon.bg # bg: fire-and-forget
        │   ├── on_Snapshot__10_chrome_tab.daemon.bg   # bg: fire-and-forget
        │   ├── on_Snapshot__11_chrome_wait             # FG: blocks until Chrome tab ready
        │   ├── on_Snapshot__30_chrome_navigate         # FG: navigates to URL
        │   ├── on_Snapshot__54_title                   # FG: extracts page title
        │   ├── on_Snapshot__58_htmltotext              # FG: html → text
        │   ├── ...                                    # more extractors
        │   ├── on_Snapshot__93_hashes                  # FG: compute output hashes
        │   │
        │   │  ── After all hook handlers return ──
        │   │
        │   └── _cleanup_bg_hooks                      # SIGTERMs snapshot bg daemons
        │       ├── ProcessKillEvent (chrome_launch)
        │       ├── ProcessKillEvent (chrome_tab)
        │       └── ...
        │
        └── ... (crawl cleanup)

    The execution model is identical to CrawlService — see CrawlService docstring
    for details on fg vs bg hooks, config propagation, and bubus behavior.

    Edge case: snapshot bg daemons (chrome_tab, chrome_launch) run throughout the
    snapshot phase and are killed at cleanup. If a bg daemon exits early with an
    error, the ProcessEvent still completes (with failed status) but doesn't block
    the remaining fg hooks from running.
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
        """Register one SnapshotEvent handler per hook, plus cleanup.

        Same pattern as CrawlService._register_hook_handlers — hooks are
        pre-sorted, handlers registered in order, cleanup handler registered last.
        """
        for plugin, hook in self.hooks:
            handler = self._make_hook_handler(plugin, hook)
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self.bus.on(SnapshotEvent, handler)

        self.bus.on(SnapshotEvent, self._cleanup_bg_hooks)

    def _make_hook_handler(self, plugin: Plugin, hook: Hook):
        """Create an async handler that emits a ProcessEvent for one snapshot hook.

        See CrawlService._make_hook_handler for the fg/bg execution model.
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
                self.bus.emit(process_event)   # fire-and-forget child of SnapshotEvent
            else:
                await self.bus.emit(process_event)

        return handler

    async def _cleanup_bg_hooks(self, event: BaseEvent) -> None:
        """SIGTERM all background snapshot daemons so they can flush and exit.

        Runs after all snapshot hook handlers have returned. Bg daemons like
        chrome_tab receive SIGTERM, giving them a chance to write final output
        (e.g. console logs, network captures) before exiting.

        Hooks that already exited naturally (finite bg hooks) will have no PID
        file — the kill is a safe no-op for them.
        """
        for plugin, hook in self.hooks:
            if hook.is_background:
                plugin_output_dir = self.output_dir / plugin.name
                await self.bus.emit(ProcessKillEvent(
                    plugin_name=plugin.name,
                    hook_name=hook.name,
                    output_dir=str(plugin_output_dir),
                ))
