"""CrawlService — orchestrates the crawl phase by dispatching plugin hooks."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import CrawlEvent, ProcessEvent, ProcessKillEvent, SnapshotEvent
from ..models import Snapshot
from ..plugins import Hook, Plugin
from .base import BaseService
from .machine_service import MachineService


class CrawlService(BaseService):
    """Orchestrates the crawl phase: installs, daemons, then snapshot extraction.

    Lifecycle (all within a single CrawlEvent)::

        CrawlEvent                                # emitted by orchestrator.download()
        │
        │  ── Crawl hook handlers run serially (sorted by hook.order) ──
        │
        ├── on_Crawl__10_wget_install.finite.bg   # bg: fire-and-forget ProcessEvent
        ├── on_Crawl__41_trafilatura_install...    # bg: fire-and-forget ProcessEvent
        ├── on_Crawl__70_chrome_install.finite.bg  # bg: fire-and-forget ProcessEvent
        ├── on_Crawl__90_chrome_launch.daemon.bg   # bg: fire-and-forget ProcessEvent
        ├── on_Crawl__91_chrome_wait               # FG: awaits ProcessEvent (blocks)
        │
        │  ── After all hook handlers return ──
        │
        ├── _emit_snapshot_event                   # FG: emits SnapshotEvent (blocks)
        │   └── SnapshotEvent (full snapshot phase runs here)
        │
        └── _cleanup_bg_hooks                      # FG: SIGTERMs all bg crawl daemons
            ├── ProcessKillEvent (chrome_launch)
            ├── ProcessKillEvent (wget_install)
            └── ...

    Hook execution model:

    - **Background hooks** (`is_background=True`, filename contains `.bg.`):
      The handler calls ``self.bus.emit(process_event)`` without await, which
      creates a fire-and-forget child ProcessEvent of the CrawlEvent. bubus runs
      it concurrently thanks to ``event_concurrency='parallel'`` on the bus.
      The handler function returns immediately, allowing the next handler to run.

    - **Foreground hooks** (`is_background=False`):
      The handler calls ``await self.bus.emit(process_event)``, which blocks until
      the ProcessEvent and all its children (BinaryEvent, MachineEvent, etc.)
      complete. This is how serial ordering and config propagation work — each
      fg hook sees the config updates from all prior hooks.

    Important: bg hooks are NOT awaited by the runner. If a fg hook depends on a
    bg hook's output (e.g. chrome_wait depends on chrome_install), the fg hook
    must poll/retry internally. The runner treats bg hooks as black boxes that
    may exit early (finite) or run until killed (daemon) — it doesn't distinguish.

    State:
    - ``self.hooks``: sorted list of (Plugin, Hook) tuples, set at init, immutable
    - ``self.machine``: MachineService reference for building plugin env dicts
    - Config propagation happens via MachineEvent → MachineService.shared_config,
      which is read each time ``get_env_for_plugin()`` is called

    bubus details:
    - Handlers are registered via ``bus.on(CrawlEvent, handler)`` in sort order.
      bubus executes handlers SERIALLY in registration order (default
      ``event_handler_concurrency='serial'``), so hook ordering is preserved.
    - ``bus.emit()`` (no await) creates a concurrent child event that bubus
      processes in the background. The child is still part of the CrawlEvent tree
      and subject to CrawlEvent's timeout.
    - ``await bus.emit()`` is a "queue-jump": bubus processes the emitted event
      and ALL its descendants synchronously before returning to the caller.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [CrawlEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ProcessEvent, ProcessKillEvent, SnapshotEvent]

    def __init__(
        self,
        bus: EventBus,
        *,
        url: str,
        snapshot: Snapshot,
        output_dir: Path,
        machine: MachineService,
        hooks: list[tuple[Plugin, Hook]],
        crawl_only: bool = False,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.machine = machine
        self.hooks = hooks
        self.crawl_only = crawl_only
        super().__init__(bus)
        self._register_hook_handlers()

    def _register_hook_handlers(self) -> None:
        """Register one CrawlEvent handler per hook, plus snapshot and cleanup.

        Registration order determines execution order (bubus runs handlers serially).
        Hooks are pre-sorted by ``hook.sort_key`` = ``(order, name)``, so registration
        order matches the numeric prefix in the hook filename (e.g. __10, __41, __70).

        Three phases are registered in order:
        1. One handler per crawl hook (install/daemon hooks)
        2. ``_emit_snapshot_event`` — starts the snapshot extraction phase
        3. ``_cleanup_bg_hooks`` — SIGTERMs any still-running bg daemons
        """
        for plugin, hook in self.hooks:
            handler = self._make_hook_handler(plugin, hook)
            # Set __name__/__qualname__ so bubus log output shows the hook name
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self.bus.on(CrawlEvent, handler)

        if not self.crawl_only:
            self.bus.on(CrawlEvent, self._emit_snapshot_event)

        self.bus.on(CrawlEvent, self._cleanup_bg_hooks)

    def _make_hook_handler(self, plugin: Plugin, hook: Hook):
        """Create an async handler that emits a ProcessEvent for one hook.

        The handler captures ``plugin`` and ``hook`` via closure defaults so each
        handler is bound to its specific hook even though they share the same
        factory method.

        The env dict is built fresh each time from ``MachineService.shared_config``,
        so fg hooks pick up config updates (e.g. CHROME_BINARY path) from earlier
        hooks in the same CrawlEvent.
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
                # Fire-and-forget: emits ProcessEvent as a concurrent child of
                # CrawlEvent. bubus processes it in the background. The handler
                # returns immediately so the next hook handler can start.
                self.bus.emit(process_event)
            else:
                # Foreground: blocks until the subprocess and all its side-effect
                # events (BinaryEvent, MachineEvent) complete. This preserves
                # ordering — the next handler won't start until this one finishes.
                await self.bus.emit(process_event)

        return handler

    async def _emit_snapshot_event(self, event: BaseEvent) -> None:
        """Start the snapshot extraction phase as a child of CrawlEvent.

        This handler runs after all crawl hook handlers have returned. Note that
        bg crawl hooks may still be running (their ProcessEvents are concurrent
        children of CrawlEvent). The snapshot phase starts regardless — if a
        snapshot hook depends on a bg crawl hook being ready (e.g. chrome), the
        snapshot hook's own fg wait handler (e.g. on_Snapshot__11_chrome_wait)
        must poll until the dependency is satisfied.

        The SnapshotEvent is awaited, so it completes (including all snapshot
        hooks and snapshot cleanup) before this handler returns.
        """
        await self.bus.emit(SnapshotEvent(
            url=self.url,
            snapshot_id=self.snapshot.id,
            output_dir=str(self.output_dir),
        ))

    async def _cleanup_bg_hooks(self, event: BaseEvent) -> None:
        """SIGTERM all background crawl daemons so they can flush and exit.

        Runs after _emit_snapshot_event completes (i.e. after the entire snapshot
        phase). Sends ProcessKillEvent for each bg hook, which ProcessService
        handles by reading the PID file and sending SIGTERM.

        Bg hooks that already exited (finite bg hooks like installers) will have
        no PID file, so the kill is a no-op for them.
        """
        for plugin, hook in self.hooks:
            if hook.is_background:
                plugin_output_dir = self.output_dir / plugin.name
                await self.bus.emit(ProcessKillEvent(
                    plugin_name=plugin.name,
                    hook_name=hook.name,
                    output_dir=str(plugin_output_dir),
                ))
