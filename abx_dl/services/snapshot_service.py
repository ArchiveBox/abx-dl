"""SnapshotService — orchestrates the snapshot extraction phase."""

import asyncio
import json

from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from abxbus import BaseEvent, EventBus

from ..events import (
    ProcessEvent,
    ProcessKillEvent,
    ProcessStdoutEvent,
    SnapshotCleanupEvent,
    SnapshotCompletedEvent,
    SnapshotEvent,
    slow_warning_timeout,
)
from ..limits import CrawlLimitState
from ..models import Snapshot
from ..models import Hook, Plugin
from .base import BaseService, make_hook_handler
from .machine_service import MachineService

if TYPE_CHECKING:
    from .process_service import ProcessService


class SnapshotService(BaseService):
    """Orchestrates the snapshot phase: extraction hooks, cleanup, completion.

    The SnapshotEvent is emitted by CrawlService.on_CrawlStartEvent::

        CrawlEvent
        ├── CrawlSetupEvent (crawl hooks)
        ├── CrawlStartEvent
        │   └── SnapshotEvent (depth=0)                    # triggers this service
        │       │
        │       │  ── Snapshot hook handlers run serially ──
        │       │  (each wrapped with depth>0 guard)
        │       │
        │       ├── on_Snapshot__06_wget.finite.bg
        │       │   └── ProcessEvent
        │       │       ├── ProcessStdoutEvent
        │       │       │   ├── SnapshotEvent (depth>0, discovered URL — ignored)
        │       │       │   └── ArchiveResultEvent (inline)
        │       │       └── ProcessCompletedEvent
        │       │           └── ArchiveResultEvent (enriched)
        │       ├── on_Snapshot__09_chrome_launch.daemon.bg
        │       ├── on_Snapshot__54_title
        │       ├── on_Snapshot__93_hashes
        │       │
        │       │  ── After all hook handlers ──
        │       │
        │       ├── SnapshotCleanupEvent
        │       │   └── ProcessKillEvent × N
        │       └── SnapshotCompletedEvent
        │
        ├── CrawlCleanupEvent
        └── CrawlCompletedEvent

    Depth guard: SnapshotEvents with depth > 0 are silently ignored by
    abx-dl. These represent discovered URLs from hook output (recursive
    crawling). ArchiveBox handles them by queueing new snapshots.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        ProcessStdoutEvent,
        SnapshotEvent,
        SnapshotCleanupEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        SnapshotEvent,
        ProcessEvent,
        ProcessKillEvent,
        SnapshotCleanupEvent,
        SnapshotCompletedEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        url: str,
        snapshot: Snapshot,
        output_dir: Path,
        machine: MachineService,
        process: "ProcessService",
        hooks: list[tuple[Plugin, Hook]],
        phase_timeout: float = 300.0,
    ):
        self.url = url
        self.snapshot = snapshot
        self.output_dir = output_dir
        self.machine = machine
        self.process = process
        self.hooks = hooks
        self.phase_timeout = phase_timeout
        self.limit_state = CrawlLimitState.from_config(machine.shared_config)
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register handlers in correct order.

        1. ProcessStdoutEvent → SnapshotEvent routing
        2. Per-hook handlers on SnapshotEvent (each wrapped with depth>0 guard)
        3. on_SnapshotEvent last (cleanup + completion)
        """
        self._register(ProcessStdoutEvent, self.on_ProcessStdoutEvent)
        self._register(SnapshotEvent, self.on_SnapshotEvent__LimitsGate)

        for plugin, hook in self.hooks:
            inner = make_hook_handler(
                self,
                plugin,
                hook,
                url=self.url,
                snapshot=self.snapshot,
                output_dir=self.output_dir,
                machine=self.machine,
                phase_timeout=self.phase_timeout,
            )

            async def guarded(event: SnapshotEvent, _inner=inner) -> None:
                if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
                    return
                if self.machine.shared_config.get("ABX_SKIP_SNAPSHOT_HOOKS"):
                    return
                await _inner(event)

            hook_handler_name = f"{hook.name}__{self.snapshot.id.replace('-', '_')[-12:]}"
            guarded.__name__ = hook_handler_name
            guarded.__qualname__ = hook_handler_name
            self._register(SnapshotEvent, guarded)

        self._register(SnapshotEvent, self.on_SnapshotEvent)
        self._register(SnapshotCleanupEvent, self.on_SnapshotCleanupEvent)

    async def on_SnapshotEvent__LimitsGate(self, event: SnapshotEvent) -> None:
        if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
            return
        if not self.limit_state.has_limits():
            self.machine.shared_config.pop("ABX_SKIP_SNAPSHOT_HOOKS", None)
            return
        admission = self.limit_state.admit_snapshot(event.snapshot_id)
        if admission.allowed:
            self.machine.shared_config.pop("ABX_SKIP_SNAPSHOT_HOOKS", None)
            return
        self.machine.shared_config["ABX_SKIP_SNAPSHOT_HOOKS"] = True

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Route type=Snapshot records to SnapshotEvent.

        Discovered URLs default to depth=1 since any hook-discovered URL
        is at least one level deep from the root snapshot.
        """
        if event.snapshot_id != self.snapshot.id:
            return
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict) or record.get("type") != "Snapshot":
            return
        if not self.limit_state.should_emit_discovered_snapshots():
            return
        await self.bus.emit(
            SnapshotEvent(
                url=record.get("url", ""),
                snapshot_id=record.get("id", record.get("snapshot_id", "")),
                output_dir=event.output_dir,
                depth=int(record.get("depth", 1)),
                parent_snapshot_id=event.snapshot_id,
                event_timeout=event.event_timeout,
                event_handler_slow_timeout=slow_warning_timeout(event.event_timeout),
            ),
        )

    async def on_SnapshotEvent(self, event: SnapshotEvent) -> None:
        """Emit cleanup and completion after all snapshot hooks have run.

        SnapshotEvents emitted from hook stdout are already ignored by the
        snapshot_id/output_dir match above, so explicit child snapshot runs
        can reuse this same service even when ``depth > 0``.
        """
        if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
            return
        await self.process.wait_for_background_monitors(include_daemons=False)
        url = self.url
        snapshot_id = self.snapshot.id
        output_dir = str(self.output_dir)
        await self.bus.emit(
            SnapshotCleanupEvent(
                url=url,
                snapshot_id=snapshot_id,
                output_dir=output_dir,
                event_timeout=self.phase_timeout,
                event_handler_slow_timeout=slow_warning_timeout(self.phase_timeout),
            ),
        )
        await self.bus.emit(SnapshotCompletedEvent(url=url, snapshot_id=snapshot_id, output_dir=output_dir))

    async def on_SnapshotCleanupEvent(self, event: SnapshotCleanupEvent) -> None:
        """SIGTERM all background snapshot daemons so they can flush and exit.

        Each daemon gets its plugin's timeout (PLUGINNAME_TIMEOUT) as the
        grace period before SIGKILL.
        """
        if event.snapshot_id != self.snapshot.id or event.output_dir != str(self.output_dir):
            return
        pending_kills = []
        for plugin, hook in self.hooks:
            if hook.is_background:
                env = self.machine.get_env_for_plugin(plugin, run_output_dir=self.output_dir)
                grace = float(env.get(f"{plugin.name.upper()}_TIMEOUT", env.get("TIMEOUT", "60")))
                plugin_output_dir = self.output_dir / plugin.name
                pending_kills.append(
                    self.bus.emit(
                        ProcessKillEvent(
                            plugin_name=plugin.name,
                            hook_name=hook.name,
                            output_dir=str(plugin_output_dir),
                            grace_period=grace,
                            event_timeout=grace + 10.0,
                        ),
                    ),
                )
        if pending_kills:
            await asyncio.gather(*pending_kills)
