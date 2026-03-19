"""SnapshotService — orchestrates the snapshot extraction phase."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import ProcessEvent, ProcessKillEvent, SnapshotEvent
from ..models import Snapshot
from ..models import Hook, Plugin
from .base import HookRunnerService
from .machine_service import MachineService


class SnapshotService(HookRunnerService):
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

    See HookRunnerService for the shared fg/bg execution model and config propagation.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [SnapshotEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ProcessEvent, ProcessKillEvent]
    EVENT_CLASS = SnapshotEvent

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
