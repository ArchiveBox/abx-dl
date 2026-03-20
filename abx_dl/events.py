"""Event schemas for the abx-dl bubus event bus.

Events form a hierarchy during execution::

    CrawlEvent                                      # orchestrator.download()
    ├── CrawlSetupEvent                             # crawl hooks run here
    │   ├── ProcessEvent (on_Crawl hooks)
    │   │   ├── BinaryEvent → ProcessEvent (provider install) → MachineEvent
    │   │   └── ProcessCompleted
    │   └── ...
    ├── CrawlStartEvent                    # triggers snapshot phase
    │   └── SnapshotEvent
    │       ├── ProcessEvent (on_Snapshot hooks)
    │       │   └── ProcessCompleted
    │       ├── SnapshotCleanupEvent
    │       │   └── ProcessKillEvent × N
    │       └── SnapshotCompletedEvent
    ├── CrawlCleanupEvent
    │   └── ProcessKillEvent × N
    └── CrawlCompletedEvent

Event types:
- **Lifecycle events** drive phases: CrawlEvent, CrawlSetupEvent,
  CrawlStartEvent, SnapshotEvent, SnapshotCleanupEvent,
  SnapshotCompletedEvent, CrawlCleanupEvent, CrawlCompletedEvent
- **Command events** trigger actions: ProcessEvent, ProcessKillEvent,
  BinaryEvent, MachineEvent
- **Completion events** notify results: ProcessCompleted

bubus behavior:
- Each event has ``event_timeout`` — the hard deadline for the event and all its
  children. If exceeded, bubus cancels pending handlers and child events.
- ``event_concurrency='parallel'`` on ProcessEvent allows bg hooks to run
  concurrently with the parent event's handler chain.
- ``event_handler_timeout`` on ProcessEvent controls the per-handler timeout
  (how long ProcessService.on_ProcessEvent can run for a single hook).
"""

from typing import Any

from bubus import BaseEvent
from pydantic import ConfigDict, Field


# ── Crawl lifecycle ──────────────────────────────────────────────────────────

class CrawlEvent(BaseEvent):
    """Root event: kicks off the full crawl → snapshot → cleanup lifecycle.

    Emitted once by orchestrator.download(). CrawlService.on_CrawlEvent
    handles this by emitting the lifecycle chain:
    CrawlSetupEvent → CrawlStartEvent → CrawlCleanupEvent → CrawlCompletedEvent
    """
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 300.0


class CrawlSetupEvent(BaseEvent):
    """Phase: run all on_Crawl hooks (installs, daemons, config propagation).

    Emitted by CrawlService.on_CrawlEvent. Per-hook handlers are registered
    on this event, so they run serially in hook sort order.
    """
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 300.0


class CrawlStartEvent(BaseEvent):
    """Phase: crawl setup finished, start the actual crawl (snapshot extraction).

    Emitted by CrawlService.on_CrawlEvent after CrawlSetupEvent completes.
    CrawlService.on_CrawlStartEvent emits SnapshotEvent (unless crawl_only).
    """
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 300.0


class CrawlCleanupEvent(BaseEvent):
    """Phase: SIGTERM all background crawl daemons.

    Emitted by CrawlService.on_CrawlEvent after snapshot phase completes.
    """
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 30.0


class CrawlCompletedEvent(BaseEvent):
    """Informational: the full crawl lifecycle has finished."""
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 10.0


# ── Snapshot lifecycle ───────────────────────────────────────────────────────

class SnapshotEvent(BaseEvent):
    """Phase: run all on_Snapshot hooks (extraction + indexing).

    Emitted by CrawlService.on_CrawlStartEvent as a child of
    CrawlStartEvent. Per-hook handlers are registered on this event.
    """
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 300.0


class SnapshotCleanupEvent(BaseEvent):
    """Phase: SIGTERM all background snapshot daemons.

    Emitted by SnapshotService.on_SnapshotEvent after all snapshot hooks complete.
    """
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 30.0


class SnapshotCompletedEvent(BaseEvent):
    """Informational: the snapshot phase has finished."""
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 10.0


# ── Process (hook subprocess execution) ──────────────────────────────────────

class ProcessEvent(BaseEvent):
    """Command: run a hook subprocess.

    Handled by ProcessService.on_ProcessEvent, which spawns the subprocess,
    streams stdout, and emits side-effect events (Binary/Machine) in realtime.

    Uses ``event_concurrency='parallel'`` so fire-and-forget bg hooks can
    process alongside the parent event's serial handler chain. Without this,
    bg ProcessEvents would queue behind the parent and deadlock.

    ``event_handler_timeout`` must be set per-hook — otherwise bubus uses the
    bus-level default (60s), which is too short for slow installs like puppeteer.
    Hook handlers set this to ``hook_timeout + 30s`` to allow
    overhead for process startup and JSONL parsing.
    """
    event_concurrency: str = 'parallel'
    event_handler_timeout: float = 360.0
    plugin_name: str
    hook_name: str
    hook_path: str
    hook_args: list[str]
    is_background: bool
    output_dir: str
    env: dict[str, str]
    snapshot_id: str = ''
    timeout: int = 60
    event_timeout: float = 360.0


class ProcessKillEvent(BaseEvent):
    """Command: SIGTERM a background daemon hook via its PID file.

    Emitted by cleanup event handlers. ProcessService reads the PID file
    and sends SIGTERM. Safe no-op if the hook already exited.
    """
    plugin_name: str
    hook_name: str
    output_dir: str
    event_timeout: float = 10.0


class ProcessCompleted(BaseEvent):
    """Notification: a hook subprocess finished.

    Emitted by ProcessService after the subprocess exits (or times out).
    Carries the full result context. Currently no handlers listen for this
    event — it exists for observability and future use (e.g. progress tracking).
    """
    plugin_name: str
    hook_name: str
    stdout: str
    stderr: str
    exit_code: int
    output_dir: str
    output_files: list[str] = []
    output_str: str = ''
    status: str = ''         # 'succeeded', 'failed', 'skipped', 'noresults'
    is_background: bool = False
    event_timeout: float = 60.0


# ── Binary resolution ────────────────────────────────────────────────────────

class BinaryEvent(BaseEvent):
    """A hook needs a binary resolved or installed.

    Emitted by ProcessService when a hook outputs ``{"type": "Binary", ...}``
    JSONL. Handled by BinaryService, which either registers the path (if abspath
    is provided) or broadcasts to provider hooks to install it.

    The ``binproviders`` field controls which providers are tried (e.g.
    "puppeteer" means only the puppeteer provider hook should attempt install).
    Provider hooks self-select based on this field.
    """
    name: str
    abspath: str = ''
    binary_id: str = ''
    machine_id: str = ''
    binproviders: str = ''
    overrides: dict[str, Any] | None = None
    custom_cmd: str = ''
    event_timeout: float = 300.0


# ── Machine config update ────────────────────────────────────────────────────

class MachineEvent(BaseEvent):
    """Update shared machine config.

    Emitted by ProcessService when a hook outputs ``{"type": "Machine", ...}``
    JSONL. Handled by MachineService, which updates shared_config and the
    persistent config store.

    Two formats:
    - Batch: ``{"type": "Machine", "config": {"KEY1": "val1", "KEY2": "val2"}}``
    - Single: ``{"type": "Machine", "_method": "update", "key": "config/KEY", "value": "val"}``
    """
    model_config = ConfigDict(populate_by_name=True)
    method: str = Field('', validation_alias='_method')
    key: str = ''
    value: str = ''
    config: dict[str, Any] | None = None
    event_timeout: float = 10.0
