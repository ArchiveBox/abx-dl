"""Event schemas for the abx-dl bubus event bus.

Events form a hierarchy during execution::

    CrawlEvent
    ├── ProcessEvent (crawl hooks)
    │   ├── BinaryEvent → ProcessEvent (provider install) → MachineEvent
    │   └── ProcessCompleted
    ├── SnapshotEvent
    │   ├── ProcessEvent (snapshot hooks)
    │   │   └── ProcessCompleted
    │   └── ProcessKillEvent (snapshot bg cleanup)
    ├── ProcessKillEvent (crawl bg cleanup)
    └── CrawlCompleted

Event types:
- **Command events** trigger actions: CrawlEvent, SnapshotEvent, ProcessEvent,
  ProcessKillEvent, BinaryEvent, MachineEvent
- **Completion events** notify results: CrawlCompleted, SnapshotCompleted,
  ProcessCompleted

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


# ── Crawl / Snapshot lifecycle ───────────────────────────────────────────────

class CrawlEvent(BaseEvent):
    """Root event: triggers all on_Crawl hook handlers (install + daemons).

    Emitted once by orchestrator.download(). The entire crawl + snapshot
    lifecycle runs as children of this event.
    """
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 300.0


class CrawlCompleted(BaseEvent):
    """Informational: emitted after the full CrawlEvent tree completes.

    No handlers react to this — it exists for logging, monitoring, and
    to mark the end of the event tree in bus.log_tree() output.
    """
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 10.0


class SnapshotEvent(BaseEvent):
    """Triggers all on_Snapshot hook handlers (extraction + indexing).

    Emitted by CrawlService._emit_snapshot_event as a child of CrawlEvent,
    so the entire snapshot phase sits inside the crawl event tree.
    """
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 300.0


class SnapshotCompleted(BaseEvent):
    """Informational: emitted after all snapshot hooks complete."""
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
    CrawlService/SnapshotService set this to ``hook_timeout + 30s`` to allow
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

    Emitted by CrawlService/SnapshotService cleanup handlers. ProcessService
    reads the PID file and sends SIGTERM. Safe no-op if the hook already exited.
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
