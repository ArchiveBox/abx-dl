"""Event schemas for the abx-dl bubus event bus.

Events form a hierarchy during execution::

    CrawlEvent                                      # orchestrator.download()
    ├── CrawlSetupEvent                             # crawl hooks run here
    │   ├── ProcessEvent (on_Crawl hooks)
    │   │   ├── ProcessRecordOutputtedEvent         # for each JSONL line
    │   │   │   ├── BinaryEvent (via BinaryService)
    │   │   │   │   ├── BinaryLoadedEvent (already installed)
    │   │   │   │   └── ProcessEvent (provider install) → BinaryInstalledEvent
    │   │   │   ├── MachineEvent (via MachineService)
    │   │   │   ├── SnapshotEvent (via SnapshotService, depth>1)
    │   │   │   └── ArchiveResultEvent (via ArchiveResultService, inline)
    │   │   └── ProcessCompletedEvent
    │   │       └── ArchiveResultEvent (enriched with process metadata)
    │   └── ...
    ├── CrawlStartEvent                    # triggers snapshot phase
    │   └── SnapshotEvent (depth=1)
    │       ├── ProcessEvent (on_Snapshot hooks)
    │       │   ├── ProcessRecordOutputtedEvent
    │       │   │   ├── SnapshotEvent (depth>1, ignored by abx-dl)
    │       │   │   └── ArchiveResultEvent (inline)
    │       │   └── ProcessCompletedEvent
    │       │       └── ArchiveResultEvent (enriched)
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
- **Routing events** decouple services: ProcessRecordOutputtedEvent
  carries raw JSONL from stdout; each service handles its own type
- **Command events** trigger actions: ProcessEvent, ProcessKillEvent,
  BinaryEvent, MachineEvent
- **Completion events** notify results: ProcessCompletedEvent,
  ArchiveResultEvent, BinaryLoadedEvent, BinaryInstalledEvent

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

    Also emitted by SnapshotService.on_ProcessRecordOutputtedEvent when a hook
    outputs ``{"type": "Snapshot", ...}`` JSONL (discovered URLs during crawling).
    In abx-dl, SnapshotService ignores events with ``depth > 1``.
    ArchiveBox handles recursive crawling by processing all depths.
    """
    url: str
    snapshot_id: str
    output_dir: str
    depth: int = 1
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
    streams stdout, and emits ProcessRecordOutputtedEvent for each typed JSONL
    record. Each service then handles its own record types independently.

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


class ProcessCompletedEvent(BaseEvent):
    """Notification: a hook subprocess finished.

    Emitted by ProcessService after the subprocess exits (or times out).
    Carries raw process-level data only — no interpretation of status or
    output. ArchiveResultService listens for this to build ArchiveResult
    records enriched with process metadata.
    """
    plugin_name: str
    hook_name: str
    stdout: str
    stderr: str
    exit_code: int
    output_dir: str
    output_files: list[str] = []
    is_background: bool = False
    process_id: str = ''
    snapshot_id: str = ''
    start_ts: str = ''
    end_ts: str = ''
    event_timeout: float = 60.0


class ProcessRecordOutputtedEvent(BaseEvent):
    """A hook subprocess outputted a typed JSON record to stdout.

    Emitted by ProcessService for each parsed JSONL line that has a ``type``
    field. ProcessService does not interpret the record — it routes it to
    this event so each service can handle its own record types:

    - BinaryService: ``type=Binary`` → emits BinaryEvent
    - MachineService: ``type=Machine`` → emits MachineEvent
    - SnapshotService: ``type=Snapshot`` → emits SnapshotEvent
    - ArchiveResultService: ``type=ArchiveResult`` → emits ArchiveResultEvent

    The ``record`` dict has the ``type`` key already stripped. Context fields
    from the parent ProcessEvent are passed through for services that need them.

    Uses ``await bus.emit()`` (queue-jump) so the emitted typed event and its
    entire handler chain complete before the next stdout line is read.
    """
    record_type: str
    record: dict[str, Any]
    plugin_name: str = ''
    hook_name: str = ''
    output_dir: str = ''
    snapshot_id: str = ''
    event_timeout: float = 360.0


# ── Binary resolution ────────────────────────────────────────────────────────

class BinaryEvent(BaseEvent):
    """A hook needs a binary resolved or installed.

    Emitted by BinaryService.on_ProcessRecordOutputtedEvent when a hook outputs
    ``{"type": "Binary", ...}`` JSONL. Handled by BinaryService's provider hooks
    and on_BinaryEvent, which either registers the path (if abspath is provided)
    or broadcasts to provider hooks to install it.

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


class BinaryLoadedEvent(BaseEvent):
    """Informational: a binary was already installed and its path was registered.

    Emitted by BinaryService.on_BinaryEvent when BinaryEvent arrives with
    ``abspath`` already set (hook detected binary at a known path).
    """
    name: str
    abspath: str
    version: str = ''
    sha256: str = ''
    binprovider: str = ''
    binary_id: str = ''
    machine_id: str = ''
    event_timeout: float = 10.0


class BinaryInstalledEvent(BaseEvent):
    """Informational: a binary was installed by a provider hook.

    Emitted by BinaryService after a provider hook successfully installs a
    binary (its env key appears in ``shared_config`` after the hook runs).
    """
    name: str
    abspath: str
    version: str = ''
    sha256: str = ''
    binprovider: str = ''
    binary_id: str = ''
    machine_id: str = ''
    event_timeout: float = 10.0


# ── Machine config update ────────────────────────────────────────────────────

class MachineEvent(BaseEvent):
    """Update shared machine config.

    Emitted by MachineService.on_ProcessRecordOutputtedEvent when a hook outputs
    ``{"type": "Machine", ...}`` JSONL. Handled by MachineService.on_MachineEvent,
    which updates shared_config and the persistent config store.

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


# ── ArchiveResult notification ────────────────────────────────────────────

class ArchiveResultEvent(BaseEvent):
    """An ArchiveResult was produced by a hook.

    Emitted by ArchiveResultService in two contexts:

    1. **Inline from stdout**: when a hook outputs ``{"type": "ArchiveResult", ...}``
       JSONL during execution (via ProcessRecordOutputtedEvent routing). These
       have partial fields (status, output_str from the hook) but no process
       metadata. Consumed by ArchiveBox to write DB rows for each extracted result.

    2. **Process completion**: when ProcessCompletedEvent arrives, the inline
       record is enriched with process metadata (process_id, output_files,
       start_ts, end_ts, error). The orchestrator collects these (identified
       by ``process_id`` being set).

    Fields match the ArchiveResult model.
    """
    snapshot_id: str = ''
    plugin: str = ''
    id: str = ''
    hook_name: str = ''
    status: str = ''
    process_id: str = ''
    output_str: str = ''
    output_files: list[str] = Field(default_factory=list)
    start_ts: str = ''
    end_ts: str = ''
    error: str = ''
    event_timeout: float = 10.0
