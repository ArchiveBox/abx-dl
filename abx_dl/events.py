"""Event schemas for the abx-dl abxbus event bus.

Events form this phase order during execution::

    InstallEvent                                    # orchestrator preflight only
    └── BinaryRequestEvent × N                      # from config.json required_binaries
        └── ProcessEvent (provider hook) → BinaryEvent

    CrawlEvent                                      # internal lifecycle root
    ├── CrawlSetupEvent                             # plugin on_CrawlSetup hooks run here
    │   ├── ProcessEvent (on_CrawlSetup hooks)
    │   └── ProcessCompletedEvent
    ├── CrawlStartEvent                             # triggers snapshot phase
    │   └── SnapshotEvent (depth=0)
    │       ├── ProcessEvent (on_Snapshot hooks)
    │       │   ├── ProcessStdoutEvent
    │       │   │   ├── SnapshotEvent (depth>0, ignored by abx-dl)
    │       │   │   ├── TagEvent
    │       │   │   └── ArchiveResultEvent (inline)
    │       │   └── ProcessCompletedEvent
    │       │       └── ArchiveResultEvent (synthetic, on_Snapshot only)
    │       ├── SnapshotCleanupEvent
    │       │   └── ProcessKillEvent × N
    │       └── SnapshotCompletedEvent
    ├── CrawlCleanupEvent
    │   └── ProcessKillEvent × N
    └── CrawlCompletedEvent

Event types:
- **Lifecycle events** drive phases: InstallEvent, CrawlEvent, CrawlSetupEvent,
  CrawlStartEvent, SnapshotEvent, SnapshotCleanupEvent,
  SnapshotCompletedEvent, CrawlCleanupEvent, CrawlCompletedEvent
- **Routing events** decouple services: ProcessStdoutEvent
  carries each stdout line; routing services translate hook JSONL into the
  typed events used by the rest of the runtime
- **Command events** trigger actions: ProcessEvent, ProcessKillEvent,
  BinaryRequestEvent, MachineEvent
- **Completion events** notify results: ProcessCompletedEvent,
  ArchiveResultEvent, BinaryEvent

abxbus behavior:
- Each event has ``event_timeout`` — the hard deadline for the event and all its
  children. If exceeded, abxbus cancels pending handlers and child events.
- ``event_concurrency='parallel'`` on ProcessEvent allows bg hooks to run
  concurrently with the parent event's handler chain.
- ``event_handler_timeout`` on ProcessEvent controls the per-handler timeout
  (how long ProcessService.on_ProcessEvent can run for a single hook).
"""

import asyncio
from pathlib import Path
from typing import Any, Literal

from abxbus import BaseEvent, EventConcurrencyMode, EventHandlerConcurrencyMode
from pydantic import ConfigDict, Field

from .output_files import OutputFile


PROCESS_EXIT_SKIPPED = 10


def slow_warning_timeout(timeout: float | int | None) -> float | None:
    """Warn only after most of the timeout budget has been consumed."""
    if timeout is None:
        return None
    return max(float(timeout) * 0.8, 1.0)


# ── Crawl lifecycle ──────────────────────────────────────────────────────────


class InstallEvent(BaseEvent):
    """Root pre-run phase for required binary resolution.

    Emitted by the orchestrator before CrawlEvent. BinaryService handles it by
    reading each enabled plugin's ``config.json > required_binaries`` and
    emitting BinaryRequestEvent records for provider plugins to satisfy.
    """

    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float | None = 300.0


class CrawlEvent(BaseEvent):
    """Root event: kicks off the full crawl → snapshot → cleanup lifecycle.

    Emitted once by orchestrator.download() after InstallEvent completes.
    CrawlService.on_CrawlEvent handles this by emitting the lifecycle chain:
    CrawlSetupEvent → CrawlStartEvent → CrawlCleanupEvent → CrawlCompletedEvent.
    """

    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float | None = 300.0


class CrawlSetupEvent(BaseEvent):
    """Phase: run all on_CrawlSetup hooks (daemons and shared runtime setup).

    Emitted by CrawlService.on_CrawlEvent. Per-hook handlers are registered
    on this event, so they run serially in hook sort order. Crawl setup hooks
    are expected to prepare shared state and emit no stdout JSONL records.
    """

    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float | None = 300.0


class CrawlStartEvent(BaseEvent):
    """Phase: crawl setup finished, start the actual crawl (snapshot extraction).

    Emitted by CrawlService.on_CrawlEvent after CrawlSetupEvent completes.
    CrawlService.on_CrawlStartEvent emits SnapshotEvent when snapshot
    execution is enabled for the current run.
    """

    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float | None = 300.0


class CrawlCleanupEvent(BaseEvent):
    """Phase: SIGTERM all background crawl daemons.

    Emitted by CrawlService.on_CrawlEvent after snapshot phase completes.
    """

    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float | None = 30.0


class CrawlCompletedEvent(BaseEvent):
    """Informational: the full crawl lifecycle has finished."""

    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float | None = 10.0


class CrawlPauseEvent(BaseEvent):
    """Request interruption of the current foreground hook and pause the crawl."""

    event_timeout: float | None = 60.0


class CrawlAbortEvent(BaseEvent):
    """Abort the crawl after the current interrupted hook has been handled."""

    event_timeout: float | None = 60.0


class CrawlResumeAndRetryEvent(BaseEvent):
    """Resume the crawl by retrying the foreground hook that was interrupted."""

    event_timeout: float | None = 60.0


class CrawlResumeAndSkipEvent(BaseEvent):
    """Resume the crawl and leave the interrupted foreground hook skipped."""

    event_timeout: float | None = 60.0


# ── Snapshot lifecycle ───────────────────────────────────────────────────────


class SnapshotEvent(BaseEvent):
    """Phase: run all on_Snapshot hooks (extraction + discovery).

    Emitted by CrawlService.on_CrawlStartEvent as a child of
    CrawlStartEvent. Per-hook handlers are registered on this event.

    Also emitted by SnapshotService.on_ProcessStdoutEvent when a hook
    outputs ``{"type": "Snapshot", ...}`` JSONL. Snapshot hooks emit
    ``ArchiveResult`` records for their own result and may also emit
    ``Snapshot`` and ``Tag`` discovery records. In abx-dl, SnapshotService
    ignores events with ``depth > 0``. ArchiveBox handles recursive crawling
    by processing all depths.
    """

    url: str
    snapshot_id: str
    output_dir: str
    depth: int = 0
    event_timeout: float | None = 300.0


class SnapshotCleanupEvent(BaseEvent):
    """Phase: SIGTERM all background snapshot daemons.

    Emitted by SnapshotService.on_SnapshotEvent after all snapshot hooks complete.
    """

    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float | None = 30.0


class SnapshotCompletedEvent(BaseEvent):
    """Informational: the snapshot phase has finished."""

    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float | None = 10.0


# ── Process (hook subprocess execution) ──────────────────────────────────────


class ProcessEvent(BaseEvent):
    """Command: run a hook subprocess.

    Handled by ProcessService.on_ProcessEvent, which spawns the subprocess,
    streams stdout, and emits ProcessStdoutEvent for each stdout
    record. Each service then handles its own record types independently.

    Uses ``event_concurrency='parallel'`` so fire-and-forget bg hooks can
    process alongside the parent event's serial handler chain. Without this,
    bg ProcessEvents would queue behind the parent and deadlock.

    ``event_handler_timeout`` must be set per-hook — otherwise abxbus uses the
    bus-level default (60s), which is too short for slow installs like puppeteer.
    Hook handlers set this to ``hook_timeout + 30s`` to allow
    overhead for process startup and JSONL parsing.
    """

    event_concurrency: EventConcurrencyMode | None = EventConcurrencyMode.PARALLEL
    event_handler_timeout: float | None = 360.0
    plugin_name: str
    hook_name: str
    hook_path: str
    hook_args: list[str]
    is_background: bool
    output_dir: str
    env: dict[str, str]
    timeout: int = 60
    url: str = ""
    process_type: str = ""
    worker_type: str = ""
    event_timeout: float | None = 360.0


class ProcessKillEvent(BaseEvent):
    """Command: SIGTERM a background hook via its process identity.

    Emitted by cleanup event handlers as a direct child of the cleanup event
    that owns the shutdown sequence. ProcessService resolves the single matching
    ProcessStartedEvent within that crawl/snapshot ancestry using
    ``plugin_name``, ``hook_name``, and ``pid``, sends SIGTERM, waits
    ``grace_period`` seconds for clean exit, then escalates to SIGKILL. The
    grace period should be the plugin's timeout (PLUGINNAME_TIMEOUT) so daemons
    get their configured time to flush.
    """

    plugin_name: str
    hook_name: str
    pid: int
    grace_period: float = 15.0
    event_timeout: float | None = 60.0


class ProcessStartedEvent(BaseEvent):
    """Notification: a hook subprocess started successfully."""

    event_handler_concurrency: EventHandlerConcurrencyMode | None = EventHandlerConcurrencyMode.PARALLEL
    plugin_name: str
    hook_name: str
    hook_path: str
    hook_args: list[str]
    output_dir: str
    env: dict[str, str]
    timeout: int
    pid: int = 0
    is_background: bool = False
    url: str = ""
    process_type: str = ""
    worker_type: str = ""
    start_ts: str = ""
    subprocess: asyncio.subprocess.Process = Field(exclude=True, repr=False)
    stdout_file: Path = Field(exclude=True, repr=False)
    stderr_file: Path = Field(exclude=True, repr=False)
    pid_file: Path = Field(exclude=True, repr=False)
    cmd_file: Path = Field(exclude=True, repr=False)
    files_before: set[Path] = Field(exclude=True, repr=False)
    event_timeout: float | None = 60.0


class ProcessCompletedEvent(BaseEvent):
    """Notification: a hook subprocess finished.

    Emitted by ProcessService after the subprocess exits (or times out).
    Carries raw process-level data plus the resolved process status.
    ArchiveResultService listens for this to build ArchiveResult records
    enriched with process metadata.
    """

    plugin_name: str
    hook_name: str
    hook_path: str = ""
    hook_args: list[str] = []
    env: dict[str, str] = {}
    timeout: int = 60
    stdout: str
    stderr: str
    exit_code: int
    status: Literal["succeeded", "failed", "skipped"]
    output_dir: str
    output_files: list[OutputFile] = Field(default_factory=list)
    is_background: bool = False
    pid: int = 0
    url: str = ""
    process_type: str = ""
    worker_type: str = ""
    start_ts: str = ""
    end_ts: str = ""
    event_timeout: float | None = 60.0


class ProcessStdoutEvent(BaseEvent):
    """A hook subprocess outputted a line to stdout.

    Emitted by ProcessService for every stdout line. Each consuming service
    parses the line and checks for the JSON shape it cares about. In the
    current hook contract:

    - provider hooks emit ``{"type": "Binary", ...}`` → BinaryEvent
    - snapshot hooks emit ``{"type": "Snapshot", ...}`` → SnapshotEvent
    - snapshot hooks emit ``{"type": "Tag", ...}`` → TagEvent
    - snapshot hooks emit ``{"type": "ArchiveResult", ...}`` → ArchiveResultEvent

    Context fields from the parent ProcessEvent are passed through for
    services that need them.

    Uses ``await bus.emit()`` (queue-jump) so the emitted typed event and its
    entire handler chain complete before the next stdout line is read.
    """

    line: str
    plugin_name: str = ""
    hook_name: str = ""
    output_dir: str = ""
    start_ts: str = ""
    end_ts: str = ""
    output_files: list[OutputFile] = Field(default_factory=list)
    event_timeout: float | None = 360.0


# ── Binary resolution ────────────────────────────────────────────────────────


class BinaryRequestEvent(BaseEvent):
    """A required binary needs to be resolved or installed.

    Emitted during InstallEvent, when CrawlService turns
    ``config.json > required_binaries`` declarations into BinaryRequestEvent
    records.

    Handled by BinaryService and by provider plugins' ``on_BinaryRequest__*``
    hooks, which either emit a cached BinaryEvent or resolve/install the
    binary and then emit BinaryEvent records.

    The ``binproviders`` field controls which providers are tried (e.g.
    "puppeteer" means only the puppeteer provider hook should attempt
    resolution). Provider hooks self-select based on this field.
    """

    name: str = Field(min_length=1)
    plugin_name: str = ""
    hook_name: str = ""
    output_dir: str = ""
    min_version: str | None = None
    binary_id: str = ""
    machine_id: str = ""
    binproviders: str = ""
    overrides: dict[str, Any] | None = None
    event_timeout: float | None = 300.0


class BinaryEvent(BaseEvent):
    """Informational: a binary request was resolved.

    Emitted by BinaryService only after a binary resolves successfully,
    whether the binary was already present (discovered by env provider) or
    freshly installed by another provider (apt, pip, npm, etc.). Carries the
    concrete metadata that later hooks should use: resolved ``abspath``,
    detected ``version``, ``sha256`` when available, and the winning provider.
    """

    model_config = ConfigDict(extra="ignore")

    name: str = Field(min_length=1)
    plugin_name: str = ""
    hook_name: str = ""
    abspath: str = Field(min_length=1)
    version: str = ""
    sha256: str = ""
    binproviders: str = ""
    binprovider: str = ""
    overrides: dict[str, Any] | None = None
    binary_id: str = ""
    machine_id: str = ""
    event_timeout: float | None = 10.0


# ── Machine config update ────────────────────────────────────────────────────


class MachineEvent(BaseEvent):
    """Update runtime machine config.

    Emitted internally by services like ``BinaryService``. Handled by
    ``MachineService.on_MachineEvent``, which updates either runtime
    ``user_config`` or ``derived_config`` and persists derived values to
    ``derived.env`` for future runs.
    """

    model_config = ConfigDict(populate_by_name=True)
    method: str = Field(default="", validation_alias="_method")
    key: str = ""
    value: Any = None
    config: dict[str, Any] | None = None
    config_type: Literal["user", "derived"] = "derived"
    event_timeout: float | None = 10.0


class TagEvent(BaseEvent):
    """A hook emitted a tag to attach to the current snapshot."""

    model_config = ConfigDict(extra="ignore")

    name: str
    snapshot_id: str = ""
    event_timeout: float | None = 10.0


# ── ArchiveResult notification ────────────────────────────────────────────


class ArchiveResultEvent(BaseEvent):
    """An ArchiveResult was produced by a hook.

    Emitted by ArchiveResultService in two cases:

    1. **Inline from stdout**: when a hook outputs ``{"type": "ArchiveResult", ...}``
       JSONL during execution (via ProcessStdoutEvent routing). Output metadata
       fields (output_files, start_ts, end_ts) reflect the current process
       context at the moment the line was emitted.

    2. **Synthetic fallback**: on ProcessCompletedEvent, only if the hook didn't
       already report an ArchiveResult — e.g. failed (nonzero exit) or succeeded
       with output files but no explicit JSONL output.

    Both cases carry output_files, start_ts, end_ts copied from the process context.
    """

    snapshot_id: str = ""
    plugin: str = ""
    id: str = ""
    hook_name: str = ""
    status: str = ""
    output_str: str = ""
    output_json: dict[str, Any] | None = None
    output_files: list[OutputFile] = Field(default_factory=list)
    start_ts: str = ""
    end_ts: str = ""
    error: str = ""
    event_timeout: float | None = 10.0
