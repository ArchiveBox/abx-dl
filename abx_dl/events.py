"""Event schemas for the abx-dl bubus event bus.

Events come in command/completion pairs:
  - Command event triggers an action (e.g. ProcessEvent → run a subprocess)
  - Handler executes the action
  - Completion event notifies the bus with results (e.g. ProcessCompleted)
  - Other handlers react to the completion (e.g. parse JSONL, emit Binary/Machine)
"""

from typing import Any

from bubus import BaseEvent


# ── Crawl / Snapshot lifecycle ───────────────────────────────────────────────

class CrawlEvent(BaseEvent):
    """Dispatched once to trigger all on_Crawl hook handlers."""
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 300.0


class SnapshotEvent(BaseEvent):
    """Dispatched after crawl phase to trigger all on_Snapshot hook handlers."""
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 300.0


# ── Process (hook subprocess execution) ──────────────────────────────────────

class ProcessEvent(BaseEvent):
    """Command: run a hook subprocess."""
    url: str
    snapshot_id: str
    plugin_name: str
    hook_name: str
    hook_path: str
    hook_language: str
    is_background: bool
    output_dir: str
    env: dict[str, str]
    timeout: int = 60
    event_timeout: float = 360.0


class ProcessCompleted(BaseEvent):
    """Notification: a hook subprocess finished. Carries full result context."""
    plugin_name: str
    hook_name: str
    stdout: str
    stderr: str
    exit_code: int
    output_dir: str
    output_files: list[str] = []
    output_str: str = ''
    status: str = ''         # 'succeeded', 'failed', etc.
    is_background: bool = False
    event_timeout: float = 60.0


# ── Binary resolution ────────────────────────────────────────────────────────

class BinaryEvent(BaseEvent):
    """A Binary JSONL record needs resolution via provider on_Binary hooks."""
    record: dict[str, Any]
    event_timeout: float = 300.0


# ── Machine config update ────────────────────────────────────────────────────

class MachineEvent(BaseEvent):
    """A Machine JSONL record — update shared config."""
    record: dict[str, Any]
    event_timeout: float = 10.0
