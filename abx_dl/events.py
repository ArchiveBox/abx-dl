"""Event schemas for the abx-dl bubus event bus.

Events come in command/completion pairs:
  - Command event triggers an action (e.g. ProcessEvent → run a subprocess)
  - Handler executes the action
  - Completion event notifies the bus with results (e.g. ProcessCompleted)
  - Other handlers react to the completion (e.g. parse JSONL, emit Binary/Machine)
"""

from typing import Any

from bubus import BaseEvent
from pydantic import ConfigDict, Field


# ── Crawl / Snapshot lifecycle ───────────────────────────────────────────────

class CrawlEvent(BaseEvent):
    """Dispatched once to trigger all on_Crawl hook handlers."""
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 300.0


class CrawlCompleted(BaseEvent):
    """Emitted after all on_Crawl hooks have finished."""
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 10.0


class SnapshotEvent(BaseEvent):
    """Dispatched after crawl phase to trigger all on_Snapshot hook handlers."""
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 300.0


class SnapshotCompleted(BaseEvent):
    """Emitted after all on_Snapshot hooks have finished."""
    url: str
    snapshot_id: str
    output_dir: str
    event_timeout: float = 10.0


# ── Process (hook subprocess execution) ──────────────────────────────────────

class ProcessEvent(BaseEvent):
    """Command: run a hook subprocess.

    Hooks are +x executables, run directly as: [hook_path, *hook_args].
    The env dict must include correct PATH (set via MachineEvent updates).
    """
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
    """A hook needs a binary resolved/installed."""
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
    """Update shared machine config."""
    model_config = ConfigDict(populate_by_name=True)
    method: str = Field('', validation_alias='_method')
    key: str = ''
    value: str = ''
    config: dict[str, Any] | None = None
    event_timeout: float = 10.0
