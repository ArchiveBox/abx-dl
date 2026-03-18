"""Event schemas for the abx-dl bubus event bus."""

from typing import Any

from bubus import BaseEvent


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


class ProcessEvent(BaseEvent):
    """A hook subprocess completed. Handlers parse JSONL and emit Binary/Machine events."""
    stdout: str
    event_timeout: float = 60.0


class BinaryEvent(BaseEvent):
    """A Binary JSONL record needs resolution via provider on_Binary hooks."""
    record: dict[str, Any]
    event_timeout: float = 300.0


class MachineEvent(BaseEvent):
    """A Machine JSONL record — update shared config."""
    record: dict[str, Any]
    event_timeout: float = 10.0
