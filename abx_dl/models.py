"""
Data models for abx-dl matching ArchiveBox's schema.
"""

import json
import os
import platform
import socket
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


def uuid7() -> str:
    """Generate a UUIDv7-like string (timestamp-based for sortability)."""
    ts = int(datetime.now().timestamp() * 1000)
    return f"{ts:012x}-{uuid4().hex[:20]}"


def now_iso() -> str:
    return datetime.now().isoformat()


class Process(BaseModel):
    """A subprocess execution."""
    cmd: list[str]
    id: str = Field(default_factory=uuid7)
    binary_id: str | None = None
    plugin: str | None = None
    hook_name: str | None = None
    pwd: str = Field(default_factory=os.getcwd)
    env: dict[str, str] = Field(default_factory=dict)
    timeout: int = 60
    started_at: str | None = None
    ended_at: str | None = None
    exit_code: int | None = None
    stdout: str = ''
    stderr: str = ''
    machine_hostname: str = Field(default_factory=socket.gethostname)
    machine_os: str = Field(default_factory=lambda: f"{platform.system()} {platform.release()}")

    def to_jsonl(self) -> str:
        d = {k: v for k, v in self.model_dump().items() if v is not None}
        d['type'] = 'Process'
        return json.dumps(d, default=str)


# PROVIDED BY ABX-PKG:
# class Binary:
#     name: str
#     id: str = Field(default_factory=uuid7)
#     version: str | None = None
#     ...

class Snapshot(BaseModel):
    """A URL being archived."""
    url: str
    id: str = Field(default_factory=uuid7)
    title: str | None = None
    timestamp: str = Field(default_factory=lambda: str(datetime.now().timestamp()))
    bookmarked_at: str = Field(default_factory=now_iso)
    created_at: str = Field(default_factory=now_iso)
    tags: str = ''

    def to_jsonl(self) -> str:
        d = {k: v for k, v in self.model_dump().items() if v is not None}
        d['type'] = 'Snapshot'
        return json.dumps(d, default=str)


class ArchiveResult(BaseModel):
    """Result from running a plugin hook."""
    snapshot_id: str
    plugin: str
    id: str = Field(default_factory=uuid7)
    hook_name: str = ''
    status: str = 'queued'
    process_id: str | None = None
    output_str: str = ''
    output_files: list[str] = Field(default_factory=list)
    start_ts: str | None = None
    end_ts: str | None = None
    error: str | None = None

    def to_jsonl(self) -> str:
        d = {k: v for k, v in self.model_dump().items() if v is not None}
        d['type'] = 'ArchiveResult'
        return json.dumps(d, default=str)


VisibleRecord = ArchiveResult | Process


def write_jsonl(path: Path, record: Any, also_print: bool = False):
    """Append a record to a JSONL file."""
    line = record.to_jsonl()
    with open(path, 'a') as f:
        f.write(line + '\n')
    if also_print:
        print(line, flush=True)
