"""Helpers for crawl-wide max_urls / max_size enforcement."""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from collections.abc import Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - non-Unix fallback
    fcntl = None  # type: ignore[assignment]


FILESIZE_UNITS: dict[str, int] = {
    "": 1,
    "b": 1,
    "byte": 1,
    "bytes": 1,
    "k": 1024,
    "kb": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "mib": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
    "gib": 1024**3,
    "t": 1024**4,
    "tb": 1024**4,
    "tib": 1024**4,
}


def parse_filesize_to_bytes(value: str | int | float | None) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        raise ValueError("Size value must be an integer or size string.")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError("Size value must resolve to a whole number of bytes.")
        return int(value)

    raw_value = str(value).strip()
    if not raw_value:
        return 0
    if raw_value.isdigit():
        return int(raw_value)

    import re

    match = re.fullmatch(r"(?i)(\d+(?:\.\d+)?)\s*([a-z]+)", raw_value)
    if not match:
        raise ValueError(f"Invalid size value: {value}")

    amount_str, unit_str = match.groups()
    multiplier = FILESIZE_UNITS.get(unit_str.lower())
    if multiplier is None:
        raise ValueError(f"Unknown size unit: {unit_str}")

    try:
        amount = Decimal(amount_str)
    except InvalidOperation as err:
        raise ValueError(f"Invalid size value: {value}") from err

    return int(amount * multiplier)


@dataclass(slots=True)
class SnapshotAdmission:
    allowed: bool
    stop_reason: str


class CrawlLimitState:
    def __init__(self, *, crawl_dir: Path, max_urls: int = 0, max_size: int = 0):
        self.crawl_dir = crawl_dir
        self.max_urls = max(0, int(max_urls or 0))
        self.max_size = max(0, int(max_size or 0))
        self.state_dir = self.crawl_dir / ".abx-dl"
        self.state_path = self.state_dir / "limits.json"
        self.lock_path = self.state_dir / "limits.lock"

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> CrawlLimitState:
        crawl_dir = Path(str(config.get("CRAWL_DIR") or config.get("SNAP_DIR") or "."))
        max_urls = int(config.get("MAX_URLS") or 0)
        max_size = parse_filesize_to_bytes(config.get("MAX_SIZE") or 0)
        return cls(crawl_dir=crawl_dir, max_urls=max_urls, max_size=max_size)

    @classmethod
    def from_env(cls, env: dict[str, str]) -> CrawlLimitState:
        crawl_dir = Path(str(env.get("CRAWL_DIR") or env.get("SNAP_DIR") or "."))
        max_urls = int(env.get("MAX_URLS") or 0)
        max_size = parse_filesize_to_bytes(env.get("MAX_SIZE") or 0)
        return cls(crawl_dir=crawl_dir, max_urls=max_urls, max_size=max_size)

    def has_limits(self) -> bool:
        return self.max_urls > 0 or self.max_size > 0

    def admit_snapshot(self, snapshot_id: str) -> SnapshotAdmission:
        if not self.has_limits():
            return SnapshotAdmission(allowed=True, stop_reason="")

        with self._locked_state() as state:
            admitted = {str(item) for item in state.get("admitted_snapshot_ids", [])}
            stop_reason = str(state.get("stop_reason") or "")

            if stop_reason == "max_size":
                return SnapshotAdmission(allowed=False, stop_reason=stop_reason)

            if snapshot_id in admitted:
                return SnapshotAdmission(allowed=True, stop_reason=stop_reason)

            if stop_reason:
                return SnapshotAdmission(allowed=False, stop_reason=stop_reason)

            if self.max_urls and len(admitted) >= self.max_urls:
                state["stop_reason"] = "max_urls"
                return SnapshotAdmission(allowed=False, stop_reason="max_urls")

            admitted.add(snapshot_id)
            state["admitted_snapshot_ids"] = sorted(admitted)

            if self.max_urls and len(admitted) >= self.max_urls:
                state["stop_reason"] = "max_urls"

            return SnapshotAdmission(allowed=True, stop_reason=str(state.get("stop_reason") or ""))

    def should_emit_discovered_snapshots(self) -> bool:
        if not self.has_limits():
            return True
        with self._locked_state(readonly=True) as state:
            return not bool(state.get("stop_reason"))

    def get_stop_reason(self) -> str:
        if not self.has_limits():
            return ""
        with self._locked_state(readonly=True) as state:
            return str(state.get("stop_reason") or "")

    def record_process_output(self, process_id: str, output_dir: Path, output_files: list[str]) -> str:
        if not self.max_size:
            return ""

        added_size = 0
        for relative_path in output_files:
            try:
                file_size = (output_dir / relative_path).stat().st_size
            except OSError:
                file_size = 0
            added_size += file_size

        with self._locked_state() as state:
            counted_process_ids = {str(item) for item in state.get("counted_process_ids", [])}
            if process_id in counted_process_ids:
                return str(state.get("stop_reason") or "")

            counted_process_ids.add(process_id)
            state["counted_process_ids"] = sorted(counted_process_ids)
            state["total_size"] = int(state.get("total_size") or 0) + added_size

            if not state.get("stop_reason") and self.max_size and int(state["total_size"]) >= self.max_size:
                state["stop_reason"] = "max_size"

            return str(state.get("stop_reason") or "")

    @contextmanager
    def _locked_state(self, readonly: bool = False) -> Iterator[dict[str, Any]]:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.lock_path.touch(exist_ok=True)

        with self.lock_path.open("r+", encoding="utf-8") as lock_handle:
            if fcntl is not None:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX if not readonly else fcntl.LOCK_SH)
            try:
                state = self._load_state()
                yield state
                if not readonly:
                    self._save_state(state)
            finally:
                if fcntl is not None:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {
                "admitted_snapshot_ids": [],
                "counted_process_ids": [],
                "total_size": 0,
                "stop_reason": "",
            }
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload.setdefault("admitted_snapshot_ids", [])
        payload.setdefault("counted_process_ids", [])
        payload.setdefault("total_size", 0)
        payload.setdefault("stop_reason", "")
        return payload

    def _save_state(self, state: dict[str, Any]) -> None:
        self.state_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
