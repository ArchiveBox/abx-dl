"""Helpers for crawl-wide and snapshot-local limit enforcement."""

from __future__ import annotations

import json
import importlib
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from collections.abc import Iterator

fcntl: Any = importlib.import_module("fcntl") if sys.platform != "win32" else None


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
    def __init__(
        self,
        *,
        crawl_dir: Path,
        crawl_max_urls: int = 0,
        crawl_max_size: int = 0,
        snapshot_max_size: int = 0,
    ):
        self.crawl_dir = crawl_dir
        self.crawl_max_urls = max(0, int(crawl_max_urls or 0))
        self.crawl_max_size = max(0, int(crawl_max_size or 0))
        self.snapshot_max_size = max(0, int(snapshot_max_size or 0))
        self.state_dir = self.crawl_dir / ".abx-dl"
        self.state_path = self.state_dir / "limits.json"
        self.lock_path = self.state_dir / "limits.lock"

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> CrawlLimitState:
        crawl_dir = Path(str(config.get("CRAWL_DIR") or config.get("SNAP_DIR") or "."))
        crawl_max_urls = int(config.get("CRAWL_MAX_URLS") or 0)
        crawl_max_size = parse_filesize_to_bytes(config.get("CRAWL_MAX_SIZE") or 0)
        snapshot_max_size = parse_filesize_to_bytes(config.get("SNAPSHOT_MAX_SIZE") or 0)
        return cls(
            crawl_dir=crawl_dir,
            crawl_max_urls=crawl_max_urls,
            crawl_max_size=crawl_max_size,
            snapshot_max_size=snapshot_max_size,
        )

    @classmethod
    def from_env(cls, env: dict[str, str]) -> CrawlLimitState:
        crawl_dir = Path(str(env.get("CRAWL_DIR") or env.get("SNAP_DIR") or "."))
        crawl_max_urls = int(env.get("CRAWL_MAX_URLS") or 0)
        crawl_max_size = parse_filesize_to_bytes(env.get("CRAWL_MAX_SIZE") or 0)
        snapshot_max_size = parse_filesize_to_bytes(env.get("SNAPSHOT_MAX_SIZE") or 0)
        return cls(
            crawl_dir=crawl_dir,
            crawl_max_urls=crawl_max_urls,
            crawl_max_size=crawl_max_size,
            snapshot_max_size=snapshot_max_size,
        )

    def has_limits(self) -> bool:
        return self.crawl_max_urls > 0 or self.crawl_max_size > 0 or self.snapshot_max_size > 0

    def admit_snapshot(self, snapshot_id: str) -> SnapshotAdmission:
        if not self.has_limits():
            return SnapshotAdmission(allowed=True, stop_reason="")

        with self._locked_state() as state:
            admitted = {str(item) for item in (state["admitted_snapshot_ids"] if "admitted_snapshot_ids" in state else [])}
            stop_reason = str(state["stop_reason"]) if "stop_reason" in state else ""

            if stop_reason == "crawl_max_size":
                return SnapshotAdmission(allowed=False, stop_reason=stop_reason)

            if snapshot_id in admitted:
                return SnapshotAdmission(allowed=True, stop_reason=stop_reason)

            if stop_reason:
                return SnapshotAdmission(allowed=False, stop_reason=stop_reason)

            if self.crawl_max_urls and len(admitted) >= self.crawl_max_urls:
                state["stop_reason"] = "crawl_max_urls"
                return SnapshotAdmission(allowed=False, stop_reason="crawl_max_urls")

            admitted.add(snapshot_id)
            state["admitted_snapshot_ids"] = sorted(admitted)

            if self.crawl_max_urls and len(admitted) >= self.crawl_max_urls:
                state["stop_reason"] = "crawl_max_urls"

            return SnapshotAdmission(
                allowed=True,
                stop_reason=str(state["stop_reason"]) if "stop_reason" in state else "",
            )

    def should_emit_discovered_snapshots(self) -> bool:
        if not self.has_limits():
            return True
        with self._locked_state(readonly=True) as state:
            return not bool(state["stop_reason"]) if "stop_reason" in state else True

    def get_stop_reason(self) -> str:
        if not self.has_limits():
            return ""
        with self._locked_state(readonly=True) as state:
            return str(state["stop_reason"]) if "stop_reason" in state else ""

    def get_snapshot_stop_reason(self, snapshot_id: str) -> str:
        if not self.snapshot_max_size:
            return ""
        with self._locked_state(readonly=True) as state:
            snapshot_stop_reasons = state.get("snapshot_stop_reasons", {})
            if not isinstance(snapshot_stop_reasons, dict):
                return ""
            return str(snapshot_stop_reasons.get(snapshot_id) or "")

    def record_process_output(self, event_id: str, output_dir: Path, output_files: list[str], *, snapshot_id: str = "") -> str:
        if not (self.crawl_max_size or self.snapshot_max_size):
            return ""

        added_size = 0
        for relative_path in output_files:
            try:
                file_size = (output_dir / relative_path).stat().st_size
            except OSError:
                file_size = 0
            added_size += file_size

        with self._locked_state() as state:
            counted_event_ids = {str(item) for item in (state["counted_event_ids"] if "counted_event_ids" in state else [])}
            if event_id in counted_event_ids:
                return str(state["stop_reason"]) if "stop_reason" in state else ""

            counted_event_ids.add(event_id)
            state["counted_event_ids"] = sorted(counted_event_ids)
            if self.crawl_max_size:
                state["total_size"] = int(state["total_size"]) + added_size if "total_size" in state else added_size
                if ("stop_reason" not in state or not state["stop_reason"]) and int(state["total_size"]) >= self.crawl_max_size:
                    state["stop_reason"] = "crawl_max_size"

            snapshot_stop_reason = ""
            if self.snapshot_max_size and snapshot_id:
                snapshot_sizes = state["snapshot_sizes"] if isinstance(state.get("snapshot_sizes"), dict) else {}
                snapshot_sizes[snapshot_id] = int(snapshot_sizes.get(snapshot_id) or 0) + added_size
                state["snapshot_sizes"] = snapshot_sizes
                if snapshot_sizes[snapshot_id] >= self.snapshot_max_size:
                    snapshot_stop_reasons = state["snapshot_stop_reasons"] if isinstance(state.get("snapshot_stop_reasons"), dict) else {}
                    snapshot_stop_reasons[snapshot_id] = "snapshot_max_size"
                    state["snapshot_stop_reasons"] = snapshot_stop_reasons
                    snapshot_stop_reason = "snapshot_max_size"

            return str(state["stop_reason"]) if "stop_reason" in state and state["stop_reason"] else snapshot_stop_reason

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
                "counted_event_ids": [],
                "snapshot_sizes": {},
                "snapshot_stop_reasons": {},
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
        payload.setdefault("counted_event_ids", [])
        payload.setdefault("snapshot_sizes", {})
        payload.setdefault("snapshot_stop_reasons", {})
        payload.setdefault("total_size", 0)
        payload.setdefault("stop_reason", "")
        return payload

    def _save_state(self, state: dict[str, Any]) -> None:
        self.state_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
