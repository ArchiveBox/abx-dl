from __future__ import annotations

from collections.abc import Collection, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


RECOVERED_SESSION_ERROR = "Server restarted while download was in progress"
SAFE_EXTERNAL_SCHEMES = {"http", "https"}


def normalize_recovered_session_info(
    info: Mapping[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    normalized = dict(info)
    if normalized.get("status") not in {"starting", "running"}:
        return normalized

    normalized["status"] = "failed"
    normalized["error"] = normalized.get("error") or RECOVERED_SESSION_ERROR
    normalized["finished_at"] = normalized.get("finished_at") or (now or datetime.now(timezone.utc)).isoformat()
    return normalized


def get_safe_external_url(url: str) -> str | None:
    candidate = url.strip()
    if not candidate:
        return None

    parsed = urlsplit(candidate)
    if parsed.scheme.lower() not in SAFE_EXTERNAL_SCHEMES or not parsed.netloc:
        return None
    return candidate


def get_visible_log_entries(logs: Mapping[str, str]) -> list[tuple[str, str]]:
    return [(name, content) for name, content in logs.items() if content.strip()]


def resolve_public_session_download(
    session_root: Path,
    requested_path: str,
    *,
    allowed_relative_paths: Collection[str],
) -> Path | None:
    root = session_root.resolve()
    target = (root / requested_path).resolve()

    try:
        relative_target = target.relative_to(root)
    except ValueError:
        return None

    relative_path = relative_target.as_posix()
    if relative_path not in allowed_relative_paths or not target.is_file():
        return None

    return target
