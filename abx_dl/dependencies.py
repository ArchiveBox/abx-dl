"""Canonical abxpkg binary request helpers for abx-dl callers."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

from abxbus import EventBus
from abxpkg.binary_service import BinaryEvent, BinaryRequestEvent


async def resolve_binary_requests(
    bus: EventBus,
    specs: Mapping[str, Mapping[str, Any]],
) -> dict[str, BinaryEvent | None]:
    """Declare load-only requests and consume abxpkg's resolved BinaryEvents."""

    requests: dict[str, BinaryRequestEvent] = {}
    for key, spec in specs.items():
        request_payload = {
            field: value
            for field, value in spec.items()
            if field in BinaryRequestEvent.model_fields and field not in {"auto_install", "extra_context"}
        }
        requests[key] = BinaryRequestEvent(
            **request_payload,
            auto_install=False,
            extra_context={"request_key": key},
        )

    emitted = {key: bus.emit(request) for key, request in requests.items()}
    await asyncio.gather(*(event.now() for event in emitted.values()))

    resolved: dict[str, BinaryEvent | None] = {}
    for key, request in emitted.items():
        event = await bus.find(
            BinaryEvent,
            child_of=request,
            past=True,
            future=False,
            name=request.name,
            where=lambda candidate: bool(candidate.abspath),
        )
        resolved[key] = event if isinstance(event, BinaryEvent) else None
    return resolved
