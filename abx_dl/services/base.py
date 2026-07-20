"""Base service class for shared bus/service utilities."""

import asyncio
import json

from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import ProcessCompletedEvent, ProcessEvent, ProcessStdoutEvent


def _is_background_ready_stdout(event: ProcessStdoutEvent) -> bool:
    """Accept only hook protocol records as background readiness signals."""
    try:
        record = json.loads(event.line)
    except (json.JSONDecodeError, ValueError):
        return False
    return isinstance(record, dict) and record.get("type") in {"ArchiveResult", "ProcessReady"}


async def wait_for_background_ready(bus: EventBus, process_event: ProcessEvent, timeout: float) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        ready_stdout = await bus.find(
            ProcessStdoutEvent,
            child_of=process_event,
            past=True,
            future=False,
            where=_is_background_ready_stdout,
        )
        if ready_stdout is not None:
            return
        completed_process = await bus.find(
            ProcessCompletedEvent,
            child_of=process_event,
            past=True,
            future=False,
        )
        if completed_process is not None:
            return
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        await asyncio.sleep(min(0.05, remaining))
    raise RuntimeError("Background hook did not emit a readiness record or exit")


class BaseService:
    """Base class for services that share one EventBus."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = []
    EMITS: ClassVar[list[type[BaseEvent]]] = []

    def __init__(self, bus: EventBus):
        self.bus = bus
