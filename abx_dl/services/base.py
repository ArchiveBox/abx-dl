"""Base service class for shared bus/service utilities."""

import asyncio
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import ProcessCompletedEvent, ProcessStartedEvent, ProcessStdoutEvent


class BaseService:
    """Base class for services that share one EventBus."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = []
    EMITS: ClassVar[list[type[BaseEvent]]] = []

    def __init__(self, bus: EventBus):
        self.bus = bus


async def wait_for_background_ready(
    bus: EventBus,
    started_event: ProcessStartedEvent,
    timeout: float,
) -> None:
    """Wait until a background hook reaches its normal stdout boundary."""
    stdout_task = asyncio.create_task(
        bus.find(
            ProcessStdoutEvent,
            child_of=started_event,
            past=True,
            future=timeout,
        ),
    )
    completed_task = asyncio.create_task(
        bus.find(
            ProcessCompletedEvent,
            child_of=started_event,
            past=True,
            future=timeout,
        ),
    )
    done, pending = await asyncio.wait(
        {stdout_task, completed_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    for task in pending:
        try:
            await task
        except asyncio.CancelledError:
            pass

    stdout_event = stdout_task.result() if stdout_task in done else None
    if isinstance(stdout_event, ProcessStdoutEvent):
        return

    completed_event = completed_task.result() if completed_task in done else None
    if isinstance(completed_event, ProcessCompletedEvent):
        if completed_event.status == "failed" or completed_event.exit_code != 0:
            raise RuntimeError(
                f"Background hook {started_event.hook_name} exited before readiness",
            )
        return

    raise RuntimeError(f"Background hook {started_event.hook_name} did not become ready")
