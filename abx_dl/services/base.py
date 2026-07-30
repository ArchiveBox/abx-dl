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
    pending = {stdout_task, completed_task}
    deadline = asyncio.get_running_loop().time() + timeout
    try:
        while pending:
            remaining = max(deadline - asyncio.get_running_loop().time(), 0.0)
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(
                pending,
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                break
            for task in done:
                ready_event = task.result()
                if isinstance(ready_event, ProcessStdoutEvent):
                    return
                if isinstance(ready_event, ProcessCompletedEvent):
                    if ready_event.status == "failed" or ready_event.exit_code != 0:
                        raise RuntimeError(
                            f"Background hook {started_event.hook_name} exited before readiness",
                        )
                    return
    finally:
        for task in pending:
            task.cancel()
        for task in pending:
            try:
                await task
            except asyncio.CancelledError:
                pass

    raise RuntimeError(f"Background hook {started_event.hook_name} did not become ready")
