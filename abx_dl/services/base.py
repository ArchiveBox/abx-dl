"""Base service class for shared bus/service utilities."""

import asyncio
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import ProcessCompletedEvent, ProcessEvent, ProcessStdoutEvent


class BaseService:
    """Base class for services that share one EventBus."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = []
    EMITS: ClassVar[list[type[BaseEvent]]] = []

    def __init__(self, bus: EventBus):
        self.bus = bus


async def wait_for_background_ready(
    bus: EventBus,
    process_event: ProcessEvent,
    timeout: float,
) -> None:
    """Wait until a background hook reaches its normal stdout boundary."""
    stdout_task = asyncio.create_task(
        bus.find(
            ProcessStdoutEvent,
            child_of=process_event,
            past=True,
            future=timeout,
        ),
    )
    completed_task = asyncio.create_task(
        bus.find(
            ProcessCompletedEvent,
            child_of=process_event,
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

    ready_event = next(iter(done)).result()
    if isinstance(ready_event, ProcessStdoutEvent):
        return
    if isinstance(ready_event, ProcessCompletedEvent):
        if ready_event.status == "failed" or ready_event.exit_code != 0:
            raise RuntimeError(
                f"Background hook {process_event.hook_name} exited before readiness",
            )
        return
    raise RuntimeError(f"Background hook {process_event.hook_name} did not become ready")
