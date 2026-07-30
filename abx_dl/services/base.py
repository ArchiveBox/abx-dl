"""Base service class for shared bus/service utilities."""

import asyncio
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import ProcessStartedEvent


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
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if started_event.stdout_file.exists() and started_event.stdout_file.stat().st_size > 0:
            return

        returncode = started_event.subprocess.returncode
        if returncode is not None:
            if returncode != 0:
                raise RuntimeError(
                    f"Background hook {started_event.hook_name} exited before readiness",
                )
            return

        await asyncio.sleep(0.05)

    raise RuntimeError(f"Background hook {started_event.hook_name} did not become ready")
