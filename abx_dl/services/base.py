"""Base service class for shared bus/service utilities."""

import asyncio
from pathlib import Path
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import ProcessStartedEvent


def _log_tail(path: Path, limit: int = 4096) -> str:
    try:
        with path.open("rb") as log_file:
            log_file.seek(max(0, path.stat().st_size - limit))
            return log_file.read().decode(errors="replace").strip()
    except OSError:
        return ""


class BaseService:
    """Base class for services that share one EventBus."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = []
    EMITS: ClassVar[list[type[BaseEvent]]] = []

    def __init__(self, bus: EventBus):
        self.bus = bus


async def wait_for_process_ready(
    started_event: ProcessStartedEvent,
    timeout: float,
) -> None:
    """Wait until a hook reaches its normal stdout boundary."""
    hook_kind = "Background hook" if started_event.is_background else "Foreground hook"
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if started_event.stdout_file.exists() and started_event.stdout_file.stat().st_size > 0:
            return

        returncode = started_event.subprocess.returncode
        if returncode is not None:
            if started_event.is_background and returncode != 0:
                stdout = _log_tail(started_event.stdout_file)
                stderr = _log_tail(started_event.stderr_file)
                output = "\n".join(
                    part
                    for part in (
                        f"stdout:\n{stdout}" if stdout else "",
                        f"stderr:\n{stderr}" if stderr else "",
                    )
                    if part
                )
                raise RuntimeError(
                    "\n".join(
                        part
                        for part in (
                            f"{hook_kind} {started_event.hook_name} exited before readiness (exit code {returncode})",
                            output,
                            f"Logs: {started_event.stdout_file} {started_event.stderr_file}",
                        )
                        if part
                    ),
                )
            return

        await asyncio.sleep(0.05)

    raise RuntimeError(f"{hook_kind} {started_event.hook_name} did not become ready")
