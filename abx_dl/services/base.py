"""Base service class for shared bus/service utilities."""

import asyncio
from pathlib import Path
from typing import ClassVar
from collections.abc import Awaitable, Callable

from abxbus import BaseEvent, EventBus

from ..events import CrawlAbortEvent, CrawlPauseEvent, CrawlResumeAndRetryEvent, CrawlResumeAndSkipEvent, ProcessStartedEvent


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


async def wait_for_crawl_resume(bus: EventBus) -> bool:
    """Gate scheduling on outer-runner intent; return True for whole-crawl abort.

    Read the latest control fact rather than clearing a per-hook pause flag.
    A newly spawned hook or a readiness notification must never consume Ctrl+C.
    Existing background hooks may flush logs while paused, but no next hook or
    cleanup phase starts until the user chooses. Abort releases every waiter so
    shutdown cannot deadlock behind the very pause the user is trying to exit.
    """
    while True:
        # Abort is terminal for this bus. Another physical Ctrl+C may enqueue
        # a later Pause fact during shutdown; it must not close the gate again.
        if await bus.find(CrawlAbortEvent, past=True, future=False) is not None:
            return True
        control = await bus.find(
            "Crawl*Event",
            where=lambda event: isinstance(event, (CrawlPauseEvent, CrawlAbortEvent, CrawlResumeAndRetryEvent, CrawlResumeAndSkipEvent)),
            past=True,
            future=False,
        )
        if not isinstance(control, CrawlPauseEvent):
            return isinstance(control, CrawlAbortEvent)
        await asyncio.sleep(0.05)


async def wait_for_process_ready(
    started_event: ProcessStartedEvent,
    timeout: float,
    abort_requested: Callable[[], Awaitable[bool]] | None = None,
) -> None:
    """Wait for readiness, not for a successful ArchiveResult.

    Spawning an OS process does not mean it has installed its listeners or
    attached to Chrome. In short captures the scheduler can otherwise reach
    cleanup and kill a background hook before it has initialized. A live hook
    must write stdout only once the next hook may safely start; earlier
    diagnostics belong on stderr. Successful process exit also releases this
    barrier because there is no longer a live hook waiting to initialize.

    Read the actual stdout file, independently of JSONL parsing/event delivery
    (see 86174379). Requiring an ArchiveResult here would couple readiness to
    output production and encourage hooks to report success before capturing
    anything. Any stdout satisfies readiness; only ArchiveResultService owns
    result status, and this function must never promote it to succeeded.

    Readiness and display precedence are intentionally different contracts.
    Users need stderr progress while a hook is still initializing, but seeing
    "connecting" must not let dependent hooks run before that connection works.
    Conversely, the initial stdout readiness line must not pin the CLI's final
    summary to "started" after later diagnostics or an actual result arrive.
    LiveBusUI owns that presentation policy; do not change this barrier to make
    a progress row update, or merge stderr into stdout to make it visible.
    """
    hook_kind = "Background hook" if started_event.is_background else "Foreground hook"
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if abort_requested is not None and await abort_requested():
            return
        if started_event.stdout_file.exists() and started_event.stdout_file.stat().st_size > 0:
            return

        if started_event.interruption_done is not None:
            # A deliberately stopped startup is a user decision, not a readiness
            # failure. Wait through skip/retry bookkeeping before advancing.
            await started_event.interruption_done.wait()
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
