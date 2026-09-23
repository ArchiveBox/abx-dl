"""ArchiveResultService — owns ArchiveResult construction from hook output."""

import json
from pathlib import Path
from typing import Any, ClassVar

from abxbus import BaseEvent, EventBus
from pydantic import ValidationError

from ..events import (
    PROCESS_EXIT_SKIPPED,
    ArchiveResultEvent,
    ProcessCompletedEvent,
    ProcessEvent,
    ProcessStartedEvent,
    ProcessStdoutEvent,
    SnapshotEvent,
)
from ..models import ArchiveResult, write_jsonl
from ..output_files import scan_output_files
from .base import BaseService


class ArchiveResultService(BaseService):
    """Builds ArchiveResult records from hook output and process completion.

    Listens for two events:

    1. **ProcessStdoutEvent** (type=ArchiveResult): the hook's
       self-reported result. Emits an ArchiveResultEvent and writes it to
       index.jsonl immediately.

    2. **ProcessCompletedEvent**: only for ``on_Snapshot`` hooks, reconciles
       the reported result with the actual exit:
       - Cancellation withdraws the attempt with a ``cancelled`` event. DB
         consumers remove its row; stopping work is not evidence of failure.
       - Nonzero exit overrides an earlier result with ``failed`` (or
         ``skipped`` for the explicit skipped sentinel).
       - Zero exit preserves a reported result; without one it produces
         ``noresult``. Finishing a process is not proof of captured output.

       Install, CrawlSetup, and BinaryRequest hooks are excluded — they don't
       produce ArchiveResults.
       Uses ``bus.find()`` to check whether an ArchiveResultEvent was already
       emitted for this hook, avoiding the need for manual pending-state tracking.

    Plain stdout is the scheduler's readiness signal, not a success record.
    Hooks must explicitly report the output they actually produced. Do not
    restore directory-based success inference (removed in 40c5e547): several
    hooks/retries share a directory containing metadata, partial or old files.
    Immediate result events let completed parsers persist discovered URLs before
    the whole snapshot finishes, but a later crash must still correct that result.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        ProcessStdoutEvent,
        ProcessCompletedEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ArchiveResultEvent]

    def __init__(self, bus: EventBus, *, emit_jsonl: bool):
        self.emit_jsonl = emit_jsonl
        super().__init__(bus)
        self.bus.on(ProcessStdoutEvent, self.on_ProcessStdoutEvent)
        self.bus.on(ProcessCompletedEvent, self.on_ProcessCompletedEvent)

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Handle inline ArchiveResult records from hook stdout.

        The owning snapshot is resolved from ancestor SnapshotEvents on the bus.
        """
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict):
            return
        archive_result_payload: dict[str, Any] = {str(key): value for key, value in record.items()}
        if "type" not in archive_result_payload or archive_result_payload["type"] != "ArchiveResult":
            return
        started_process = await self.bus.find(
            ProcessStartedEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_parent_of(candidate, event),
        )
        assert isinstance(started_process, ProcessStartedEvent)
        process_event = await self.bus.find(
            ProcessEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_parent_of(candidate, started_process),
        )
        assert isinstance(process_event, ProcessEvent)
        snapshot_event = await self.bus.find(
            SnapshotEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_child_of(process_event, candidate),
        )
        assert isinstance(snapshot_event, SnapshotEvent)

        output_dir = Path(event.output_dir)
        output_files = scan_output_files(
            output_dir,
            containment_root=output_dir.parent,
        )
        archive_result_payload["snapshot_id"] = snapshot_event.snapshot_id
        archive_result_payload["plugin"] = event.plugin_name
        archive_result_payload["hook_name"] = event.hook_name
        archive_result_payload["output_files"] = output_files
        try:
            ar = ArchiveResult(**archive_result_payload)
        except ValidationError:
            return

        index_path = Path(event.output_dir).parent / "index.jsonl"
        write_jsonl(index_path, ar, also_print=self.emit_jsonl)

        await event.emit(
            ArchiveResultEvent(
                snapshot_id=ar.snapshot_id,
                plugin=ar.plugin,
                id=ar.id,
                hook_name=ar.hook_name,
                status=ar.status,
                output_files=output_files,
                start_ts=event.start_ts,
                end_ts=event.end_ts,
                output_str=ar.output_str,
                output_json=ar.output_json,
                error=ar.error or "",
            ),
        ).now()

    async def on_ProcessCompletedEvent(self, event: ProcessCompletedEvent) -> None:
        """Reconcile reported results with the actual hook exit status."""
        if not event.hook_name.startswith("on_Snapshot"):
            return

        process_event = await self.bus.find(
            ProcessEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_parent_of(candidate, event),
        )
        assert isinstance(process_event, ProcessEvent)
        started_process = await self.bus.find(
            ProcessStartedEvent,
            child_of=process_event,
            past=True,
            future=False,
        )
        assert isinstance(started_process, ProcessStartedEvent)
        snapshot_event = await self.bus.find(
            SnapshotEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_child_of(process_event, candidate),
        )
        assert isinstance(snapshot_event, SnapshotEvent)
        existing = await self.bus.find(
            ArchiveResultEvent,
            child_of=started_process,
            past=True,
            future=False,
        )
        # A hook may report output while its process is still alive. Returning
        # merely because a record exists would leave the DB succeeded after a
        # later crash, or after the user withdrew the attempt. Only a clean,
        # uncancelled exit preserves that report.
        if existing is not None and event.exit_code == 0 and not event.cancelled:
            return

        if event.cancelled:
            # Failure cards and "retry failed" should describe broken captures,
            # not a user's choice to stop waiting. This final event withdraws
            # even an earlier success report from an unfinished attempt.
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="cancelled",
                output_files=event.output_files,
            )
        elif event.exit_code == PROCESS_EXIT_SKIPPED:
            # The explicit skipped sentinel is distinct from a failed exit.
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="skipped",
                output_files=event.output_files,
            )
        elif event.exit_code != 0:
            # An unexpected failure overrides even an earlier success record.
            # Controlled cancellation was handled above; do not mix it into
            # errors that a user may need to investigate before retrying.
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="failed",
                output_files=event.output_files,
                error=event.stderr or f"Hook exited with code {event.exit_code}",
            )
        else:
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="noresult",
                output_files=event.output_files,
            )

        if existing is not None:
            # Reconcile the same result, rather than leave a separate successful
            # record behind for JSONL consumers when the process later fails.
            ar.id = existing.id
            # Correct lifecycle status without erasing the hook's own output
            # description/error details or structured partial-result metadata.
            ar.output_str = existing.output_str
            ar.output_json = existing.output_json

        index_path = Path(event.output_dir).parent / "index.jsonl"
        write_jsonl(index_path, ar, also_print=self.emit_jsonl)

        await event.emit(
            ArchiveResultEvent(
                snapshot_id=ar.snapshot_id,
                plugin=ar.plugin,
                id=ar.id,
                hook_name=ar.hook_name,
                status=ar.status,
                output_files=event.output_files,
                start_ts=event.start_ts,
                end_ts=event.end_ts,
                output_str=ar.output_str,
                output_json=ar.output_json,
                error=ar.error or "",
            ),
        ).now()
