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
from ..limits import CrawlLimitState
from ..models import ArchiveResult, write_jsonl
from ..output_files import OutputFile, scan_output_files
from .base import BaseService


class ArchiveResultService(BaseService):
    """Builds ArchiveResult records from hook output and process completion.

    Listens for two events:

    1. **ProcessStdoutEvent** (type=ArchiveResult): the hook's
       self-reported result. Emits an ArchiveResultEvent and writes it to
       index.jsonl immediately.

    2. **ProcessCompletedEvent**: only for ``on_Snapshot`` hooks, emits a
       synthetic ArchiveResultEvent when the hook didn't already report one:
       - If exit_code is nonzero and not the skipped sentinel → synthetic
         ``failed`` result (with stderr as error).
       - If exit_code == 0 and non-metadata output files exist → synthetic
         ``succeeded`` result.
       - If exit_code == 0 and no content files → synthetic ``noresult`` result.

       Install, CrawlSetup, and BinaryRequest hooks are excluded — they don't
       produce ArchiveResults.
       Uses ``bus.find()`` to check whether an ArchiveResultEvent was already
       emitted for this hook, avoiding the need for manual pending-state tracking.
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

        output_files = scan_output_files(Path(event.output_dir))
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
        """Emit a synthetic ArchiveResult only for Snapshot hooks that didn't self-report."""
        if not event.hook_name.startswith("on_Snapshot"):
            return

        limit_state = CrawlLimitState.from_env(event.env)
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
        limit_state.record_process_output(
            started_process.event_id,
            Path(event.output_dir),
            [output_file.path for output_file in event.output_files],
            snapshot_id=snapshot_event.snapshot_id,
        )

        existing = await self.bus.find(
            ArchiveResultEvent,
            child_of=started_process,
            past=True,
            future=False,
        )
        if existing is not None:
            return

        if event.exit_code == PROCESS_EXIT_SKIPPED:
            # Skipped process with no inline result → synthetic skipped result
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="skipped",
                output_files=event.output_files,
            )
        elif event.exit_code != 0:
            # Failed process with no inline result → synthetic failure
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="failed",
                output_files=event.output_files,
                error=event.stderr or None,
            )
        elif _has_content_files(event.output_files):
            # Succeeded with real output files but no inline result → synthetic success
            primary_output = next(
                (
                    output_file
                    for output_file in event.output_files
                    if output_file.mimetype == "text/html" or output_file.extension in {"html", "htm", "shtml"}
                ),
                event.output_files[0],
            )
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="succeeded",
                output_str=f"{event.plugin_name}/{primary_output.path}",
                output_files=event.output_files,
            )
        else:
            # Succeeded but no content files → noresult
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="noresult",
                output_files=event.output_files,
            )

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
                output_json=ar.output_json,
                error=ar.error or "",
            ),
        ).now()


# ── Pure helpers ────────────────────────────────────────────────────────────


def _has_content_files(output_files: list[OutputFile]) -> bool:
    """Return True if the hook produced any real output files."""
    return bool(output_files)
