"""ArchiveResultService — owns ArchiveResult construction from hook output."""

import asyncio
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
from ..output_files import OutputFile, changed_output_paths, scan_output_files
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
        # Keep sanitization and metadata validation, but not on the event loop:
        # a remote tree walk must not prevent unrelated hooks from reporting
        # readiness/exiting, or delay the cleanup that lets the snapshot seal.
        # Scan only what this hook created or rewrote. Scanning the whole shared
        # plugin dir made every hook of a plugin (chrome launch/tab/navigate,
        # archivewebpage start/stop) claim its siblings' files too, so summing
        # output sizes counted those bytes once per hook.
        output_files = await asyncio.to_thread(
            lambda: scan_output_files(
                output_dir,
                file_paths=changed_output_paths(output_dir, started_process.files_before),
                containment_root=output_dir.parent,
            ),
        )
        output_files = self._unclaimed_output_files(output_files, snapshot_event, event.plugin_name, event.hook_name)
        archive_result_payload["snapshot_id"] = snapshot_event.snapshot_id
        archive_result_payload["plugin"] = event.plugin_name
        archive_result_payload["hook_name"] = event.hook_name
        archive_result_payload["output_files"] = output_files
        try:
            ar = ArchiveResult(**archive_result_payload)
        except ValidationError:
            return

        # Emitting records this result's file claims in bus history at once;
        # only then may an await (the index append, off the event loop) let a
        # concurrent report of the same files run.
        archive_result_event = event.emit(
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
        )
        index_path = Path(event.output_dir).parent / "index.jsonl"
        await asyncio.to_thread(write_jsonl, index_path, ar, also_print=self.emit_jsonl)
        await archive_result_event.now()

    def _unclaimed_output_files(
        self,
        output_files: list[OutputFile],
        snapshot_event: SnapshotEvent,
        plugin: str,
        hook_name: str,
    ) -> list[OutputFile]:
        """Drop files another hook of this snapshot run already reported.

        Each file belongs to exactly one ArchiveResult, so output sizes sum
        without double counting. Hooks of one plugin can overlap in time (a
        background daemon runs while its siblings write), and both then see the
        same files change; the first result to report a file owns it. The same
        hook's earlier report is the same result being reconciled, and results
        from before this SnapshotEvent belong to a previous run whose output
        this run overwrites, so neither blocks a claim.

        Synchronous on purpose: callers emit the result right after this with
        no await between, and emitting records it in bus history immediately,
        so two concurrent reports can never both claim one file.
        """
        claimed: set[str] = set()
        for candidate in self.bus.event_history.values():
            if (
                isinstance(candidate, ArchiveResultEvent)
                and candidate.snapshot_id == snapshot_event.snapshot_id
                and candidate.plugin == plugin
                and candidate.hook_name != hook_name
                and candidate.event_created_at >= snapshot_event.event_created_at
            ):
                claimed.update(output_file.path for output_file in candidate.output_files)
        return [output_file for output_file in output_files if output_file.path not in claimed]

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

        # A background hook's lifetime spans its siblings' runs, so its process
        # also touched the files they already reported.
        output_files = self._unclaimed_output_files(event.output_files, snapshot_event, event.plugin_name, event.hook_name)

        if event.cancelled:
            # Failure cards and "retry failed" should describe broken captures,
            # not a user's choice to stop waiting. This final event withdraws
            # even an earlier success report from an unfinished attempt.
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="cancelled",
                output_files=output_files,
            )
        elif event.exit_code == PROCESS_EXIT_SKIPPED:
            # The explicit skipped sentinel is distinct from a failed exit.
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="skipped",
                output_files=output_files,
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
                output_files=output_files,
                error=event.stderr or f"Hook exited with code {event.exit_code}",
            )
        else:
            ar = ArchiveResult(
                snapshot_id=snapshot_event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="noresult",
                output_files=output_files,
            )

        if existing is not None:
            # Reconcile the same result, rather than leave a separate successful
            # record behind for JSONL consumers when the process later fails.
            ar.id = existing.id
            # Correct lifecycle status without erasing the hook's own output
            # description/error details or structured partial-result metadata.
            ar.output_str = existing.output_str
            ar.output_json = existing.output_json

        # Emitting records this result's file claims in bus history at once;
        # only then may an await (the index append, off the event loop) let a
        # concurrent report of the same files run.
        archive_result_event = event.emit(
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
        )
        index_path = Path(event.output_dir).parent / "index.jsonl"
        await asyncio.to_thread(write_jsonl, index_path, ar, also_print=self.emit_jsonl)
        await archive_result_event.now()
