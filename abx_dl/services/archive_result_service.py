"""ArchiveResultService — owns ArchiveResult construction from hook output."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import ArchiveResultEvent, ProcessCompletedEvent, ProcessRecordOutputtedEvent
from ..models import ArchiveResult, write_jsonl
from .base import BaseService

# File extensions that are process metadata, not real hook output
_METADATA_SUFFIXES = {'.log', '.pid', '.sh'}


class ArchiveResultService(BaseService):
    """Builds ArchiveResult records from hook output and process completion.

    Listens for two events:

    1. **ProcessRecordOutputtedEvent** (type=ArchiveResult): the hook's
       self-reported result. Emits an ArchiveResultEvent and writes it to
       index.jsonl immediately.

    2. **ProcessCompletedEvent**: emits a synthetic ArchiveResultEvent only
       when the hook didn't already report one itself:
       - If exit_code != 0 → synthetic ``failed`` result (with stderr as error).
       - If exit_code == 0 and non-metadata output files exist → synthetic
         ``succeeded`` result.
       - Otherwise → no-op.

       Uses ``bus.find()`` to check whether an ArchiveResultEvent was already
       emitted for this hook, avoiding the need for manual pending-state tracking.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        ProcessRecordOutputtedEvent, ProcessCompletedEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ArchiveResultEvent]

    def __init__(self, bus: EventBus, *, index_path: Path, emit_jsonl: bool):
        self.index_path = index_path
        self.emit_jsonl = emit_jsonl
        super().__init__(bus)

    async def on_ProcessRecordOutputtedEvent(self, event: ProcessRecordOutputtedEvent) -> None:
        """Handle inline ArchiveResult records from hook stdout."""
        if event.record_type != 'ArchiveResult':
            return
        record = event.record

        ar = ArchiveResult(
            snapshot_id=record.get('snapshot_id', event.snapshot_id),
            plugin=record.get('plugin', event.plugin_name),
            hook_name=record.get('hook_name', event.hook_name),
            status=record.get('status', ''),
            output_str=record.get('output_str', ''),
            error=record.get('error') or None,
        )

        write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)

        await self.bus.emit(ArchiveResultEvent(
            snapshot_id=ar.snapshot_id, plugin=ar.plugin, id=ar.id,
            hook_name=ar.hook_name, status=ar.status,
            output_str=ar.output_str, error=ar.error or '',
        ))

    async def on_ProcessCompletedEvent(self, event: ProcessCompletedEvent) -> None:
        """Emit a synthetic ArchiveResult only if the hook didn't report one."""
        existing = await self.bus.find(
            ArchiveResultEvent,
            plugin=event.plugin_name,
            hook_name=event.hook_name,
        )
        if existing is not None:
            return

        if event.exit_code != 0:
            # Failed process with no inline result → synthetic failure
            ar = ArchiveResult(
                snapshot_id=event.snapshot_id, plugin=event.plugin_name,
                hook_name=event.hook_name, status='failed',
                error=event.stderr or None,
            )
        elif _has_content_files(event.output_files):
            # Succeeded with real output files but no inline result → synthetic success
            ar = ArchiveResult(
                snapshot_id=event.snapshot_id, plugin=event.plugin_name,
                hook_name=event.hook_name, status='succeeded',
            )
        else:
            return

        write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)

        await self.bus.emit(ArchiveResultEvent(
            snapshot_id=ar.snapshot_id, plugin=ar.plugin, id=ar.id,
            hook_name=ar.hook_name, status=ar.status,
            error=ar.error or '',
        ))


# ── Pure helpers ────────────────────────────────────────────────────────────

def _has_content_files(output_files: list[str]) -> bool:
    """Return True if any output file is not process metadata (.log, .pid, .sh)."""
    return any(
        Path(f).suffix not in _METADATA_SUFFIXES
        for f in output_files
    )
