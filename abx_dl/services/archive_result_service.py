"""ArchiveResultService — owns ArchiveResult construction from hook output."""

from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import ArchiveResultEvent, ProcessCompletedEvent, ProcessRecordOutputtedEvent
from ..models import ArchiveResult, write_jsonl
from .base import BaseService


class ArchiveResultService(BaseService):
    """Builds ArchiveResult records from hook output and process completion.

    Listens for two events:

    1. **ProcessRecordOutputtedEvent** (type=ArchiveResult): captures the hook's
       self-reported status and output_str. Emits an inline ArchiveResultEvent
       (partial — no process metadata yet). ArchiveBox uses these to write DB
       rows for each extracted result in real time.

    2. **ProcessCompletedEvent**: enriches the captured inline data with process
       metadata (process_id, output_files, timestamps, error). Emits the
       complete ArchiveResultEvent and writes the ArchiveResult to index.jsonl.
       If no inline record was captured, derives status from exit_code.

    The orchestrator collects ArchiveResultEvents that have ``process_id`` set
    (i.e. the enriched ones from process completion).
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        ProcessRecordOutputtedEvent, ProcessCompletedEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ArchiveResultEvent]

    def __init__(self, bus: EventBus, *, index_path: Path, emit_jsonl: bool):
        self.index_path = index_path
        self.emit_jsonl = emit_jsonl
        # Track inline ArchiveResult state per hook: (plugin_name, hook_name) → {status, output_str}
        self._pending: dict[tuple[str, str], dict[str, str]] = {}
        super().__init__(bus)

    async def on_ProcessRecordOutputtedEvent(self, event: ProcessRecordOutputtedEvent) -> None:
        """Capture inline ArchiveResult records and emit informational events."""
        if event.record_type != 'ArchiveResult':
            return
        record = event.record

        # Track the latest status/output_str for this hook
        key = (event.plugin_name, event.hook_name)
        self._pending[key] = {
            'status': record.get('status', ''),
            'output_str': record.get('output_str', ''),
        }

        # Emit inline event (informational — for ArchiveBox)
        await self.bus.emit(ArchiveResultEvent(
            snapshot_id=record.get('snapshot_id', event.snapshot_id),
            plugin=record.get('plugin', event.plugin_name),
            id=record.get('id', ''),
            hook_name=record.get('hook_name', event.hook_name),
            status=record.get('status', ''),
            output_str=record.get('output_str', ''),
            error=record.get('error', ''),
        ))

    async def on_ProcessCompletedEvent(self, event: ProcessCompletedEvent) -> None:
        """Build the complete ArchiveResult from inline data + process metadata."""
        key = (event.plugin_name, event.hook_name)
        pending = self._pending.pop(key, None)

        # Derive status: inline record's status, overridden by exit_code
        if event.exit_code != 0:
            status = 'failed'
        elif pending and pending['status']:
            status = pending['status']
        else:
            status = 'succeeded'

        # output_str from inline record, normalized against output dir
        raw_output_str = pending['output_str'] if pending else ''
        output_dir = Path(event.output_dir)
        output_str = _normalize_output_str(raw_output_str, output_dir, event.output_files)

        ar = ArchiveResult(
            snapshot_id=event.snapshot_id, plugin=event.plugin_name,
            hook_name=event.hook_name, status=status, process_id=event.process_id,
            output_str=output_str, output_files=event.output_files,
            start_ts=event.start_ts or None, end_ts=event.end_ts or None,
            error=event.stderr if event.exit_code != 0 else None,
        )

        write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)

        await self.bus.emit(ArchiveResultEvent(
            snapshot_id=ar.snapshot_id, plugin=ar.plugin, id=ar.id,
            hook_name=ar.hook_name, status=ar.status,
            process_id=ar.process_id or '', output_str=ar.output_str,
            output_files=ar.output_files, start_ts=ar.start_ts or '',
            end_ts=ar.end_ts or '', error=ar.error or '',
        ))


# ── Pure helpers ────────────────────────────────────────────────────────────

def _normalize_output_str(output_str: str, output_dir: Path, output_files: list[str]) -> str:
    """Convert absolute paths in output_str to relative paths within output_dir.

    Hooks sometimes report their output as an absolute path. This normalizes it
    to a relative path so the output_str is portable and matches output_files.
    """
    text = output_str.strip()
    if not text:
        return ''
    try:
        output_path = Path(text)
    except Exception:
        return text
    if not output_path.is_absolute():
        return text
    try:
        rel_path = output_path.relative_to(output_dir)
    except ValueError:
        return text
    rel_text = str(rel_path)
    if rel_text in ('', '.'):
        return output_files[0] if output_files else ''
    return rel_text
