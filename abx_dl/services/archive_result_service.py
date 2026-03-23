"""ArchiveResultService — owns ArchiveResult construction from hook output."""

import json
from pathlib import Path
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import ArchiveResultEvent, ProcessCompletedEvent, ProcessKillEvent, ProcessStdoutEvent
from ..limits import CrawlLimitState
from ..models import ArchiveResult, write_jsonl
from ..output_files import OutputFile
from .base import BaseService

# File extensions that are process metadata, not real hook output
_METADATA_SUFFIXES = {".log", ".pid", ".sh"}


class ArchiveResultService(BaseService):
    """Builds ArchiveResult records from hook output and process completion.

    Listens for two events:

    1. **ProcessStdoutEvent** (type=ArchiveResult): the hook's
       self-reported result. Emits an ArchiveResultEvent and writes it to
       index.jsonl immediately.

    2. **ProcessCompletedEvent**: only for ``on_Snapshot`` hooks, emits a
       synthetic ArchiveResultEvent when the hook didn't already report one:
       - If exit_code != 0 → synthetic ``failed`` result (with stderr as error).
       - If exit_code == 0 and non-metadata output files exist → synthetic
         ``succeeded`` result.
       - If exit_code == 0 and no content files → synthetic ``noresult`` result.

       Crawl and Binary hooks are excluded — they don't produce ArchiveResults.
       Uses ``bus.find()`` to check whether an ArchiveResultEvent was already
       emitted for this hook, avoiding the need for manual pending-state tracking.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        ProcessStdoutEvent,
        ProcessCompletedEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ArchiveResultEvent, ProcessKillEvent]

    def __init__(self, bus: EventBus, *, emit_jsonl: bool):
        self.emit_jsonl = emit_jsonl
        super().__init__(bus)

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Handle inline ArchiveResult records from hook stdout."""
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict) or record.get("type") != "ArchiveResult":
            return

        ar = ArchiveResult(
            snapshot_id=record.get("snapshot_id", event.snapshot_id),
            plugin=record.get("plugin", event.plugin_name),
            hook_name=record.get("hook_name", event.hook_name),
            status=record.get("status", ""),
            output_str=record.get("output_str", ""),
            output_json=record.get("output_json") if isinstance(record.get("output_json"), dict) else None,
            output_files=event.output_files,
            error=record.get("error") or None,
        )

        index_path = Path(event.output_dir).parent / "index.jsonl"
        write_jsonl(index_path, ar, also_print=self.emit_jsonl)

        await self.bus.emit(
            ArchiveResultEvent(
                snapshot_id=ar.snapshot_id,
                plugin=ar.plugin,
                id=ar.id,
                hook_name=ar.hook_name,
                status=ar.status,
                process_id=event.process_id,
                output_files=event.output_files,
                start_ts=event.start_ts,
                end_ts=event.end_ts,
                output_str=ar.output_str,
                output_json=ar.output_json,
                error=ar.error or "",
            ),
        )

    async def on_ProcessCompletedEvent(self, event: ProcessCompletedEvent) -> None:
        """Emit a synthetic ArchiveResult only for Snapshot hooks that didn't self-report."""
        if not event.hook_name.startswith("on_Snapshot"):
            return

        limit_state = CrawlLimitState.from_env(event.env)
        stop_reason = limit_state.record_process_output(
            event.process_id,
            Path(event.output_dir),
            [output_file.path for output_file in event.output_files],
        )
        if stop_reason == "max_size":
            await self._kill_running_snapshot_processes(limit_state.crawl_dir)

        existing = await self.bus.find(
            ArchiveResultEvent,
            snapshot_id=event.snapshot_id,
            plugin=event.plugin_name,
            hook_name=event.hook_name,
            process_id=event.process_id,
        )
        if existing is not None:
            return

        if event.exit_code != 0:
            # Failed process with no inline result → synthetic failure
            ar = ArchiveResult(
                snapshot_id=event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="failed",
                output_files=event.output_files,
                error=event.stderr or None,
            )
        elif _has_content_files(event.output_files):
            # Succeeded with real output files but no inline result → synthetic success
            ar = ArchiveResult(
                snapshot_id=event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="succeeded",
                output_files=event.output_files,
            )
        else:
            # Succeeded but no content files → noresult
            ar = ArchiveResult(
                snapshot_id=event.snapshot_id,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                status="noresult",
                output_files=event.output_files,
            )

        index_path = Path(event.output_dir).parent / "index.jsonl"
        write_jsonl(index_path, ar, also_print=self.emit_jsonl)

        await self.bus.emit(
            ArchiveResultEvent(
                snapshot_id=ar.snapshot_id,
                plugin=ar.plugin,
                id=ar.id,
                hook_name=ar.hook_name,
                status=ar.status,
                process_id=event.process_id,
                output_files=event.output_files,
                start_ts=event.start_ts,
                end_ts=event.end_ts,
                output_json=ar.output_json,
                error=ar.error or "",
            ),
        )

    async def _kill_running_snapshot_processes(self, crawl_dir: Path) -> None:
        for pid_file in sorted(crawl_dir.glob("**/*.pid")):
            await self.bus.emit(
                ProcessKillEvent(
                    plugin_name=pid_file.parent.name,
                    hook_name=pid_file.stem,
                    output_dir=str(pid_file.parent),
                    grace_period=1.0,
                    event_timeout=15.0,
                ),
            )


# ── Pure helpers ────────────────────────────────────────────────────────────


def _has_content_files(output_files: list[OutputFile]) -> bool:
    """Return True if any output file is not process metadata (.log, .pid, .sh)."""
    return any(Path(output_file.path).suffix not in _METADATA_SUFFIXES for output_file in output_files)
