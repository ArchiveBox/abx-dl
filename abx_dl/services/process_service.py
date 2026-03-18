"""ProcessService — executes hook subprocesses and parses JSONL output."""

from pathlib import Path
from typing import Callable, ClassVar

from bubus import BaseEvent, EventBus

from ..events import BinaryEvent, MachineEvent, ProcessCompleted, ProcessEvent
from ..models import VisibleRecord, write_jsonl
from ..plugins import Hook
from .base import BaseService


class ProcessService(BaseService):
    """Handles executing hook processes and parsing JSONL output to emit more events."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessEvent, ProcessCompleted]
    EMITS: ClassVar[list[type[BaseEvent]]] = [ProcessCompleted, BinaryEvent, MachineEvent]

    def __init__(
        self,
        bus: EventBus,
        *,
        index_path: Path,
        output_dir: Path,
        emit_jsonl: bool,
        stderr_is_tty: bool,
        emit_result: Callable[[VisibleRecord], None],
        known_background_meta_files: set[Path],
    ):
        self.index_path = index_path
        self.output_dir = output_dir
        self.emit_jsonl = emit_jsonl
        self.stderr_is_tty = stderr_is_tty
        self.emit_result = emit_result
        self.known_background_meta_files = known_background_meta_files
        super().__init__(bus)

    async def on_ProcessEvent(self, event: ProcessEvent) -> None:
        # Import here to avoid circular import (orchestrator imports services)
        from ..orchestrator import run_hook

        hook = Hook(
            name=event.hook_name, plugin_name=event.plugin_name,
            path=Path(event.hook_path), step=0, priority=0,
            is_background=event.is_background, language=event.hook_language,
        )
        plugin_output_dir = Path(event.output_dir)

        proc, ar, is_background = run_hook(
            hook, event.url, event.snapshot_id, plugin_output_dir, event.env, event.timeout,
        )

        if is_background:
            self.known_background_meta_files.add(plugin_output_dir / f'{hook.name}.meta.json')
            self.emit_result(ar)
        else:
            write_jsonl(self.index_path, proc, also_print=self.emit_jsonl)
            write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)

            # Emit ProcessCompleted notification so side-effect handlers can react
            await self.bus.emit(ProcessCompleted(
                plugin_name=event.plugin_name, hook_name=event.hook_name,
                stdout=proc.stdout, stderr=proc.stderr,
                exit_code=proc.exit_code or 0, output_dir=event.output_dir,
                output_files=ar.output_files, output_str=ar.output_str,
                status=ar.status, is_background=False,
            ))
            self.emit_result(ar)

        # Poll for completed bg hooks between each hook
        await self.poll_and_process_bg_hooks()

    async def on_ProcessCompleted(self, event: ProcessCompleted) -> None:
        """Parse JSONL stdout from completed processes, emit Binary/Machine events."""
        from ..orchestrator import _parse_jsonl_records

        for record in _parse_jsonl_records(event.stdout):
            record_type = record.get('type')
            if record_type == 'Binary':
                await self.bus.emit(BinaryEvent(record=record))
            elif record_type == 'Machine':
                await self.bus.emit(MachineEvent(record=record))

    async def poll_and_process_bg_hooks(self) -> None:
        """Poll for completed background hooks and process their side effects."""
        from ..orchestrator import _poll_background_hooks

        for bg_proc, bg_ar in _poll_background_hooks(
            self.output_dir, self.index_path, self.stderr_is_tty, emit_jsonl=self.emit_jsonl,
            known_meta_files=self.known_background_meta_files,
        ):
            # bg hooks already ran — emit ProcessCompleted for their output
            await self.bus.emit(ProcessCompleted(
                plugin_name=bg_ar.plugin, hook_name=bg_ar.hook_name,
                stdout=bg_proc.stdout, stderr=bg_proc.stderr,
                exit_code=bg_proc.exit_code or 0, output_dir=str(self.output_dir / bg_ar.plugin),
                output_files=bg_ar.output_files, output_str=bg_ar.output_str,
                status=bg_ar.status, is_background=True,
            ))
            self.emit_result(bg_ar)
