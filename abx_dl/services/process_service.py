"""ProcessService — owns ALL hook subprocess execution."""

import asyncio
import json
import os
import signal
import time
from pathlib import Path
from typing import Any, Callable, ClassVar

from bubus import BaseEvent, EventBus

from ..events import BinaryEvent, MachineEvent, ProcessCompleted, ProcessEvent, ProcessKillEvent
from ..models import ArchiveResult, Process, VisibleRecord, write_jsonl, now_iso
from ..process_utils import write_pid_file_with_mtime, write_cmd_file, safe_kill_process
from .base import BaseService


class ProcessService(BaseService):
    """Owns all hook subprocess execution.

    Stdout is streamed line-by-line: Binary/Machine JSONL events are emitted
    in realtime as the process writes them, so config and binary propagation
    works even for long-running or daemon hooks.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessEvent, ProcessKillEvent]
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
    ):
        self.index_path = index_path
        self.output_dir = output_dir
        self.emit_jsonl = emit_jsonl
        self.stderr_is_tty = stderr_is_tty
        self.emit_result = emit_result
        super().__init__(bus)

    # ── Event handlers ──────────────────────────────────────────────────────

    async def on_ProcessEvent(self, event: ProcessEvent) -> None:
        plugin_output_dir = Path(event.output_dir)
        plugin_output_dir.mkdir(parents=True, exist_ok=True)

        cmd = [event.hook_path, *event.hook_args]
        stdout_file = plugin_output_dir / f'{event.hook_name}.stdout.log'
        stderr_file = plugin_output_dir / f'{event.hook_name}.stderr.log'
        pid_file = plugin_output_dir / f'{event.hook_name}.pid'
        cmd_file = plugin_output_dir / f'{event.hook_name}.sh'

        proc = Process(
            cmd=cmd, pwd=str(plugin_output_dir), timeout=event.timeout,
            started_at=now_iso(), plugin=event.plugin_name,
            hook_name=event.hook_name,
        )
        write_cmd_file(cmd_file, cmd)
        files_before = set(plugin_output_dir.rglob('*')) if plugin_output_dir.exists() else set()

        try:
            with open(stderr_file, 'w') as err_fh:
                process = await asyncio.create_subprocess_exec(
                    *cmd, cwd=str(plugin_output_dir),
                    stdout=asyncio.subprocess.PIPE, stderr=err_fh,
                    env=event.env,
                    start_new_session=event.is_background,
                )
            write_pid_file_with_mtime(pid_file, process.pid, time.time())

            # For bg daemons, emit a preliminary "started" result so callers
            # know the daemon launched before it finishes (or gets SIGTERM'd).
            if event.is_background:
                started_ar = ArchiveResult(
                    snapshot_id=event.snapshot_id, plugin=event.plugin_name,
                    hook_name=event.hook_name, status='started',
                    process_id=proc.id, start_ts=proc.started_at,
                )
                write_jsonl(self.index_path, started_ar, also_print=self.emit_jsonl)
                self.emit_result(started_ar)

            # Stream stdout line-by-line, emitting side-effect events in realtime
            stdout_lines: list[str] = []
            status = 'succeeded'
            output_str = ''

            async def _stream_stdout() -> None:
                nonlocal status, output_str
                assert process.stdout is not None
                with open(stdout_file, 'w') as out_fh:
                    async for raw_line in process.stdout:
                        line = raw_line.decode(errors='replace')
                        out_fh.write(line)
                        out_fh.flush()
                        stdout_lines.append(line)

                        stripped = line.strip()
                        if not stripped.startswith('{'):
                            continue
                        try:
                            record = json.loads(stripped)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(record, dict):
                            continue

                        record_type = record.get('type')
                        if record_type == 'Binary':
                            record.pop('type')
                            await self.bus.emit(BinaryEvent(**record))
                        elif record_type == 'Machine':
                            record.pop('type')
                            await self.bus.emit(MachineEvent(**record))
                        elif record_type == 'ArchiveResult':
                            status = record.get('status', status)
                            output_str = record.get('output_str', '')

            async def _stream_and_wait() -> None:
                await _stream_stdout()
                await process.wait()

            timed_out = False
            try:
                await asyncio.wait_for(_stream_and_wait(), timeout=event.timeout or None)
            except asyncio.TimeoutError:
                timed_out = True
                await _kill_process(process)

            returncode = process.returncode or 0

        except Exception as e:
            proc.exit_code = -1
            proc.stderr = f'{type(e).__name__}: {e}'
            proc.ended_at = now_iso()
            ar = ArchiveResult(
                snapshot_id=event.snapshot_id, plugin=event.plugin_name,
                hook_name=event.hook_name, status='failed',
                process_id=proc.id, error=proc.stderr,
            )
            write_jsonl(self.index_path, proc, also_print=self.emit_jsonl)
            write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)
            await self.bus.emit(ProcessCompleted(
                plugin_name=event.plugin_name, hook_name=event.hook_name,
                stdout='', stderr=proc.stderr, exit_code=-1,
                output_dir=event.output_dir, status='failed',
                is_background=event.is_background,
            ))
            self.emit_result(ar)
            return

        # Finalize
        stdout = ''.join(stdout_lines)
        stderr = stderr_file.read_text() if stderr_file.exists() else ''

        if timed_out:
            returncode = -1
            stderr = f'Hook timed out after {event.timeout} seconds'
            status = 'failed'

        proc.exit_code = returncode
        proc.stdout = stdout
        proc.stderr = stderr
        proc.ended_at = now_iso()

        files_after = set(plugin_output_dir.rglob('*')) if plugin_output_dir.exists() else set()
        new_files = sorted(
            str(f.relative_to(plugin_output_dir))
            for f in (files_after - files_before) if f.is_file()
        )
        excluded_suffixes = ('.stdout.log', '.stderr.log', '.pid', '.sh')
        new_files = [f for f in new_files if not any(f.endswith(s) for s in excluded_suffixes)]

        if returncode != 0:
            status = 'failed'

        output_str = _normalize_output_str(output_str, plugin_output_dir, new_files)

        if returncode == 0:
            stdout_file.unlink(missing_ok=True)
            stderr_file.unlink(missing_ok=True)
            pid_file.unlink(missing_ok=True)

        ar = ArchiveResult(
            snapshot_id=event.snapshot_id, plugin=event.plugin_name,
            hook_name=event.hook_name, status=status, process_id=proc.id,
            output_str=output_str, output_files=new_files,
            start_ts=proc.started_at, end_ts=proc.ended_at,
            error=stderr if returncode != 0 else None,
        )

        write_jsonl(self.index_path, proc, also_print=self.emit_jsonl)
        write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)

        await self.bus.emit(ProcessCompleted(
            plugin_name=event.plugin_name, hook_name=event.hook_name,
            stdout=stdout, stderr=stderr, exit_code=returncode,
            output_dir=event.output_dir, output_files=new_files,
            output_str=output_str, status=status,
            is_background=event.is_background,
        ))
        self.emit_result(ar)

    async def on_ProcessKillEvent(self, event: ProcessKillEvent) -> None:
        """SIGTERM a background daemon process via its PID file."""
        output_dir = Path(event.output_dir)
        pid_file = output_dir / f'{event.hook_name}.pid'
        cmd_file = output_dir / f'{event.hook_name}.sh'
        safe_kill_process(pid_file, cmd_file, signal_num=signal.SIGTERM)


# ── Process cleanup ────────────────────────────────────────────────────────

async def _kill_process(process: asyncio.subprocess.Process) -> None:
    """SIGTERM → wait → SIGKILL an asyncio subprocess."""
    pid = process.pid
    if pid is None:
        return
    try:
        try:
            os.killpg(pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        await asyncio.wait_for(process.wait(), timeout=2.0)
        return
    except asyncio.TimeoutError:
        pass

    try:
        try:
            os.killpg(pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return

    try:
        await asyncio.wait_for(process.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        pass


# ── Pure helpers ────────────────────────────────────────────────────────────

def _normalize_output_str(output_str: str, output_dir: Path, output_files: list[str]) -> str:
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
