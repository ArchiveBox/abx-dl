"""ProcessService — owns ALL hook subprocess execution."""

import asyncio
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, ClassVar

from bubus import BaseEvent, EventBus

from ..events import BinaryEvent, MachineEvent, ProcessCompleted, ProcessEvent
from ..models import ArchiveResult, Process, VisibleRecord, write_jsonl, now_iso
from ..process_utils import write_pid_file_with_mtime, write_cmd_file
from .base import BaseService


class ProcessService(BaseService):
    """Owns all hook subprocess execution.

    All hook execution flows through ProcessEvent — no other service runs
    processes directly. Background and foreground hooks follow the same
    flow: spawn, wait, finalize, emit ProcessCompleted. bubus concurrency
    handles running multiple handlers in parallel.
    """

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

        proc, ar = await asyncio.get_event_loop().run_in_executor(
            None, _run_hook,
            event.hook_path, event.hook_args,
            event.plugin_name, event.hook_name, event.snapshot_id,
            event.is_background, plugin_output_dir, event.env, event.timeout,
        )

        write_jsonl(self.index_path, proc, also_print=self.emit_jsonl)
        write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)

        await self.bus.emit(ProcessCompleted(
            plugin_name=event.plugin_name, hook_name=event.hook_name,
            stdout=proc.stdout, stderr=proc.stderr,
            exit_code=proc.exit_code or 0, output_dir=event.output_dir,
            output_files=ar.output_files, output_str=ar.output_str,
            status=ar.status, is_background=event.is_background,
        ))
        self.emit_result(ar)

    async def on_ProcessCompleted(self, event: ProcessCompleted) -> None:
        """Parse JSONL stdout from completed processes, emit Binary/Machine events."""
        for record in _parse_jsonl_records(event.stdout):
            record_type = record.pop('type', None)
            if record_type == 'Binary':
                await self.bus.emit(BinaryEvent(**record))
            elif record_type == 'Machine':
                await self.bus.emit(MachineEvent(**record))


# ── Hook execution ──────────────────────────────────────────────────────────

def _run_hook(
    hook_path: str, hook_args: list[str],
    plugin_name: str, hook_name: str, snapshot_id: str,
    is_background: bool, output_dir: Path, env: dict[str, str], timeout: int = 60,
) -> tuple[Process, ArchiveResult]:
    """Run a hook subprocess (foreground or background) and block until it exits."""
    cmd = [hook_path, *hook_args]
    proc = Process(cmd=cmd, pwd=str(output_dir), timeout=timeout, started_at=now_iso(),
                   plugin=plugin_name, hook_name=hook_name)

    stdout_file = output_dir / f'{hook_name}.stdout.log'
    stderr_file = output_dir / f'{hook_name}.stderr.log'
    pid_file = output_dir / f'{hook_name}.pid'
    cmd_file = output_dir / f'{hook_name}.sh'

    try:
        write_cmd_file(cmd_file, cmd)
        files_before = set(output_dir.rglob('*')) if output_dir.exists() else set()

        with open(stdout_file, 'w') as out, open(stderr_file, 'w') as err:
            process = subprocess.Popen(
                cmd, cwd=str(output_dir), stdout=out, stderr=err,
                env=env, start_new_session=is_background,
            )
            write_pid_file_with_mtime(pid_file, process.pid, time.time())

            try:
                returncode = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                _kill_process(process)
                proc.exit_code = -1
                proc.stderr = f'Hook timed out after {timeout} seconds'
                proc.ended_at = now_iso()
                return proc, ArchiveResult(
                    snapshot_id=snapshot_id, plugin=plugin_name, hook_name=hook_name,
                    status='failed', process_id=proc.id, start_ts=proc.started_at,
                    end_ts=proc.ended_at, error=proc.stderr,
                )

        stdout = stdout_file.read_text() if stdout_file.exists() else ''
        stderr = stderr_file.read_text() if stderr_file.exists() else ''
        proc.exit_code = returncode
        proc.stdout = stdout
        proc.stderr = stderr
        proc.ended_at = now_iso()

        files_after = set(output_dir.rglob('*')) if output_dir.exists() else set()
        new_files = sorted(str(f.relative_to(output_dir)) for f in (files_after - files_before) if f.is_file())
        excluded_suffixes = ('.stdout.log', '.stderr.log', '.pid', '.sh')
        new_files = [f for f in new_files if not any(f.endswith(suffix) for suffix in excluded_suffixes)]

        status = 'succeeded' if returncode == 0 else 'failed'
        output_str = ''
        for line in stdout.strip().split('\n'):
            if line.strip():
                try:
                    record = json.loads(line)
                    if record.get('type') == 'ArchiveResult':
                        status = record.get('status', status)
                        output_str = record.get('output_str', '')
                except json.JSONDecodeError:
                    pass

        output_str = _normalize_output_str(output_str, output_dir, new_files)

        if returncode == 0:
            stdout_file.unlink(missing_ok=True)
            stderr_file.unlink(missing_ok=True)
            pid_file.unlink(missing_ok=True)

        ar = ArchiveResult(
            snapshot_id=snapshot_id, plugin=plugin_name, hook_name=hook_name,
            status=status, process_id=proc.id, output_str=output_str, output_files=new_files,
            start_ts=proc.started_at, end_ts=proc.ended_at,
            error=stderr if returncode != 0 else None,
        )
        return proc, ar

    except Exception as e:
        proc.exit_code = -1
        proc.stderr = f'{type(e).__name__}: {e}'
        proc.ended_at = now_iso()
        ar = ArchiveResult(
            snapshot_id=snapshot_id, plugin=plugin_name, hook_name=hook_name,
            status='failed', process_id=proc.id, error=proc.stderr,
        )
        return proc, ar


def _kill_process(process: subprocess.Popen) -> None:
    """SIGTERM → wait → SIGKILL a subprocess."""
    pid = process.pid
    try:
        try:
            os.killpg(pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        process.wait(timeout=2.0)
        return
    except subprocess.TimeoutExpired:
        pass

    try:
        try:
            os.killpg(pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return

    try:
        process.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        pass


# ── Pure helpers ────────────────────────────────────────────────────────────

def _parse_jsonl_records(stdout: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith('{'):
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


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
