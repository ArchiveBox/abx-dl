"""ProcessService — owns ALL hook subprocess execution."""

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, ClassVar

from bubus import BaseEvent, EventBus

from ..events import BinaryEvent, MachineEvent, ProcessCompleted, ProcessEvent
from ..models import ArchiveResult, Process, VisibleRecord, write_jsonl, now_iso, uuid7
from ..process_utils import write_pid_file_with_mtime, write_cmd_file, is_process_alive
from .base import BaseService


class ProcessService(BaseService):
    """Owns all hook subprocess execution.

    All hook execution flows through ProcessEvent — no other service runs
    processes directly. This is the single place that spawns subprocesses.

    Background hooks are awaited via asyncio tasks that emit ProcessCompleted
    when the subprocess exits.
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
        self._background_tasks: dict[str, _BackgroundHook] = {}
        super().__init__(bus)

    # ── Event handlers ──────────────────────────────────────────────────────

    async def on_ProcessEvent(self, event: ProcessEvent) -> None:
        plugin_output_dir = Path(event.output_dir)

        if event.is_background:
            bg = _spawn_background_hook(
                event.hook_path, event.hook_args,
                plugin_name=event.plugin_name, hook_name=event.hook_name,
                snapshot_id=event.snapshot_id,
                output_dir=plugin_output_dir, env=event.env, timeout=event.timeout,
            )
            self._background_tasks[event.hook_name] = bg

            # Emit a "started" result immediately
            ar = ArchiveResult(
                snapshot_id=event.snapshot_id, plugin=event.plugin_name,
                hook_name=event.hook_name, status='started',
                process_id=bg.proc.id, start_ts=bg.proc.started_at,
            )
            self.emit_result(ar)

            # Launch async task to wait for completion and emit ProcessCompleted
            asyncio.ensure_future(self._wait_for_background(event, bg))
        else:
            proc, ar = _run_foreground_hook(
                event.hook_path, event.hook_args,
                plugin_name=event.plugin_name, hook_name=event.hook_name,
                snapshot_id=event.snapshot_id,
                output_dir=plugin_output_dir, env=event.env, timeout=event.timeout,
            )

            write_jsonl(self.index_path, proc, also_print=self.emit_jsonl)
            write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)

            await self.bus.emit(ProcessCompleted(
                plugin_name=event.plugin_name, hook_name=event.hook_name,
                stdout=proc.stdout, stderr=proc.stderr,
                exit_code=proc.exit_code or 0, output_dir=event.output_dir,
                output_files=ar.output_files, output_str=ar.output_str,
                status=ar.status, is_background=False,
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

    # ── Background hook async wait ─────────────────────────────────────────

    async def _wait_for_background(self, event: ProcessEvent, bg: '_BackgroundHook') -> None:
        """Wait for a background subprocess to finish, then emit ProcessCompleted."""
        try:
            proc, ar = await asyncio.get_event_loop().run_in_executor(
                None, bg.wait_and_finalize,
            )
        except asyncio.CancelledError:
            bg.kill()
            proc, ar = bg.finalize(error='Cancelled')
        finally:
            self._background_tasks.pop(event.hook_name, None)

        write_jsonl(self.index_path, proc, also_print=self.emit_jsonl)
        write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)

        await self.bus.emit(ProcessCompleted(
            plugin_name=event.plugin_name, hook_name=event.hook_name,
            stdout=proc.stdout, stderr=proc.stderr,
            exit_code=proc.exit_code or 0, output_dir=event.output_dir,
            output_files=ar.output_files, output_str=ar.output_str,
            status=ar.status, is_background=True,
        ))
        self.emit_result(ar)

    # ── Background hook cleanup (called by orchestrator in finally) ────────

    async def wait_for_all_background(self) -> None:
        """Wait for all background hooks to complete."""
        tasks = list(self._background_tasks.values())
        if not tasks:
            return
        # The async tasks are already running; just gather them
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()
                   and not t.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    def cleanup_background_hooks(self) -> list[ArchiveResult]:
        """Kill remaining background hooks. Returns final ArchiveResults."""
        results: list[ArchiveResult] = []
        for hook_name, bg in list(self._background_tasks.items()):
            bg.kill()
            proc, ar = bg.finalize(error=None)
            write_jsonl(self.index_path, proc, also_print=self.emit_jsonl)
            write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)
            results.append(ar)
        self._background_tasks.clear()
        return results


class _BackgroundHook:
    """Holds state for a running background hook subprocess."""

    def __init__(
        self, process: subprocess.Popen, proc: Process, *,
        snapshot_id: str, plugin_name: str, hook_name: str,
        output_dir: Path, timeout: int, files_before: set[Path],
        stdout_file: Path, stderr_file: Path, pid_file: Path,
    ):
        self.process = process
        self.proc = proc
        self.snapshot_id = snapshot_id
        self.plugin_name = plugin_name
        self.hook_name = hook_name
        self.output_dir = output_dir
        self.timeout = timeout
        self.files_before = files_before
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file
        self.pid_file = pid_file

    def wait_and_finalize(self) -> tuple[Process, ArchiveResult]:
        """Block until the process exits, then build Process/ArchiveResult."""
        try:
            self.process.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            return self.finalize(error=f'Hook timed out after {self.timeout} seconds')
        return self.finalize()

    def kill(self) -> None:
        """SIGTERM → wait → SIGKILL the background process."""
        pid = self.process.pid
        try:
            try:
                os.killpg(pid, signal.SIGTERM)
            except (OSError, ProcessLookupError):
                os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return

        try:
            self.process.wait(timeout=2.0)
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
            self.process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            pass

    def finalize(self, error: str | None = None) -> tuple[Process, ArchiveResult]:
        """Read logs and build final Process/ArchiveResult."""
        stdout = self.stdout_file.read_text() if self.stdout_file.exists() else ''
        stderr = self.stderr_file.read_text() if self.stderr_file.exists() else ''

        exit_code = self.process.returncode if self.process.returncode is not None else -1
        self.proc.exit_code = exit_code
        self.proc.stdout = stdout
        self.proc.stderr = stderr
        self.proc.ended_at = now_iso()

        files_after = set(self.output_dir.rglob('*')) if self.output_dir.exists() else set()
        new_files = sorted(str(f.relative_to(self.output_dir)) for f in (files_after - self.files_before) if f.is_file())
        excluded_suffixes = ('.stdout.log', '.stderr.log', '.pid', '.sh')
        new_files = [f for f in new_files if not any(f.endswith(suffix) for suffix in excluded_suffixes)]

        status = 'succeeded' if exit_code == 0 and error is None else 'failed'
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

        output_str = _normalize_output_str(output_str, self.output_dir, new_files)

        if exit_code == 0 and error is None:
            self.stdout_file.unlink(missing_ok=True)
            self.stderr_file.unlink(missing_ok=True)
        self.pid_file.unlink(missing_ok=True)

        ar = ArchiveResult(
            snapshot_id=self.snapshot_id, plugin=self.plugin_name,
            hook_name=self.hook_name, status=status, process_id=self.proc.id,
            output_str=output_str, output_files=new_files,
            start_ts=self.proc.started_at, end_ts=self.proc.ended_at,
            error=error or (stderr if exit_code != 0 else None),
        )
        return self.proc, ar


# ── Module-level helpers ────────────────────────────────────────────────────

def _spawn_background_hook(
    hook_path: str, hook_args: list[str], *,
    plugin_name: str, hook_name: str, snapshot_id: str,
    output_dir: Path, env: dict[str, str], timeout: int = 60,
) -> _BackgroundHook:
    """Spawn a background hook subprocess, return a _BackgroundHook handle."""
    cmd = [hook_path, *hook_args]
    proc = Process(cmd=cmd, pwd=str(output_dir), timeout=timeout, started_at=now_iso(),
                   plugin=plugin_name, hook_name=hook_name)

    stdout_file = output_dir / f'{hook_name}.stdout.log'
    stderr_file = output_dir / f'{hook_name}.stderr.log'
    pid_file = output_dir / f'{hook_name}.pid'
    cmd_file = output_dir / f'{hook_name}.sh'

    write_cmd_file(cmd_file, cmd)
    files_before = set(output_dir.rglob('*')) if output_dir.exists() else set()

    out = open(stdout_file, 'w')
    err = open(stderr_file, 'w')
    process = subprocess.Popen(cmd, cwd=str(output_dir), stdout=out, stderr=err,
                               env=env, start_new_session=True)
    write_pid_file_with_mtime(pid_file, process.pid, time.time())

    return _BackgroundHook(
        process, proc, snapshot_id=snapshot_id, plugin_name=plugin_name,
        hook_name=hook_name, output_dir=output_dir, timeout=timeout,
        files_before=files_before, stdout_file=stdout_file,
        stderr_file=stderr_file, pid_file=pid_file,
    )


def _run_foreground_hook(
    hook_path: str, hook_args: list[str], *,
    plugin_name: str, hook_name: str, snapshot_id: str,
    output_dir: Path, env: dict[str, str], timeout: int = 60,
) -> tuple[Process, ArchiveResult]:
    """Run a foreground hook synchronously and return (Process, ArchiveResult)."""
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
            process = subprocess.Popen(cmd, cwd=str(output_dir), stdout=out, stderr=err, env=env)
            write_pid_file_with_mtime(pid_file, process.pid, time.time())

            try:
                returncode = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                proc.exit_code = -1
                proc.stderr = f'Hook timed out after {timeout} seconds'
                proc.ended_at = now_iso()
                ar = ArchiveResult(
                    snapshot_id=snapshot_id, plugin=plugin_name, hook_name=hook_name,
                    status='failed', process_id=proc.id, start_ts=proc.started_at,
                    end_ts=proc.ended_at, error=proc.stderr,
                )
                return proc, ar

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
