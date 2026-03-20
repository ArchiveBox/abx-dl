"""ProcessService — owns all hook subprocess execution and JSONL output parsing."""

import asyncio
import json
import time
from pathlib import Path
from typing import Callable, ClassVar

from bubus import BaseEvent, EventBus

from ..events import ArchiveResultEvent, ProcessCompletedEvent, ProcessEvent, ProcessKillEvent, ProcessRecordOutputtedEvent
from ..models import ArchiveResult, Process, write_jsonl, now_iso
from ..process_utils import write_pid_file_with_mtime, write_cmd_file, graceful_kill_process, graceful_kill_by_pid_file
from .base import BaseService


class ProcessService(BaseService):
    """Runs hook subprocesses and routes their JSONL output to the bus.

    This is the only service that spawns subprocesses. Every hook execution
    flows through here via ProcessEvent. ProcessService does NOT interpret
    record types — it emits ProcessRecordOutputtedEvent for each typed JSONL
    line, and each service handles its own record types independently.

    Subprocess lifecycle::

        ProcessEvent received
        │
        ├── Create subprocess: [hook_path, *hook_args]
        │   - cwd = plugin output dir
        │   - env = full env dict from MachineService
        │   - stdout = PIPE (streamed line-by-line)
        │   - stderr = written to {hook_name}.stderr.log
        │   - PID written to {hook_name}.pid (for daemon kill)
        │
        ├── Stream stdout line-by-line (realtime):
        │   - Lines starting with '{' are parsed as JSON
        │   - Any JSON with a "type" field → ProcessRecordOutputtedEvent
        │     (each service handles its own types via on_ProcessRecordOutputtedEvent)
        │   - ArchiveResult records also capture status/output_str for finalization
        │   - Non-JSON lines are logged but not parsed
        │
        ├── Wait for process exit (with timeout)
        │   - On timeout: SIGTERM → wait 2s → SIGKILL
        │
        └── Finalize:
            - Write Process + ArchiveResult to index.jsonl
            - Emit ProcessCompletedEvent event
            - Emit ArchiveResultEvent(final=True)
            - Call on_process() callback for Process record

    Realtime event emission is critical: ProcessRecordOutputtedEvent is emitted
    with ``await self.bus.emit()`` (queue-jump), so the entire handler chain
    (e.g. BinaryEvent → provider install → MachineEvent) completes before the
    next stdout line is read.

    State:
    - No mutable state beyond constructor args. Each ProcessEvent is handled
      independently. The subprocess's output determines what events are emitted.

    File artifacts per hook execution:
    - ``{hook_name}.stdout.log`` — full stdout (deleted on success)
    - ``{hook_name}.stderr.log`` — full stderr (deleted on success)
    - ``{hook_name}.pid`` — PID file for daemon hooks (deleted on success)
    - ``{hook_name}.sh`` — command line for debugging

    bubus details:
    - ProcessRecordOutputtedEvent is emitted with ``await``, making it a
      synchronous child. The typed event emitted by the handling service
      (and its entire chain) completes before the next line is processed.
    - ProcessCompletedEvent and final ArchiveResultEvent are also emitted with
      ``await`` as informational children.
    - Background hooks use ``start_new_session=True`` so SIGTERM to the parent
      process group doesn't accidentally kill them.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessEvent, ProcessKillEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        ProcessRecordOutputtedEvent, ProcessCompletedEvent, ArchiveResultEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        index_path: Path,
        output_dir: Path,
        emit_jsonl: bool,
        stderr_is_tty: bool,
        on_process: Callable[[Process], None] | None = None,
    ):
        self.index_path = index_path
        self.output_dir = output_dir
        self.emit_jsonl = emit_jsonl
        self.stderr_is_tty = stderr_is_tty
        self.on_process = on_process
        super().__init__(bus)

    # ── Event handlers ──────────────────────────────────────────────────────

    async def on_ProcessEvent(self, event: ProcessEvent) -> None:
        """Run a hook subprocess, stream its output, and emit completion events."""
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
                    # bg hooks get their own session so killing the parent process
                    # group (e.g. on CrawlEvent timeout) doesn't cascade to daemons
                    start_new_session=event.is_background,
                )
            write_pid_file_with_mtime(pid_file, process.pid, time.time())

            stdout_lines: list[str] = []
            status = 'succeeded'
            output_str = ''

            async def _stream_stdout() -> None:
                """Read stdout line-by-line, routing typed JSONL to the bus.

                JSON lines with a "type" field are emitted as
                ProcessRecordOutputtedEvent. Each service handles its own
                record types (Binary, Machine, Snapshot, ArchiveResult, etc.).

                ArchiveResult records also capture status/output_str locally
                for use in the finalization phase.
                """
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

                        record_type = record.pop('type', '')
                        if not record_type:
                            continue

                        # Capture ArchiveResult status/output_str for finalization
                        if record_type == 'ArchiveResult':
                            status = record.get('status', status)
                            output_str = record.get('output_str', '')

                        # Route to services — await = queue-jump: the entire
                        # handler chain (e.g. BinaryEvent → provider install)
                        # completes before we read the next stdout line
                        await self.bus.emit(ProcessRecordOutputtedEvent(
                            record_type=record_type,
                            record=record,
                            plugin_name=event.plugin_name,
                            hook_name=event.hook_name,
                            output_dir=event.output_dir,
                            snapshot_id=event.snapshot_id,
                        ))

            async def _stream_and_wait() -> None:
                await _stream_stdout()
                await process.wait()

            timed_out = False
            try:
                await asyncio.wait_for(_stream_and_wait(), timeout=event.timeout or None)
            except asyncio.TimeoutError:
                timed_out = True
                await graceful_kill_process(process)

            returncode = process.returncode if process.returncode is not None else 0

        except Exception as e:
            # Kill the subprocess if it was created — without this, bg hooks
            # started with start_new_session=True would leak as orphan processes
            # (e.g. if BinaryEvent(**record) raises ValidationError on malformed
            # JSONL, or if bus.emit() fails during the streaming phase).
            if 'process' in locals():
                await graceful_kill_process(process)
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
            await self.bus.emit(ProcessCompletedEvent(
                plugin_name=event.plugin_name, hook_name=event.hook_name,
                stdout='', stderr=proc.stderr, exit_code=-1,
                output_dir=event.output_dir, status='failed',
                is_background=event.is_background,
            ))
            await self.bus.emit(ArchiveResultEvent(
                snapshot_id=ar.snapshot_id, plugin=ar.plugin, id=ar.id,
                hook_name=ar.hook_name, status=ar.status,
                process_id=ar.process_id or '', error=ar.error or '',
                final=True,
            ))
            if self.on_process:
                self.on_process(proc)
            return

        # ── Finalize: build ArchiveResult from process output ──

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

        # Detect new files created by the hook (excluding our own log files)
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

        # Clean up log files on success (keep them on failure for debugging)
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

        await self.bus.emit(ProcessCompletedEvent(
            plugin_name=event.plugin_name, hook_name=event.hook_name,
            stdout=stdout, stderr=stderr, exit_code=returncode,
            output_dir=event.output_dir, output_files=new_files,
            output_str=output_str, status=status,
            is_background=event.is_background,
        ))
        await self.bus.emit(ArchiveResultEvent(
            snapshot_id=ar.snapshot_id, plugin=ar.plugin, id=ar.id,
            hook_name=ar.hook_name, status=ar.status,
            process_id=ar.process_id or '', output_str=ar.output_str,
            output_files=ar.output_files, start_ts=ar.start_ts or '',
            end_ts=ar.end_ts or '', error=ar.error or '',
            final=True,
        ))
        if self.on_process:
            self.on_process(proc)

    async def on_ProcessKillEvent(self, event: ProcessKillEvent) -> None:
        """Gracefully shut down a background daemon via its PID file.

        Triggered by CrawlCleanupEvent/SnapshotCleanupEvent handlers. Validates the
        PID file (mtime + command check), sends SIGTERM, waits up to 15s for
        clean exit, then escalates to SIGKILL if the process is still alive.

        The 15s grace period is important for Chrome, which needs time to flush
        its user_data_dir (cookies, local storage, session state) on shutdown.
        """
        output_dir = Path(event.output_dir)
        pid_file = output_dir / f'{event.hook_name}.pid'
        cmd_file = output_dir / f'{event.hook_name}.sh'
        await graceful_kill_by_pid_file(pid_file, cmd_file)


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
