"""ProcessService — owns all hook subprocess execution and JSONL output parsing."""

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Callable, ClassVar

from bubus import BaseEvent, EventBus

from ..events import BinaryEvent, MachineEvent, ProcessCompleted, ProcessEvent, ProcessKillEvent
from ..models import ArchiveResult, Process, VisibleRecord, write_jsonl, now_iso
from ..process_utils import write_pid_file_with_mtime, write_cmd_file, graceful_kill_process, graceful_kill_by_pid_file
from .base import BaseService


class ProcessService(BaseService):
    """Runs hook subprocesses and parses their JSONL output into bus events.

    This is the only service that spawns subprocesses. Every hook execution
    flows through here via ProcessEvent.

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
        │   - {"type": "Binary", ...}  → emit BinaryEvent (triggers install chain)
        │   - {"type": "Machine", ...} → emit MachineEvent (updates shared config)
        │   - {"type": "ArchiveResult", ...} → captures status/output_str
        │   - Non-JSON lines are logged but not parsed
        │
        ├── Wait for process exit (with timeout)
        │   - On timeout: SIGTERM → wait 2s → SIGKILL
        │
        └── Finalize:
            - Write Process + ArchiveResult to index.jsonl
            - Emit ProcessCompleted event
            - Call emit_result() callback

    Realtime event emission is critical: when a hook outputs Binary/Machine JSONL,
    the corresponding event is emitted immediately (via ``await self.bus.emit()``).
    This triggers a bubus "queue-jump" — the BinaryEvent and its entire install
    chain complete synchronously before the next stdout line is read. This is how
    a Crawl install hook can request a binary, have it installed by provider hooks,
    and see the result in config — all within a single ProcessEvent.

    State:
    - No mutable state beyond constructor args. Each ProcessEvent is handled
      independently. The subprocess's output determines what events are emitted.

    File artifacts per hook execution:
    - ``{hook_name}.stdout.log`` — full stdout (deleted on success)
    - ``{hook_name}.stderr.log`` — full stderr (deleted on success)
    - ``{hook_name}.pid`` — PID file for daemon hooks (deleted on success)
    - ``{hook_name}.sh`` — command line for debugging

    bubus details:
    - Side-effect events (BinaryEvent, MachineEvent) are emitted with ``await``,
      making them synchronous children of the ProcessEvent. They complete before
      the next stdout line is processed.
    - ProcessCompleted is also emitted with ``await`` as an informational child.
    - Background hooks use ``start_new_session=True`` so SIGTERM to the parent
      process group doesn't accidentally kill them.
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
                """Read stdout line-by-line, emitting side-effect events in realtime.

                JSON lines with "type" field trigger immediate event emission:
                - Binary → BinaryEvent (may trigger full install chain via queue-jump)
                - Machine → MachineEvent (updates shared config immediately)
                - ArchiveResult → captures status/output_str (no event emitted)
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

                        record_type = record.get('type')
                        if record_type == 'Binary':
                            record.pop('type')
                            # await = queue-jump: entire binary install chain
                            # completes before we read the next stdout line
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
            await self.bus.emit(ProcessCompleted(
                plugin_name=event.plugin_name, hook_name=event.hook_name,
                stdout='', stderr=proc.stderr, exit_code=-1,
                output_dir=event.output_dir, status='failed',
                is_background=event.is_background,
            ))
            self.emit_result(proc)
            self.emit_result(ar)
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

        await self.bus.emit(ProcessCompleted(
            plugin_name=event.plugin_name, hook_name=event.hook_name,
            stdout=stdout, stderr=stderr, exit_code=returncode,
            output_dir=event.output_dir, output_files=new_files,
            output_str=output_str, status=status,
            is_background=event.is_background,
        ))
        self.emit_result(proc)
        self.emit_result(ar)

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
