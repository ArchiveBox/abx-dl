"""ProcessService — owns all hook subprocess execution and JSONL output parsing."""

import asyncio
import time
from pathlib import Path
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..events import BinaryProcessEvent, ProcessCompletedEvent, ProcessEvent, ProcessKillEvent, ProcessStartedEvent, ProcessStdoutEvent
from ..models import Process, write_jsonl, now_iso
from ..process_utils import write_pid_file_with_mtime, write_cmd_file, graceful_kill_process, graceful_kill_by_pid_file
from .base import BaseService


class ProcessService(BaseService):
    """Runs hook subprocesses and routes their JSONL output to the bus.

    This is the only service that spawns subprocesses. Every hook execution
    flows through here via ProcessEvent. ProcessService does NOT interpret
    stdout — it emits ProcessStdoutEvent for every stdout line, and each
    consuming service parses lines for the shapes it cares about.

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
        │   - Every line → ProcessStdoutEvent
        │     (each service parses and handles its own types)
        │
        ├── Wait for process exit (with timeout)
        │   - FG hooks: per-plugin timeout (PLUGINNAME_TIMEOUT)
        │   - BG daemons: phase timeout ceiling (sum of all plugin timeouts
        │     in the phase); normally killed earlier by ProcessKillEvent
        │   - On timeout: SIGTERM → wait 15s → SIGKILL
        │
        └── Finalize:
            - Write Process record to index.jsonl
            - Emit ProcessCompletedEvent (raw process info)

    Realtime event emission is critical: ProcessStdoutEvent is emitted with
    ``await self.bus.emit()`` (queue-jump), so the entire handler chain
    (e.g. BinaryEvent → provider install → MachineEvent) completes before the
    next stdout line is read.

    ArchiveResult construction is handled entirely by ArchiveResultService,
    which listens for ProcessCompletedEvent and enriches inline ArchiveResult
    records with process metadata.

    State:
    - No mutable state beyond constructor args. Each ProcessEvent is handled
      independently. The subprocess's output determines what events are emitted.

    File artifacts per hook execution:
    - ``{hook_name}.stdout.log`` — full stdout (deleted on success)
    - ``{hook_name}.stderr.log`` — full stderr (deleted on success)
    - ``{hook_name}.pid`` — PID file used while the subprocess is alive (deleted on exit)
    - ``{hook_name}.sh`` — command line for debugging

    abxbus details:
    - ProcessStdoutEvent is emitted with ``await``, making it a
      synchronous child. Each consuming service's handler chain completes
      before the next line is processed.
    - ProcessCompletedEvent is emitted with ``await`` as an informational child.
      ArchiveResultService handles it to build the final ArchiveResult.
    - Background hooks use ``start_new_session=True`` so SIGTERM to the parent
      process group doesn't accidentally kill them.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessEvent, BinaryProcessEvent, ProcessKillEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        ProcessStartedEvent,
        ProcessStdoutEvent,
        ProcessCompletedEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        emit_jsonl: bool,
        stderr_is_tty: bool,
    ):
        self.emit_jsonl = emit_jsonl
        self.stderr_is_tty = stderr_is_tty
        self._background_monitors: set[asyncio.Task[None]] = set()
        super().__init__(bus)

    # ── Event handlers ──────────────────────────────────────────────────────

    async def on_ProcessEvent(self, event: ProcessEvent) -> None:
        await self._run_process_event(event)

    async def on_BinaryProcessEvent(self, event: BinaryProcessEvent) -> None:
        await self._run_process_event(event)

    async def _run_process_event(self, event: ProcessEvent) -> None:
        """Run a hook subprocess, stream its output, and emit completion events."""
        plugin_output_dir = Path(event.output_dir)
        plugin_output_dir.mkdir(parents=True, exist_ok=True)

        cmd = [event.hook_path, *event.hook_args]
        stdout_file = plugin_output_dir / f"{event.hook_name}.stdout.log"
        stderr_file = plugin_output_dir / f"{event.hook_name}.stderr.log"
        pid_file = plugin_output_dir / f"{event.hook_name}.pid"
        cmd_file = plugin_output_dir / f"{event.hook_name}.sh"

        proc = Process(
            cmd=cmd,
            pwd=str(plugin_output_dir),
            timeout=event.timeout,
            started_at=now_iso(),
            plugin=event.plugin_name,
            hook_name=event.hook_name,
        )
        write_cmd_file(cmd_file, cmd)
        files_before = set(plugin_output_dir.rglob("*")) if plugin_output_dir.exists() else set()

        process: asyncio.subprocess.Process | None = None
        try:
            with open(stderr_file, "w") as err_fh:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=str(plugin_output_dir),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=err_fh,
                    env=event.env,
                    # bg hooks get their own session so killing the parent process
                    # group (e.g. on CrawlEvent timeout) doesn't cascade to daemons
                    start_new_session=event.is_background,
                )
            write_pid_file_with_mtime(pid_file, process.pid, time.time())
            await self.bus.emit(
                ProcessStartedEvent(
                    plugin_name=event.plugin_name,
                    hook_name=event.hook_name,
                    hook_path=event.hook_path,
                    hook_args=event.hook_args,
                    output_dir=event.output_dir,
                    env=event.env,
                    timeout=event.timeout,
                    pid=process.pid,
                    process_id=proc.id,
                    snapshot_id=event.snapshot_id,
                    is_background=event.is_background,
                    start_ts=proc.started_at or "",
                ),
            )

            if event.is_background:
                monitor = asyncio.create_task(
                    self._monitor_background_process(
                        event=event,
                        proc=proc,
                        process=process,
                        plugin_output_dir=plugin_output_dir,
                        stdout_file=stdout_file,
                        stderr_file=stderr_file,
                        pid_file=pid_file,
                        files_before=files_before,
                    ),
                )
                self._background_monitors.add(monitor)
                monitor.add_done_callback(self._background_monitors.discard)
                return

            await self._monitor_process_until_exit(
                event=event,
                proc=proc,
                process=process,
                plugin_output_dir=plugin_output_dir,
                stdout_file=stdout_file,
                stderr_file=stderr_file,
                pid_file=pid_file,
                files_before=files_before,
                detach_events=False,
            )

        except Exception as e:
            # Kill the subprocess if it was created — without this, bg hooks
            # started with start_new_session=True would leak as orphan processes
            # (e.g. if BinaryEvent(**record) raises ValidationError on malformed
            # JSONL, or if bus.emit() fails during the streaming phase).
            if process is not None:
                await graceful_kill_process(process)
            pid_file.unlink(missing_ok=True)
            proc.exit_code = -1
            proc.stderr = f"{type(e).__name__}: {e}"
            proc.ended_at = now_iso()
            index_path = plugin_output_dir.parent / "index.jsonl"
            write_jsonl(index_path, proc, also_print=self.emit_jsonl)
            await self.bus.emit(
                ProcessCompletedEvent(
                    plugin_name=event.plugin_name,
                    hook_name=event.hook_name,
                    hook_path=event.hook_path,
                    hook_args=event.hook_args,
                    env=event.env,
                    stdout="",
                    stderr=proc.stderr,
                    exit_code=-1,
                    output_dir=event.output_dir,
                    output_files=[],
                    is_background=event.is_background,
                    process_id=proc.id,
                    snapshot_id=event.snapshot_id,
                    pid=process.pid if process is not None else 0,
                    start_ts=proc.started_at or "",
                    end_ts=proc.ended_at or "",
                ),
            )
            return

    async def on_ProcessKillEvent(self, event: ProcessKillEvent) -> None:
        """Gracefully shut down a background daemon via its PID file.

        Triggered by CrawlCleanupEvent/SnapshotCleanupEvent handlers. Validates the
        PID file (mtime + command check), sends SIGTERM, waits up to
        ``grace_period`` for clean exit (the plugin's PLUGINNAME_TIMEOUT),
        then escalates to SIGKILL if the process is still alive.
        """
        output_dir = Path(event.output_dir)
        pid_file = output_dir / f"{event.hook_name}.pid"
        cmd_file = output_dir / f"{event.hook_name}.sh"
        await graceful_kill_by_pid_file(pid_file, cmd_file, grace_period=event.grace_period)

    async def wait_for_background_monitors(self) -> None:
        """Wait for all detached background-process monitor tasks to finish."""
        while self._background_monitors:
            pending = tuple(self._background_monitors)
            await asyncio.gather(*pending, return_exceptions=True)

    async def _emit_event(self, event: BaseEvent, *, detach_from_parent: bool) -> None:
        if not detach_from_parent:
            await self.bus.emit(event)
            return
        event_token = EventBus.current_event_context.set(None)
        handler_token = EventBus.current_handler_id_context.set(None)
        try:
            await self.bus.emit(event)
        finally:
            EventBus.current_handler_id_context.reset(handler_token)
            EventBus.current_event_context.reset(event_token)

    async def _stream_stdout(
        self,
        *,
        event: ProcessEvent,
        proc: Process,
        process: asyncio.subprocess.Process,
        stdout_file: Path,
        detach_events: bool,
    ) -> list[str]:
        stdout_lines: list[str] = []
        assert process.stdout is not None
        with open(stdout_file, "w") as out_fh:
            async for raw_line in process.stdout:
                line = raw_line.decode(errors="replace")
                out_fh.write(line)
                out_fh.flush()
                stdout_lines.append(line)

                output_dir = Path(event.output_dir)
                current_files = (
                    [str(f.relative_to(output_dir)) for f in output_dir.rglob("*") if f.is_file()] if output_dir.is_dir() else []
                )

                await self._emit_event(
                    ProcessStdoutEvent(
                        line=line.strip(),
                        plugin_name=event.plugin_name,
                        hook_name=event.hook_name,
                        output_dir=event.output_dir,
                        snapshot_id=event.snapshot_id,
                        process_id=proc.id,
                        start_ts=proc.started_at or "",
                        end_ts=now_iso(),
                        output_files=current_files,
                    ),
                    detach_from_parent=detach_events,
                )
        return stdout_lines

    async def _monitor_process_until_exit(
        self,
        *,
        event: ProcessEvent,
        proc: Process,
        process: asyncio.subprocess.Process,
        plugin_output_dir: Path,
        stdout_file: Path,
        stderr_file: Path,
        pid_file: Path,
        files_before: set[Path],
        detach_events: bool,
    ) -> int:
        timed_out = False
        try:
            stdout_lines = await asyncio.wait_for(
                self._stream_stdout(
                    event=event,
                    proc=proc,
                    process=process,
                    stdout_file=stdout_file,
                    detach_events=detach_events,
                ),
                timeout=event.timeout or None,
            )
            if process.returncode is None:
                remaining_timeout = event.timeout or None
                await asyncio.wait_for(process.wait(), timeout=remaining_timeout)
        except TimeoutError:
            timed_out = True
            await graceful_kill_process(process)
            stdout_lines = []
        except Exception:
            await graceful_kill_process(process)
            raise

        returncode = process.returncode if process.returncode is not None else 0
        stdout = "".join(stdout_lines)
        stderr = stderr_file.read_text() if stderr_file.exists() else ""

        if timed_out:
            returncode = -1
            stderr = f"Hook timed out after {event.timeout} seconds"

        proc.exit_code = returncode
        proc.stdout = stdout
        proc.stderr = stderr
        proc.ended_at = now_iso()

        files_after = set(plugin_output_dir.rglob("*")) if plugin_output_dir.exists() else set()
        new_files = sorted(str(f.relative_to(plugin_output_dir)) for f in (files_after - files_before) if f.is_file())
        excluded_suffixes = (".stdout.log", ".stderr.log", ".pid", ".sh")
        new_files = [f for f in new_files if not any(f.endswith(s) for s in excluded_suffixes)]

        pid_file.unlink(missing_ok=True)

        if returncode == 0:
            stdout_file.unlink(missing_ok=True)
            stderr_file.unlink(missing_ok=True)

        index_path = plugin_output_dir.parent / "index.jsonl"
        write_jsonl(index_path, proc, also_print=self.emit_jsonl)

        await self._emit_event(
            ProcessCompletedEvent(
                plugin_name=event.plugin_name,
                hook_name=event.hook_name,
                hook_path=event.hook_path,
                hook_args=event.hook_args,
                env=event.env,
                stdout=stdout,
                stderr=stderr,
                exit_code=returncode,
                output_dir=event.output_dir,
                output_files=new_files,
                is_background=event.is_background,
                process_id=proc.id,
                snapshot_id=event.snapshot_id,
                pid=process.pid if process is not None else 0,
                start_ts=proc.started_at or "",
                end_ts=proc.ended_at or "",
            ),
            detach_from_parent=detach_events,
        )
        return returncode

    async def _monitor_background_process(
        self,
        *,
        event: ProcessEvent,
        proc: Process,
        process: asyncio.subprocess.Process,
        plugin_output_dir: Path,
        stdout_file: Path,
        stderr_file: Path,
        pid_file: Path,
        files_before: set[Path],
    ) -> None:
        try:
            await self._monitor_process_until_exit(
                event=event,
                proc=proc,
                process=process,
                plugin_output_dir=plugin_output_dir,
                stdout_file=stdout_file,
                stderr_file=stderr_file,
                pid_file=pid_file,
                files_before=files_before,
                detach_events=True,
            )
        except Exception:
            pid_file.unlink(missing_ok=True)
