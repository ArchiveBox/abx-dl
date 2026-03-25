"""ProcessService — owns hook subprocess execution and raw process events."""

import asyncio
import signal
import time
from pathlib import Path
from typing import ClassVar, Literal
from collections.abc import Callable

from abxbus import BaseEvent, EventBus
import click
from ..events import (
    PROCESS_EXIT_SKIPPED,
    CrawlCleanupEvent,
    CrawlEvent,
    ProcessCompletedEvent,
    ProcessEvent,
    ProcessKillEvent,
    ProcessStartedEvent,
    ProcessStdoutEvent,
    SnapshotCleanupEvent,
    SnapshotEvent,
)
from ..models import Process, write_jsonl, now_iso
from ..output_files import scan_output_files
from ..process_utils import write_pid_file_with_mtime, write_cmd_file, graceful_kill_process, graceful_kill_by_pid_file
from .base import BaseService


ProcessStatus = Literal["succeeded", "failed", "skipped"]


def _process_status(exit_code: int) -> ProcessStatus:
    """Normalize process exit codes into ArchiveResult-compatible statuses."""
    if exit_code == 0:
        return "succeeded"
    if exit_code == PROCESS_EXIT_SKIPPED:
        return "skipped"
    return "failed"


def _rotate_existing_log(path: Path) -> Path | None:
    """Move an existing non-empty log file aside before reusing its canonical name.

    Hook retries reuse the same ``{hook_name}.stdout.log`` / ``.stderr.log`` paths.
    Without rotation, a later retry overwrites the previous attempt's logs, and a
    later successful retry deletes the canonical files entirely. Preserve the old
    contents under a timestamped filename so failed attempts remain debuggable.
    """
    if not path.exists():
        return None

    try:
        if path.stat().st_size == 0:
            path.unlink(missing_ok=True)
            return None
    except OSError:
        return None

    timestamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    suffix = path.suffix
    stem = path.name[: -len(suffix)] if suffix else path.name
    archived = path.with_name(f"{stem}.{timestamp}{suffix}")
    counter = 1
    while archived.exists():
        archived = path.with_name(f"{stem}.{timestamp}.{counter}{suffix}")
        counter += 1

    path.replace(archived)
    return archived


class ProcessService(BaseService):
    """Runs hook subprocesses and emits only process-level lifecycle events.

    ProcessService does not interpret hook stdout JSONL. Its job is only to:

    1. spawn the subprocess for a ``ProcessEvent``
    2. emit ``ProcessStartedEvent`` with the live subprocess handle and file paths
    3. stream stdout into ``ProcessStdoutEvent`` lines
    4. wait for exit or kill
    5. emit ``ProcessCompletedEvent``

    Background behavior is owned by abxbus and the dispatch site:
    callers ``await bus.emit(ProcessEvent(...))`` for foreground hooks and use
    ``bus.emit(ProcessEvent(...))`` without awaiting for background hooks. The
    ``ProcessEvent`` itself still stays alive until the subprocess exits because
    ``on_ProcessEvent()`` awaits its child ``ProcessStartedEvent``.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        ProcessEvent,
        ProcessKillEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        ProcessStartedEvent,
        ProcessStdoutEvent,
        ProcessCompletedEvent,
        ProcessKillEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        emit_jsonl: bool,
        interactive_tty: bool,
        interrupt_requested: asyncio.Event | None = None,
        force_interrupt: asyncio.Event | None = None,
        set_live_paused: Callable[[bool], None] | None = None,
    ):
        self.emit_jsonl = emit_jsonl
        self.interactive_tty = interactive_tty
        self.interrupt_requested = interrupt_requested or asyncio.Event()
        self.force_interrupt = force_interrupt or asyncio.Event()
        self.set_live_paused = set_live_paused or (lambda _paused: None)
        super().__init__(bus)
        self.bus.on(ProcessEvent, self.on_ProcessEvent)
        self.bus.on(ProcessKillEvent, self.on_ProcessKillEvent)

    # ── Event handlers ──────────────────────────────────────────────────────

    def on_InterruptedHookPrompt(self, hook_name: str) -> Literal["abort", "retry", "skip"]:
        """Ask the user what to do after interrupting one foreground hook."""
        click.echo("", err=True)
        click.echo(f"Interrupted {hook_name}. Choose what to do next:", err=True)
        click.echo("  1. exit now and abort the whole crawl", err=True)
        click.echo("  2. continue and retry the aborted hook", err=True)
        click.echo("  3. continue and skip the aborted hook", err=True)

        def on_prompt_sigint(_signum, _frame) -> None:
            raise EOFError

        previous_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, on_prompt_sigint)
        try:
            click.echo("Choice [1]: ", nl=False, err=True)
            while True:
                choice_char = click.getchar()
                click.echo("", err=True)
                if choice_char in ("\x04", "\r", "\n"):
                    return "abort"
                if choice_char == "1":
                    return "abort"
                if choice_char == "2":
                    return "retry"
                if choice_char == "3":
                    return "skip"
                click.echo("Enter 1, 2, or 3.", err=True)
                click.echo("Choice [1]: ", nl=False, err=True)
        except (EOFError, KeyboardInterrupt, click.Abort):
            return "abort"
        finally:
            signal.signal(signal.SIGINT, previous_sigint_handler)

    async def on_ProcessEvent(self, event: ProcessEvent) -> None:
        """Spawn one hook subprocess and emit ProcessStartedEvent.

        Foreground hooks stay inside this one handler for spawn, stdout, user
        interrupts, completion, retry, and abort. ProcessStartedEvent is only a
        notification record for downstream consumers like the TUI and cleanup.
        """
        interactive_interrupts = not event.is_background and self.interactive_tty
        if interactive_interrupts:
            self.interrupt_requested.clear()
            self.force_interrupt.clear()
        plugin_output_dir = Path(event.output_dir)
        plugin_output_dir.mkdir(parents=True, exist_ok=True)

        proc = Process(
            cmd=[event.hook_path, *event.hook_args],
            pwd=str(plugin_output_dir),
            timeout=event.timeout,
            started_at=now_iso(),
            plugin=event.plugin_name,
            hook_name=event.hook_name,
        )

        artifact_stem = event.hook_name
        # Provider hooks can install the same named binary more than once in a run.
        # Include the process id so retries/install attempts keep separate logs.
        if event.hook_name.startswith("on_BinaryRequest__"):
            artifact_stem = f"{event.hook_name}.{proc.id}"

        cmd = [event.hook_path, *event.hook_args]
        stdout_file = plugin_output_dir / f"{artifact_stem}.stdout.log"
        stderr_file = plugin_output_dir / f"{artifact_stem}.stderr.log"
        pid_file = plugin_output_dir / f"{artifact_stem}.pid"
        cmd_file = plugin_output_dir / f"{artifact_stem}.sh"

        _rotate_existing_log(stdout_file)
        _rotate_existing_log(stderr_file)

        write_cmd_file(cmd_file, cmd)
        # Track the directory contents before the hook runs so completion can
        # report only newly created output files.
        files_before = set(plugin_output_dir.rglob("*")) if plugin_output_dir.exists() else set()

        process: asyncio.subprocess.Process | None = None
        try:
            try:
                with open(stderr_file, "w") as err_fh:
                    process = await asyncio.create_subprocess_exec(
                        *cmd,
                        cwd=str(plugin_output_dir),
                        stdout=asyncio.subprocess.PIPE,
                        stderr=err_fh,
                        env=event.env,
                        # Give every hook its own process group so interrupts and
                        # cleanup can target the hook explicitly instead of relying
                        # on terminal-delivered SIGINT reaching the right child.
                        start_new_session=True,
                    )
                write_pid_file_with_mtime(pid_file, process.pid, time.time())
            except Exception as e:
                # If spawn partially succeeded, shut it down before surfacing the
                # failure as a normal ProcessCompletedEvent.
                if process is not None:
                    await graceful_kill_process(process)
                pid_file.unlink(missing_ok=True)
                proc.exit_code = -1
                proc.status = "failed"
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
                        timeout=event.timeout,
                        stdout="",
                        stderr=proc.stderr,
                        exit_code=-1,
                        status=proc.status,
                        output_dir=event.output_dir,
                        output_files=[],
                        is_background=event.is_background,
                        pid=process.pid if process is not None else 0,
                        url=event.url,
                        process_type=event.process_type,
                        worker_type=event.worker_type,
                        start_ts=proc.started_at or "",
                        end_ts=proc.ended_at or "",
                    ),
                )
                return
            started_event = await self.bus.emit(
                ProcessStartedEvent(
                    plugin_name=event.plugin_name,
                    hook_name=event.hook_name,
                    hook_path=event.hook_path,
                    hook_args=event.hook_args,
                    output_dir=event.output_dir,
                    env=event.env,
                    timeout=event.timeout,
                    pid=process.pid,
                    is_background=event.is_background,
                    url=event.url,
                    process_type=event.process_type,
                    worker_type=event.worker_type,
                    start_ts=proc.started_at or "",
                    # These runtime-only fields carry the rest of the subprocess
                    # lifetime through bus history.
                    subprocess=process,
                    stdout_file=stdout_file,
                    stderr_file=stderr_file,
                    pid_file=pid_file,
                    cmd_file=cmd_file,
                    files_before=files_before,
                    event_timeout=event.timeout + 30.0,
                    event_handler_timeout=event.timeout + 30.0,
                    event_handler_slow_timeout=10000.0,
                ),
            )
            proc = Process(
                cmd=[event.hook_path, *event.hook_args],
                pwd=event.output_dir,
                timeout=event.timeout,
                started_at=started_event.start_ts,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
            )
            stream_task = asyncio.create_task(
                self._stream_stdout(
                    event=started_event,
                    proc=proc,
                    process=process,
                    stdout_file=stdout_file,
                ),
            )
            wait_task = asyncio.create_task(process.wait())
            interrupted = False
            timed_out = False
            try:
                deadline = asyncio.get_running_loop().time() + event.timeout if event.timeout else None
                while True:
                    interrupt_task: asyncio.Task[bool] | None = None
                    pending = {wait_task}
                    if interactive_interrupts:
                        interrupt_task = asyncio.create_task(self.interrupt_requested.wait())
                        pending.add(interrupt_task)
                    remaining = None if deadline is None else max(deadline - asyncio.get_running_loop().time(), 0.0)
                    done, pending = await asyncio.wait(
                        pending,
                        timeout=remaining,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if interrupt_task is not None and interrupt_task not in done:
                        interrupt_task.cancel()
                    if not done:
                        timed_out = True
                        await graceful_kill_process(process)
                        await wait_task
                        break
                    if wait_task in done:
                        break
                    if interrupt_task is not None and interrupt_task in done:
                        self.interrupt_requested.clear()
                        interrupted = True
                        self.set_live_paused(True)
                        await self.bus.emit(
                            ProcessKillEvent(
                                plugin_name=event.plugin_name,
                                hook_name=event.hook_name,
                                pid=process.pid,
                                grace_period=float(event.timeout),
                                event_parent_id=started_event.event_id,
                            ),
                        )
                        await wait_task
                        break
                await stream_task
            except TimeoutError:
                timed_out = True
                await graceful_kill_process(process)
            except Exception:
                await graceful_kill_process(process)
                raise

            returncode = process.returncode if process.returncode is not None else 0
            stdout = stdout_file.read_text() if stdout_file.exists() else ""
            stderr = stderr_file.read_text() if stderr_file.exists() else ""

            if timed_out:
                returncode = -1
                stderr = f"Hook timed out after {event.timeout} seconds"

            action = "skip"
            status = _process_status(returncode)
            if interrupted:
                returncode = PROCESS_EXIT_SKIPPED
                status = "skipped"
                stderr = "Hook interrupted by user"
                if self.force_interrupt.is_set():
                    action = "abort"
                else:
                    action = self.on_InterruptedHookPrompt(event.hook_name)
                self.set_live_paused(False)
                if action == "abort":
                    self.force_interrupt.set()

            proc.exit_code = returncode
            proc.status = status
            proc.stdout = stdout
            proc.stderr = stderr
            proc.ended_at = now_iso()

            files_after = set(plugin_output_dir.rglob("*")) if plugin_output_dir.exists() else set()
            new_files = scan_output_files(
                plugin_output_dir,
                file_paths=files_after - files_before,
            )

            pid_file.unlink(missing_ok=True)

            if returncode == 0:
                stdout_file.unlink(missing_ok=True)
                stderr_file.unlink(missing_ok=True)

            index_path = plugin_output_dir.parent / "index.jsonl"
            write_jsonl(index_path, proc, also_print=self.emit_jsonl)

            await self.bus.emit(
                ProcessCompletedEvent(
                    plugin_name=event.plugin_name,
                    hook_name=event.hook_name,
                    hook_path=event.hook_path,
                    hook_args=event.hook_args,
                    env=event.env,
                    timeout=event.timeout,
                    stdout=stdout,
                    stderr=stderr,
                    exit_code=returncode,
                    status=status,
                    output_dir=event.output_dir,
                    output_files=new_files,
                    is_background=event.is_background,
                    pid=process.pid,
                    url=event.url,
                    process_type=event.process_type,
                    worker_type=event.worker_type,
                    start_ts=proc.started_at or "",
                    end_ts=proc.ended_at or "",
                    event_parent_id=started_event.event_id,
                ),
            )
            if action == "retry":
                await self.bus.emit(
                    ProcessEvent(
                        plugin_name=event.plugin_name,
                        hook_name=event.hook_name,
                        hook_path=event.hook_path,
                        hook_args=event.hook_args,
                        is_background=event.is_background,
                        output_dir=event.output_dir,
                        env=event.env,
                        timeout=event.timeout,
                        url=event.url,
                        process_type=event.process_type,
                        worker_type=event.worker_type,
                        event_timeout=event.event_timeout,
                        event_handler_timeout=event.event_handler_timeout,
                        event_handler_slow_timeout=event.event_handler_slow_timeout,
                    ),
                )
        finally:
            self.interrupt_requested.clear()

    async def on_ProcessKillEvent(self, event: ProcessKillEvent) -> None:
        """Gracefully shut down a running background hook.

        Cleanup emits ProcessKillEvent as a direct child of a cleanup event.
        Interactive interrupts emit it as a direct child of the current
        ProcessStartedEvent. If the process is already gone, pid-file
        validation makes this a safe no-op.
        """
        parent_event = self.bus.event_history.get(event.event_parent_id or "")
        if isinstance(parent_event, ProcessStartedEvent):
            started_process = parent_event
        else:
            if not isinstance(parent_event, (SnapshotCleanupEvent, CrawlCleanupEvent)):
                raise RuntimeError(f"Missing cleanup parent for ProcessKillEvent {event.event_id}")
            root_event: SnapshotEvent | CrawlEvent | None
            if isinstance(parent_event, SnapshotCleanupEvent):
                root_event = await self.bus.find(
                    SnapshotEvent,
                    past=True,
                    future=False,
                    where=lambda candidate: self.bus.event_is_child_of(parent_event, candidate),
                )
            else:
                root_event = await self.bus.find(
                    CrawlEvent,
                    past=True,
                    future=False,
                    where=lambda candidate: self.bus.event_is_child_of(parent_event, candidate),
                )
            if root_event is None:
                raise RuntimeError(f"Missing root event for ProcessKillEvent {event.event_id}")

            matches: list[ProcessStartedEvent] = []
            seen_started_event_ids: set[str] = set()
            while True:
                started_process = await self.bus.find(
                    ProcessStartedEvent,
                    past=True,
                    future=False,
                    where=lambda candidate: (
                        self.bus.event_is_child_of(candidate, root_event)
                        and candidate.is_background
                        and candidate.plugin_name == event.plugin_name
                        and candidate.hook_name == event.hook_name
                        and candidate.pid == event.pid
                        and candidate.event_id not in seen_started_event_ids
                    ),
                )
                if started_process is None:
                    break
                seen_started_event_ids.add(started_process.event_id)
                matches.append(started_process)
            if len(matches) != 1:
                raise RuntimeError(
                    f"Expected exactly one ProcessStartedEvent for {event.plugin_name}:{event.hook_name}, found {len(matches)}",
                )
            started_process = matches[0]
        await graceful_kill_by_pid_file(
            started_process.pid_file,
            started_process.cmd_file,
            grace_period=event.grace_period,
        )

    async def _stream_stdout(
        self,
        *,
        event: ProcessStartedEvent,
        proc: Process,
        process: asyncio.subprocess.Process,
        stdout_file: Path,
    ) -> list[str]:
        """Stream hook stdout line-by-line and emit ProcessStdoutEvent for each line.

        Stdout is routed immediately through the bus so downstream services can
        react while the subprocess is still running.
        """
        stdout_lines: list[str] = []
        assert process.stdout is not None
        with open(stdout_file, "w") as out_fh:
            try:
                async for raw_line in process.stdout:
                    line = raw_line.decode(errors="replace")
                    out_fh.write(line)
                    out_fh.flush()
                    stdout_lines.append(line)

                    # Emit each line immediately so routing services can react
                    # while the subprocess is still running.
                    await self.bus.emit(
                        ProcessStdoutEvent(
                            line=line.strip(),
                            plugin_name=event.plugin_name,
                            hook_name=event.hook_name,
                            output_dir=event.output_dir,
                            start_ts=proc.started_at or "",
                            end_ts=now_iso(),
                            event_parent_id=event.event_id,
                        ),
                    )
            except asyncio.CancelledError:
                return stdout_lines
        return stdout_lines
