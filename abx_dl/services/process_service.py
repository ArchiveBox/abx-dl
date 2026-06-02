"""ProcessService — owns hook subprocess execution and raw process events."""

import asyncio
import os
import signal
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from typing import ClassVar, Literal

from abxbus import BaseEvent, EventBus
import click
from ..events import (
    PROCESS_EXIT_SKIPPED,
    CrawlAbortEvent,
    CrawlCleanupEvent,
    CrawlEvent,
    CrawlPauseEvent,
    CrawlResumeAndRetryEvent,
    CrawlResumeAndSkipEvent,
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
STDOUT_POLL_INTERVAL = 0.5


def _perf_trace(label):
    def decorator(func):
        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                if os.environ.get("ARCHIVEBOX_PERF_TRACE") != "1":
                    return await func(*args, **kwargs)
                started_at = time.perf_counter()
                try:
                    return await func(*args, **kwargs)
                finally:
                    elapsed_ms = (time.perf_counter() - started_at) * 1000
                    print(f"PERF_TRACE label={label} ms={elapsed_ms:.3f}", file=sys.stderr, flush=True)

            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            if os.environ.get("ARCHIVEBOX_PERF_TRACE") != "1":
                return func(*args, **kwargs)
            started_at = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed_ms = (time.perf_counter() - started_at) * 1000
                print(f"PERF_TRACE label={label} ms={elapsed_ms:.3f}", file=sys.stderr, flush=True)

        return sync_wrapper

    return decorator


@contextmanager
def _perf_span(label: str):
    if os.environ.get("ARCHIVEBOX_PERF_TRACE") != "1":
        yield
        return
    started_at = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        print(f"PERF_TRACE label={label} ms={elapsed_ms:.3f}", file=sys.stderr, flush=True)


@dataclass
class _StdoutStreamState:
    stdout_lines: list[str] = field(default_factory=list)
    pending_line: str = ""
    offset: int = 0


def _process_status(exit_code: int) -> ProcessStatus:
    """Normalize process exit codes into ArchiveResult-compatible statuses."""
    if exit_code == 0:
        return "succeeded"
    if exit_code == PROCESS_EXIT_SKIPPED:
        return "skipped"
    return "failed"


def _process_command(event: ProcessEvent) -> list[str]:
    if event.env.get("ABX_RUNTIME", "").lower() == "archivebox" and event.hook_path.endswith(".py"):
        return [sys.executable, event.hook_path, *event.hook_args]
    return [event.hook_path, *event.hook_args]


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


@contextmanager
def _default_sigint_during_prompt():
    """Restore Python's default SIGINT-raises-KeyboardInterrupt behavior while a
    synchronous user-input prompt is open.

    The CLI installs an asyncio-level SIGINT handler
    (``loop.add_signal_handler``) that emits a ``CrawlPauseEvent`` /
    ``CrawlAbortEvent`` on Ctrl+C. That handler can only fire when the
    asyncio loop is running, but the interrupt prompt below blocks the loop
    on a synchronous ``click.getchar`` call — so Ctrl+C would be silently
    absorbed by asyncio's signal-handler queue and the user would see their
    ``^C`` keystrokes accumulate with no effect. Installing Python's default
    SIGINT handler for the prompt's duration restores the normal "Ctrl+C
    raises KeyboardInterrupt" path that the prompt's own ``except`` clause
    catches and turns into an ``"abort"`` choice. The CLI's asyncio handler
    is reinstated on exit, so the second-press-aborts behavior is preserved
    for any subsequent prompts.
    """
    try:
        previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    except (ValueError, OSError):
        # Not on the main thread, or signals unavailable — leave handler alone.
        yield
        return
    try:
        yield
    finally:
        try:
            signal.signal(signal.SIGINT, previous)
        except (ValueError, OSError):
            pass


class ProcessService(BaseService):
    """Runs hook subprocesses and emits only process-level lifecycle events.

    ProcessService does not interpret hook stdout JSONL. Its job is only to:

    1. spawn the subprocess for a ``ProcessEvent``
    2. emit ``ProcessStartedEvent`` with the live subprocess handle and file paths
    3. stream stdout into ``ProcessStdoutEvent`` lines
    4. wait for exit or kill
    5. emit ``ProcessCompletedEvent``

    Background behavior is owned by abxbus and the dispatch site. The
    ``ProcessEvent`` itself stays alive until the subprocess exits; parent events
    can still proceed because background ProcessEvents are parallel and do not
    block parent completion.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        CrawlPauseEvent,
        CrawlAbortEvent,
        ProcessEvent,
        ProcessKillEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        CrawlAbortEvent,
        CrawlResumeAndRetryEvent,
        CrawlResumeAndSkipEvent,
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
    ):
        self.emit_jsonl = emit_jsonl
        self.interactive_tty = interactive_tty
        self.pause_requested = asyncio.Event()
        self.abort_requested = False
        self._active_process_event_tasks: dict[str, asyncio.Task[Process | None]] = {}
        self._background_completion_tasks: set[asyncio.Task[Process | None]] = set()
        self._completed_process_event_ids: set[str] = set()
        super().__init__(bus)
        self.bus.on(CrawlPauseEvent, self.on_CrawlPauseEvent)
        self.bus.on(CrawlAbortEvent, self.on_CrawlAbortEvent)
        self.bus.on(ProcessEvent, self.on_ProcessEvent)
        self.bus.on(ProcessKillEvent, self.on_ProcessKillEvent)

    # ── Event handlers ──────────────────────────────────────────────────────

    def on_InterruptedHookPrompt(self, hook_name: str) -> Literal["abort", "retry", "skip"]:
        """Ask the user what to do after interrupting one foreground hook.

        Runs synchronously inside the orchestrator's asyncio loop, so
        ``loop.add_signal_handler(SIGINT, ...)`` installed by the CLI can't
        fire while ``click.getchar`` blocks — the loop's pending callbacks
        only run between awaits, and we're not yielding. Temporarily swap in
        Python's default SIGINT handler so Ctrl+C actually raises
        ``KeyboardInterrupt`` while the prompt is open; the ``except`` below
        catches it and turns it into ``"abort"``.
        """
        click.echo("", err=True)
        click.echo(f"Interrupted {hook_name}. Choose what to do next:", err=True)
        click.echo("  Enter: continue and skip the aborted hook", err=True)
        click.echo("  r: continue and retry the aborted hook", err=True)
        click.echo("  a or Ctrl+C: exit now and abort the whole crawl", err=True)
        try:
            with _default_sigint_during_prompt():
                click.echo("Choice [skip]: ", nl=False, err=True)
                while True:
                    choice_char = click.getchar()
                    click.echo("", err=True)
                    if choice_char in ("\x03", "\x04"):
                        return "abort"
                    if choice_char in ("\r", "\n", "3", "s", "S"):
                        return "skip"
                    if choice_char in ("1", "a", "A"):
                        return "abort"
                    if choice_char in ("2", "r", "R"):
                        return "retry"
                    click.echo("Press Enter to skip, r to retry, or a/Ctrl+C to abort.", err=True)
                    click.echo("Choice [skip]: ", nl=False, err=True)
        except (EOFError, KeyboardInterrupt, click.Abort):
            return "abort"

    async def on_CrawlPauseEvent(self, event: CrawlPauseEvent) -> None:
        """Wake the current foreground hook so it can interrupt itself cleanly."""
        self.pause_requested.set()

    async def on_CrawlAbortEvent(self, event: CrawlAbortEvent) -> None:
        """Interrupt any current foreground hook and abort the crawl."""
        self.abort_requested = True
        self.pause_requested.set()

    async def on_ProcessEvent(self, event: ProcessEvent) -> Process | None:
        """Run each ProcessEvent exactly once even if the bus observes it twice."""
        if event.event_id in self._completed_process_event_ids:
            return None
        active_task = self._active_process_event_tasks.get(event.event_id)
        if active_task is not None:
            return await asyncio.shield(active_task)

        task = asyncio.create_task(self._run_process_event(event))
        self._active_process_event_tasks[event.event_id] = task
        try:
            result = await task
            self._completed_process_event_ids.add(event.event_id)
            return result
        finally:
            if self._active_process_event_tasks.get(event.event_id) is task:
                self._active_process_event_tasks.pop(event.event_id, None)

    @_perf_trace("abx_dl.ProcessService._run_process_event")
    async def _run_process_event(self, event: ProcessEvent) -> Process | None:
        """Spawn one hook subprocess and emit ProcessStartedEvent.

        Foreground hooks stay inside this one handler for spawn, stdout, user
        interrupts, completion, retry, and abort. ProcessStartedEvent is only a
        notification record for downstream consumers like the TUI and cleanup.
        """
        foreground_interrupts = not event.is_background
        if foreground_interrupts:
            self.pause_requested.clear()
            self.abort_requested = False
        with _perf_span("abx_dl.ProcessService._run_process_event.prepare_output_dir"):
            plugin_output_dir = Path(event.output_dir)
            plugin_output_dir.mkdir(parents=True, exist_ok=True)

        with _perf_span("abx_dl.ProcessService._run_process_event.prepare_process_model"):
            cmd = _process_command(event)
            proc = Process(
                cmd=cmd,
                pwd=str(plugin_output_dir),
                timeout=event.timeout,
                started_at=now_iso(),
                plugin=event.plugin_name,
                hook_name=event.hook_name,
            )

        # A hook can be retried, requeued, or briefly overlap with another run
        # for the same plugin/output dir. Keep every subprocess' artifacts
        # distinct so one completion path cannot rotate/delete another's logs.
        artifact_stem = f"{event.hook_name}.{proc.id}"

        stdout_file = plugin_output_dir / f"{artifact_stem}.stdout.log"
        stderr_file = plugin_output_dir / f"{artifact_stem}.stderr.log"
        pid_file = plugin_output_dir / f"{artifact_stem}.pid"
        cmd_file = plugin_output_dir / f"{artifact_stem}.sh"

        with _perf_span("abx_dl.ProcessService._run_process_event.rotate_logs"):
            _rotate_existing_log(stdout_file)
            _rotate_existing_log(stderr_file)

        with _perf_span("abx_dl.ProcessService._run_process_event.write_cmd_file"):
            write_cmd_file(cmd_file, cmd)
        # Track the directory contents before the hook runs so completion can
        # report only newly created output files.
        with _perf_span("abx_dl.ProcessService._run_process_event.scan_files_before"):
            files_before = set(plugin_output_dir.rglob("*")) if plugin_output_dir.exists() else set()

        process: asyncio.subprocess.Process | None = None
        started_event: ProcessStartedEvent | None = None
        completion_owns_process = False
        try:
            try:
                with _perf_span("abx_dl.ProcessService._run_process_event.spawn_subprocess"):
                    with open(stdout_file, "wb") as out_fh, open(stderr_file, "w") as err_fh:
                        process = await asyncio.create_subprocess_exec(
                            *cmd,
                            cwd=str(plugin_output_dir),
                            stdout=out_fh,
                            stderr=err_fh,
                            env=event.env,
                            # Give every hook its own process group so interrupts and
                            # cleanup can target the hook explicitly instead of relying
                            # on terminal-delivered SIGINT reaching the right child.
                            start_new_session=True,
                        )
                with _perf_span("abx_dl.ProcessService._run_process_event.write_pid_file"):
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
                await event.emit(
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
                        event_timeout=event.event_timeout,
                        event_handler_timeout=event.event_handler_timeout,
                        event_handler_slow_timeout=event.event_handler_slow_timeout,
                    ),
                ).now()
                return proc
            with _perf_span("abx_dl.ProcessService._run_process_event.emit_started_event"):
                started_event = await event.emit(
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
                ).now()
            assert started_event is not None
            proc = Process(
                cmd=cmd,
                pwd=event.output_dir,
                timeout=event.timeout,
                started_at=started_event.start_ts,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
            )
            completion = self._complete_process_event(
                event=event,
                started_event=started_event,
                proc=proc,
                process=process,
                plugin_output_dir=plugin_output_dir,
                stdout_file=stdout_file,
                stderr_file=stderr_file,
                pid_file=pid_file,
                files_before=files_before,
                foreground_interrupts=foreground_interrupts,
            )
            if event.is_background:
                completion_task = asyncio.create_task(completion)
                self._background_completion_tasks.add(completion_task)

                def forget_background_completion(task: asyncio.Task[Process | None]) -> None:
                    self._background_completion_tasks.discard(task)
                    try:
                        task.result()
                    except Exception:
                        pass

                completion_task.add_done_callback(forget_background_completion)
                completion_owns_process = True
                return proc
            completion_owns_process = True
            with _perf_span("abx_dl.ProcessService._run_process_event.complete_foreground"):
                return await completion
        except asyncio.CancelledError:
            if process is not None and not completion_owns_process:
                await graceful_kill_process(process)
            raise
        finally:
            self.pause_requested.clear()

    @_perf_trace("abx_dl.ProcessService._complete_process_event")
    async def _complete_process_event(
        self,
        *,
        event: ProcessEvent,
        started_event: ProcessStartedEvent,
        proc: Process,
        process: asyncio.subprocess.Process,
        plugin_output_dir: Path,
        stdout_file: Path,
        stderr_file: Path,
        pid_file: Path,
        files_before: set[Path],
        foreground_interrupts: bool,
    ) -> Process | None:
        with _perf_span("abx_dl.ProcessService._complete_process_event.start_stream_tasks"):
            stdout_state = _StdoutStreamState()
            stream_task = asyncio.create_task(
                self._stream_stdout(
                    event=started_event,
                    proc=proc,
                    stdout_file=stdout_file,
                    state=stdout_state,
                ),
            )
            wait_task = asyncio.create_task(process.wait())
        interrupted = False
        timed_out = False
        try:
            with _perf_span("abx_dl.ProcessService._complete_process_event.wait_subprocess"):
                deadline = asyncio.get_running_loop().time() + event.timeout if event.timeout and not event.is_background else None
                while True:
                    pending = {wait_task}
                    interrupt_task: asyncio.Task[bool] | None = None
                    if foreground_interrupts:
                        interrupt_task = asyncio.create_task(self.pause_requested.wait())
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
                        self.pause_requested.clear()
                        interrupted = True
                        await started_event.emit(
                            ProcessKillEvent(
                                plugin_name=event.plugin_name,
                                hook_name=event.hook_name,
                                pid=process.pid,
                                grace_period=float(event.timeout),
                            ),
                        ).now()
                        await wait_task
                        break
            with _perf_span("abx_dl.ProcessService._complete_process_event.finish_stdout"):
                stream_task.cancel()
                await self._finish_stream_stdout(stream_task)
                await self._emit_new_stdout_lines(
                    event=started_event,
                    proc=proc,
                    stdout_file=stdout_file,
                    state=stdout_state,
                    emit_partial=True,
                )
        except TimeoutError:
            timed_out = True
            await graceful_kill_process(process)
        except asyncio.CancelledError:
            stream_task.cancel()
            await self._finish_stream_stdout(stream_task)
            await graceful_kill_process(process)
            raise
        except Exception:
            await graceful_kill_process(process)
            raise

        with _perf_span("abx_dl.ProcessService._complete_process_event.read_logs"):
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
            if self.abort_requested:
                action = "abort"
            elif not self.interactive_tty:
                action = "abort"
            else:
                action = self.on_InterruptedHookPrompt(event.hook_name)
            await event.emit(
                {
                    "abort": CrawlAbortEvent,
                    "retry": CrawlResumeAndRetryEvent,
                    "skip": CrawlResumeAndSkipEvent,
                }[action](),
            ).now()

        proc.exit_code = returncode
        proc.status = status
        proc.stdout = stdout
        proc.stderr = stderr
        proc.ended_at = now_iso()

        with _perf_span("abx_dl.ProcessService._complete_process_event.scan_files_after"):
            files_after = set(plugin_output_dir.rglob("*")) if plugin_output_dir.exists() else set()
        with _perf_span("abx_dl.ProcessService._complete_process_event.scan_output_files"):
            new_files = scan_output_files(
                plugin_output_dir,
                file_paths=files_after - files_before,
            )

        with _perf_span("abx_dl.ProcessService._complete_process_event.cleanup_pid_file"):
            pid_file.unlink(missing_ok=True)

        if returncode == 0:
            with _perf_span("abx_dl.ProcessService._complete_process_event.cleanup_logs"):
                stdout_file.unlink(missing_ok=True)
                stderr_file.unlink(missing_ok=True)

        index_path = plugin_output_dir.parent / "index.jsonl"
        with _perf_span("abx_dl.ProcessService._complete_process_event.write_jsonl"):
            write_jsonl(index_path, proc, also_print=self.emit_jsonl)

        with _perf_span("abx_dl.ProcessService._complete_process_event.emit_completed_event"):
            await started_event.emit(
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
                    event_timeout=event.event_timeout,
                    event_handler_timeout=event.event_handler_timeout,
                    event_handler_slow_timeout=event.event_handler_slow_timeout,
                ),
            ).now()
        if action == "retry":
            await event.emit(
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
            ).now()
        return proc

    async def _finish_stream_stdout(self, stream_task: asyncio.Task[list[str]]) -> list[str]:
        """Finish reading hook stdout after the hook process exits."""
        if stream_task.done():
            return await stream_task
        try:
            return await stream_task
        except asyncio.CancelledError:
            return []

    async def on_ProcessKillEvent(self, event: ProcessKillEvent) -> None:
        """Gracefully shut down a running hook.

        Cleanup emits ProcessKillEvent as a direct child of a cleanup event.
        Interactive interrupts emit it as a direct child of the current
        ProcessStartedEvent. If the process is already gone, pid-file
        validation makes this a safe no-op.
        """
        parent_event = await self.bus.find(
            ProcessStartedEvent,
            past=True,
            future=False,
            where=lambda candidate: self.bus.event_is_parent_of(candidate, event),
        )
        if isinstance(parent_event, ProcessStartedEvent):
            started_process = parent_event
        else:
            parent_event = await self.bus.find(
                SnapshotCleanupEvent,
                past=True,
                future=False,
                where=lambda candidate: self.bus.event_is_parent_of(candidate, event),
            )
            if parent_event is None:
                parent_event = await self.bus.find(
                    CrawlCleanupEvent,
                    past=True,
                    future=False,
                    where=lambda candidate: self.bus.event_is_parent_of(candidate, event),
                )
            if not isinstance(parent_event, (SnapshotCleanupEvent, CrawlCleanupEvent)):
                raise RuntimeError(f"Missing cleanup parent for ProcessKillEvent {event.event_id}")
            root_event: SnapshotEvent | CrawlEvent | None
            if isinstance(parent_event, SnapshotCleanupEvent):
                found_root_event = await self.bus.find(
                    SnapshotEvent,
                    past=True,
                    future=False,
                    where=lambda candidate: self.bus.event_is_child_of(parent_event, candidate),
                )
                root_event = found_root_event if isinstance(found_root_event, SnapshotEvent) else None
            else:
                found_root_event = await self.bus.find(
                    CrawlEvent,
                    past=True,
                    future=False,
                    where=lambda candidate: self.bus.event_is_child_of(parent_event, candidate),
                )
                root_event = found_root_event if isinstance(found_root_event, CrawlEvent) else None
            if root_event is None:
                raise RuntimeError(f"Missing root event for ProcessKillEvent {event.event_id}")

            matches = await self.bus.filter(
                ProcessStartedEvent,
                child_of=root_event,
                past=True,
                future=False,
                plugin_name=event.plugin_name,
                hook_name=event.hook_name,
                pid=event.pid,
            )
            if len(matches) != 1:
                raise RuntimeError(
                    f"Expected exactly one ProcessStartedEvent for {event.plugin_name}:{event.hook_name}, found {len(matches)}",
                )
            started_process = matches[0]
        if started_process.subprocess.returncode is None:
            await graceful_kill_process(
                started_process.subprocess,
                grace_period=event.grace_period,
            )
            return

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
        stdout_file: Path,
        state: _StdoutStreamState,
    ) -> list[str]:
        """Stream hook stdout from its log file and emit ProcessStdoutEvent lines.

        Hooks write stdout directly to a regular file instead of an asyncio pipe.
        Some browser/provider hooks spawn descendants that inherit stdout; if
        stdout is a pipe, asyncio keeps the process transport open until every
        descendant closes it. A file keeps live logs and decouples process
        completion from inherited descriptors.
        """
        try:
            while True:
                await self._emit_new_stdout_lines(
                    event=event,
                    proc=proc,
                    stdout_file=stdout_file,
                    state=state,
                    emit_partial=False,
                )
                await asyncio.sleep(STDOUT_POLL_INTERVAL)
        except asyncio.CancelledError:
            return state.stdout_lines
        return state.stdout_lines

    async def _emit_new_stdout_lines(
        self,
        *,
        event: ProcessStartedEvent,
        proc: Process,
        stdout_file: Path,
        state: _StdoutStreamState,
        emit_partial: bool,
    ) -> None:
        if not stdout_file.exists():
            return

        with open(stdout_file, errors="replace") as out_fh:
            out_fh.seek(state.offset)
            chunk = out_fh.read()
            state.offset = out_fh.tell()

        if not chunk and not (emit_partial and state.pending_line):
            return

        state.pending_line += chunk
        lines = state.pending_line.splitlines(keepends=True)
        state.pending_line = ""
        if lines and not lines[-1].endswith(("\n", "\r")):
            state.pending_line = lines.pop()
        if emit_partial and state.pending_line:
            lines.append(state.pending_line)
            state.pending_line = ""

        for line in lines:
            state.stdout_lines.append(line)
            stripped = line.strip()
            if event.env.get("ABX_RUNTIME", "").lower() == "archivebox" and '"type": "Snapshot"' in stripped:
                continue
            try:
                await event.emit(
                    ProcessStdoutEvent(
                        line=stripped,
                        plugin_name=event.plugin_name,
                        hook_name=event.hook_name,
                        output_dir=event.output_dir,
                        start_ts=proc.started_at or "",
                        end_ts=now_iso(),
                    ),
                ).now()
            except RuntimeError as err:
                if "event has no bus attached" in str(err):
                    return
                raise
