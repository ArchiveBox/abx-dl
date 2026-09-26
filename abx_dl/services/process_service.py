"""ProcessService — owns hook subprocess execution and raw process events."""

import asyncio
import contextvars
import os
import re
import signal
import sys
import select
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, ClassVar, Literal, TextIO
from collections.abc import Awaitable, Callable

import click
import psutil
from abxbus import BaseEvent, EventBus

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
    ProcessStderrEvent,
    SnapshotCleanupEvent,
    SnapshotEvent,
)
from ..models import Process, now_iso, write_jsonl
from ..output_files import scan_output_files
from ..process_utils import (
    GRACEFUL_SHUTDOWN_TIMEOUT,
    graceful_kill_by_pid_file,
    graceful_kill_process,
    validate_pid_file,
    write_cmd_file,
    write_pid_file_with_mtime,
    _send_signal,
)
from .base import BaseService, wait_for_crawl_resume, wait_for_process_ready

ProcessStatus = Literal["succeeded", "failed", "skipped"]


def interrupted_hook_prompt_text(hook_name: str) -> str:
    return (
        f"Interrupted {hook_name}. Choose what to do next:\n"
        "  Enter: continue and skip the aborted hook\n"
        "  r: continue and retry the aborted hook\n"
        "  a or Ctrl+C: exit now and abort the whole crawl\n"
        "Choice [skip]: "
    )


STDOUT_POLL_INTERVAL = 0.05
SHELL_SIGNAL_STDERR_RE = re.compile(r"(?:Terminated|Killed):\s*(\d+)")
POLITE_CLEANUP_SIGNAL_EXIT_CODES = {
    -signal.SIGINT,
    -signal.SIGTERM,
    128 + signal.SIGINT,
    128 + signal.SIGTERM,
}


def _hook_child_identity(
    *,
    real_uid: int | None = None,
    effective_uid: int | None = None,
    effective_gid: int | None = None,
) -> tuple[int, int] | None:
    """Return the permanent identity needed by a mixed-root hook child."""

    real_uid = os.getuid() if real_uid is None else real_uid
    effective_uid = os.geteuid() if effective_uid is None else effective_uid
    effective_gid = os.getegid() if effective_gid is None else effective_gid
    if real_uid == 0 and effective_uid != 0:
        return effective_uid, effective_gid
    return None


def _permanently_drop_child_privileges(uid: int, gid: int) -> Callable[[], None]:
    def drop_privileges() -> None:
        if os.getuid() == 0 and os.geteuid() != 0:
            os.seteuid(0)
        os.setgroups([gid])
        os.setgid(gid)
        os.setuid(uid)

    return drop_privileges


@dataclass
class _OutputStreamState:
    stdout_lines: list[str] = field(default_factory=list)
    pending_line: str = ""
    offset: int = 0
    stop_requested: bool = False


def _open_process_spawn_files(
    stdout_file: Path,
    stderr_file: Path,
    reader_stack: ExitStack,
) -> tuple[ExitStack, BinaryIO, TextIO, TextIO, TextIO]:
    spawn_stack = ExitStack()
    try:
        out_fh = spawn_stack.enter_context(stdout_file.open("wb"))
        err_fh = spawn_stack.enter_context(stderr_file.open("w"))
        stdout_reader = reader_stack.enter_context(stdout_file.open(errors="replace"))
        stderr_reader = reader_stack.enter_context(stderr_file.open(errors="replace"))
    except Exception:
        spawn_stack.close()
        raise
    return spawn_stack, out_fh, err_fh, stdout_reader, stderr_reader


def _process_status(exit_code: int) -> ProcessStatus:
    """Normalize process exit codes into ArchiveResult-compatible statuses."""
    if exit_code == 0:
        return "succeeded"
    if exit_code == PROCESS_EXIT_SKIPPED:
        return "skipped"
    return "failed"


def _process_command(event: ProcessEvent) -> list[str]:
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
    """Let the synchronous terminal owner's prompt turn SIGINT into abort.

    Add's supervisor transport normally queues SIGINT until its XML-RPC call
    finishes. While reading a choice there is no request to protect: a second
    SIGINT must cancel the input wait immediately instead of sitting in that
    queue. asyncio.run translates this cancellation to KeyboardInterrupt, which
    the adapter converts to an abort answer. Always restore the transport's
    handler afterward. Async terminal owners use the shared controller directly
    and never enter this temporary synchronous-adapter signal context.
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

    Background ProcessEvent handlers return after spawn; a retained completion
    task owns each subprocess and its logs until it exits. The outer controller
    handles terminal intent independently of both foreground and background
    completion. No hook or per-hook waiter decides when to read user input.
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
        ProcessStderrEvent,
        ProcessCompletedEvent,
        ProcessKillEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        emit_jsonl: bool,
        interactive_tty: bool,
        interrupted_hook_prompt: Callable[[str], Awaitable[Literal["abort", "retry", "skip"]]] | None = None,
    ):
        self.emit_jsonl = emit_jsonl
        self.interactive_tty = interactive_tty
        self.interrupted_hook_prompt = interrupted_hook_prompt
        self._abort_signal = asyncio.Event()
        self._interrupt_task: asyncio.Task[None] | None = None
        self._active_hooks: dict[str, tuple[ProcessEvent, ProcessStartedEvent]] = {}
        self._interrupt_choices: dict[str, asyncio.Future[str]] = {}
        self.abort_requested = False
        self._active_process_event_tasks: dict[str, asyncio.Task[Process | None]] = {}
        self._background_completion_tasks: set[asyncio.Task[Process | None]] = set()
        self._shutdown_hook_ids: set[str] = set()
        self._completed_process_event_ids: set[str] = set()
        super().__init__(bus)
        self.bus.on(CrawlPauseEvent, self.on_CrawlPauseEvent)
        self.bus.on(CrawlAbortEvent, self.on_CrawlAbortEvent)
        self.bus.on(ProcessEvent, self.on_ProcessEvent)
        self.bus.on(ProcessKillEvent, self.on_ProcessKillEvent)

    async def wait_for_background_completions(self) -> None:
        """Finish owned hook readers and completion events before bus teardown."""

        while self._background_completion_tasks:
            results = await asyncio.gather(*tuple(self._background_completion_tasks), return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result

    async def stop_background_hooks(self) -> None:
        """Stop remaining owned background hooks and record their completions."""

        running = [
            (event, started)
            for event, started in tuple(self._active_hooks.values())
            if event.is_background and started.subprocess.returncode is None
        ]
        self._shutdown_hook_ids.update(event.event_id for event, _ in running)
        await asyncio.gather(
            *(
                self.bus.emit(
                    ProcessKillEvent(
                        event_parent_id=started.event_id,
                        plugin_name=started.plugin_name,
                        hook_name=started.hook_name,
                        pid=started.pid,
                        grace_period=min(float(started.timeout), GRACEFUL_SHUTDOWN_TIMEOUT),
                    ),
                ).now()
                for _, started in running
            ),
        )
        await self.wait_for_background_completions()

    # ── Event handlers ──────────────────────────────────────────────────────

    @staticmethod
    async def read_interrupt_choice(
        hook_name: str,
        *,
        render: bool = True,
        is_active: Callable[[], bool] | None = None,
        on_abort: Callable[[], None] | None = None,
    ) -> Literal["abort", "retry", "skip"] | None:
        """Read one decision in the outer terminal without blocking its loop.

        Standalone abx-dl and direct ArchiveBox run own both terminal and event
        loop: background completion must keep rendering ABOVE the live prompt.
        Supervised add uses the same reader from its synchronous transport loop.
        Raw Ctrl+C is a byte here; an OS SIGINT still reaches the outer controller
        (or the synchronous adapter below). Hooks never inherit terminal input.
        """
        import termios
        import tty

        with ExitStack() as terminal:
            input_fd = sys.stdin.fileno() if sys.stdin.isatty() else terminal.enter_context(open("/dev/tty")).fileno()
            old_settings = termios.tcgetattr(input_fd)
            terminal.callback(termios.tcsetattr, input_fd, termios.TCSADRAIN, old_settings)
            # The UI may already have displayed the prompt (especially across
            # add's worker/parent boundary). TCSAFLUSH, used by click's helper,
            # discards an Enter or r typed in that interval and appears frozen.
            # Preserve queued input when entering raw mode; users should never
            # need to wait for an invisible terminal-mode transition.
            tty.setraw(input_fd, when=termios.TCSANOW)
            # Keep background rows' newlines anchored at column zero while
            # waiting for a key; raw mode normally clears output processing too.
            settings = termios.tcgetattr(input_fd)
            settings[1] |= termios.OPOST | termios.ONLCR
            termios.tcsetattr(input_fd, termios.TCSANOW, settings)
            if render:
                click.echo("\n" + interrupted_hook_prompt_text(hook_name), nl=False, err=True)
            while True:
                if is_active is not None and not is_active():
                    return None
                if not select.select([input_fd], [], [], 0)[0]:
                    await asyncio.sleep(0.05)
                    continue
                choice = os.read(input_fd, 1).decode("utf-8", errors="replace")
                if render:
                    click.echo("", err=True)
                if not choice or choice in ("\x03", "\x04", "1", "a", "A"):
                    # Record the decision at the keypress, before restoring
                    # terminal mode or returning to a caller that may await
                    # cleanup. The next OS SIGINT must already mean force exit.
                    if on_abort is not None:
                        on_abort()
                    return "abort"
                if choice in ("\r", "\n", "3", "s", "S"):
                    return "skip"
                if choice in ("2", "r", "R"):
                    return "retry"
                if render:
                    click.echo("Press Enter to skip, r to retry, or a/Ctrl+C to abort.", err=True)
                    click.echo("Choice [skip]: ", nl=False, err=True)

    @staticmethod
    def on_InterruptedHookPrompt(
        hook_name: str,
        *,
        render: bool = True,
        is_active: Callable[[], bool] | None = None,
        on_abort: Callable[[], None] | None = None,
    ) -> Literal["abort", "retry", "skip"] | None:
        """Synchronous adapter for add's supervisor/log transport, not hook code.

        The transport normally queues SIGINT at safe XML-RPC boundaries. While
        reading input it must instead turn a second SIGINT into an abort answer.
        Restoring the prior handler afterward preserves safe RPC transport and
        takeover semantics. Prompt liveness checks must not perform XML-RPC here.
        """
        try:
            with _default_sigint_during_prompt():
                return asyncio.run(ProcessService.read_interrupt_choice(hook_name, render=render, is_active=is_active, on_abort=on_abort))
        except (EOFError, KeyboardInterrupt, click.Abort):
            if on_abort is not None:
                on_abort()
            return "abort"

    async def on_CrawlPauseEvent(self, event: CrawlPauseEvent) -> None:
        """One outer-runner controller owns every Ctrl+C, including idle gaps.

        Signals are user intent, not subprocess I/O. Never require a foreground
        hook to notice a flag: the runner may be installing, awaiting background
        readiness, or between hooks. The pause fact gates scheduling immediately;
        stopping a selected hook and reading the answer happen independently.
        A second interrupt always aborts, even before the prompt is displayed.
        """
        if self.abort_requested:
            return
        if not self.interactive_tty or (self._interrupt_task is not None and not self._interrupt_task.done()):
            await self.bus.emit(CrawlAbortEvent()).now()
            return
        # Detached from the short signal-event handler, just like background
        # process completion. Human input has no hook/event execution deadline.
        self._interrupt_task = asyncio.create_task(self._handle_interrupt(), context=contextvars.Context())

    async def _handle_interrupt(self) -> None:
        choice_future = None
        try:
            active = [pair for pair in self._active_hooks.values() if pair[1].subprocess.returncode is None]
            # Foreground work is the scheduling barrier the user is most likely
            # waiting for. With only background work, stop the most recently
            # started hook. This selection never decides whether we can prompt.
            foreground = [pair for pair in active if not pair[0].is_background]
            selected = (foreground or active)[-1] if active else None
            if selected is not None:
                process_event, started = selected
                choice_future = asyncio.get_running_loop().create_future()
                self._interrupt_choices[process_event.event_id] = choice_future
                started.interruption_done = asyncio.Event()
                await self.bus.emit(
                    ProcessKillEvent(
                        event_parent_id=started.event_id,
                        plugin_name=started.plugin_name,
                        hook_name=started.hook_name,
                        pid=started.pid,
                        grace_period=min(float(started.timeout), GRACEFUL_SHUTDOWN_TIMEOUT),
                    ),
                ).now()
                await started.subprocess.wait()
            hook_name = selected[0].hook_name if selected else "crawl (between hooks)"
            if self.abort_requested:
                action = "abort"
            elif self.interrupted_hook_prompt is not None:
                action = await self.interrupted_hook_prompt(hook_name)
            else:
                action = await self.read_interrupt_choice(hook_name, is_active=lambda: not self.abort_requested) or "abort"
            assert action is not None
            if choice_future is not None and not choice_future.done():
                choice_future.set_result(action)
            if not self.abort_requested:
                await self.bus.emit(
                    {
                        "abort": CrawlAbortEvent,
                        "retry": CrawlResumeAndRetryEvent,
                        "skip": CrawlResumeAndSkipEvent,
                    }[action](),
                ).now()
        except asyncio.CancelledError:
            if choice_future is not None and not choice_future.done():
                choice_future.set_result("abort")
            raise
        except Exception:
            # A broken prompt must release paused schedulers into cleanup, never
            # silently strand a crawl behind a gate that nobody can reopen.
            await self.bus.emit(CrawlAbortEvent()).now()
            raise

    async def on_CrawlAbortEvent(self, event: CrawlAbortEvent) -> None:
        """Abort is sticky for this runner; starting another hook cannot reset it."""
        self.abort_requested = True
        self._abort_signal.set()
        for choice in self._interrupt_choices.values():
            if not choice.done():
                choice.set_result("abort")

    @property
    def interrupt_in_progress(self) -> bool:
        return self._interrupt_task is not None and not self._interrupt_task.done()

    def force_kill_owned_processes(self) -> None:
        """Immediately kill only this runner's children after a confirmed abort.

        This runs from the terminal owner's signal path, where awaiting the
        normal per-hook grace period would defeat a third Ctrl+C. Hook PID files
        carry start-time identity so a reused PID cannot receive SIGKILL; hook
        groups include browser descendants that a plain parent kill can leave
        behind. Remaining descendants cover installs still running before a
        ProcessStartedEvent exists. The caller exits only its own CLI/worker.
        """
        self.abort_requested = True
        try:
            descendants = psutil.Process(os.getpid()).children(recursive=True)
        except psutil.Error:
            descendants = []
        for _event, started in tuple(self._active_hooks.values()):
            if started.subprocess.returncode is None and validate_pid_file(started.pid_file, started.cmd_file):
                _send_signal(started.pid, signal.SIGKILL)
        for child in reversed(descendants):
            try:
                if child.is_running() and child.status() != psutil.STATUS_ZOMBIE:
                    child.kill()
            except psutil.Error:
                pass

    async def on_ProcessEvent(self, event: ProcessEvent) -> Process | None:
        """Run each ProcessEvent exactly once even if the bus observes it twice."""
        if event.event_id in self._completed_process_event_ids:
            return None
        active_task = self._active_process_event_tasks.get(event.event_id)
        if active_task is not None:
            # A duplicate event observer does not own this subprocess. Cancelling
            # that observer must not cancel the original owner's task. This shield
            # is ONLY deduplication protection: the real owner remains cancellable
            # on takeover, and explicit abort uses _abort_signal through the shield.
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

    async def _run_process_event(self, event: ProcessEvent) -> Process | None:
        """Spawn one hook subprocess and emit ProcessStartedEvent.

        Foreground execution holds the scheduling barrier through completion and
        retry; background execution hands lifetime ownership to a retained task.
        Both obey the same crawl-control gate. ProcessStartedEvent publishes the
        actual process identity for progress, readiness, and explicit cleanup.
        """
        if await wait_for_crawl_resume(self.bus) or self.abort_requested:
            return None
        plugin_output_dir = Path(event.output_dir)
        plugin_output_dir.mkdir(parents=True, exist_ok=True)

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

        _rotate_existing_log(stdout_file)
        _rotate_existing_log(stderr_file)

        write_cmd_file(cmd_file, cmd)
        # Track the directory contents before the hook runs so completion can
        # report only newly created output files.
        files_before = set(plugin_output_dir.rglob("*")) if plugin_output_dir.exists() else set()

        process: asyncio.subprocess.Process | None = None
        started_event: ProcessStartedEvent | None = None
        stdout_reader: TextIO | None = None
        stderr_reader: TextIO | None = None
        reader_stack: ExitStack | None = None
        completion_owns_process = False
        try:
            try:
                reader_stack = ExitStack()
                spawn_stack, out_fh, err_fh, stdout_reader, stderr_reader = _open_process_spawn_files(
                    stdout_file,
                    stderr_file,
                    reader_stack,
                )
                with spawn_stack:
                    # Open independent readers before awaiting spawn. Snapshot
                    # hooks may mutate their output tree while background hooks
                    # are still running, so there must never be a window where
                    # the only durable reference to these inodes is a pathname.
                    child_identity = _hook_child_identity()
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
                        preexec_fn=(_permanently_drop_child_privileges(*child_identity) if child_identity is not None else None),
                    )
                write_pid_file_with_mtime(pid_file, process.pid, time.time())
            except (OSError, ValueError) as e:
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
            started_event = ProcessStartedEvent(
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
            )
            # Publish control ownership before awaiting persistence/UI observers.
            # Otherwise Ctrl+C in that await sees no active hook despite its PID
            # file already existing and lets a real subprocess escape the pause.
            self._active_hooks[event.event_id] = (event, started_event)
            await event.emit(started_event).now()
            proc = Process(
                cmd=cmd,
                pwd=event.output_dir,
                timeout=event.timeout,
                started_at=started_event.start_ts,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
            )
            assert stdout_reader is not None
            assert stderr_reader is not None

            async def complete_and_close_readers() -> Process | None:
                try:
                    return await self._complete_process_event(
                        event=event,
                        started_event=started_event,
                        proc=proc,
                        process=process,
                        plugin_output_dir=plugin_output_dir,
                        stdout_file=stdout_file,
                        stderr_file=stderr_file,
                        stdout_reader=stdout_reader,
                        stderr_reader=stderr_reader,
                        pid_file=pid_file,
                        files_before=files_before,
                    )
                finally:
                    self._active_hooks.pop(event.event_id, None)
                    self._interrupt_choices.pop(event.event_id, None)
                    if started_event.interruption_done is not None:
                        started_event.interruption_done.set()
                    if reader_stack is not None:
                        reader_stack.close()

            completion = complete_and_close_readers()
            if event.is_background:
                # This task outlives the ProcessEvent handler. Inheriting that
                # handler's context makes later stdout/completion emissions
                # look like work from a finished handler, which abxbus rejects.
                # Stdout/completion events carry their parent ID explicitly.
                completion_task = asyncio.create_task(completion, context=contextvars.Context())
                self._background_completion_tasks.add(completion_task)

                def forget_background_completion(task: asyncio.Task[Process | None]) -> None:
                    self._background_completion_tasks.discard(task)
                    try:
                        task.result()
                    except (RuntimeError, OSError, ValueError, asyncio.CancelledError) as err:
                        click.echo(f"Background hook completion failed: {err}", err=True)

                completion_task.add_done_callback(forget_background_completion)
                completion_owns_process = True
                return proc
            completion_owns_process = True
            return await completion
        except asyncio.CancelledError:
            if process is not None and not completion_owns_process:
                await graceful_kill_process(process)
            raise
        finally:
            if not completion_owns_process and reader_stack is not None:
                reader_stack.close()

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
        stdout_reader: TextIO,
        stderr_reader: TextIO,
        pid_file: Path,
        files_before: set[Path],
    ) -> Process | None:
        stdout_state = _OutputStreamState()
        # Independent offsets preserve both complete logs. Stderr is observed
        # for progress only: it must never satisfy stdout readiness or JSONL
        # consumers, even if a diagnostic happens to contain valid JSON.
        stderr_state = _OutputStreamState()
        stream_task = asyncio.create_task(
            self._stream_output(
                event=started_event,
                proc=proc,
                stdout_reader=stdout_reader,
                state=stdout_state,
                stderr_reader=stderr_reader,
                stderr_state=stderr_state,
            ),
        )
        wait_task = asyncio.create_task(process.wait())
        interrupted = False
        timed_out = False
        cancellation: asyncio.CancelledError | None = None
        try:
            deadline = asyncio.get_running_loop().time() + event.timeout if event.timeout and not event.is_background else None
            while True:
                pending = {wait_task}
                # Cleanup owns background resource termination. Waking those
                # processes here would route them through the foreground user
                # interrupt path before cleanup can record the scoped stop.
                interrupt_task = None
                if not event.is_background:
                    interrupt_task = asyncio.create_task(self._abort_signal.wait())
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
                    interrupted = True
                    await self.bus.emit(
                        ProcessKillEvent(
                            event_parent_id=started_event.event_id,
                            plugin_name=event.plugin_name,
                            hook_name=event.hook_name,
                            pid=process.pid,
                            grace_period=min(float(event.timeout), GRACEFUL_SHUTDOWN_TIMEOUT),
                        ),
                    ).now()
                    await wait_task
                    break
            # Let an in-flight ProcessStdoutEvent finish before emitting
            # ProcessCompletedEvent. Cancelling here could advance the file
            # offset, cancel ArchiveResult consumers, and then lose the line
            # before the final drain below.
            stdout_state.stop_requested = True
            await self._finish_stream_output(stream_task)
            await self._emit_new_output_lines(
                event=started_event,
                proc=proc,
                stdout_reader=stdout_reader,
                state=stdout_state,
                emit_partial=True,
            )
        except TimeoutError:
            timed_out = True
            await graceful_kill_process(process)
        except asyncio.CancelledError as error:
            # Cancelling the runner still has to finalize this process/result.
            # Re-raising here used to bypass ProcessCompletedEvent, leaving an
            # earlier succeeded DB row intact after the hook was killed.
            cancellation = error
            await graceful_kill_process(process)
            stdout_state.stop_requested = True
            await self._finish_stream_output(stream_task)
        except Exception:
            await graceful_kill_process(process)
            raise

        await self._emit_new_output_lines(
            event=started_event,
            proc=proc,
            stdout_reader=stderr_reader,
            state=stderr_state,
            emit_partial=True,
            event_class=ProcessStderrEvent,
        )
        returncode = process.returncode if process.returncode is not None else 0
        stdout_reader.seek(0)
        stdout = stdout_reader.read()
        stderr_reader.seek(0)
        stderr = stderr_reader.read()
        stdout_reader.close()
        stderr_reader.close()

        files_after = set(plugin_output_dir.rglob("*")) if plugin_output_dir.exists() else set()
        new_files = scan_output_files(
            plugin_output_dir,
            file_paths=files_after - files_before,
            containment_root=plugin_output_dir.parent,
        )
        if returncode == 0 and not stdout.strip() and (signal_match := SHELL_SIGNAL_STDERR_RE.search(stderr)):
            returncode = 128 + int(signal_match.group(1))

        if timed_out:
            returncode = -1
            stderr = f"Hook timed out after {event.timeout} seconds"

        if cancellation is not None:
            returncode = 130
            stderr = "Hook execution cancelled"

        # A user stopping work has not discovered an extractor/site failure.
        # Carry that intent explicitly so consumers can discard the unfinished
        # attempt, without classifying organic crashes by their signal number.
        cancelled = interrupted or cancellation is not None or event.event_id in self._shutdown_hook_ids
        if event.is_background and self.abort_requested:
            cancelled = cancelled or await self._process_was_stopped_by_cleanup(event, process.pid)

        if (
            event.is_background
            and not timed_out
            and cancellation is None
            and returncode in POLITE_CLEANUP_SIGNAL_EXIT_CODES
            and await self._process_was_stopped_by_cleanup(event, process.pid)
        ):
            # Background hooks are long-lived resources owned by the snapshot or
            # crawl cleanup phase. When abx-dl asks one to stop with SIGTERM and
            # it exits from that signal, the crawl completed the intended
            # lifecycle; recording that Process as failed makes successful
            # archive results look broken in index.jsonl and Docker smoke tests.
            # SIGKILL escalation and organic nonzero exits still surface as
            # failures because they do not match this polite cleanup path.
            # This normalizes Process lifecycle only. A zero exit without an
            # explicit output record becomes noresult in ArchiveResultService;
            # it must never manufacture a successful capture from readiness.
            returncode = 0
            stderr = SHELL_SIGNAL_STDERR_RE.sub("", stderr).strip()

        action = "skip"
        choice_future = self._interrupt_choices.get(event.event_id)
        interrupted = interrupted or choice_future is not None
        cancelled = cancelled or interrupted
        status = _process_status(returncode)
        if interrupted:
            returncode = 130
            status = "failed"
            stderr = "Hook interrupted by user"
            # Completion records the stopped attempt, but never reads stdin or
            # chooses crawl control. Holding completion until the answer also
            # keeps a background-only capture from entering cleanup mid-prompt.
            action = await choice_future if choice_future is not None else "abort"

        proc.exit_code = returncode
        proc.status = status
        proc.stdout = stdout
        proc.stderr = stderr
        proc.ended_at = now_iso()

        index_path = plugin_output_dir.parent / "index.jsonl"
        write_jsonl(index_path, proc, also_print=self.emit_jsonl)

        pid_file.unlink(missing_ok=True)

        if returncode == 0 and not cancelled:
            stdout_file.unlink(missing_ok=True)
            stderr_file.unlink(missing_ok=True)

        await self.bus.emit(
            ProcessCompletedEvent(
                event_parent_id=started_event.event_id,
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
                cancelled=cancelled,
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
        if cancellation is not None:
            # Preserve cancellation control flow, after consumers
            # have corrected the durable result for this same hook.
            raise cancellation
        if action == "retry":
            retry_event = self.bus.emit(
                ProcessEvent(
                    event_parent_id=event.event_id,
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
            await retry_event.now()
            if event.is_background and not self.abort_requested:
                restarted = await self.bus.find(ProcessStartedEvent, event_parent_id=retry_event.event_id, past=True, future=False)
                if restarted is not None:
                    # Retrying startup must re-establish the same readiness
                    # barrier as the first attempt. Merely spawning its PID
                    # would let dependent hooks run before listeners/resources
                    # exist, recreating the original readiness race on retry.
                    async def retry_aborted() -> bool:
                        return self.abort_requested or await wait_for_crawl_resume(self.bus)

                    await wait_for_process_ready(restarted, float(event.timeout), retry_aborted)
        return proc

    async def _process_was_stopped_by_cleanup(self, event: ProcessEvent, pid: int) -> bool:
        kill_events = await self.bus.filter(
            ProcessKillEvent,
            past=True,
            future=False,
            plugin_name=event.plugin_name,
            hook_name=event.hook_name,
            pid=pid,
        )
        for kill_event in kill_events:
            snapshot_cleanup = await self.bus.find(
                SnapshotCleanupEvent,
                past=True,
                future=False,
                where=lambda candidate, kill_event=kill_event: self.bus.event_is_parent_of(candidate, kill_event),
            )
            if snapshot_cleanup is not None:
                return True
            crawl_cleanup = await self.bus.find(
                CrawlCleanupEvent,
                past=True,
                future=False,
                where=lambda candidate, kill_event=kill_event: self.bus.event_is_parent_of(candidate, kill_event),
            )
            if crawl_cleanup is not None:
                return True
        return False

    async def _finish_stream_output(self, stream_task: asyncio.Task[list[str]]) -> list[str]:
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
                raise TypeError(f"Missing cleanup parent for ProcessKillEvent {event.event_id}")
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
        # Capture timeouts can be hours. A user abort still lets recorders flush,
        # but must not wait an entire capture timeout for a hook ignoring SIGTERM.
        # The abort event is visible in bus history before its parallel handlers
        # have all updated their local flags. Cleanup must honor that fact when
        # choosing a grace period for hooks that ignore SIGTERM.
        aborting = self.abort_requested or await self.bus.find(CrawlAbortEvent, past=True, future=False) is not None
        grace_period = min(event.grace_period, GRACEFUL_SHUTDOWN_TIMEOUT) if aborting else event.grace_period
        if started_process.subprocess.returncode is None:
            await graceful_kill_process(
                started_process.subprocess,
                grace_period=grace_period,
            )
            return

        await graceful_kill_by_pid_file(
            started_process.pid_file,
            started_process.cmd_file,
            grace_period=grace_period,
        )

    async def _stream_output(
        self,
        *,
        event: ProcessStartedEvent,
        proc: Process,
        stdout_reader: TextIO,
        state: _OutputStreamState,
        stderr_reader: TextIO,
        stderr_state: _OutputStreamState,
    ) -> list[str]:
        """Stream stdout records and stderr diagnostics as separate event types.

        Hooks write stdout directly to a regular file instead of an asyncio pipe.
        Some browser/provider hooks spawn descendants that inherit stdout; if
        stdout is a pipe, asyncio keeps the process transport open until every
        descendant closes it. A file keeps live logs and decouples process
        completion from inherited descriptors.
        """
        try:
            while not state.stop_requested:
                await self._emit_new_output_lines(
                    event=event,
                    proc=proc,
                    stdout_reader=stdout_reader,
                    state=state,
                    emit_partial=False,
                )
                await self._emit_new_output_lines(
                    event=event,
                    proc=proc,
                    stdout_reader=stderr_reader,
                    state=stderr_state,
                    emit_partial=False,
                    event_class=ProcessStderrEvent,
                )
                if state.stop_requested:
                    break
                await asyncio.sleep(STDOUT_POLL_INTERVAL)
        except asyncio.CancelledError:
            return state.stdout_lines
        return state.stdout_lines

    async def _emit_new_output_lines(
        self,
        *,
        event: ProcessStartedEvent,
        proc: Process,
        stdout_reader: TextIO,
        state: _OutputStreamState,
        emit_partial: bool,
        event_class: type[ProcessStdoutEvent] | type[ProcessStderrEvent] = ProcessStdoutEvent,
    ) -> None:
        stdout_reader.seek(state.offset)
        chunk = stdout_reader.read()
        state.offset = stdout_reader.tell()

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
            try:
                # Background readers can outlive the handler that spawned them.
                # Emit on the owning bus with explicit ancestry rather than using
                # event.emit(), whose ambient handler context may already be gone.
                await self.bus.emit(
                    event_class(
                        event_parent_id=event.event_id,
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
                    # Stdout progress events are best-effort during shutdown.
                    # The owning runner may already have detached the bus after
                    # a SIGINT/SIGTERM; do not turn that late cosmetic flush
                    # into an unhandled task exception while the process exits.
                    return
                raise
