"""ProcessService — owns hook subprocess execution and raw process events."""

import asyncio
import time
from pathlib import Path
from typing import ClassVar, Literal

from abxbus import BaseEvent, EventBus
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
        ProcessStartedEvent,
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
        stderr_is_tty: bool,
    ):
        self.emit_jsonl = emit_jsonl
        self.stderr_is_tty = stderr_is_tty
        super().__init__(bus)
        self.bus.on(ProcessEvent, self.on_ProcessEvent)
        self.bus.on(ProcessStartedEvent, self.on_ProcessStartedEvent)
        self.bus.on(ProcessKillEvent, self.on_ProcessKillEvent)

    # ── Event handlers ──────────────────────────────────────────────────────

    async def on_ProcessEvent(self, event: ProcessEvent) -> None:
        """Spawn one hook subprocess and emit ProcessStartedEvent.

        The child ProcessStartedEvent carries the live subprocess handle and
        artifact paths needed for the rest of the subprocess lifetime.
        """
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
            with open(stderr_file, "w") as err_fh:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=str(plugin_output_dir),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=err_fh,
                    env=event.env,
                    # Background hooks get their own process group so cleanup can
                    # signal the hook and its children together via killpg().
                    start_new_session=event.is_background,
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

    async def on_ProcessStartedEvent(self, event: ProcessStartedEvent) -> None:
        """Own the subprocess after spawn: stdout, exit, and completion."""
        # ProcessStartedEvent is now the source of truth for the live subprocess.
        process = event.subprocess
        proc = Process(
            cmd=[event.hook_path, *event.hook_args],
            pwd=event.output_dir,
            timeout=event.timeout,
            started_at=event.start_ts,
            plugin=event.plugin_name,
            hook_name=event.hook_name,
        )
        plugin_output_dir = Path(event.output_dir)
        try:
            try:
                await asyncio.wait_for(
                    self._stream_stdout(
                        event=event,
                        proc=proc,
                        process=process,
                        stdout_file=event.stdout_file,
                    ),
                    timeout=event.timeout or None,
                )
                if process.returncode is None:
                    await asyncio.wait_for(process.wait(), timeout=event.timeout or None)
                timed_out = False
            except TimeoutError:
                timed_out = True
                # Timeout is part of normal process lifecycle handling, so turn it
                # into a killed process and emit a regular ProcessCompletedEvent.
                await graceful_kill_process(process)
            except Exception:
                # Any streaming/parsing failure should still shut the subprocess
                # down before the error is surfaced as ProcessCompletedEvent.
                await graceful_kill_process(process)
                raise

            returncode = process.returncode if process.returncode is not None else 0
            stdout = event.stdout_file.read_text() if event.stdout_file.exists() else ""
            stderr = event.stderr_file.read_text() if event.stderr_file.exists() else ""

            if timed_out:
                returncode = -1
                stderr = f"Hook timed out after {event.timeout} seconds"

            status = _process_status(returncode)
            proc.exit_code = returncode
            proc.status = status
            proc.stdout = stdout
            proc.stderr = stderr
            proc.ended_at = now_iso()

            # Output files are derived from the directory diff, not from stdout.
            files_after = set(plugin_output_dir.rglob("*")) if plugin_output_dir.exists() else set()
            new_files = scan_output_files(
                plugin_output_dir,
                file_paths=files_after - event.files_before,
            )

            event.pid_file.unlink(missing_ok=True)

            if returncode == 0:
                event.stdout_file.unlink(missing_ok=True)
                event.stderr_file.unlink(missing_ok=True)

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
                ),
            )
        except Exception as e:
            if process.returncode is None:
                await graceful_kill_process(process)
            event.pid_file.unlink(missing_ok=True)
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
                    pid=process.pid,
                    url=event.url,
                    process_type=event.process_type,
                    worker_type=event.worker_type,
                    start_ts=proc.started_at or "",
                    end_ts=proc.ended_at or "",
                ),
            )

    async def on_ProcessKillEvent(self, event: ProcessKillEvent) -> None:
        """Gracefully shut down a running background hook.

        Cleanup emits ProcessKillEvent as a direct child of the cleanup event
        that requested shutdown. The target ProcessStartedEvent is resolved by
        ancestry plus ``plugin_name``/``hook_name``/``pid``. If the process is
        already gone, pid-file validation makes this a safe no-op.
        """
        cleanup_event = self.bus.event_history.get(event.event_parent_id or "")
        if not isinstance(cleanup_event, (SnapshotCleanupEvent, CrawlCleanupEvent)):
            raise RuntimeError(f"Missing cleanup parent for ProcessKillEvent {event.event_id}")
        root_event: SnapshotEvent | CrawlEvent | None
        if isinstance(cleanup_event, SnapshotCleanupEvent):
            root_event = await self.bus.find(
                SnapshotEvent,
                past=True,
                future=False,
                where=lambda candidate: self.bus.event_is_child_of(cleanup_event, candidate),
            )
        else:
            root_event = await self.bus.find(
                CrawlEvent,
                past=True,
                future=False,
                where=lambda candidate: self.bus.event_is_child_of(cleanup_event, candidate),
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
                        ),
                    )
            except asyncio.CancelledError:
                return stdout_lines
        return stdout_lines
