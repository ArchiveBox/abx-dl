"""ProcessService — owns ALL hook subprocess execution and background hook management."""

import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, ClassVar

from bubus import BaseEvent, EventBus

from ..events import BinaryEvent, MachineEvent, ProcessCompleted, ProcessEvent
from ..models import ArchiveResult, Process, VisibleRecord, write_jsonl, now_iso, uuid7
from ..process_utils import (
    validate_pid_file,
    write_pid_file_with_mtime,
    write_cmd_file,
    is_process_alive,
)
from .base import BaseService


class ProcessService(BaseService):
    """Owns all hook subprocess execution and background hook lifecycle.

    All hook execution flows through ProcessEvent — no other service runs
    processes directly. This is the single place that spawns subprocesses.
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
        known_background_meta_files: set[Path],
    ):
        self.index_path = index_path
        self.output_dir = output_dir
        self.emit_jsonl = emit_jsonl
        self.stderr_is_tty = stderr_is_tty
        self.emit_result = emit_result
        self.known_background_meta_files = known_background_meta_files
        super().__init__(bus)

    # ── Event handlers ──────────────────────────────────────────────────────

    async def on_ProcessEvent(self, event: ProcessEvent) -> None:
        plugin_output_dir = Path(event.output_dir)

        proc, ar, is_background = self._run_hook(
            event.hook_path, event.hook_args,
            plugin_name=event.plugin_name, hook_name=event.hook_name,
            snapshot_id=event.snapshot_id, is_background=event.is_background,
            output_dir=plugin_output_dir, env=event.env, timeout=event.timeout,
        )

        if is_background:
            self.known_background_meta_files.add(plugin_output_dir / f'{event.hook_name}.meta.json')
            self.emit_result(ar)
        else:
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

        # Poll for completed bg hooks between each foreground hook
        await self._poll_and_emit_bg_hooks()

    async def on_ProcessCompleted(self, event: ProcessCompleted) -> None:
        """Parse JSONL stdout from completed processes, emit Binary/Machine events."""
        for record in _parse_jsonl_records(event.stdout):
            record_type = record.pop('type', None)
            if record_type == 'Binary':
                await self.bus.emit(BinaryEvent(**record))
            elif record_type == 'Machine':
                await self.bus.emit(MachineEvent(**record))

    # ── Background hook management (called by download() lifecycle) ─────────

    async def wait_for_background_hooks(self) -> None:
        """Block until all known background hooks complete, emitting ProcessCompleted for each."""
        if not self.known_background_meta_files:
            return
        timeout_seconds = 0
        for meta_file in self.known_background_meta_files:
            if not meta_file.exists():
                continue
            try:
                timeout_seconds = max(timeout_seconds, int(json.loads(meta_file.read_text()).get('timeout', 0)))
            except Exception:
                continue
        deadline = time.time() + max(timeout_seconds, 1)
        while time.time() < deadline:
            pending = {mf for mf in self.known_background_meta_files if mf.exists()}
            if not pending:
                break
            finalized = self._poll_background_hooks(known_meta_files=pending)
            if finalized:
                for bg_proc, bg_ar in finalized:
                    await self.bus.emit(ProcessCompleted(
                        plugin_name=bg_ar.plugin, hook_name=bg_ar.hook_name,
                        stdout=bg_proc.stdout, stderr=bg_proc.stderr,
                        exit_code=bg_proc.exit_code or 0, output_dir=str(self.output_dir / bg_ar.plugin),
                        output_files=bg_ar.output_files, output_str=bg_ar.output_str,
                        status=bg_ar.status, is_background=True,
                    ))
                    self.emit_result(bg_ar)
                continue
            time.sleep(0.1)

    def cleanup_background_hooks(self) -> list[ArchiveResult]:
        """Kill remaining background hooks (SIGTERM → wait → SIGKILL). Returns final results.

        ADAPTED FROM: ArchiveBox/archivebox/crawls/models.py Crawl.cleanup()
        COMMIT: 69965a27820507526767208c179c62f4a579555c
        DATE: 2024-12-30
        """
        if not self.output_dir.exists():
            return []
        meta_files = (
            sorted(self.known_background_meta_files)
            if self.known_background_meta_files
            else list(self.output_dir.glob('**/on_*.meta.json'))
        )
        if not meta_files:
            return []

        final_results: list[ArchiveResult] = []
        for meta_file in meta_files:
            if not meta_file.exists():
                continue
            hook_basename = meta_file.name.removesuffix('.meta.json')
            pid_file = meta_file.parent / f'{hook_basename}.pid'

            if not pid_file.exists():
                _, ar = self._finalize_background_hook(meta_file.parent, hook_basename)
                final_results.append(ar)
                continue

            try:
                pid = int(pid_file.read_text().strip())
            except (ValueError, OSError):
                continue

            exit_code = _try_reap_process(pid)
            if exit_code is not None:
                pid_file.unlink(missing_ok=True)
                _, ar = self._finalize_background_hook(pid_file.parent, hook_basename, exit_code=exit_code)
                final_results.append(ar)
                continue

            cmd_file = pid_file.parent / f'{hook_basename}.sh'
            if not self.known_background_meta_files and not validate_pid_file(pid_file, cmd_file):
                pid_file.unlink(missing_ok=True)
                _, ar = self._finalize_background_hook(pid_file.parent, hook_basename)
                final_results.append(ar)
                continue

            try:
                # Step 1: Send SIGTERM for graceful shutdown
                try:
                    try:
                        os.killpg(pid, signal.SIGTERM)
                    except (OSError, ProcessLookupError):
                        os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pid_file.unlink(missing_ok=True)
                    exit_code = _try_reap_process(pid)
                    _, ar = self._finalize_background_hook(pid_file.parent, hook_basename, exit_code=exit_code)
                    final_results.append(ar)
                    continue

                # Step 2: Wait for graceful shutdown
                exit_code = _wait_for_process_exit(pid, timeout=2.0)

                # Step 3: Check if exited after SIGTERM
                if exit_code is None and self.known_background_meta_files:
                    exit_code = _wait_for_process_exit(pid, timeout=0.2)

                if exit_code is not None:
                    pid_file.unlink(missing_ok=True)
                    _, ar = self._finalize_background_hook(pid_file.parent, hook_basename, exit_code=exit_code)
                    final_results.append(ar)
                    continue

                # Step 4: Force kill ENTIRE process group with SIGKILL
                try:
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except (OSError, ProcessLookupError):
                        os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pid_file.unlink(missing_ok=True)
                    exit_code = _try_reap_process(pid)
                    _, ar = self._finalize_background_hook(pid_file.parent, hook_basename, exit_code=exit_code)
                    final_results.append(ar)
                    continue

                # Step 5: Wait and verify death
                exit_code = _wait_for_process_exit(pid, timeout=1.0)
                if exit_code is None and is_process_alive(pid):
                    if self.stderr_is_tty:
                        print(f'Warning: Process {pid} is unkillable (likely crashed in kernel). Will remain until reboot.', file=sys.stderr)
                    _, ar = self._finalize_background_hook(pid_file.parent, hook_basename, error='Process unkillable')
                    final_results.append(ar)
                else:
                    pid_file.unlink(missing_ok=True)
                    _, ar = self._finalize_background_hook(pid_file.parent, hook_basename, exit_code=exit_code)
                    final_results.append(ar)

            except (ValueError, OSError):
                pass

        return final_results

    # ── Private: subprocess execution ───────────────────────────────────────

    @staticmethod
    def _run_hook(
        hook_path: str, hook_args: list[str], *,
        plugin_name: str, hook_name: str, snapshot_id: str,
        is_background: bool, output_dir: Path, env: dict[str, str], timeout: int = 60,
    ) -> tuple[Process, ArchiveResult, bool]:
        """Run a +x hook executable and return (Process, ArchiveResult, is_background).

        ADAPTED FROM: ArchiveBox/archivebox/hooks.py run_hook()
        COMMIT: 69965a27820507526767208c179c62f4a579555c
        """
        cmd = [hook_path, *hook_args]
        proc = Process(cmd=cmd, pwd=str(output_dir), timeout=timeout, started_at=now_iso(), plugin=plugin_name, hook_name=hook_name)

        stdout_file = output_dir / f'{hook_name}.stdout.log'
        stderr_file = output_dir / f'{hook_name}.stderr.log'
        pid_file = output_dir / f'{hook_name}.pid'
        cmd_file = output_dir / f'{hook_name}.sh'
        meta_file = output_dir / f'{hook_name}.meta.json'

        try:
            write_cmd_file(cmd_file, cmd)
            files_before = set(output_dir.rglob('*')) if output_dir.exists() else set()

            with open(stdout_file, 'w') as out, open(stderr_file, 'w') as err:
                process = subprocess.Popen(cmd, cwd=str(output_dir), stdout=out, stderr=err, env=env, start_new_session=is_background)
                process_start_time = time.time()
                write_pid_file_with_mtime(pid_file, process.pid, process_start_time)

                if is_background:
                    meta_file.write_text(json.dumps({
                        'process_id': proc.id, 'snapshot_id': snapshot_id, 'plugin': plugin_name,
                        'hook_name': hook_name, 'cmd': cmd, 'pwd': str(output_dir),
                        'started_at': proc.started_at, 'timeout': timeout,
                    }))
                    ar = ArchiveResult(
                        snapshot_id=snapshot_id, plugin=plugin_name, hook_name=hook_name,
                        status='started', process_id=proc.id, start_ts=proc.started_at,
                    )
                    return proc, ar, True

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
                    return proc, ar, False

            stdout = stdout_file.read_text() if stdout_file.exists() else ''
            stderr = stderr_file.read_text() if stderr_file.exists() else ''
            proc.exit_code = returncode
            proc.stdout = stdout
            proc.stderr = stderr
            proc.ended_at = now_iso()

            files_after = set(output_dir.rglob('*')) if output_dir.exists() else set()
            new_files = sorted(str(f.relative_to(output_dir)) for f in (files_after - files_before) if f.is_file())
            excluded_suffixes = ('.stdout.log', '.stderr.log', '.pid', '.sh', '.meta.json')
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
            return proc, ar, False

        except Exception as e:
            proc.exit_code = -1
            proc.stderr = f'{type(e).__name__}: {e}'
            proc.ended_at = now_iso()
            ar = ArchiveResult(
                snapshot_id=snapshot_id, plugin=plugin_name, hook_name=hook_name,
                status='failed', process_id=proc.id, error=proc.stderr,
            )
            return proc, ar, False

    # ── Private: background hook helpers ────────────────────────────────────

    async def _poll_and_emit_bg_hooks(self) -> None:
        """Poll for completed bg hooks and emit ProcessCompleted for each."""
        for bg_proc, bg_ar in self._poll_background_hooks():
            await self.bus.emit(ProcessCompleted(
                plugin_name=bg_ar.plugin, hook_name=bg_ar.hook_name,
                stdout=bg_proc.stdout, stderr=bg_proc.stderr,
                exit_code=bg_proc.exit_code or 0, output_dir=str(self.output_dir / bg_ar.plugin),
                output_files=bg_ar.output_files, output_str=bg_ar.output_str,
                status=bg_ar.status, is_background=True,
            ))
            self.emit_result(bg_ar)

    def _poll_background_hooks(
        self, *, known_meta_files: set[Path] | None = None,
    ) -> list[tuple[Process, ArchiveResult]]:
        if not self.output_dir.exists():
            return []
        meta_files_raw = known_meta_files if known_meta_files is not None else self.known_background_meta_files
        if not meta_files_raw:
            meta_files_raw = set(self.output_dir.glob('**/on_*.meta.json'))
        meta_files = sorted(meta_files_raw, key=_background_hook_sort_key)
        finalized: list[tuple[Process, ArchiveResult]] = []
        for meta_file in meta_files:
            if not meta_file.exists():
                continue
            hook_basename = meta_file.name.removesuffix('.meta.json')
            pid_file = meta_file.parent / f'{hook_basename}.pid'
            if not pid_file.exists():
                finalized.append(self._finalize_background_hook(meta_file.parent, hook_basename))
                continue
            try:
                pid = int(pid_file.read_text().strip())
            except (ValueError, OSError):
                continue
            exit_code = _try_reap_process(pid)
            if exit_code is not None:
                pid_file.unlink(missing_ok=True)
                finalized.append(self._finalize_background_hook(meta_file.parent, hook_basename, exit_code=exit_code))
                continue
            cmd_file = meta_file.parent / f'{hook_basename}.sh'
            if known_meta_files is None and not validate_pid_file(pid_file, cmd_file):
                continue
        return finalized

    def _finalize_background_hook(
        self, plugin_dir: Path, hook_basename: str, *,
        exit_code: int | None = None, error: str | None = None,
    ) -> tuple[Process, ArchiveResult]:
        time.sleep(0.1)
        stdout_file = plugin_dir / f'{hook_basename}.stdout.log'
        stderr_file = plugin_dir / f'{hook_basename}.stderr.log'
        meta_file = plugin_dir / f'{hook_basename}.meta.json'

        stdout, stderr = _read_background_logs(stdout_file, stderr_file, wait_for_archive_result=(exit_code is not None or error is None))
        excluded_suffixes = ('.stdout.log', '.stderr.log', '.pid', '.sh', '.meta.json')
        new_files = sorted(
            str(f.relative_to(plugin_dir)) for f in plugin_dir.rglob('*')
            if f.is_file() and not any(f.name.endswith(suffix) for suffix in excluded_suffixes)
        )

        meta: dict[str, Any] = {}
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text())
            except json.JSONDecodeError:
                meta = {}

        effective_exit_code = exit_code if exit_code is not None else (0 if error is None else -1)
        status = 'succeeded' if effective_exit_code == 0 and error is None else 'failed'
        output_str = ''
        snapshot_id = str(meta.get('snapshot_id', ''))
        hook_name = str(meta.get('hook_name', hook_basename))
        plugin_name = str(meta.get('plugin', plugin_dir.name))

        for line in stdout.strip().split('\n'):
            if line.strip():
                try:
                    record = json.loads(line)
                    if record.get('type') == 'ArchiveResult':
                        status = record.get('status', status)
                        output_str = record.get('output_str', '')
                        snapshot_id = record.get('snapshot_id', snapshot_id)
                        hook_name = record.get('hook_name', hook_name)
                except json.JSONDecodeError:
                    pass

        output_str = _normalize_output_str(output_str, plugin_dir, new_files)
        proc = Process(
            cmd=[str(part) for part in meta.get('cmd', [])],
            id=str(meta.get('process_id', uuid7())),
            plugin=plugin_name, hook_name=hook_name,
            pwd=str(meta.get('pwd', plugin_dir)),
            started_at=meta.get('started_at'),
            exit_code=effective_exit_code,
            stdout=stdout, stderr=stderr, ended_at=now_iso(),
        )
        ar = ArchiveResult(
            snapshot_id=snapshot_id, plugin=plugin_name, hook_name=hook_name,
            status=status, process_id=proc.id, output_str=output_str, output_files=new_files,
            start_ts=meta.get('started_at'), end_ts=proc.ended_at,
            error=error or (stderr if effective_exit_code != 0 else None),
        )

        write_jsonl(self.index_path, proc, also_print=self.emit_jsonl)
        write_jsonl(self.index_path, ar, also_print=self.emit_jsonl)

        if effective_exit_code == 0 and error is None:
            stdout_file.unlink(missing_ok=True)
            stderr_file.unlink(missing_ok=True)
        meta_file.unlink(missing_ok=True)

        return proc, ar


# ── Module-level pure helpers (no state, used by ProcessService internals) ──

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


def _stdout_contains_archive_result(stdout: str) -> bool:
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith('{'):
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get('type') == 'ArchiveResult':
            return True
    return False


def _read_background_logs(stdout_file: Path, stderr_file: Path, wait_for_archive_result: bool) -> tuple[str, str]:
    stdout = stdout_file.read_text() if stdout_file.exists() else ''
    stderr = stderr_file.read_text() if stderr_file.exists() else ''
    if not wait_for_archive_result or _stdout_contains_archive_result(stdout):
        return stdout, stderr
    deadline = time.time() + 1.0
    last_stdout = stdout
    while time.time() < deadline:
        time.sleep(0.05)
        stdout = stdout_file.read_text() if stdout_file.exists() else ''
        stderr = stderr_file.read_text() if stderr_file.exists() else ''
        if _stdout_contains_archive_result(stdout):
            return stdout, stderr
        if stdout == last_stdout:
            continue
        last_stdout = stdout
    return stdout, stderr


def _try_reap_process(pid: int) -> int | None:
    try:
        waited_pid, wait_status = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return None
    if waited_pid == 0:
        return None
    return os.waitstatus_to_exitcode(wait_status)


def _wait_for_process_exit(pid: int, timeout: float) -> int | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        exit_code = _try_reap_process(pid)
        if exit_code is not None:
            return exit_code
        time.sleep(0.1)
    return _try_reap_process(pid)


def _background_hook_sort_key(meta_path: Path) -> tuple[int, int, int, str]:
    hook_name = meta_path.name.removesuffix('.meta.json')
    match = re.match(r'^on_(\w+)__(\d)(\d)_', hook_name)
    if not match:
        return (99, 9, 9, hook_name)
    event_order = {'Machine': 0, 'Binary': 1, 'Crawl': 2, 'Snapshot': 3}
    return (event_order.get(match.group(1), 99), int(match.group(2)), int(match.group(3)), hook_name)
