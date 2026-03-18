"""
Event-driven orchestrator for abx-dl using bubus.

Each plugin hook is registered as its own handler on the EventBus, keyed by
CrawlEvent or SnapshotEvent. The bus's default serial handler execution ensures
hooks run in registration order (sorted by step/priority).

Events follow command/completion pairs:
  - ProcessEvent (command) → handler runs subprocess
  - ProcessCompleted (notification) → handlers parse JSONL, emit Binary/Machine

Side-effect cascades (Binary→Machine→config) chain through ``await bus.emit()``
queue-jumps: when a handler awaits an emitted event, bubus processes it and all
its children synchronously before returning.
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

from bubus import EventBus

from .events import (
    CrawlEvent,
    SnapshotEvent,
    ProcessCompleted,
)
from .models import Snapshot, Process, ArchiveResult, VisibleRecord, write_jsonl, now_iso, uuid7
from .plugins import Hook, Plugin, filter_plugins
from .process_utils import (
    validate_pid_file,
    write_pid_file_with_mtime,
    write_cmd_file,
    is_process_alive,
)


# ============================================================================
# Utility helpers (pure functions, no bus interaction)
# ============================================================================

def get_interpreter(language: str) -> list[str]:
    return {'py': [sys.executable], 'js': ['node'], 'sh': ['bash']}.get(language, [])


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


def _binary_env_key(name: str) -> str:
    normalized = ''.join(ch if ch.isalnum() else '_' for ch in name).upper()
    return f'{normalized}_BINARY'


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
    import re as _re
    hook_name = meta_path.name.removesuffix('.meta.json')
    match = _re.match(r'^on_(\w+)__(\d)(\d)_', hook_name)
    if not match:
        return (99, 9, 9, hook_name)
    event_order = {'Machine': 0, 'Binary': 1, 'Crawl': 2, 'Snapshot': 3}
    return (event_order.get(match.group(1), 99), int(match.group(2)), int(match.group(3)), hook_name)


# ============================================================================
# Subprocess execution (no bus interaction — just runs and returns results)
# ============================================================================

# ============================================================================
# ADAPTED FROM: ArchiveBox/archivebox/hooks.py run_hook()
# COMMIT: 69965a27820507526767208c179c62f4a579555c
# DATE: 2024-12-30
# MODIFICATIONS:
#   - Removed Django settings references
#   - Removed Machine model references
#   - Simplified return type to (Process, ArchiveResult, is_background)
#   - Uses abx-dl's Process/ArchiveResult models instead of HookResult dict
#   - Writes to files like ArchiveBox but still returns parsed output
# ============================================================================
def run_hook(hook: Hook, url: str, snapshot_id: str, output_dir: Path, env: dict[str, str], timeout: int = 60) -> tuple[Process, ArchiveResult, bool]:
    """
    Run a single hook and return Process, ArchiveResult, and is_background flag.

    For background hooks, returns immediately with is_background=True.
    Process output is written to stdout.log/stderr.log files.
    PID files are created with mtime set to process start time for validation.
    """
    interpreter = get_interpreter(hook.language)
    if not interpreter:
        proc = Process(cmd=[], exit_code=1, stderr=f'Unknown language: {hook.language}', plugin=hook.plugin_name, hook_name=hook.name)
        result = ArchiveResult(snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name, status='failed', error=proc.stderr)
        return proc, result, False

    npm_bin_dir = env.get('NPM_BIN_DIR', '').strip()
    if npm_bin_dir:
        path = env.get('PATH', '')
        env['PATH'] = f"{npm_bin_dir}:{path}" if path else npm_bin_dir

    cmd = [*interpreter, str(hook.path), f'--url={url}', f'--snapshot-id={snapshot_id}']
    proc = Process(cmd=cmd, pwd=str(output_dir), timeout=timeout, started_at=now_iso(), plugin=hook.plugin_name, hook_name=hook.name)
    is_background = hook.is_background

    hook_basename = hook.name
    stdout_file = output_dir / f'{hook_basename}.stdout.log'
    stderr_file = output_dir / f'{hook_basename}.stderr.log'
    pid_file = output_dir / f'{hook_basename}.pid'
    cmd_file = output_dir / f'{hook_basename}.sh'
    meta_file = output_dir / f'{hook_basename}.meta.json'

    try:
        write_cmd_file(cmd_file, cmd)
        files_before = set(output_dir.rglob('*')) if output_dir.exists() else set()

        with open(stdout_file, 'w') as out, open(stderr_file, 'w') as err:
            process = subprocess.Popen(cmd, cwd=str(output_dir), stdout=out, stderr=err, env=env, start_new_session=is_background)
            process_start_time = time.time()
            write_pid_file_with_mtime(pid_file, process.pid, process_start_time)

            if is_background:
                meta_file.write_text(json.dumps({
                    'process_id': proc.id, 'snapshot_id': snapshot_id, 'plugin': hook.plugin_name,
                    'hook_name': hook.name, 'cmd': cmd, 'pwd': str(output_dir),
                    'started_at': proc.started_at, 'timeout': timeout,
                }))
                ar = ArchiveResult(
                    snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name,
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
                    snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name,
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
            snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name,
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
            snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name,
            status='failed', process_id=proc.id, error=proc.stderr,
        )
        return proc, ar, False


def _run_binary_hook(hook: Hook, record: dict[str, Any], output_dir: Path, env: dict[str, str], timeout: int = 300) -> Process:
    """Execute a provider plugin on_Binary hook."""
    interpreter = get_interpreter(hook.language)
    if not interpreter:
        return Process(cmd=[], exit_code=1, stderr=f'Unknown language: {hook.language}', pwd=str(output_dir), timeout=timeout, plugin=hook.plugin_name, hook_name=hook.name)

    proc_env = env.copy()
    npm_bin_dir = proc_env.get('NPM_BIN_DIR', '').strip()
    if npm_bin_dir:
        path = proc_env.get('PATH', '')
        proc_env['PATH'] = f"{npm_bin_dir}:{path}" if path else npm_bin_dir

    binary_id = str(record.get('binary_id') or uuid7())
    machine_id = str(record.get('machine_id') or proc_env.get('MACHINE_ID', ''))
    name = str(record.get('name', '')).strip()

    cmd = [*interpreter, str(hook.path), f'--binary-id={binary_id}', f'--machine-id={machine_id}', f'--name={name}']
    binproviders = str(record.get('binproviders') or record.get('binprovider') or '').strip()
    if binproviders:
        cmd.append(f'--binproviders={binproviders}')
    overrides = record.get('overrides')
    if overrides is not None:
        cmd.append(f'--overrides={json.dumps(overrides)}')
    custom_cmd = record.get('custom_cmd', record.get('custom-cmd'))
    if custom_cmd:
        cmd.append(f'--custom-cmd={custom_cmd}')

    proc = Process(cmd=cmd, binary_id=binary_id, pwd=str(output_dir), timeout=timeout, started_at=now_iso(), plugin=hook.plugin_name, hook_name=hook.name)

    try:
        completed = subprocess.run(cmd, cwd=str(output_dir), capture_output=True, text=True, timeout=timeout, env=proc_env)
        proc.exit_code = completed.returncode
        proc.stdout = completed.stdout
        proc.stderr = completed.stderr
        proc.ended_at = now_iso()
    except subprocess.TimeoutExpired:
        proc.exit_code = -1
        proc.stderr = f'Binary hook timed out after {timeout} seconds'
        proc.ended_at = now_iso()
    except Exception as err:
        proc.exit_code = -1
        proc.stderr = f'{type(err).__name__}: {err}'
        proc.ended_at = now_iso()

    return proc


# ============================================================================
# Background hook management (no bus interaction)
# ============================================================================

def _poll_background_hooks(
    output_dir: Path, index_path: Path, stderr_is_tty: bool, *, emit_jsonl: bool,
    known_meta_files: set[Path] | None = None,
) -> list[tuple[Process, ArchiveResult]]:
    if not output_dir.exists():
        return []
    finalized: list[tuple[Process, ArchiveResult]] = []
    meta_files_raw = known_meta_files if known_meta_files is not None else output_dir.glob('**/on_*.meta.json')
    meta_files = sorted(meta_files_raw, key=_background_hook_sort_key)
    for meta_file in meta_files:
        if not meta_file.exists():
            continue
        hook_basename = meta_file.name.removesuffix('.meta.json')
        pid_file = meta_file.parent / f'{hook_basename}.pid'
        if not pid_file.exists():
            finalized.append(_finalize_background_hook(meta_file.parent, hook_basename, index_path, stderr_is_tty, emit_jsonl=emit_jsonl))
            continue
        try:
            pid = int(pid_file.read_text().strip())
        except (ValueError, OSError):
            continue
        exit_code = _try_reap_process(pid)
        if exit_code is not None:
            pid_file.unlink(missing_ok=True)
            finalized.append(_finalize_background_hook(meta_file.parent, hook_basename, index_path, stderr_is_tty, emit_jsonl=emit_jsonl, exit_code=exit_code))
            continue
        cmd_file = meta_file.parent / f'{hook_basename}.sh'
        if known_meta_files is None and not validate_pid_file(pid_file, cmd_file):
            continue
    return finalized


def _wait_for_background_hooks(
    output_dir: Path, index_path: Path, stderr_is_tty: bool, *, emit_jsonl: bool,
    known_meta_files: set[Path],
) -> list[tuple[Process, ArchiveResult]]:
    if not known_meta_files:
        return []
    timeout_seconds = 0
    for meta_file in known_meta_files:
        if not meta_file.exists():
            continue
        try:
            timeout_seconds = max(timeout_seconds, int(json.loads(meta_file.read_text()).get('timeout', 0)))
        except Exception:
            continue
    deadline = time.time() + max(timeout_seconds, 1)
    finalized: list[tuple[Process, ArchiveResult]] = []
    while time.time() < deadline:
        pending_meta_files = {mf for mf in known_meta_files if mf.exists()}
        if not pending_meta_files:
            break
        newly_finalized = _poll_background_hooks(output_dir, index_path, stderr_is_tty, emit_jsonl=emit_jsonl, known_meta_files=pending_meta_files)
        if newly_finalized:
            finalized.extend(newly_finalized)
            continue
        time.sleep(0.1)
    return finalized


def _finalize_background_hook(
    plugin_dir: Path, hook_basename: str, index_path: Path, stderr_is_tty: bool, *,
    emit_jsonl: bool, exit_code: int | None = None, error: str | None = None,
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

    write_jsonl(index_path, proc, also_print=emit_jsonl)
    write_jsonl(index_path, ar, also_print=emit_jsonl)

    if effective_exit_code == 0 and error is None:
        stdout_file.unlink(missing_ok=True)
        stderr_file.unlink(missing_ok=True)
    meta_file.unlink(missing_ok=True)

    return proc, ar


# ============================================================================
# ADAPTED FROM: ArchiveBox/archivebox/crawls/models.py Crawl.cleanup()
# COMMIT: 69965a27820507526767208c179c62f4a579555c
# DATE: 2024-12-30
# MODIFICATIONS:
#   - Standalone function instead of model method
#   - Takes output_dir parameter instead of self.OUTPUT_DIR
#   - Writes final results to index.jsonl
#   - No Django ORM updates
# ============================================================================
def cleanup_background_hooks(
    output_dir: Path, index_path: Path, stderr_is_tty: bool, *,
    emit_jsonl: bool, known_meta_files: set[Path] | None = None,
) -> list[ArchiveResult]:
    """
    Clean up background hooks by scanning for all .meta.json files.

    Sends SIGTERM, waits, then SIGKILL if needed.
    Uses process group killing to handle Chrome and its children.
    Handles unkillable processes gracefully.
    """
    if not output_dir.exists():
        return []
    meta_files = sorted(known_meta_files) if known_meta_files is not None else list(output_dir.glob('**/on_*.meta.json'))
    if not meta_files:
        return []

    final_results: list[ArchiveResult] = []
    for meta_file in meta_files:
        if not meta_file.exists():
            continue
        hook_basename = meta_file.name.removesuffix('.meta.json')
        pid_file = meta_file.parent / f'{hook_basename}.pid'

        if not pid_file.exists():
            _, ar = _finalize_background_hook(meta_file.parent, hook_basename, index_path, stderr_is_tty, emit_jsonl=emit_jsonl)
            final_results.append(ar)
            continue

        try:
            pid = int(pid_file.read_text().strip())
        except (ValueError, OSError):
            continue

        exit_code = _try_reap_process(pid)
        if exit_code is not None:
            pid_file.unlink(missing_ok=True)
            _, ar = _finalize_background_hook(pid_file.parent, hook_basename, index_path, stderr_is_tty, emit_jsonl=emit_jsonl, exit_code=exit_code)
            final_results.append(ar)
            continue

        cmd_file = pid_file.parent / f'{hook_basename}.sh'
        if known_meta_files is None and not validate_pid_file(pid_file, cmd_file):
            pid_file.unlink(missing_ok=True)
            _, ar = _finalize_background_hook(pid_file.parent, hook_basename, index_path, stderr_is_tty, emit_jsonl=emit_jsonl)
            final_results.append(ar)
            continue

        try:
            # Step 1: Send SIGTERM for graceful shutdown
            # Try to kill process group first (handles detached processes like Chrome)
            try:
                try:
                    os.killpg(pid, signal.SIGTERM)
                except (OSError, ProcessLookupError):
                    # Fall back to killing just the process
                    os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                # Already dead
                pid_file.unlink(missing_ok=True)
                exit_code = _try_reap_process(pid)
                _, ar = _finalize_background_hook(pid_file.parent, hook_basename, index_path, stderr_is_tty, emit_jsonl=emit_jsonl, exit_code=exit_code)
                final_results.append(ar)
                continue

            # Step 2: Wait for graceful shutdown
            exit_code = _wait_for_process_exit(pid, timeout=2.0)

            # Step 3: Check if exited after SIGTERM
            # For current-run children, only finalize once we've actually reaped
            # the child so their final stdout/stderr is definitely available.
            if exit_code is None and known_meta_files is not None:
                exit_code = _wait_for_process_exit(pid, timeout=0.2)

            if exit_code is not None:
                # Process terminated gracefully and was reaped
                pid_file.unlink(missing_ok=True)
                _, ar = _finalize_background_hook(pid_file.parent, hook_basename, index_path, stderr_is_tty, emit_jsonl=emit_jsonl, exit_code=exit_code)
                final_results.append(ar)
                continue

            # Step 4: Process still alive, force kill ENTIRE process group with SIGKILL
            try:
                try:
                    # Always kill entire process group with SIGKILL (not individual processes)
                    os.killpg(pid, signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    # Process group kill failed, try single process as fallback
                    os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                # Process died between check and kill
                pid_file.unlink(missing_ok=True)
                exit_code = _try_reap_process(pid)
                _, ar = _finalize_background_hook(pid_file.parent, hook_basename, index_path, stderr_is_tty, emit_jsonl=emit_jsonl, exit_code=exit_code)
                final_results.append(ar)
                continue

            # Step 5: Wait and verify death
            exit_code = _wait_for_process_exit(pid, timeout=1.0)
            if exit_code is None and is_process_alive(pid):
                # Process is unkillable (likely in UNE state on macOS)
                # This happens when Chrome crashes in kernel syscall (IOSurface)
                # Log but don't block cleanup - process will remain until reboot
                if stderr_is_tty:
                    print(f'Warning: Process {pid} is unkillable (likely crashed in kernel). Will remain until reboot.', file=sys.stderr)
                _, ar = _finalize_background_hook(pid_file.parent, hook_basename, index_path, stderr_is_tty, emit_jsonl=emit_jsonl, error='Process unkillable')
                final_results.append(ar)
            else:
                # Successfully killed
                pid_file.unlink(missing_ok=True)
                _, ar = _finalize_background_hook(pid_file.parent, hook_basename, index_path, stderr_is_tty, emit_jsonl=emit_jsonl, exit_code=exit_code)
                final_results.append(ar)

        except (ValueError, OSError):
            # Invalid PID file or permission error
            pass

    return final_results


# ============================================================================
# Core orchestrator — sets up the bus, registers handlers, drives lifecycle
# ============================================================================

async def download(
    url: str,
    plugins: dict[str, Plugin],
    output_dir: Path,
    selected_plugins: list[str] | None = None,
    config_overrides: dict[str, Any] | None = None,
    auto_install: bool = True,
    crawl_only: bool = False,
    *,
    emit_jsonl: bool | None = None,
    on_result: Callable[[VisibleRecord], None] | None = None,
) -> list[VisibleRecord]:
    """Download a URL using plugins, coordinated through a bubus EventBus."""

    output_dir = output_dir or Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / 'index.jsonl'
    stdout_is_tty = sys.stdout.isatty()
    stderr_is_tty = sys.stderr.isatty()
    if emit_jsonl is None:
        emit_jsonl = not stdout_is_tty

    # Filter plugins (no binary pre-check — on_Crawl hooks handle installation)
    if selected_plugins:
        plugins = filter_plugins(plugins, selected_plugins)

    # Create snapshot
    snapshot = Snapshot(url=url)
    write_jsonl(index_path, snapshot, also_print=emit_jsonl)

    # Collect and sort hooks
    crawl_hooks: list[tuple[Plugin, Hook]] = []
    snapshot_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in plugins.values():
        for hook in plugin.get_crawl_hooks():
            crawl_hooks.append((plugin, hook))
        for hook in plugin.get_snapshot_hooks():
            snapshot_hooks.append((plugin, hook))
    crawl_hooks.sort(key=lambda x: x[1].sort_key)
    snapshot_hooks.sort(key=lambda x: x[1].sort_key)

    # Shared mutable state (closed over by handlers)
    shared_config: dict[str, Any] = dict(config_overrides) if config_overrides else {}
    results: list[VisibleRecord] = []
    known_background_meta_files: set[Path] = set()

    def emit_result(record: VisibleRecord) -> None:
        results.append(record)
        if on_result:
            on_result(record)

    # --- Create event bus and register services ---
    bus = EventBus(name='AbxDl')

    # Import services here to avoid circular imports (services import from orchestrator)
    from .services import MachineService, BinaryService, ProcessService, CrawlService

    MachineService(bus, shared_config=shared_config)
    BinaryService(
        bus, shared_config=shared_config, plugins=plugins, auto_install=auto_install,
        output_dir=output_dir, index_path=index_path, emit_jsonl=emit_jsonl,
        emit_result=emit_result,
    )
    ProcessService(
        bus, index_path=index_path, output_dir=output_dir, emit_jsonl=emit_jsonl,
        stderr_is_tty=stderr_is_tty, emit_result=emit_result,
        known_background_meta_files=known_background_meta_files,
    )
    CrawlService(
        bus, url=url, snapshot=snapshot, output_dir=output_dir,
        shared_config=shared_config, crawl_hooks=crawl_hooks, snapshot_hooks=snapshot_hooks,
    )

    # --- Drive the lifecycle through the bus ---
    try:
        # Emit CrawlEvent — all crawl hook handlers fire serially via queue-jump
        await bus.emit(CrawlEvent(url=url, snapshot_id=snapshot.id, output_dir=str(output_dir)))

        if crawl_only and known_background_meta_files:
            for bg_proc, bg_ar in _wait_for_background_hooks(
                output_dir, index_path, stderr_is_tty, emit_jsonl=emit_jsonl,
                known_meta_files=known_background_meta_files,
            ):
                await bus.emit(ProcessCompleted(
                    plugin_name=bg_ar.plugin, hook_name=bg_ar.hook_name,
                    stdout=bg_proc.stdout, stderr=bg_proc.stderr,
                    exit_code=bg_proc.exit_code or 0, output_dir=str(output_dir / bg_ar.plugin),
                    output_files=bg_ar.output_files, output_str=bg_ar.output_str,
                    status=bg_ar.status, is_background=True,
                ))
                emit_result(bg_ar)
        elif not crawl_only:
            # Emit SnapshotEvent — all snapshot hook handlers fire serially
            await bus.emit(SnapshotEvent(url=url, snapshot_id=snapshot.id, output_dir=str(output_dir)))

    finally:
        background_results = cleanup_background_hooks(
            output_dir, index_path, stderr_is_tty, emit_jsonl=emit_jsonl,
            known_meta_files=known_background_meta_files,
        )
        for ar in background_results:
            emit_result(ar)
        await bus.stop()

    return results
