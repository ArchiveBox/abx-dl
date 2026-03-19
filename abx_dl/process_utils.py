"""
Process lifecycle utilities: PID file management, validation, and graceful shutdown.

Uses mtime as a "password": PID files are timestamped with process start time.
Since filesystem mtimes can be set arbitrarily but process start times cannot,
comparing them detects PID reuse (a new process that happens to get the same PID
after the original exited).
"""

import asyncio
import os
import shlex
import shutil
import signal
import time
from pathlib import Path
from typing import Optional


try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


# Grace period before escalating from SIGTERM to SIGKILL. Chrome needs up to
# 15s to flush its user_data_dir (cookies, local storage, session data) before
# exiting. Shorter timeouts risk corrupting the profile.
GRACEFUL_SHUTDOWN_TIMEOUT = 15.0


def _normalize_exec_name(exec_path: str) -> str:
    """Normalize an executable path/name for conservative equality checks."""
    name = Path(exec_path).name.lower()
    if name.startswith("python"):
        return "python"
    if name.startswith("node"):
        return "node"
    return name


def _read_recorded_argv(cmd_file: Path) -> list[str]:
    """Parse the recorded command line from a generated cmd.sh helper."""
    lines = [line.strip() for line in cmd_file.read_text().splitlines() if line.strip()]
    if not lines:
        return []
    try:
        return shlex.split(lines[-1])
    except ValueError:
        return []


def _resolve_recorded_exec(exec_path: str) -> str:
    """Resolve a recorded executable to a stable absolute path when possible."""
    if not exec_path:
        return ""
    if os.path.isabs(exec_path):
        return os.path.realpath(exec_path)
    resolved = shutil.which(exec_path)
    if resolved:
        return os.path.realpath(resolved)
    return ""


def _resolve_live_exec(exec_path: str) -> str:
    """Resolve a live process argv[0] to a stable absolute path when possible."""
    if not exec_path:
        return ""
    if os.path.isabs(exec_path):
        return os.path.realpath(exec_path)
    resolved = shutil.which(exec_path)
    if resolved:
        return os.path.realpath(resolved)
    return ""


def _normalize_recorded_arg(arg: str) -> str:
    """Normalize a recorded argv entry for equality checks."""
    if not arg:
        return ""
    if os.path.isabs(arg):
        return os.path.realpath(arg)
    return arg


def validate_pid_file(pid_file: Path, cmd_file: Optional[Path] = None, tolerance: float = 5.0) -> bool:
    """Validate PID using mtime and optional cmd.sh. Returns True if process is ours.

    Uses psutil to check that the PID is still alive and that the process start
    time matches the PID file's mtime (within ``tolerance`` seconds). Optionally
    validates the running process's command line against the recorded cmd.sh.

    Returns False (safe no-op) if:
    - PID file doesn't exist
    - PID was reused by a different process (mtime mismatch)
    - Running process doesn't match recorded command
    - psutil is unavailable or raises an error
    """
    if not pid_file.exists():
        return False

    try:
        pid = int(pid_file.read_text().strip())
        proc = psutil.Process(pid)

        # Check mtime matches process start time
        if abs(pid_file.stat().st_mtime - proc.create_time()) > tolerance:
            return False  # PID reused

        # Validate command if provided
        if cmd_file and cmd_file.exists():
            cmd = cmd_file.read_text()
            proc_argv = proc.cmdline()
            cmdline = ' '.join(proc_argv)
            if '--remote-debugging-port' in cmd and '--remote-debugging-port' not in cmdline:
                return False

            recorded_argv = _read_recorded_argv(cmd_file)
            recorded_exec = _resolve_recorded_exec(recorded_argv[0]) if recorded_argv else ''
            process_exec = _resolve_live_exec(proc_argv[0]) if proc_argv else ''
            shebang_rewrite = False
            if recorded_exec and process_exec and recorded_exec != process_exec:
                # Shebang rewrite: kernel turns "./script.py" into "python3 ./script.py",
                # shifting argv by 1. Check if the recorded script appears as proc_argv[1].
                if len(proc_argv) > 1:
                    live_script = _resolve_live_exec(proc_argv[1])
                    if live_script and live_script == recorded_exec:
                        shebang_rewrite = True
                    else:
                        return False
                else:
                    return False
            if not recorded_exec:
                recorded_exec_name = _normalize_exec_name(recorded_argv[0]) if recorded_argv else ''
                process_exec_name = _normalize_exec_name(proc_argv[0]) if proc_argv else _normalize_exec_name(proc.name())
                if recorded_exec_name and process_exec_name and recorded_exec_name != process_exec_name:
                    return False

            # If the hook launches an interpreter + script, validate the script path too.
            # Skip when shebang rewrite was detected (argv already shifted by 1).
            if not shebang_rewrite and len(recorded_argv) > 1 and len(proc_argv) > 1:
                recorded_entrypoint = _normalize_recorded_arg(recorded_argv[1])
                process_entrypoint = _normalize_recorded_arg(proc_argv[1])
                if recorded_entrypoint and process_entrypoint and recorded_entrypoint != process_entrypoint:
                    return False

        return True
    except (ValueError, OSError):
        return False
    except Exception:
        # psutil exceptions: NoSuchProcess, AccessDenied, ZombieProcess
        return False


def write_pid_file_with_mtime(pid_file: Path, pid: int, start_time: float):
    """Write PID file and set mtime to process start time.

    The mtime serves as a "password" for later PID validation: if the PID gets
    reused by a different process, its create_time won't match the mtime.
    """
    pid_file.write_text(str(pid))
    try:
        os.utime(pid_file, (start_time, start_time))
    except OSError:
        pass  # mtime optional, validation degrades gracefully


def write_cmd_file(cmd_file: Path, cmd: list[str]):
    """Write shell command script for debugging and PID validation."""
    def escape(arg: str) -> str:
        return f'"{arg.replace(chr(34), chr(92)+chr(34))}"' if any(c in arg for c in ' "$') else arg

    script = '#!/bin/bash\n' + ' '.join(escape(arg) for arg in cmd) + '\n'
    cmd_file.write_text(script)
    try:
        cmd_file.chmod(0o755)
    except OSError:
        pass


def is_process_alive(pid: int) -> bool:
    """Check if a process exists (signal 0 probes without killing)."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


# ── Signal helpers ──────────────────────────────────────────────────────────

def _send_signal(pid: int, sig: int) -> bool:
    """Send a signal to a process, trying process group first.

    Tries ``os.killpg`` first because background hooks are started with
    ``start_new_session=True``, making the hook PID the process group leader.
    Killing the group ensures child processes (e.g. Chrome's renderer processes)
    are also signaled. Falls back to direct ``os.kill`` if the process isn't a
    group leader (e.g. foreground hooks that share the parent's session).

    Returns True if the signal was sent, False if the process doesn't exist.
    """
    try:
        try:
            os.killpg(pid, sig)
        except (OSError, ProcessLookupError):
            os.kill(pid, sig)
        return True
    except ProcessLookupError:
        return False


# ── Async graceful shutdown ─────────────────────────────────────────────────

async def graceful_kill_process(
    process: asyncio.subprocess.Process,
    *,
    grace_period: float = GRACEFUL_SHUTDOWN_TIMEOUT,
) -> None:
    """Graceful shutdown of a live asyncio subprocess: SIGTERM → wait → SIGKILL.

    Uses ``process.wait()`` for reliable exit detection (tied to asyncio's
    subprocess watcher). This is the right choice when we have the process
    handle — i.e., killing a subprocess from the same handler that spawned it
    (timeout path, exception path in ProcessService.on_ProcessEvent).

    Args:
        process: The asyncio subprocess to kill.
        grace_period: Seconds to wait after SIGTERM before escalating to SIGKILL.
            Default is 15s — Chrome needs up to 15s to flush its user_data_dir
            (cookies, local storage, session state). Shorter values risk profile
            corruption.
    """
    pid = process.pid
    if pid is None:
        return

    if not _send_signal(pid, signal.SIGTERM):
        return  # already dead

    try:
        await asyncio.wait_for(process.wait(), timeout=grace_period)
        return  # exited cleanly
    except asyncio.TimeoutError:
        pass

    # Process ignored SIGTERM — force kill
    _send_signal(pid, signal.SIGKILL)
    try:
        await asyncio.wait_for(process.wait(), timeout=2.0)
    except asyncio.TimeoutError:
        pass  # zombie — will be reaped when parent exits


async def graceful_kill_by_pid_file(
    pid_file: Path,
    cmd_file: Optional[Path] = None,
    *,
    grace_period: float = GRACEFUL_SHUTDOWN_TIMEOUT,
) -> bool:
    """Validate a PID file, then SIGTERM → wait → SIGKILL the process.

    Used for daemon cleanup when we don't have the process handle — e.g.,
    killing a background Chrome daemon from a cleanup handler that runs in a
    different event than the one that spawned it. The PID file + cmd file are
    validated via ``validate_pid_file()`` to avoid killing an unrelated process
    that reused the PID.

    Polls ``is_process_alive()`` instead of ``process.wait()`` because we don't
    have the asyncio subprocess handle. Polling interval starts at 0.2s and
    increases to 1.0s to avoid busy-waiting.

    Args:
        pid_file: Path to the PID file written by ``write_pid_file_with_mtime``.
        cmd_file: Optional path to the cmd.sh file for command-line validation.
        grace_period: Seconds to wait after SIGTERM before SIGKILL.

    Returns:
        True if the process was found and signaled, False if validation failed
        or the process was already dead.
    """
    if not validate_pid_file(pid_file, cmd_file):
        pid_file.unlink(missing_ok=True)
        return False

    try:
        pid = int(pid_file.read_text().strip())
    except (OSError, ValueError):
        return False

    if not _send_signal(pid, signal.SIGTERM):
        return False

    # Poll for exit with increasing intervals (0.2s → 0.5s → 1.0s)
    elapsed = 0.0
    for interval in _poll_intervals(grace_period):
        await asyncio.sleep(interval)
        elapsed += interval
        if not is_process_alive(pid):
            return True

    # Still alive after grace period — force kill
    _send_signal(pid, signal.SIGKILL)
    # Brief wait for SIGKILL to take effect
    await asyncio.sleep(0.5)
    return True


def _poll_intervals(total: float):
    """Yield sleep intervals that sum to approximately ``total`` seconds.

    Starts at 0.2s, ramps to 1.0s to balance responsiveness and CPU usage.
    """
    elapsed = 0.0
    interval = 0.2
    while elapsed < total:
        interval = min(interval, total - elapsed)
        yield interval
        elapsed += interval
        interval = min(interval * 1.5, 1.0)


# ── Sync fire-and-forget (deprecated) ──────────────────────────────────────

def safe_kill_process(pid_file: Path, cmd_file: Optional[Path] = None, signal_num: int = 15) -> bool:
    """Send a single signal after PID validation. No escalation.

    Deprecated: prefer ``graceful_kill_by_pid_file()`` which does SIGTERM → wait
    → SIGKILL escalation. This sync version is kept for callers that can't await.
    """
    if not validate_pid_file(pid_file, cmd_file):
        pid_file.unlink(missing_ok=True)
        return False

    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, signal_num)
        return True
    except (OSError, ValueError, ProcessLookupError):
        return False
