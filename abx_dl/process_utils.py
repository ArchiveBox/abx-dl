"""
Process validation using psutil and filesystem mtime.

Uses mtime as a "password": PID files are timestamped with process start time.
Since filesystem mtimes can be set arbitrarily but process start times cannot,
comparing them detects PID reuse.

# ============================================================================
# COPIED FROM: ArchiveBox/archivebox/misc/process_utils.py
# COMMIT: 69965a27820507526767208c179c62f4a579555c
# DATE: 2024-12-30
# MODIFICATIONS: Minimal - removed __package__ declaration
# ============================================================================
"""

import os
import shlex
import shutil
import time
from pathlib import Path
from typing import Optional

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


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
    """Validate PID using mtime and optional cmd.sh. Returns True if process is ours."""
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
            if recorded_exec and process_exec and recorded_exec != process_exec:
                return False
            if not recorded_exec:
                recorded_exec_name = _normalize_exec_name(recorded_argv[0]) if recorded_argv else ''
                process_exec_name = _normalize_exec_name(proc_argv[0]) if proc_argv else _normalize_exec_name(proc.name())
                if recorded_exec_name and process_exec_name and recorded_exec_name != process_exec_name:
                    return False

            # If the hook launches an interpreter + script, validate the script path too.
            if len(recorded_argv) > 1 and len(proc_argv) > 1:
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
    """Write PID file and set mtime to process start time."""
    pid_file.write_text(str(pid))
    try:
        os.utime(pid_file, (start_time, start_time))
    except OSError:
        pass  # mtime optional, validation degrades gracefully


def write_cmd_file(cmd_file: Path, cmd: list[str]):
    """Write shell command script."""
    def escape(arg: str) -> str:
        return f'"{arg.replace(chr(34), chr(92)+chr(34))}"' if any(c in arg for c in ' "$') else arg

    script = '#!/bin/bash\n' + ' '.join(escape(arg) for arg in cmd) + '\n'
    cmd_file.write_text(script)
    try:
        cmd_file.chmod(0o755)
    except OSError:
        pass


def safe_kill_process(pid_file: Path, cmd_file: Optional[Path] = None, signal_num: int = 15) -> bool:
    """Kill process after validation. Returns True if killed."""
    if not validate_pid_file(pid_file, cmd_file):
        pid_file.unlink(missing_ok=True)  # Clean stale file
        return False

    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, signal_num)
        return True
    except (OSError, ValueError, ProcessLookupError):
        return False


def is_process_alive(pid: int) -> bool:
    """Check if a process exists."""
    try:
        os.kill(pid, 0)  # Signal 0 checks existence without killing
        return True
    except (OSError, ProcessLookupError):
        return False
