"""
Plugin execution engine for abx-dl.

Hook execution and process management logic adapted from ArchiveBox.
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Generator

from .config import build_env_for_plugin, LIB_DIR, NPM_BIN_DIR, NODE_MODULES_DIR
from .dependencies import load_binary, install_binary
from .models import Snapshot, Process, ArchiveResult, write_jsonl, now_iso
from .plugins import Hook, Plugin
from .process_utils import (
    validate_pid_file,
    write_pid_file_with_mtime,
    write_cmd_file,
    is_process_alive,
)


def get_interpreter(language: str) -> list[str]:
    """Get interpreter command for a hook language."""
    return {'py': [sys.executable], 'js': ['node'], 'sh': ['bash']}.get(language, [])


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
    start_time = time.time()

    interpreter = get_interpreter(hook.language)
    if not interpreter:
        proc = Process(cmd=[], exit_code=1, stderr=f'Unknown language: {hook.language}')
        result = ArchiveResult(snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name, status='failed', error=proc.stderr)
        return proc, result, False

    # Set lib paths
    env.update({
        'LIB_DIR': str(LIB_DIR),
        'NODE_MODULES_DIR': str(NODE_MODULES_DIR),
        'NPM_BIN_DIR': str(NPM_BIN_DIR),
        'PATH': f"{NPM_BIN_DIR}:{env.get('PATH', '')}",
    })

    cmd = [*interpreter, str(hook.path), f'--url={url}', f'--snapshot-id={snapshot_id}']
    proc = Process(cmd=cmd, pwd=str(output_dir), timeout=timeout, started_at=now_iso())

    # Detect if this is a background hook (long-running daemon)
    # Convention: .bg. suffix (e.g., on_Snapshot__21_consolelog.bg.js)
    is_background = hook.is_background

    # Set up output files for ALL hooks (matches ArchiveBox structure)
    stdout_file = output_dir / 'stdout.log'
    stderr_file = output_dir / 'stderr.log'
    pid_file = output_dir / 'hook.pid'
    cmd_file = output_dir / 'cmd.sh'

    try:
        # Write command script for validation/debugging
        write_cmd_file(cmd_file, cmd)

        # Capture files before execution to detect new output
        files_before = set(output_dir.rglob('*')) if output_dir.exists() else set()

        # Open log files for writing (like ArchiveBox)
        with open(stdout_file, 'w') as out, open(stderr_file, 'w') as err:
            process = subprocess.Popen(
                cmd,
                cwd=str(output_dir),
                stdout=out,
                stderr=err,
                env=env,
            )

            # Write PID with mtime set to process start time for validation
            process_start_time = time.time()
            write_pid_file_with_mtime(pid_file, process.pid, process_start_time)

            if is_background:
                # Background hook - return immediately, don't wait
                # Process continues running, writing to stdout.log
                # Cleanup will poll for completion later via PID file
                ar = ArchiveResult(
                    snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name,
                    status='started', process_id=proc.id, start_ts=proc.started_at,
                )
                return proc, ar, True

            # Normal hook - wait for completion with timeout
            try:
                returncode = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()  # Clean up zombie
                proc.exit_code = -1
                proc.stderr = f'Hook timed out after {timeout} seconds'
                proc.ended_at = now_iso()
                ar = ArchiveResult(
                    snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name,
                    status='failed', process_id=proc.id, start_ts=proc.started_at,
                    end_ts=proc.ended_at, error=proc.stderr,
                )
                return proc, ar, False

        # Read output from files (after closing them)
        stdout = stdout_file.read_text() if stdout_file.exists() else ''
        stderr = stderr_file.read_text() if stderr_file.exists() else ''

        proc.exit_code = returncode
        proc.stdout = stdout
        proc.stderr = stderr
        proc.ended_at = now_iso()

        # Detect new files created by the hook
        files_after = set(output_dir.rglob('*')) if output_dir.exists() else set()
        new_files = [str(f.relative_to(output_dir)) for f in (files_after - files_before) if f.is_file()]
        # Exclude the log files themselves from new_files
        new_files = [f for f in new_files if f not in ('stdout.log', 'stderr.log', 'hook.pid', 'cmd.sh')]

        # Parse JSONL output from stdout
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

        if not new_files and status == 'succeeded':
            status = 'skipped'

        # Clean up log files on success (keep on failure for debugging)
        if returncode == 0:
            stdout_file.unlink(missing_ok=True)
            stderr_file.unlink(missing_ok=True)
            pid_file.unlink(missing_ok=True)
            # Keep cmd.sh for reference

        ar = ArchiveResult(
            snapshot_id=snapshot_id,
            plugin=hook.plugin_name,
            hook_name=hook.name,
            status=status,
            process_id=proc.id,
            output_str=output_str,
            output_files=new_files,
            start_ts=proc.started_at,
            end_ts=proc.ended_at,
            error=stderr[:500] if returncode != 0 else None,
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
def cleanup_background_hooks(output_dir: Path, index_path: Path, is_tty: bool):
    """
    Clean up background hooks by scanning for all .pid files.

    Sends SIGTERM, waits, then SIGKILL if needed.
    Uses process group killing to handle Chrome and its children.
    Handles unkillable processes gracefully.
    """
    if not output_dir.exists():
        return

    # Find all .pid files in the output directory
    pid_files = list(output_dir.glob('**/*.pid'))
    if not pid_files:
        return

    for pid_file in pid_files:
        # Validate PID before killing to avoid killing unrelated processes
        cmd_file = pid_file.parent / 'cmd.sh'
        if not validate_pid_file(pid_file, cmd_file):
            # PID reused by different process or process dead
            pid_file.unlink(missing_ok=True)
            continue

        try:
            pid = int(pid_file.read_text().strip())

            # Step 1: Send SIGTERM for graceful shutdown
            try:
                # Try to kill process group first (handles detached processes like Chrome)
                try:
                    os.killpg(pid, signal.SIGTERM)
                except (OSError, ProcessLookupError):
                    # Fall back to killing just the process
                    os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                # Already dead
                pid_file.unlink(missing_ok=True)
                continue

            # Step 2: Wait for graceful shutdown
            time.sleep(2)

            # Step 3: Check if still alive
            if not is_process_alive(pid):
                # Process terminated gracefully
                pid_file.unlink(missing_ok=True)
                _finalize_background_hook(pid_file.parent, index_path, is_tty, success=True)
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
                _finalize_background_hook(pid_file.parent, index_path, is_tty, success=True)
                continue

            # Step 5: Wait and verify death
            time.sleep(1)

            if is_process_alive(pid):
                # Process is unkillable (likely in UNE state on macOS)
                # This happens when Chrome crashes in kernel syscall (IOSurface)
                # Log but don't block cleanup - process will remain until reboot
                if is_tty:
                    print(f'Warning: Process {pid} is unkillable (likely crashed in kernel). Will remain until reboot.', file=sys.stderr)
                _finalize_background_hook(pid_file.parent, index_path, is_tty, success=False, error='Process unkillable')
            else:
                # Successfully killed
                pid_file.unlink(missing_ok=True)
                _finalize_background_hook(pid_file.parent, index_path, is_tty, success=True)

        except (ValueError, OSError):
            # Invalid PID file or permission error
            pass


def _finalize_background_hook(plugin_dir: Path, index_path: Path, is_tty: bool, success: bool, error: str | None = None):
    """
    Read output from a background hook's log files and write final results to index.jsonl.
    """
    stdout_file = plugin_dir / 'stdout.log'
    stderr_file = plugin_dir / 'stderr.log'
    cmd_file = plugin_dir / 'cmd.sh'

    # Read output
    stdout = stdout_file.read_text() if stdout_file.exists() else ''
    stderr = stderr_file.read_text() if stderr_file.exists() else ''

    # Detect new files
    new_files = [
        str(f.relative_to(plugin_dir)) for f in plugin_dir.rglob('*')
        if f.is_file() and f.name not in ('stdout.log', 'stderr.log', 'hook.pid', 'cmd.sh')
    ]

    # Parse JSONL output for final status
    status = 'succeeded' if success else 'failed'
    output_str = ''
    snapshot_id = ''
    hook_name = ''
    plugin_name = plugin_dir.name

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

    # Create final Process and ArchiveResult
    proc = Process(
        cmd=[],  # cmd.sh has the full command
        pwd=str(plugin_dir),
        exit_code=0 if success else -1,
        stdout=stdout,
        stderr=stderr,
        ended_at=now_iso(),
    )

    ar = ArchiveResult(
        snapshot_id=snapshot_id,
        plugin=plugin_name,
        hook_name=hook_name,
        status=status,
        process_id=proc.id,
        output_str=output_str,
        output_files=new_files,
        end_ts=proc.ended_at,
        error=error or (stderr[:500] if not success else None),
    )

    # Write final results
    write_jsonl(index_path, proc, also_print=not is_tty)
    write_jsonl(index_path, ar, also_print=not is_tty)

    # Clean up log files on success
    if success:
        stdout_file.unlink(missing_ok=True)
        stderr_file.unlink(missing_ok=True)


def check_plugin_dependencies(plugin: Plugin, auto_install: bool = True) -> tuple[bool, list[str]]:
    """
    Check if a plugin's dependencies are available.
    If auto_install=True, attempt to install missing dependencies.
    Returns (all_available, list_of_missing_binary_names).

    NOTE: Plugins with Crawl hooks are assumed to self-install their dependencies
    via those hooks, so we skip pre-checking them here.
    """
    # Plugins with Crawl hooks handle their own dependency installation
    # (e.g., chrome plugin's on_Crawl__00_install_puppeteer_chromium.py)
    if plugin.get_crawl_hooks():
        return True, []

    missing = []
    for spec in plugin.binaries:
        binary = load_binary(spec)
        if not binary.is_valid:
            if auto_install:
                binary = install_binary(spec)
            if not binary.is_valid:
                missing.append(spec.get('name', '?'))
    return len(missing) == 0, missing


def download(url: str, plugins: dict[str, Plugin], output_dir: Path, selected_plugins: list[str] | None = None, config_overrides: dict[str, Any] | None = None, auto_install: bool = True) -> Generator[ArchiveResult, None, Snapshot]:
    """
    Download a URL using plugins. Yields ArchiveResults as they complete.
    Writes all output to index.jsonl.

    If auto_install=True (default), missing plugin dependencies are lazily installed.
    If auto_install=False, plugins with missing dependencies are skipped with a warning.
    """
    output_dir = output_dir or Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / 'index.jsonl'
    is_tty = sys.stdout.isatty()

    # Create snapshot
    snapshot = Snapshot(url=url)
    write_jsonl(index_path, snapshot, also_print=not is_tty)

    # Filter plugins
    if selected_plugins:
        selected_lower = [p.lower() for p in selected_plugins]
        plugins = {n: p for n, p in plugins.items() if n.lower() in selected_lower}

    # Check/install dependencies and filter unavailable plugins
    available_plugins: dict[str, Plugin] = {}
    skipped_plugins: list[tuple[str, list[str]]] = []

    for name, plugin in plugins.items():
        if plugin.binaries:
            deps_ok, missing = check_plugin_dependencies(plugin, auto_install=auto_install)
            if deps_ok:
                available_plugins[name] = plugin
            else:
                skipped_plugins.append((name, missing))
        else:
            available_plugins[name] = plugin

    # Warn about skipped plugins
    if skipped_plugins and is_tty:
        for plugin_name, missing in skipped_plugins:
            print(f"Warning: Skipping plugin '{plugin_name}' - missing dependencies: {', '.join(missing)}", file=sys.stderr)
        if not auto_install:
            print("Hint: Run without --no-install to auto-install dependencies, or run 'abx-dl plugins --install'", file=sys.stderr)

    # Collect hooks: Crawl hooks first (setup), then Snapshot hooks (extraction)
    crawl_hooks: list[tuple[Plugin, Hook]] = []
    snapshot_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in available_plugins.values():
        for hook in plugin.get_crawl_hooks():
            crawl_hooks.append((plugin, hook))
        for hook in plugin.get_snapshot_hooks():
            snapshot_hooks.append((plugin, hook))
    crawl_hooks.sort(key=lambda x: x[1].sort_key)
    snapshot_hooks.sort(key=lambda x: x[1].sort_key)
    all_hooks = crawl_hooks + snapshot_hooks

    shared_config = dict(config_overrides) if config_overrides else {}

    try:
        for plugin, hook in all_hooks:
            env = build_env_for_plugin(plugin.name, plugin.config_schema, shared_config)
            timeout = int(env.get(f"{plugin.name.upper()}_TIMEOUT", env.get('TIMEOUT', '60')))

            # Executor creates plugin subdir, hooks write to cwd directly
            plugin_output_dir = output_dir / plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)
            proc, ar, is_background = run_hook(hook, url, snapshot.id, plugin_output_dir, env, timeout)

            if is_background:
                # Background hook - started, will be cleaned up later via PID file
                # Yield initial "started" result
                yield ar
            else:
                # Foreground hook - write results immediately
                write_jsonl(index_path, proc, also_print=not is_tty)
                write_jsonl(index_path, ar, also_print=not is_tty)

                # Extract config updates from stdout
                for line in proc.stdout.split('\n'):
                    if line.strip():
                        try:
                            record = json.loads(line)
                            if record.get('type') == 'Binary':
                                name = record.get('name', '')
                                abspath = record.get('abspath', '')
                                if name and abspath:
                                    shared_config[f'{name.upper()}_BINARY'] = abspath
                            elif record.get('type') == 'Machine' and record.get('_method') == 'update':
                                key = record.get('key', '').replace('config/', '')
                                value = record.get('value', '')
                                if key and value:
                                    shared_config[key] = value
                        except json.JSONDecodeError:
                            pass

                yield ar

    finally:
        # Cleanup background hooks - scan for PID files, send SIGTERM, collect output
        cleanup_background_hooks(output_dir, index_path, is_tty)

    return snapshot
