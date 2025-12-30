"""
Plugin execution engine for abx-dl.
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


def get_interpreter(language: str) -> list[str]:
    """Get interpreter command for a hook language."""
    return {'py': [sys.executable], 'js': ['node'], 'sh': ['bash']}.get(language, [])


def run_hook(hook: Hook, url: str, snapshot_id: str, output_dir: Path, env: dict[str, str], timeout: int = 60) -> tuple[Process, ArchiveResult, subprocess.Popen | None]:
    """
    Run a single hook and return Process, ArchiveResult, and optionally Popen handle.

    For background hooks, returns the Popen object so caller can manage cleanup.
    For foreground hooks, returns None for the Popen.
    """
    files_before = set(output_dir.rglob('*'))

    interpreter = get_interpreter(hook.language)
    if not interpreter:
        proc = Process(cmd=[], exit_code=1, stderr=f'Unknown language: {hook.language}')
        result = ArchiveResult(snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name, status='failed', error=proc.stderr)
        return proc, result, None

    # Set lib paths
    env.update({
        'LIB_DIR': str(LIB_DIR),
        'NODE_MODULES_DIR': str(NODE_MODULES_DIR),
        'NPM_BIN_DIR': str(NPM_BIN_DIR),
        'PATH': f"{NPM_BIN_DIR}:{env.get('PATH', '')}",
    })

    cmd = [*interpreter, str(hook.path), f'--url={url}', f'--snapshot-id={snapshot_id}']
    proc = Process(cmd=cmd, pwd=str(output_dir), timeout=timeout, started_at=now_iso())

    try:
        if hook.is_background:
            # Background hook - start and don't wait, return Popen handle
            popen = subprocess.Popen(cmd, cwd=str(output_dir), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # Give it a moment to start and potentially fail fast
            time.sleep(0.3)
            if popen.poll() is not None:
                # Process already exited (fast failure)
                stdout, stderr = popen.communicate()
                proc.exit_code = popen.returncode
                proc.stdout = stdout.decode('utf-8', errors='replace')
                proc.stderr = stderr.decode('utf-8', errors='replace')
                proc.ended_at = now_iso()
                status = 'failed' if popen.returncode != 0 else 'succeeded'
                ar = ArchiveResult(
                    snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name,
                    status=status, process_id=proc.id, start_ts=proc.started_at, end_ts=proc.ended_at,
                    error=proc.stderr[:500] if proc.exit_code != 0 else None,
                )
                return proc, ar, None  # No popen to track since it already exited
            else:
                # Still running - return handle for later cleanup
                ar = ArchiveResult(
                    snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name,
                    status='started', process_id=proc.id, start_ts=proc.started_at,
                )
                return proc, ar, popen

        # Foreground hook - wait for completion
        result = subprocess.run(cmd, cwd=str(output_dir), env=env, capture_output=True, timeout=timeout)
        proc.exit_code = result.returncode
        proc.stdout = result.stdout.decode('utf-8', errors='replace')
        proc.stderr = result.stderr.decode('utf-8', errors='replace')
        proc.ended_at = now_iso()

        # Detect new files
        files_after = set(output_dir.rglob('*'))
        new_files = [str(f.relative_to(output_dir)) for f in (files_after - files_before) if f.is_file()]

        # Parse JSONL output
        status = 'succeeded' if result.returncode == 0 else 'failed'
        output_str = ''
        for line in proc.stdout.strip().split('\n'):
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
            error=proc.stderr[:500] if result.returncode != 0 else None,
        )
        return proc, ar, None

    except subprocess.TimeoutExpired:
        proc.exit_code = -1
        proc.stderr = f'Timed out after {timeout}s'
        proc.ended_at = now_iso()
        ar = ArchiveResult(snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name, status='failed', process_id=proc.id, error=proc.stderr)
        return proc, ar, None
    except Exception as e:
        proc.exit_code = -1
        proc.stderr = f'{type(e).__name__}: {e}'
        proc.ended_at = now_iso()
        ar = ArchiveResult(snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name, status='failed', process_id=proc.id, error=proc.stderr)
        return proc, ar, None


def cleanup_background_hooks(bg_hooks: list[tuple[subprocess.Popen, Process, ArchiveResult, Path]], index_path: Path, is_tty: bool):
    """
    Send SIGTERM to all background hooks, wait for them to finish, and collect output.
    """
    for popen, proc, ar, output_dir in bg_hooks:
        if popen.poll() is None:
            # Still running - send SIGTERM
            try:
                popen.send_signal(signal.SIGTERM)
            except (ProcessLookupError, OSError):
                pass

    # Wait for all to finish (with timeout)
    for popen, proc, ar, output_dir in bg_hooks:
        try:
            stdout, stderr = popen.communicate(timeout=10)
            proc.exit_code = popen.returncode
            proc.stdout = stdout.decode('utf-8', errors='replace')
            proc.stderr = stderr.decode('utf-8', errors='replace')
            proc.ended_at = now_iso()

            # Detect new files
            files_after = set(output_dir.rglob('*'))
            new_files = [str(f.relative_to(output_dir)) for f in files_after if f.is_file()]

            # Update ArchiveResult
            ar.end_ts = proc.ended_at
            ar.output_files = new_files

            # Parse JSONL output for final status
            status = 'succeeded' if popen.returncode == 0 else 'failed'
            for line in proc.stdout.strip().split('\n'):
                if line.strip():
                    try:
                        record = json.loads(line)
                        if record.get('type') == 'ArchiveResult':
                            status = record.get('status', status)
                            ar.output_str = record.get('output_str', '')
                    except json.JSONDecodeError:
                        pass
            ar.status = status
            if popen.returncode != 0:
                ar.error = proc.stderr[:500]

        except subprocess.TimeoutExpired:
            popen.kill()
            popen.wait()
            proc.exit_code = -1
            proc.stderr = 'Background hook did not exit after SIGTERM'
            proc.ended_at = now_iso()
            ar.status = 'failed'
            ar.error = proc.stderr
            ar.end_ts = proc.ended_at
        except Exception as e:
            proc.exit_code = -1
            proc.stderr = f'{type(e).__name__}: {e}'
            proc.ended_at = now_iso()
            ar.status = 'failed'
            ar.error = proc.stderr
            ar.end_ts = proc.ended_at

        # Write final results
        write_jsonl(index_path, proc, also_print=not is_tty)
        write_jsonl(index_path, ar, also_print=not is_tty)


def check_plugin_dependencies(plugin: Plugin, auto_install: bool = True) -> tuple[bool, list[str]]:
    """
    Check if a plugin's dependencies are available.
    If auto_install=True, attempt to install missing dependencies.
    Returns (all_available, list_of_missing_binary_names).
    """
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

    # Track background hooks for cleanup
    background_hooks: list[tuple[subprocess.Popen, Process, ArchiveResult, Path]] = []
    shared_config = dict(config_overrides) if config_overrides else {}

    try:
        for plugin, hook in all_hooks:
            env = build_env_for_plugin(plugin.name, plugin.config_schema, shared_config)
            timeout = int(env.get(f"{plugin.name.upper()}_TIMEOUT", env.get('TIMEOUT', '60')))

            # Executor creates plugin subdir, hooks write to cwd directly
            plugin_output_dir = output_dir / plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)
            proc, ar, popen = run_hook(hook, url, snapshot.id, plugin_output_dir, env, timeout)

            if popen:
                # Background hook - track for later cleanup
                background_hooks.append((popen, proc, ar, plugin_output_dir))
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
        # Cleanup background hooks - send SIGTERM, collect output
        if background_hooks:
            cleanup_background_hooks(background_hooks, index_path, is_tty)

    return snapshot
