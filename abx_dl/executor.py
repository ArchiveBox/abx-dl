"""
Plugin execution engine for abx-dl.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Generator

from .config import build_env_for_plugin, LIB_DIR, NPM_BIN_DIR, NODE_MODULES_DIR
from .models import Snapshot, Process, ArchiveResult, write_jsonl, now_iso
from .plugins import Hook, Plugin


def get_interpreter(language: str) -> list[str]:
    """Get interpreter command for a hook language."""
    return {'py': [sys.executable], 'js': ['node'], 'sh': ['bash']}.get(language, [])


def run_hook(hook: Hook, url: str, snapshot_id: str, output_dir: Path, env: dict[str, str], timeout: int = 60) -> tuple[Process, ArchiveResult]:
    """Run a single hook and return Process and ArchiveResult."""
    files_before = set(output_dir.rglob('*'))

    interpreter = get_interpreter(hook.language)
    if not interpreter:
        proc = Process(cmd=[], exit_code=1, stderr=f'Unknown language: {hook.language}')
        result = ArchiveResult(snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name, status='failed', error=proc.stderr)
        return proc, result

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
        return proc, ar

    except subprocess.TimeoutExpired:
        proc.exit_code = -1
        proc.stderr = f'Timed out after {timeout}s'
        proc.ended_at = now_iso()
        ar = ArchiveResult(snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name, status='failed', process_id=proc.id, error=proc.stderr)
        return proc, ar
    except Exception as e:
        proc.exit_code = -1
        proc.stderr = f'{type(e).__name__}: {e}'
        proc.ended_at = now_iso()
        ar = ArchiveResult(snapshot_id=snapshot_id, plugin=hook.plugin_name, hook_name=hook.name, status='failed', process_id=proc.id, error=proc.stderr)
        return proc, ar


def download(url: str, plugins: dict[str, Plugin], output_dir: Path, selected_plugins: list[str] | None = None, config_overrides: dict[str, Any] | None = None) -> Generator[ArchiveResult, None, Snapshot]:
    """
    Download a URL using plugins. Yields ArchiveResults as they complete.
    Writes all output to index.jsonl.
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

    # Collect all hooks sorted by execution order
    all_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in plugins.values():
        for hook in plugin.get_crawl_hooks() + plugin.get_snapshot_hooks():
            all_hooks.append((plugin, hook))
    all_hooks.sort(key=lambda x: x[1].sort_key)

    # Run hooks
    shared_config = dict(config_overrides) if config_overrides else {}

    for plugin, hook in all_hooks:
        env = build_env_for_plugin(plugin.name, plugin.config_schema, shared_config)
        timeout = int(env.get(f"{plugin.name.upper()}_TIMEOUT", env.get('TIMEOUT', '60')))

        plugin_output_dir = output_dir / plugin.name
        plugin_output_dir.mkdir(parents=True, exist_ok=True)

        proc, ar = run_hook(hook, url, snapshot.id, plugin_output_dir, env, timeout)

        # Write to index.jsonl
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
                except json.JSONDecodeError:
                    pass

        yield ar

    return snapshot
