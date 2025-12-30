"""
Plugin execution engine for abx-dl.

Runs plugin hooks and collects their output.
"""

import json
import subprocess
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generator

from .config import build_env_for_plugin, get_env_bool
from .dependencies import LIB_DIR
from .plugins import Hook, Plugin


# Shared npm locations
NODE_MODULES_DIR = LIB_DIR / 'npm' / 'node_modules'
NPM_BIN_DIR = NODE_MODULES_DIR / '.bin'


@dataclass
class ArchiveResult:
    """Result from running a hook."""
    hook: Hook
    status: str  # 'succeeded', 'failed', 'skipped'
    output_path: str | None = None
    error: str | None = None
    output_files: list[str] = field(default_factory=list)
    jsonl_records: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class DownloadResult:
    """Result from downloading a URL."""
    url: str
    snapshot_id: str
    output_dir: Path
    hook_results: list[ArchiveResult] = field(default_factory=list)

    @property
    def succeeded(self) -> list[ArchiveResult]:
        return [r for r in self.hook_results if r.status == 'succeeded']

    @property
    def failed(self) -> list[ArchiveResult]:
        return [r for r in self.hook_results if r.status == 'failed']

    @property
    def skipped(self) -> list[ArchiveResult]:
        return [r for r in self.hook_results if r.status == 'skipped']


def get_interpreter(language: str) -> list[str]:
    """Get interpreter command for a hook language."""
    if language == 'py':
        return [sys.executable]
    elif language == 'js':
        return ['node']
    elif language == 'sh':
        return ['bash']
    return []


def run_hook(hook: Hook, url: str, snapshot_id: str, output_dir: Path,
             env: dict[str, str], timeout: int = 60) -> ArchiveResult:
    """Run a single hook."""
    # Get files before running
    files_before = set(output_dir.rglob('*'))

    # Build command
    interpreter = get_interpreter(hook.language)
    if not interpreter:
        return ArchiveResult(
            hook=hook,
            status='failed',
            error=f'Unknown language: {hook.language}',
        )

    # Set lib paths and add npm bin to PATH
    env['LIB_DIR'] = str(LIB_DIR)
    env['NODE_MODULES_DIR'] = str(NODE_MODULES_DIR)
    env['NPM_BIN_DIR'] = str(NPM_BIN_DIR)
    env['PATH'] = f"{NPM_BIN_DIR}:{env.get('PATH', '')}"

    cmd = [
        *interpreter,
        str(hook.path),
        f'--url={url}',
        f'--snapshot-id={snapshot_id}',
    ]

    try:
        result = subprocess.run(
            cmd,
            cwd=str(output_dir),
            env=env,
            capture_output=True,
            timeout=timeout,
        )

        # Parse JSONL output from stdout
        jsonl_records = []
        stdout = result.stdout.decode('utf-8', errors='replace')
        for line in stdout.strip().split('\n'):
            if line.strip():
                try:
                    record = json.loads(line)
                    jsonl_records.append(record)
                except json.JSONDecodeError:
                    pass

        # Detect new files
        files_after = set(output_dir.rglob('*'))
        new_files = [str(f.relative_to(output_dir)) for f in (files_after - files_before) if f.is_file()]

        # Determine status from JSONL or exit code
        status = 'failed'
        output_path = None
        error = None

        for record in jsonl_records:
            if record.get('type') == 'ArchiveResult':
                status = record.get('status', 'failed')
                output_path = record.get('output_str')
                break

        if not jsonl_records:
            # No JSONL output - use exit code
            if result.returncode == 0:
                status = 'succeeded' if new_files else 'skipped'
            else:
                status = 'failed'
                error = result.stderr.decode('utf-8', errors='replace')[:500]

        return ArchiveResult(
            hook=hook,
            status=status,
            output_path=output_path,
            error=error,
            output_files=new_files,
            jsonl_records=jsonl_records,
        )

    except subprocess.TimeoutExpired:
        return ArchiveResult(
            hook=hook,
            status='failed',
            error=f'Timed out after {timeout}s',
        )
    except Exception as e:
        return ArchiveResult(
            hook=hook,
            status='failed',
            error=f'{type(e).__name__}: {e}',
        )


def run_plugin(plugin: Plugin, url: str, snapshot_id: str, output_dir: Path,
               config_overrides: dict[str, Any] | None = None) -> list[ArchiveResult]:
    """Run all snapshot hooks for a plugin."""
    results = []

    # Check if plugin is enabled
    enabled_key = plugin.enabled_key
    if not get_env_bool(enabled_key, True):
        return results

    # Build environment for plugin
    env = build_env_for_plugin(plugin.name, plugin.config_schema, config_overrides)

    # Get timeout from config
    timeout_key = f"{plugin.name.upper()}_TIMEOUT"
    timeout = int(env.get(timeout_key, env.get('TIMEOUT', '60')))

    # Create plugin output directory
    plugin_output_dir = output_dir / plugin.name
    plugin_output_dir.mkdir(parents=True, exist_ok=True)

    # Run each snapshot hook
    for hook in plugin.get_snapshot_hooks():
        result = run_hook(hook, url, snapshot_id, plugin_output_dir, env, timeout)
        results.append(result)

    return results


def download(url: str, plugins: dict[str, Plugin], output_dir: Path | None = None,
             selected_plugins: list[str] | None = None,
             config_overrides: dict[str, Any] | None = None) -> DownloadResult:
    """
    Download a URL using all enabled plugins.

    Args:
        url: URL to download
        plugins: Dict of all available plugins
        output_dir: Output directory (default: current directory)
        selected_plugins: List of plugin names to run (default: all)
        config_overrides: Config overrides to apply

    Returns:
        DownloadResult with all hook results
    """
    snapshot_id = str(uuid.uuid4())
    output_dir = output_dir or Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)

    result = DownloadResult(
        url=url,
        snapshot_id=snapshot_id,
        output_dir=output_dir,
    )

    # Filter plugins if specified
    if selected_plugins:
        selected_lower = [p.lower() for p in selected_plugins]
        plugins_to_run = {
            name: plugin
            for name, plugin in plugins.items()
            if name.lower() in selected_lower
        }
    else:
        plugins_to_run = plugins

    # Get all hooks sorted by execution order
    all_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in plugins_to_run.values():
        for hook in plugin.get_snapshot_hooks():
            all_hooks.append((plugin, hook))

    # Sort by step and priority
    all_hooks.sort(key=lambda x: x[1].sort_key)

    # Group by step for parallel execution within steps
    current_step = -1
    step_hooks: list[tuple[Plugin, Hook]] = []

    for plugin, hook in all_hooks:
        if hook.step != current_step:
            # Run previous step's hooks
            for p, h in step_hooks:
                env = build_env_for_plugin(p.name, p.config_schema, config_overrides)
                timeout_key = f"{p.name.upper()}_TIMEOUT"
                timeout = int(env.get(timeout_key, env.get('TIMEOUT', '60')))

                plugin_output_dir = output_dir / p.name
                plugin_output_dir.mkdir(parents=True, exist_ok=True)

                hook_result = run_hook(h, url, snapshot_id, plugin_output_dir, env, timeout)
                result.hook_results.append(hook_result)

            step_hooks = []
            current_step = hook.step

        step_hooks.append((plugin, hook))

    # Run final step
    for p, h in step_hooks:
        env = build_env_for_plugin(p.name, p.config_schema, config_overrides)
        timeout_key = f"{p.name.upper()}_TIMEOUT"
        timeout = int(env.get(timeout_key, env.get('TIMEOUT', '60')))

        plugin_output_dir = output_dir / p.name
        plugin_output_dir.mkdir(parents=True, exist_ok=True)

        hook_result = run_hook(h, url, snapshot_id, plugin_output_dir, env, timeout)
        result.hook_results.append(hook_result)

    # Write index.json
    write_index(result)

    return result


def extract_config_updates(jsonl_records: list[dict[str, Any]]) -> dict[str, str]:
    """Extract config updates from Machine JSONL records."""
    updates = {}
    for record in jsonl_records:
        if record.get('type') == 'Machine' and record.get('_method') == 'update':
            key = record.get('key', '')
            value = record.get('value', '')
            # Convert config/CHROME_BINARY -> CHROME_BINARY
            if key.startswith('config/'):
                key = key[7:]
            if key and value:
                updates[key] = str(value)
        elif record.get('type') == 'Binary':
            # Also extract binary paths
            name = record.get('name', '')
            abspath = record.get('abspath', '')
            if name and abspath:
                updates[f'{name.upper()}_BINARY'] = abspath
    return updates


def download_live(url: str, plugins: dict[str, Plugin], output_dir: Path | None = None,
                  selected_plugins: list[str] | None = None,
                  config_overrides: dict[str, Any] | None = None) -> tuple[int, Generator[ArchiveResult, None, None]]:
    """
    Download a URL using all enabled plugins, yielding results as they complete.

    Returns:
        Tuple of (total_hooks_count, generator of HookResults)
    """
    snapshot_id = str(uuid.uuid4())
    output_dir = output_dir or Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Filter plugins if specified
    if selected_plugins:
        selected_lower = [p.lower() for p in selected_plugins]
        plugins_to_run = {
            name: plugin
            for name, plugin in plugins.items()
            if name.lower() in selected_lower
        }
    else:
        plugins_to_run = plugins

    # Get all hooks sorted by execution order
    all_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in plugins_to_run.values():
        for hook in plugin.get_snapshot_hooks():
            all_hooks.append((plugin, hook))

    all_hooks.sort(key=lambda x: x[1].sort_key)

    # Also get crawl hooks (run once for setup/install)
    crawl_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in plugins_to_run.values():
        for hook in plugin.get_crawl_hooks():
            crawl_hooks.append((plugin, hook))
    crawl_hooks.sort(key=lambda x: x[1].sort_key)

    total_hooks = len(crawl_hooks) + len(all_hooks)

    def run_hooks() -> Generator[ArchiveResult, None, None]:
        hook_results = []
        # Shared config that accumulates updates from hooks
        shared_config = dict(config_overrides) if config_overrides else {}

        # Run crawl hooks first (setup/install)
        for plugin, hook in crawl_hooks:
            env = build_env_for_plugin(plugin.name, plugin.config_schema, shared_config)
            timeout_key = f"{plugin.name.upper()}_TIMEOUT"
            timeout = int(env.get(timeout_key, env.get('TIMEOUT', '120')))

            plugin_output_dir = output_dir / plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)

            hook_result = run_hook(hook, url, snapshot_id, plugin_output_dir, env, timeout)
            hook_results.append(hook_result)

            # Extract and propagate config updates from this hook
            config_updates = extract_config_updates(hook_result.jsonl_records)
            shared_config.update(config_updates)

            yield hook_result

        # Run snapshot hooks
        for plugin, hook in all_hooks:
            env = build_env_for_plugin(plugin.name, plugin.config_schema, shared_config)
            timeout_key = f"{plugin.name.upper()}_TIMEOUT"
            timeout = int(env.get(timeout_key, env.get('TIMEOUT', '60')))

            plugin_output_dir = output_dir / plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)

            hook_result = run_hook(hook, url, snapshot_id, plugin_output_dir, env, timeout)
            hook_results.append(hook_result)

            # Extract and propagate config updates from this hook
            config_updates = extract_config_updates(hook_result.jsonl_records)
            shared_config.update(config_updates)

            yield hook_result

        # Write index.json at the end
        result = DownloadResult(url=url, snapshot_id=snapshot_id, output_dir=output_dir, hook_results=hook_results)
        write_index(result)

    return total_hooks, run_hooks()


def write_index(result: DownloadResult):
    """Write index.json with download metadata."""
    index_path = result.output_dir / 'index.json'

    index_data = {
        'url': result.url,
        'snapshot_id': result.snapshot_id,
        'plugins': {},
    }

    for hook_result in result.hook_results:
        plugin_name = hook_result.hook.plugin_name
        if plugin_name not in index_data['plugins']:
            index_data['plugins'][plugin_name] = {
                'status': 'pending',
                'hooks': {},
            }

        index_data['plugins'][plugin_name]['hooks'][hook_result.hook.name] = {
            'status': hook_result.status,
            'output': hook_result.output_path,
            'files': hook_result.output_files,
        }

        # Update plugin status
        if hook_result.status == 'succeeded':
            index_data['plugins'][plugin_name]['status'] = 'succeeded'
        elif hook_result.status == 'failed' and index_data['plugins'][plugin_name]['status'] != 'succeeded':
            index_data['plugins'][plugin_name]['status'] = 'failed'

    index_path.write_text(json.dumps(index_data, indent=2))
