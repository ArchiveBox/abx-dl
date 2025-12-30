"""
Plugin discovery and management for abx-dl.

Discovers plugins from the plugins directory and provides access to their hooks.
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# Plugins directory (symlinked to archivebox/plugins)
PLUGINS_DIR = Path(__file__).parent / 'plugins'


@dataclass
class Hook:
    """Represents a plugin hook."""
    name: str
    plugin_name: str
    path: Path
    step: int  # Execution step (0-9)
    priority: int  # Priority within step (0-9)
    is_background: bool
    language: str  # 'py', 'js', 'sh'

    @property
    def full_name(self) -> str:
        return f"{self.plugin_name}/{self.name}"

    @property
    def sort_key(self) -> tuple[int, int, str]:
        return (self.step, self.priority, self.name)


@dataclass
class Plugin:
    """Represents a plugin with its config and hooks."""
    name: str
    path: Path
    config_schema: dict[str, Any] = field(default_factory=dict)
    binaries: list[dict[str, Any]] = field(default_factory=list)
    hooks: list[Hook] = field(default_factory=list)

    @property
    def enabled_key(self) -> str:
        """Config key for enabling/disabling this plugin."""
        return f"{self.name.upper()}_ENABLED"

    def get_snapshot_hooks(self) -> list[Hook]:
        """Get hooks that run on snapshots."""
        return sorted(
            [h for h in self.hooks if 'Snapshot' in h.name],
            key=lambda h: h.sort_key
        )

    def get_crawl_hooks(self) -> list[Hook]:
        """Get hooks that run once per crawl (setup/install)."""
        return sorted(
            [h for h in self.hooks if 'Crawl' in h.name],
            key=lambda h: h.sort_key
        )


def parse_hook_filename(filename: str) -> tuple[str, int, int, bool, str] | None:
    """
    Parse hook filename to extract metadata.

    Format: on_{Event}__{XX}_{description}[.bg].{ext}

    Returns: (event, step, priority, is_background, language) or None
    """
    pattern = r'^on_(\w+)__(\d)(\d)_(\w+)(\.bg)?\.(\w+)$'
    match = re.match(pattern, filename)
    if not match:
        return None

    event = match.group(1)
    step = int(match.group(2))
    priority = int(match.group(3))
    is_background = match.group(5) is not None
    language = match.group(6)

    if language not in ('py', 'js', 'sh'):
        return None

    return (event, step, priority, is_background, language)


def load_plugin(plugin_dir: Path) -> Plugin | None:
    """Load a plugin from a directory."""
    if not plugin_dir.is_dir():
        return None

    plugin_name = plugin_dir.name

    # Skip hidden dirs and special dirs
    if plugin_name.startswith('.') or plugin_name.startswith('_'):
        return None

    plugin = Plugin(name=plugin_name, path=plugin_dir)

    # Load config schema
    config_file = plugin_dir / 'config.json'
    if config_file.exists():
        try:
            schema = json.loads(config_file.read_text())
            plugin.config_schema = schema.get('properties', {})
        except json.JSONDecodeError:
            pass

    # Load binaries manifest
    binaries_file = plugin_dir / 'binaries.jsonl'
    if binaries_file.exists():
        try:
            for line in binaries_file.read_text().strip().split('\n'):
                if line.strip():
                    plugin.binaries.append(json.loads(line))
        except (json.JSONDecodeError, Exception):
            pass

    # Discover hooks
    for hook_file in plugin_dir.glob('on_*'):
        if not hook_file.is_file():
            continue

        parsed = parse_hook_filename(hook_file.name)
        if not parsed:
            continue

        event, step, priority, is_background, language = parsed

        hook = Hook(
            name=hook_file.stem,
            plugin_name=plugin_name,
            path=hook_file,
            step=step,
            priority=priority,
            is_background=is_background,
            language=language,
        )
        plugin.hooks.append(hook)

    return plugin


def discover_plugins(plugins_dir: Path = PLUGINS_DIR) -> dict[str, Plugin]:
    """Discover all plugins in the plugins directory."""
    plugins = {}

    if not plugins_dir.exists():
        return plugins

    for plugin_dir in sorted(plugins_dir.iterdir()):
        plugin = load_plugin(plugin_dir)
        if plugin:
            plugins[plugin.name] = plugin

    return plugins


def get_all_snapshot_hooks(plugins: dict[str, Plugin]) -> list[Hook]:
    """Get all snapshot hooks from all plugins, sorted by execution order."""
    hooks = []
    for plugin in plugins.values():
        hooks.extend(plugin.get_snapshot_hooks())
    return sorted(hooks, key=lambda h: h.sort_key)


def get_plugin_names(plugins: dict[str, Plugin]) -> list[str]:
    """Get list of available plugin names."""
    return sorted(plugins.keys())


def filter_plugins(plugins: dict[str, Plugin], names: list[str] | None) -> dict[str, Plugin]:
    """Filter plugins to only include specified names."""
    if not names:
        return plugins

    names_lower = [n.lower() for n in names]
    return {
        name: plugin
        for name, plugin in plugins.items()
        if name.lower() in names_lower
    }
