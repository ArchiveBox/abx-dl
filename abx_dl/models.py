"""
Data models and plugin discovery for abx-dl.

All domain models (Hook, Plugin, Process, Snapshot, ArchiveResult) are defined
here as Pydantic BaseModels. Plugin discovery functions (discover_plugins,
filter_plugins, etc.) are also here since they operate on these models.
"""

import json
import os
import platform
import re
import socket
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


# ── Utility functions ──────────────────────────────────────────────────────

def uuid7() -> str:
    """Generate a UUIDv7-like string (timestamp-based for sortability)."""
    ts = int(datetime.now().timestamp() * 1000)
    return f"{ts:012x}-{uuid4().hex[:20]}"


def now_iso() -> str:
    return datetime.now().isoformat()


# ── Special URLs ───────────────────────────────────────────────────────────

INSTALL_URL = 'archivebox://install'
"""Sentinel URL used by ``abx install`` to run crawl hooks (install/setup)
without triggering the snapshot phase. CrawlService detects this URL and
skips emitting SnapshotEvent."""


# ── Plugin models ──────────────────────────────────────────────────────────

class Hook(BaseModel):
    """A plugin hook — a +x executable script, language-agnostic.

    Hook filenames follow the convention::

        on_{Event}__{XX}_{description}[.finite][.daemon][.bg].{ext}

    Where:
    - ``{Event}`` is the event type (Crawl, Snapshot, Binary)
    - ``{XX}`` is the two-digit execution order (00-99)
    - ``.bg.`` in the filename marks it as a background hook
    - ``.finite.`` means the bg hook exits on its own (vs ``.daemon.`` which runs until killed)
    """
    model_config = {'arbitrary_types_allowed': True}

    name: str
    plugin_name: str
    path: Path
    order: int  # Two-digit execution order (00-99)
    is_background: bool

    @property
    def full_name(self) -> str:
        return f"{self.plugin_name}/{self.name}"

    @property
    def sort_key(self) -> tuple[int, str]:
        """Sort hooks by (order, name) so execution order matches filename prefixes."""
        return (self.order, self.name)


class Plugin(BaseModel):
    """A plugin directory containing config, binaries manifest, and hook scripts.

    Plugins are discovered from the plugins directory (``ABX_PLUGINS_DIR`` env var,
    ``abx_plugins`` package, or monorepo fallback). Each plugin directory may contain:

    - ``config.json``: schema with config properties and ``required_plugins`` list
    - ``binaries.jsonl``: binary dependency declarations
    - ``on_*`` scripts: hook executables matching the naming convention
    """
    model_config = {'arbitrary_types_allowed': True}

    name: str
    path: Path
    config_schema: dict[str, Any] = Field(default_factory=dict)
    binaries: list[dict[str, Any]] = Field(default_factory=list)
    hooks: list[Hook] = Field(default_factory=list)
    required_plugins: list[str] = Field(default_factory=list)

    @property
    def enabled_key(self) -> str:
        """Config key for enabling/disabling this plugin (e.g. CHROME_ENABLED)."""
        return f"{self.name.upper()}_ENABLED"

    def get_snapshot_hooks(self) -> list[Hook]:
        """Get hooks that run during the snapshot phase (extraction/indexing)."""
        return sorted(
            [h for h in self.hooks if 'Snapshot' in h.name],
            key=lambda h: h.sort_key
        )

    def get_crawl_hooks(self) -> list[Hook]:
        """Get hooks that run during the crawl phase (install/setup/daemons)."""
        return sorted(
            [h for h in self.hooks if 'Crawl' in h.name],
            key=lambda h: h.sort_key
        )

    def get_binary_hooks(self) -> list[Hook]:
        """Get hooks that resolve/install binary dependencies."""
        return sorted(
            [h for h in self.hooks if 'Binary' in h.name],
            key=lambda h: h.sort_key
        )


# ── Execution models ──────────────────────────────────────────────────────

class Process(BaseModel):
    """A subprocess execution — one per hook invocation."""
    cmd: list[str]
    id: str = Field(default_factory=uuid7)
    binary_id: str | None = None
    plugin: str | None = None
    hook_name: str | None = None
    pwd: str = Field(default_factory=os.getcwd)
    env: dict[str, str] = Field(default_factory=dict)
    timeout: int = 60
    started_at: str | None = None
    ended_at: str | None = None
    exit_code: int | None = None
    stdout: str = ''
    stderr: str = ''
    machine_hostname: str = Field(default_factory=socket.gethostname)
    machine_os: str = Field(default_factory=lambda: f"{platform.system()} {platform.release()}")

    def to_jsonl(self) -> str:
        d = {k: v for k, v in self.model_dump().items() if v is not None}
        d['type'] = 'Process'
        return json.dumps(d, default=str)


# PROVIDED BY ABX-PKG:
# class Binary:
#     name: str
#     id: str = Field(default_factory=uuid7)
#     version: str | None = None
#     ...

class Snapshot(BaseModel):
    """A URL being archived — one per download() call."""
    url: str
    id: str = Field(default_factory=uuid7)
    title: str | None = None
    timestamp: str = Field(default_factory=lambda: str(datetime.now().timestamp()))
    bookmarked_at: str = Field(default_factory=now_iso)
    created_at: str = Field(default_factory=now_iso)
    tags: str = ''

    def to_jsonl(self) -> str:
        d = {k: v for k, v in self.model_dump().items() if v is not None}
        d['type'] = 'Snapshot'
        return json.dumps(d, default=str)


class ArchiveResult(BaseModel):
    """Result from running a single plugin hook."""
    snapshot_id: str
    plugin: str
    id: str = Field(default_factory=uuid7)
    hook_name: str = ''
    status: str = 'queued'
    process_id: str | None = None
    output_str: str = ''
    output_files: list[str] = Field(default_factory=list)
    start_ts: str | None = None
    end_ts: str | None = None
    error: str | None = None

    def to_jsonl(self) -> str:
        d = {k: v for k, v in self.model_dump().items() if v is not None}
        d['type'] = 'ArchiveResult'
        return json.dumps(d, default=str)


VisibleRecord = ArchiveResult | Process


def write_jsonl(path: Path, record: Any, also_print: bool = False):
    """Append a record to a JSONL file."""
    line = record.to_jsonl()
    with open(path, 'a') as f:
        f.write(line + '\n')
    if also_print:
        print(line, flush=True)


# ── Plugin discovery ──────────────────────────────────────────────────────

def _default_plugins_dir() -> Path:
    """Determine the plugins directory.

    Priority: ABX_PLUGINS_DIR env var > abx_plugins package > monorepo path > local.
    """
    override = os.environ.get('ABX_PLUGINS_DIR')
    if override:
        return Path(override)
    try:
        from abx_plugins import get_plugins_dir
    except Exception:
        repo_root = Path(__file__).resolve().parents[3]
        monorepo_plugins = repo_root / 'abx-plugins' / 'abx_plugins' / 'plugins'
        if monorepo_plugins.exists():
            return monorepo_plugins
        return Path(__file__).parent / 'plugins'
    return get_plugins_dir()


# Plugins directory (prefer abx-plugins package, fallback to local)
PLUGINS_DIR = _default_plugins_dir()


def parse_hook_filename(filename: str) -> tuple[str, int, bool] | None:
    """Parse a hook filename to extract (event_type, order, is_background).

    Format: ``on_{Event}__{XX}_{description}[.bg].{ext}``

    Returns None if the filename doesn't match the hook convention.
    """
    pattern = r'^on_(\w+)__(\d{2})_.+'
    match = re.match(pattern, filename)
    if not match:
        return None

    event = match.group(1)
    order = int(match.group(2))
    is_background = '.bg.' in filename

    return (event, order, is_background)


def load_plugin(plugin_dir: Path) -> Plugin | None:
    """Load a single plugin from a directory.

    Reads config.json for schema/dependencies, binaries.jsonl for binary specs,
    and discovers hook scripts matching the ``on_*`` naming convention.
    """
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
            plugin.required_plugins = schema.get('required_plugins', [])
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

        event, order, is_background = parsed

        hook = Hook(
            name=hook_file.stem,
            plugin_name=plugin_name,
            path=hook_file,
            order=order,
            is_background=is_background,
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
    """Filter plugins to only include specified names, plus transitive dependencies.

    Dependencies are resolved via the ``required_plugins`` field in each plugin's
    config.json. For example, if plugin "chrome" requires "pip", selecting "chrome"
    automatically includes "pip".
    """
    if not names:
        return plugins

    # Resolve transitive dependencies via required_plugins in config.json
    resolved: set[str] = set()
    queue = [n.lower() for n in names]
    while queue:
        name = queue.pop()
        if name in resolved:
            continue
        resolved.add(name)
        plugin = next((plugin for plugin_name, plugin in plugins.items() if plugin_name.lower() == name), None)
        if plugin:
            for dep in plugin.required_plugins:
                dep_lower = dep.lower()
                if dep_lower not in resolved:
                    queue.append(dep_lower)

    return {
        name: plugin
        for name, plugin in plugins.items()
        if name.lower() in resolved
    }
