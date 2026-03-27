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
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from abx_pkg import BinaryOverrides
from pydantic import BaseModel, ConfigDict, Field
from abx_plugins import get_plugins_dir

from .output_files import OutputFile


# ── Utility functions ──────────────────────────────────────────────────────


def uuid7() -> str:
    """Generate a UUIDv7-like string (timestamp-based for sortability)."""
    ts = int(datetime.now().timestamp() * 1000)
    return f"{ts:012x}-{uuid4().hex[:20]}"


def now_iso() -> str:
    return datetime.now().isoformat()


# ── Plugin models ──────────────────────────────────────────────────────────


class Hook(BaseModel):
    """A plugin hook — a +x executable script, language-agnostic.

    Hook filenames follow the convention::

        on_{Event}__[{order}_]{description}[.finite][.daemon][.bg].{ext}

    Where:
    - `{Event}` is the exact bus event family (Install, BinaryRequest, CrawlSetup, Snapshot)
    - `{order}` is an optional execution order prefix; omitted order defaults to 0
    - `.bg.` in the filename marks it as a background hook
    - `.finite.` means the bg hook exits on its own (vs `.daemon.` which runs until killed)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str  # e.g. on_Snapshot__24_chrome_navigate.js
    event: str  # e.g. SnapshotEvent
    plugin_name: str  # e.g. chrome
    path: Path  # e.g. /path/to/plugins/chrome/on_Snapshot__24_chrome_navigate.js
    order: int  # Execution order parsed from filename, defaults to 0 when omitted
    is_background: bool  # whether hook file has .bg or not in the name

    # is_finite / is_daemon <--- DO NOT ADD THESE. the only introspection on hooks we should do is bg/fg, otherwise treat all bg hooks the same (some exit early some dont, it's up to them)
    # interpreter <--- DO NOT ADD THIS. treat hooks like black-box +x executables, do not attempt to introspect how they are implemented

    @property
    def full_name(self) -> str:
        return f"{self.plugin_name}/{self.name}"

    @property
    def sort_key(self) -> tuple[int, str]:
        """Sort hooks by (order, name) so execution order matches filename prefixes."""
        return (self.order, self.name)


class RequiredBinary(BaseModel):
    """A single required binary definition from plugins/<pluginname>/config.json > required_binaries[]"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    binproviders: str = "env"
    min_version: str | None = None
    overrides: BinaryOverrides = Field(default_factory=dict)


class PluginConfig(BaseModel):
    """Plugin config definition loaded from plugins/<pluginname>/config.json"""

    title: str = ""
    description: str = ""
    output_mimetypes: list[str] = Field(default_factory=list)  # e.g. ['text/html', 'video/']
    properties: dict[str, dict[str, Any]] = Field(default_factory=dict)  # JSONSchema format describing plugin config
    required_binaries: list[RequiredBinary] = Field(default_factory=list)  # e.g. [{'name': 'wget', 'binproviders': 'env,apt,brew'}]
    required_plugins: list[str] = Field(default_factory=list)  # e.g. ['chrome', 'pdf']


class Plugin(BaseModel):
    """A plugin directory containing config and hook scripts.

    Plugins are discovered from the plugins directory (`ABX_PLUGINS_DIR` env var
    or the installed `abx_plugins` package). Each plugin directory may contain:

    - `config.json`: schema with metadata, config properties, and `required_plugins`
    - `on_*` scripts: hook executables matching the naming convention
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    path: Path
    config: PluginConfig = Field(default_factory=PluginConfig)
    hooks: list[Hook] = Field(default_factory=list)

    @property
    def enabled_key(self) -> str:
        """Config key for enabling/disabling this plugin (e.g. CHROME_ENABLED)."""
        return f"{self.name.upper()}_ENABLED"

    def filter_hooks(self, event_name: str) -> list[Hook]:
        """Return hooks for one event family sorted in execution order."""
        return sorted(
            [hook for hook in self.hooks if hook.event == event_name],
            key=lambda h: h.sort_key,
        )


class PluginEnv(BaseModel):
    """Flat assembled plugin runtime config with env serialization."""

    model_config = ConfigDict(extra="allow")
    DRY_RUN: bool = False
    TIMEOUT: int = 60

    def __getitem__(self, key: str) -> Any:
        if key in type(self).model_fields:
            return self.__dict__[key]
        if self.__pydantic_extra__ and key in self.__pydantic_extra__:
            return self.__pydantic_extra__[key]
        raise KeyError(key)

    @classmethod
    def from_config(
        cls,
        config: BaseModel | dict[str, Any],
        *,
        run_output_dir: Path,
        extra_context: dict[str, Any] | None = None,
    ) -> "PluginEnv":
        """Assemble the flat runtime config model for one plugin execution.

        ``config`` is already fully resolved at this point. This method only
        applies the runtime-specific adjustments that belong on every hook run.
        """
        from .config import GlobalConfig

        config_payload = config.model_dump(mode="json") if isinstance(config, BaseModel) else dict(config)
        payload = GlobalConfig(**config_payload).model_dump(mode="json")
        payload.pop("UV_RUN_RECURSION_DEPTH", None)
        for key, value in config_payload.items():
            if key == "UV_RUN_RECURSION_DEPTH":
                continue
            if key not in payload:
                payload[key] = value

        if extra_context:
            existing_extra_context = payload.get("EXTRA_CONTEXT") or {}
            if isinstance(existing_extra_context, str):
                try:
                    existing_extra_context = json.loads(existing_extra_context)
                except json.JSONDecodeError:
                    existing_extra_context = {}
            payload["EXTRA_CONTEXT"] = {
                **dict(existing_extra_context or {}),
                **extra_context,
            }

        run_dir = str(run_output_dir.expanduser().resolve())
        data_dir = str(Path(payload["DATA_DIR"]).expanduser().resolve())
        crawl_dir = str(Path(payload["CRAWL_DIR"]).expanduser().resolve())
        snap_dir = str(Path(payload["SNAP_DIR"]).expanduser().resolve())
        # Shared defaults point at DATA_DIR. For an actual run, remap those
        # defaults to the run-local output dir unless the caller explicitly
        # configured separate crawl/snapshot dirs.
        if crawl_dir == data_dir:
            payload["CRAWL_DIR"] = run_dir
        if snap_dir == data_dir:
            payload["SNAP_DIR"] = run_dir

        return cls(**payload)

    def to_env(self) -> dict[str, str]:
        """Serialize the flat runtime model into a subprocess env dict."""
        from .config import dump_to_dotenv_format

        env = os.environ.copy()
        env.pop("UV_RUN_RECURSION_DEPTH", None)

        # Let Google traffic bypass local proxies so Chrome/CDP and provider
        # installs do not inherit host NO_PROXY rules that break them.
        no_proxy_strip = {"googleapis.com", "google.com", "*.googleapis.com", "*.google.com", ".googleapis.com", ".google.com"}
        for key in ("NO_PROXY", "no_proxy"):
            if key in env:
                env[key] = ",".join(part.strip() for part in env[key].split(",") if part.strip() not in no_proxy_strip)

        payload = self.model_dump(mode="json")
        for key, value in payload.items():
            if value is not None:
                env[key] = dump_to_dotenv_format(value)

        path_dirs = [part for part in env["PATH"].split(os.pathsep) if part]
        runtime_bin_dirs: list[str] = []

        for key, raw_value in env.items():
            if not key.endswith("_BINARY"):
                continue
            value = str(raw_value).strip()
            if not value:
                continue
            path_value = Path(value).expanduser()
            if not (path_value.is_absolute() or "/" in value or "\\" in value):
                continue
            binary_dir = str(path_value.resolve(strict=False).parent)
            if binary_dir and binary_dir not in runtime_bin_dirs:
                runtime_bin_dirs.append(binary_dir)

        for extra_dir in (
            str(Path(env["LIB_BIN_DIR"])),
            str(Path(sys.executable).parent),
            str(Path(env["PIP_BIN_DIR"])),
            str(Path(env["NPM_BIN_DIR"])),
        ):
            if extra_dir and extra_dir not in runtime_bin_dirs:
                runtime_bin_dirs.append(extra_dir)
        if "UV" in env:
            uv_bin_dir = str(Path(env["UV"]).expanduser().resolve(strict=False).parent)
            if uv_bin_dir not in runtime_bin_dirs:
                runtime_bin_dirs.append(uv_bin_dir)

        # Prepend runtime-managed bin dirs ahead of the inherited PATH so hooks
        # see abx-managed binaries before host-global ones.
        derived_path = env["PATH"]
        for extra_dir in reversed(runtime_bin_dirs):
            if extra_dir and extra_dir not in path_dirs:
                derived_path = f"{extra_dir}{os.pathsep}{derived_path}" if derived_path else extra_dir
                path_dirs.insert(0, extra_dir)
        env["PATH"] = derived_path
        return env


# ── Execution models ──────────────────────────────────────────────────────


class Process(BaseModel):
    """A subprocess execution — one per hook invocation."""

    id: str = Field(default_factory=uuid7)
    cmd: list[str]
    binary_id: str | None = None
    plugin: str | None = None
    hook_name: str | None = None
    pwd: str = Field(default_factory=os.getcwd)
    env: dict[str, str] = Field(default_factory=dict)
    timeout: int = 60
    started_at: str | None = None
    ended_at: str | None = None
    exit_code: int | None = None
    status: Literal["succeeded", "failed", "skipped"] | None = None
    stdout: str = ""
    stderr: str = ""
    machine_hostname: str = Field(default_factory=socket.gethostname)
    machine_os: str = Field(default_factory=lambda: f"{platform.system()} {platform.release()}")

    def to_jsonl(self) -> str:
        d = {k: v for k, v in self.model_dump().items() if v is not None}
        d["type"] = "Process"
        return json.dumps(d, default=str)


# PROVIDED BY ABX-PKG:
# class Binary:
#     name: str
#     id: str = Field(default_factory=uuid7)
#     version: str | None = None
#     abspath: Path | None = None
#     min_version: SemVer | str | None = None
#     binprovider
#     overrides
#     ...


class Snapshot(BaseModel):
    """A URL being archived — one per download() call."""

    model_config = ConfigDict(extra="ignore")

    url: str
    id: str = Field(default_factory=uuid7)
    depth: int = 0
    crawl_id: str | None = None

    def to_jsonl(self) -> str:
        d = {k: v for k, v in self.model_dump().items() if v is not None}
        d["type"] = "Snapshot"
        return json.dumps(d, default=str)


class Tag(BaseModel):
    """A tag emitted by a snapshot hook."""

    model_config = ConfigDict(extra="ignore")

    name: str
    snapshot_id: str = ""

    def to_jsonl(self) -> str:
        d = {k: v for k, v in self.model_dump().items() if v is not None}
        d["type"] = "Tag"
        return json.dumps(d, default=str)


class ArchiveResult(BaseModel):
    """Result from running a single plugin hook."""

    model_config = ConfigDict(extra="ignore")

    snapshot_id: str
    plugin: str
    id: str = Field(default_factory=uuid7)
    hook_name: str = ""
    status: str = "queued"
    output_str: str = ""
    output_json: dict[str, Any] | None = None
    output_files: list[OutputFile] = Field(default_factory=list)
    start_ts: str | None = None
    end_ts: str | None = None
    error: str | None = None

    def to_jsonl(self) -> str:
        d = {k: v for k, v in self.model_dump().items() if v is not None}
        d["type"] = "ArchiveResult"
        return json.dumps(d, default=str)


def write_jsonl(path: Path, record: Any, also_print: bool = False):
    """Append a record to a JSONL file."""
    line = record.to_jsonl()
    with open(path, "a") as f:
        f.write(line + "\n")
    if also_print:
        print(line, flush=True)


# ── Plugin discovery ──────────────────────────────────────────────────────


def _default_plugins_dir() -> Path:
    """Determine the plugins directory.

    Priority: ABX_PLUGINS_DIR env var > abx_plugins package.
    """
    override = os.environ.get("ABX_PLUGINS_DIR")
    if override:
        return Path(override)
    return get_plugins_dir()


# Plugins directory
PLUGINS_DIR = _default_plugins_dir()


def parse_hook_filename(filename: str) -> tuple[str, int, bool] | None:
    """Parse a hook filename to extract (event_type, order, is_background).

    Format: `on_{Event}__[{order}_]{description}[.bg].{ext}`

    Returns None if the filename doesn't match the hook convention.
    Never attempt to determine .finite/.daemon/interpreter, hooks should be treated like black-box executables.
    """
    pattern = r"^on_(\w+)__(?:(\d+)_)?(.+)$"
    match = re.match(pattern, filename)
    if not match:
        return None

    event = match.group(1)
    order = int(match.group(2) or 0)
    is_background = ".bg." in filename

    return (event, order, is_background)


def load_plugin(plugin_dir: Path) -> Plugin | None:
    """Load a single plugin from a directory.

    Reads config.json for metadata/schema/dependencies and discovers hook scripts
    matching the `on_*` naming convention.
    """
    if not plugin_dir.is_dir():
        return None

    plugin_name = plugin_dir.name

    # Skip hidden dirs and special dirs
    if plugin_name.startswith(".") or plugin_name.startswith("_"):
        return None

    plugin = Plugin(name=plugin_name, path=plugin_dir)

    # Load config schema
    config_file = plugin_dir / "config.json"
    if config_file.exists():
        plugin.config = PluginConfig.model_validate_json(config_file.read_text())

    # Discover hooks
    for hook_file in plugin_dir.glob("on_*"):
        if not hook_file.is_file():
            continue
        if not os.access(hook_file, os.X_OK):
            continue

        parsed = parse_hook_filename(hook_file.name)
        if not parsed:
            continue

        event, order, is_background = parsed

        hook = Hook(
            name=hook_file.stem,
            event=event,
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


def plugins_matching_output(plugins: dict[str, Plugin], output_prefixes: list[str]) -> list[str]:
    """Return plugin names whose output_mimetypes match any of the given prefixes.

    A prefix like 'video/' matches 'video/mp4', 'video/webm', etc.
    A prefix like 'text/html' matches exactly 'text/html'.
    Matching is bidirectional: prefix 'video/' matches mimetype 'video/mp4',
    and mimetype 'video/' (wildcard) matches prefix 'video/mp4'.
    """
    matched: list[str] = []
    for name, plugin in plugins.items():
        for mimetype in plugin.config.output_mimetypes:
            if any(mimetype.startswith(p) or p.startswith(mimetype) for p in output_prefixes):
                matched.append(name)
                break
    return matched


def filter_plugins(plugins: dict[str, Plugin], names: list[str] | None, *, include_providers: bool = True) -> dict[str, Plugin]:
    """Filter plugins to only include specified names, plus transitive dependencies.

    Dependencies are resolved via:
    1. `required_plugins` field in each plugin's config.json
    2. When *include_providers* is True (default), provider plugins named in
       `required_binaries[].binproviders` are included transitively, along with
       any required_plugins / provider dependencies they declare themselves.
    """
    if not names:
        return plugins

    # walk the required_plugins DAG and add required plugins
    resolved: set[str] = set()
    queue = [n.lower() for n in names]
    while queue:
        name = queue.pop()
        if name in resolved:
            continue
        resolved.add(name)
        plugin = next((plugin for plugin_name, plugin in plugins.items() if plugin_name.lower() == name), None)
        if plugin:
            for dep in plugin.config.required_plugins:
                dep_lower = dep.lower()
                if dep_lower not in resolved:
                    queue.append(dep_lower)

    # walk the required_binaries DAG and add required binprovider plugins
    if include_providers:
        queue = list(resolved)
        while queue:
            name = queue.pop()
            plugin = next((plugin for plugin_name, plugin in plugins.items() if plugin_name.lower() == name), None)
            if plugin is None:
                continue
            for spec in plugin.config.required_binaries:
                for provider_name in (part.strip().lower() for part in spec.binproviders.split(",") if part.strip()):
                    provider = next(
                        (candidate for plugin_name, candidate in plugins.items() if plugin_name.lower() == provider_name),
                        None,
                    )
                    if provider is None or not provider.filter_hooks("BinaryRequest"):
                        continue
                    if provider_name not in resolved:
                        resolved.add(provider_name)
                        queue.append(provider_name)
                    for dep in provider.config.required_plugins:
                        dep_lower = dep.lower()
                        if dep_lower not in resolved:
                            resolved.add(dep_lower)
                            queue.append(dep_lower)

    return {name: plugin for name, plugin in plugins.items() if name.lower() in resolved}
