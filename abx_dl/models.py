"""
Data models for abx-dl.

All domain models (Hook, Plugin, Process, Snapshot, ArchiveResult) are defined
here as Pydantic BaseModels. Hook filename parsing and selection live in
``abx_dl.catalog``.
"""

import importlib.metadata
import json
import os
import platform
import re
import socket
import sysconfig
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from abxpkg import BinaryOverrides
from pydantic import BaseModel, ConfigDict, Field

from .output_files import OutputFile

try:
    LIBRARY_VERSION = importlib.metadata.version("abx-dl")
except importlib.metadata.PackageNotFoundError:
    LIBRARY_VERSION = "0.0.1"


# ── Utility functions ──────────────────────────────────────────────────────


def uuid7() -> str:
    """Generate a UUIDv7-like string (timestamp-based for sortability)."""
    ts = int(datetime.now(UTC).timestamp() * 1000)
    return f"{ts:012x}{uuid4().hex[:20]}"


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


# ── Plugin models ──────────────────────────────────────────────────────────


class Hook(BaseModel):
    """A plugin hook — a +x executable script, language-agnostic.

    Hook filenames follow the convention::

        on_{Event}__[{order}_]{description}[.finite][.daemon][.bg].{ext}

    Where:
    - `{Event}` is the exact bus event family (Install, BinaryRequest, CrawlSetup, Snapshot)
    - `{order}` is an optional execution order prefix; omitted order defaults to 0
    - `.bg.` in the filename marks it as a background hook
    - `.finite.` / `.daemon.` are human-readable hints only, not lifecycle behavior
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

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    name: str
    binproviders: str | None = None
    min_version: str | None = None
    overrides: BinaryOverrides = Field(default_factory=dict)


class PluginConfig(BaseModel):
    """Plugin config definition loaded from plugins/<pluginname>/config.json"""

    title: str = ""
    description: str = ""
    x_runtimes: list[str] = Field(default_factory=list, alias="x-runtimes")
    x_auto_run: bool = Field(default=True, alias="x-auto-run")
    x_accepts_internal_input: bool = Field(default=False, alias="x-accepts-internal-input")
    output_mimetypes: list[str] = Field(default_factory=list)  # e.g. ['text/html', 'video/']
    properties: dict[str, dict[str, Any]] = Field(default_factory=dict)  # JSONSchema format describing plugin config
    required_binaries: list[RequiredBinary] = Field(default_factory=list)  # e.g. [{'name': 'wget', 'binproviders': 'env,brew,apt'}]
    required_plugins: list[str] = Field(default_factory=list)  # e.g. ['chrome', 'pdf']
    wait_for_plugins: list[str] = Field(default_factory=list)
    category: str = ""
    display_order: int = 1000
    hidden: bool = False


class Plugin(BaseModel):
    """A plugin directory containing config and hook scripts.

    Plugins are discovered from the plugins directory (`ABX_PLUGINS_DIR` env var
    or the installed `abx_plugins` package). Each plugin directory may contain:

    - `config.json`: schema with metadata, config properties, and plugin dependencies
    - `on_*` scripts: hook executables matching the naming convention
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    path: Path
    config: PluginConfig = Field(default_factory=PluginConfig)
    manifest: dict[str, Any] = Field(default_factory=dict)
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
        # Run GlobalConfig validators/derivation without re-reading BaseSettings
        # sources; the plugin config payload has already been resolved upstream.
        global_config = GlobalConfig.__pydantic_validator__.validate_python(
            config_payload,
            self_instance=GlobalConfig.model_construct(),
        )
        payload = global_config.model_dump(mode="json")
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
        personas_dir = str(Path(payload["PERSONAS_DIR"]).expanduser().resolve())
        default_personas_dir = str(Path(payload["CONFIG_DIR"]).expanduser().resolve() / "personas")
        # Shared defaults point at DATA_DIR. For an actual run, remap those
        # defaults to the run-local output dir unless the caller explicitly
        # configured separate crawl/snapshot dirs.
        if crawl_dir == data_dir:
            payload["CRAWL_DIR"] = run_dir
        if snap_dir == data_dir:
            payload["SNAP_DIR"] = run_dir
        if personas_dir == default_personas_dir:
            payload["PERSONAS_DIR"] = str(Path(run_dir) / ".persona")

        return cls(**payload)

    def to_env(self) -> dict[str, str]:
        """Serialize the flat runtime model into a subprocess env dict."""
        from .config import dump_to_dotenv_format

        env = os.environ.copy()
        env.pop("UV_RUN_RECURSION_DEPTH", None)
        env["LIBRARY_VERSION"] = LIBRARY_VERSION

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

        scripts_dir = sysconfig.get_path("scripts")
        path_entries = [entry for entry in env.get("PATH", "").split(os.pathsep) if entry]
        if scripts_dir and scripts_dir not in path_entries:
            env["PATH"] = os.pathsep.join([scripts_dir, *path_entries])

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


# ── Hook filename parsing ──────────────────────────────────────────────────────


def parse_hook_filename(filename: str) -> tuple[str, int, bool] | None:
    """Parse a hook filename to extract (event_type, order, is_background).

    Format: `on_{Event}__[{order}_]{description}[.bg].{ext}`.

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
