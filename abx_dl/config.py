"""Configuration management for abx-dl.

``config.env`` stores only user-provided values.
``derived.env`` stores runtime-derived cache entries (e.g. resolved binary paths).
Only user config participates in ``get_config()``. Derived values are read
separately by the binary-resolution layer and never blindly merged into config.
"""

import json
import os
import platform
import re
import sys
import tempfile
from importlib import import_module
from pathlib import Path
from typing import Any

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


plugin_utils: Any = import_module("abx_plugins.plugins.base.utils")


def get_arch() -> str:
    """Get architecture string like arm64-darwin or x86_64-linux."""
    machine = platform.machine().lower()
    system = platform.system().lower()
    return f"{machine}-{system}"


class BootstrapConfig(BaseSettings):
    """Minimal settings needed to locate config files before full settings load."""

    CONFIG_DIR: Path = Field(default_factory=lambda: Path.home() / ".config" / "abx")
    DATA_DIR: Path = Field(default_factory=Path.cwd)

    model_config = SettingsConfigDict(
        env_prefix="",
        extra="ignore",
        validate_default=True,
    )


class GlobalConfig(BaseSettings):
    """Global abx-dl config backed by pydantic-settings."""

    CONFIG_DIR: Path = Field(default_factory=lambda: BOOTSTRAP_CONFIG.CONFIG_DIR)
    DATA_DIR: Path = Field(default_factory=lambda: BOOTSTRAP_CONFIG.DATA_DIR)
    ABX_RUNTIME: str = "abx-dl"
    DRY_RUN: bool = False
    TIMEOUT: int = 60
    USER_AGENT: str = "Mozilla/5.0 (compatible; abx-dl/1.0; +https://github.com/ArchiveBox/abx-dl)"
    CHECK_SSL_VALIDITY: bool = True
    COOKIES_FILE: str = ""
    LIB_DIR: Path | None = None
    LIB_BIN_DIR: Path | None = None
    PERSONAS_DIR: Path | None = None
    CRAWL_DIR: Path | None = None
    SNAP_DIR: Path | None = None
    TMP_DIR: Path | None = None
    PIP_HOME: Path | None = None
    PIP_BIN_DIR: Path | None = None
    NPM_HOME: Path | None = None
    NODE_MODULES_DIR: Path | None = None
    NODE_PATH: str | None = None
    NPM_BIN_DIR: Path | None = None
    PUPPETEER_SKIP_DOWNLOAD: str = "1"
    PUPPETEER_CACHE_DIR: Path | None = None
    CHROME_SANDBOX: str = "true"

    model_config = SettingsConfigDict(
        env_prefix="",
        extra="ignore",
        validate_default=True,
    )

    @model_validator(mode="after")
    def derive_runtime_paths(self) -> "GlobalConfig":
        if self.LIB_DIR is None:
            self.LIB_DIR = self.CONFIG_DIR / "lib" / get_arch()
        if self.LIB_BIN_DIR is None:
            self.LIB_BIN_DIR = self.LIB_DIR / "bin"
        if self.PERSONAS_DIR is None:
            self.PERSONAS_DIR = self.CONFIG_DIR / "personas"
        if self.CRAWL_DIR is None:
            self.CRAWL_DIR = self.DATA_DIR
        if self.SNAP_DIR is None:
            self.SNAP_DIR = self.DATA_DIR
        if self.TMP_DIR is None:
            self.TMP_DIR = TMP_DIR
        if self.PIP_HOME is None:
            self.PIP_HOME = self.LIB_DIR / "pip"
        if self.PIP_BIN_DIR is None:
            self.PIP_BIN_DIR = self.PIP_HOME / "venv" / "bin"
        if self.NPM_HOME is None:
            self.NPM_HOME = self.LIB_DIR / "npm"
        if self.NODE_MODULES_DIR is None:
            self.NODE_MODULES_DIR = self.NPM_HOME / "node_modules"
        if self.NODE_PATH is None:
            self.NODE_PATH = str(self.NODE_MODULES_DIR)
        if self.NPM_BIN_DIR is None:
            self.NPM_BIN_DIR = self.NODE_MODULES_DIR / ".bin"
        if self.PUPPETEER_CACHE_DIR is None:
            self.PUPPETEER_CACHE_DIR = self.LIB_DIR / "puppeteer"
        return self


BOOTSTRAP_CONFIG = BootstrapConfig()

# Paths
CONFIG_DIR = BOOTSTRAP_CONFIG.CONFIG_DIR
CONFIG_FILE = CONFIG_DIR / "config.env"
DERIVED_CONFIG_FILE = CONFIG_DIR / "derived.env"
DATA_DIR = BOOTSTRAP_CONFIG.DATA_DIR
LIB_DIR = CONFIG_DIR / "lib" / get_arch()
PERSONAS_DIR = CONFIG_DIR / "personas"
TMP_DIR = Path(tempfile.mkdtemp(prefix="abx-dl-"))

# Ensure directories exist
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
LIB_DIR.mkdir(parents=True, exist_ok=True)
PERSONAS_DIR.mkdir(parents=True, exist_ok=True)


def ensure_default_persona_dir() -> Path:
    """Ensure the default persona directory exists and return its path."""
    default_persona_dir = PERSONAS_DIR / "Default"
    default_persona_dir.mkdir(parents=True, exist_ok=True)
    return default_persona_dir


def _load_env_file(path: Path) -> dict[str, str]:
    """Load key=value records from an env-style file."""
    config = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Parse KEY="value" or KEY=value format
            match = re.match(r"^([A-Z_][A-Z0-9_]*)=(.*)$", line, re.IGNORECASE)
            if match:
                key = match.group(1)
                value = match.group(2)
                # Strip quotes if present
                if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                config[key] = value
    return config


def _write_env_file(path: Path, config: dict[str, str]) -> None:
    lines = [f"{k}={v}" for k, v in sorted(config.items())]
    path.write_text(("\n".join(lines) + "\n") if lines else "")


def _parse_persisted_value(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _serialize_persisted_value(value: Any) -> str:
    return json.dumps(value)


def load_config_file() -> dict[str, str]:
    """Load user config from ~/.config/abx/config.env."""
    return _load_env_file(CONFIG_FILE)


def load_derived_config_file() -> dict[str, str]:
    """Load derived config from ~/.config/abx/derived.env."""
    return _load_env_file(DERIVED_CONFIG_FILE)


def _settings_to_config_dict(settings: BaseSettings) -> dict[str, Any]:
    return dict(settings.model_dump(mode="json"))


def _build_settings_instance() -> BaseSettings:
    settings_kwargs: dict[str, Any] = {"_env_file": CONFIG_FILE}
    return GlobalConfig(**settings_kwargs)


def get_plugin_config(
    plugin_name: str,
    schema: dict[str, Any],
    *,
    config_path: Path | None = None,
    base_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = _build_settings_instance()
    global_config = {key: value for key, value in _settings_to_config_dict(settings).items() if key in GLOBAL_DEFAULTS}
    if base_config:
        global_config.update(base_config)
    user_config = load_config_file()
    if config_path is not None and config_path.exists():
        return plugin_utils.resolve_plugin_config(
            plugin_name,
            schema,
            global_config=global_config,
            user_config=user_config,
            environ=os.environ,
            all_plugin_schemas={plugin_name: schema},
        )
    return plugin_utils.resolve_plugin_configs(
        {plugin_name: schema},
        global_config=global_config,
        user_config=user_config,
        environ=os.environ,
    ).get(plugin_name, {})


def get_config(*keys: str, plugin_schemas: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """
    Get config values. Returns all config if no keys specified.

    If plugin_schemas is provided (dict of {plugin_name: schema}), includes plugin config.
    Returns dict grouped as: {'GLOBAL': {...}, 'plugins/name': {...}, ...}
    If plugin_schemas is None, returns flat dict of global config only.
    """
    settings = _build_settings_instance()
    all_config = _settings_to_config_dict(settings)
    global_config = {key: all_config[key] for key in GLOBAL_DEFAULTS if key in all_config}

    if plugin_schemas is None:
        if keys:
            return {k: global_config.get(k) for k in keys}
        return dict(sorted(global_config.items()))

    result: dict[str, dict[str, Any]] = {"GLOBAL": dict(sorted(global_config.items()))}
    user_config = load_config_file()
    resolved_plugins = plugin_utils.resolve_plugin_configs(
        plugin_schemas,
        global_config=global_config,
        user_config=user_config,
        environ=os.environ,
    )
    for plugin_name, schema in sorted(plugin_schemas.items()):
        plugin_config = resolved_plugins.get(plugin_name, {})
        if plugin_config:
            result[f"plugins/{plugin_name}"] = plugin_config

    if keys:
        flat = {}
        alias_map = {key: plugin_utils.resolve_alias(key, plugin_schemas) for key in keys}
        for section_config in result.values():
            for k in keys:
                canonical_key = alias_map[k]
                if canonical_key in section_config:
                    flat[k] = section_config[canonical_key]
        return flat

    return result


def set_config(plugin_schemas: dict[str, dict[str, Any]] | None = None, **kwargs: Any) -> dict[str, Any]:
    """
    Set config values persistently in ~/.config/abx/config.env.
    Resolves aliases to canonical keys if plugin_schemas provided.
    Returns dict of {canonical_key: value} that was actually saved.
    """
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing user config
    config = load_config_file()

    # Resolve aliases and update values (store as JSON)
    saved = {}
    for key, value in kwargs.items():
        canonical_key = plugin_utils.resolve_alias(key, plugin_schemas)
        config[canonical_key] = _serialize_persisted_value(value)
        saved[canonical_key] = value

    _write_env_file(CONFIG_FILE, config)

    return saved


def unset_config(*keys: str) -> list[str]:
    """Remove user config keys from ~/.config/abx/config.env if present."""
    if not keys:
        return []

    config = load_config_file()
    removed: list[str] = []
    for key in keys:
        if key in config:
            removed.append(key)
            config.pop(key, None)

    _write_env_file(CONFIG_FILE, config)
    return removed


def get_derived_config(current_config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return derived persisted values from ~/.config/abx/derived.env."""
    raw = load_derived_config_file()
    if not raw:
        return {}

    parsed: dict[str, Any] = {}
    for key, value in raw.items():
        parsed[key] = _parse_persisted_value(value)
    return parsed


def set_derived_config(current_config: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
    """Persist derived runtime values in ~/.config/abx/derived.env."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    config = load_derived_config_file()

    saved = {}
    for key, value in kwargs.items():
        if value is None:
            continue
        config[key] = _serialize_persisted_value(value)
        saved[key] = value

    _write_env_file(DERIVED_CONFIG_FILE, config)
    return saved


def unset_derived_config(*keys: str, current_config: dict[str, Any] | None = None) -> list[str]:
    """Remove derived keys from ~/.config/abx/derived.env if present."""
    if not keys:
        return []

    config = load_derived_config_file()
    removed: list[str] = []
    for key in keys:
        if key in config:
            removed.append(key)
            config.pop(key, None)

    _write_env_file(DERIVED_CONFIG_FILE, config)
    return removed


# Derived paths for package managers
LIB_BIN_DIR = LIB_DIR / "bin"
PIP_HOME = LIB_DIR / "pip"
NPM_HOME = LIB_DIR / "npm"
PIP_BIN_DIR = PIP_HOME / "venv" / "bin"
NODE_MODULES_DIR = NPM_HOME / "node_modules"
NPM_BIN_DIR = NODE_MODULES_DIR / ".bin"

# Global config defaults
GLOBAL_DEFAULTS = {
    "DATA_DIR": str(DATA_DIR),
    "ABX_RUNTIME": "abx-dl",
    "DRY_RUN": False,
    "TIMEOUT": 60,
    "USER_AGENT": "Mozilla/5.0 (compatible; abx-dl/1.0; +https://github.com/ArchiveBox/abx-dl)",
    "CHECK_SSL_VALIDITY": True,
    "COOKIES_FILE": "",
    "LIB_DIR": str(LIB_DIR),
    "LIB_BIN_DIR": str(LIB_BIN_DIR),
    "PERSONAS_DIR": str(PERSONAS_DIR),
    "CRAWL_DIR": str(DATA_DIR),
    "SNAP_DIR": str(DATA_DIR),
    "TMP_DIR": str(TMP_DIR),
    "PIP_HOME": str(PIP_HOME),
    "PIP_BIN_DIR": str(PIP_BIN_DIR),
    "NPM_HOME": str(NPM_HOME),
    "NODE_MODULES_DIR": str(NODE_MODULES_DIR),
    "NODE_PATH": str(NODE_MODULES_DIR),
    "NPM_BIN_DIR": str(NPM_BIN_DIR),
    # Prevent puppeteer's postinstall from downloading Chrome automatically;
    # abx-dl handles Chromium installation via on_BinaryRequest__12_puppeteer_install instead.
    "PUPPETEER_SKIP_DOWNLOAD": "1",
    "PUPPETEER_CACHE_DIR": str(LIB_DIR / "puppeteer"),
    # Keep Chrome's sandbox enabled by default; callers that need --no-sandbox
    # in Docker/root environments must opt out explicitly.
    "CHROME_SANDBOX": "true",
}


def load_plugin_schema(plugin_dir: Path) -> dict[str, Any]:
    """Load config schema from a plugin's config.json."""
    config_file = plugin_dir / "config.json"
    if not config_file.exists():
        return {}

    try:
        schema = json.loads(config_file.read_text())
        return schema.get("properties", {})
    except json.JSONDecodeError:
        return {}


def _derive_runtime_paths(
    env: dict[str, str],
    explicit_keys: set[str],
    run_output_dir: Path | None = None,
) -> dict[str, str]:
    """
    Derive path defaults that depend on LIB_DIR and per-run output location.
    Explicit env/config values always win over these derived defaults.
    """
    lib_dir = Path(env["LIB_DIR"])
    lib_bin_dir = Path(env["LIB_BIN_DIR"]) if "LIB_BIN_DIR" in explicit_keys else lib_dir / "bin"
    pip_home = Path(env["PIP_HOME"]) if "PIP_HOME" in explicit_keys else lib_dir / "pip"
    pip_bin_dir = Path(env["PIP_BIN_DIR"]) if "PIP_BIN_DIR" in explicit_keys else pip_home / "venv" / "bin"
    npm_home = Path(env["NPM_HOME"]) if "NPM_HOME" in explicit_keys else lib_dir / "npm"
    node_modules_dir = Path(env["NODE_MODULES_DIR"]) if "NODE_MODULES_DIR" in explicit_keys else npm_home / "node_modules"
    node_path = env["NODE_PATH"] if "NODE_PATH" in explicit_keys else str(node_modules_dir)
    npm_bin_dir = Path(env["NPM_BIN_DIR"]) if "NPM_BIN_DIR" in explicit_keys else node_modules_dir / ".bin"

    derived: dict[str, str] = {}

    if "LIB_BIN_DIR" not in explicit_keys:
        derived["LIB_BIN_DIR"] = str(lib_bin_dir)
    if "PIP_HOME" not in explicit_keys:
        derived["PIP_HOME"] = str(pip_home)
    if "PIP_BIN_DIR" not in explicit_keys:
        derived["PIP_BIN_DIR"] = str(pip_bin_dir)
    if "NPM_HOME" not in explicit_keys:
        derived["NPM_HOME"] = str(npm_home)
    if "NODE_MODULES_DIR" not in explicit_keys:
        derived["NODE_MODULES_DIR"] = str(node_modules_dir)
    if "NODE_PATH" not in explicit_keys:
        derived["NODE_PATH"] = str(node_path)
    if "NPM_BIN_DIR" not in explicit_keys:
        derived["NPM_BIN_DIR"] = str(npm_bin_dir)
    if "PUPPETEER_CACHE_DIR" not in explicit_keys:
        derived["PUPPETEER_CACHE_DIR"] = str(lib_dir / "puppeteer")

    if run_output_dir is not None:
        run_dir = str(run_output_dir.expanduser().resolve())
        if "CRAWL_DIR" not in explicit_keys:
            derived["CRAWL_DIR"] = run_dir
        if "SNAP_DIR" not in explicit_keys:
            derived["SNAP_DIR"] = run_dir

    path_dirs: list[str] = []
    current_path = env.get("PATH", "")
    if current_path:
        path_dirs = [part for part in current_path.split(os.pathsep) if part]

    derived_path = current_path
    runtime_bin_dirs: list[str] = []

    # If a hook relies on a shebang like `#!/usr/bin/env node`, PATH still
    # needs to surface any configured path-like *_BINARY values.
    for key, raw_value in env.items():
        if not key.endswith("_BINARY"):
            continue
        value = str(raw_value or "").strip()
        if not value:
            continue
        path_value = Path(value).expanduser()
        if not (path_value.is_absolute() or "/" in value or "\\" in value):
            continue
        binary_dir = str(path_value.resolve(strict=False).parent)
        if binary_dir and binary_dir not in runtime_bin_dirs:
            runtime_bin_dirs.append(binary_dir)

    for extra_dir in (
        str(lib_bin_dir),
        str(Path(sys.executable).parent),
    ):
        if extra_dir and extra_dir not in runtime_bin_dirs:
            runtime_bin_dirs.append(extra_dir)
    resolved_uv = str(env.get("UV") or "").strip()
    if resolved_uv:
        uv_bin_dir = str(Path(resolved_uv).expanduser().resolve(strict=False).parent)
        if uv_bin_dir not in runtime_bin_dirs:
            runtime_bin_dirs.append(uv_bin_dir)

    for extra_dir in reversed([*runtime_bin_dirs, str(pip_bin_dir), str(npm_bin_dir)]):
        if extra_dir and extra_dir not in path_dirs:
            derived_path = f"{extra_dir}{os.pathsep}{derived_path}" if derived_path else extra_dir
            path_dirs.insert(0, extra_dir)
    if derived_path:
        derived["PATH"] = derived_path

    return derived


def _runtime_override_is_explicit(key: str, value: Any, overrides: dict[str, Any]) -> bool:
    """Treat default SNAP/CRAWL paths from shared config as non-explicit.

    ``MachineService.shared_config`` is seeded from ``get_config()``, which always
    includes default ``SNAP_DIR``/``CRAWL_DIR`` values pointing at ``DATA_DIR``.
    Those defaults should not block per-run ``run_output_dir`` from taking over.
    Only preserve these runtime-path overrides when the caller supplied a
    non-default value.
    """
    if key not in {"SNAP_DIR", "CRAWL_DIR"}:
        return True

    serialized = _serialize_value(value)
    default_runtime = _serialize_value(GLOBAL_DEFAULTS.get(key, ""))
    default_data_dir = _serialize_value(overrides.get("DATA_DIR", GLOBAL_DEFAULTS.get("DATA_DIR", "")))
    return serialized not in {"", default_runtime, default_data_dir}


def build_env_for_plugin(
    plugin_name: str,
    schema: dict[str, Any],
    overrides: dict[str, Any] | None = None,
    *,
    config_path: Path | None = None,
    run_output_dir: Path | None = None,
) -> dict[str, str]:
    """
    Build environment variables dict for running a plugin.
    Exports all config as environment variables.
    """
    env = os.environ.copy()
    # Hook scripts commonly use `#!/usr/bin/env -S uv run --script`.
    # Do not leak uv's internal recursion state into child hook processes,
    # whether it came from the parent environment or caller-provided overrides.
    blocked_env_keys = {"UV_RUN_RECURSION_DEPTH"}
    for key in blocked_env_keys:
        env.pop(key, None)
    explicit_keys = set(env.keys())
    if overrides:
        explicit_keys.update(
            key
            for key, value in overrides.items()
            if key not in blocked_env_keys and value is not None and _runtime_override_is_explicit(key, value, overrides)
        )

    # Fix NO_PROXY to allow proxied downloads from googleapis.com/google.com.
    # In some sandboxed environments (e.g. Claude Code), NO_PROXY includes
    # *.googleapis.com and *.google.com, which causes tools like @puppeteer/browsers
    # to bypass the egress proxy and fail DNS resolution for storage.googleapis.com.
    _no_proxy_strip = {"googleapis.com", "google.com", "*.googleapis.com", "*.google.com", ".googleapis.com", ".google.com"}
    for _key in ("NO_PROXY", "no_proxy"):
        if _key in env:
            _entries = [e.strip() for e in env[_key].split(",") if e.strip() not in _no_proxy_strip]
            env[_key] = ",".join(_entries)

    # Add global defaults
    for key, value in get_config().items():
        if key not in env:
            env[key] = _serialize_value(value)

    # Add plugin config values from schema defaults
    plugin_config = get_plugin_config(
        plugin_name,
        schema,
        config_path=config_path,
    )
    for key, value in plugin_config.items():
        if key not in env:
            if value is not None:
                env[key] = _serialize_value(value)

    # Apply overrides
    if overrides:
        for key, value in overrides.items():
            if key in blocked_env_keys:
                continue
            if value is None:
                env.pop(key, None)
                continue
            env[key] = _serialize_value(value)

    # Keep path values coherent with the effective LIB_DIR and current run dir.
    env.update(_derive_runtime_paths(env, explicit_keys=explicit_keys, run_output_dir=run_output_dir))

    return env


def _binary_request_signature(record: dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, default=str)


def get_required_binary_requests(
    plugin_name: str,
    schema: dict[str, Any],
    binaries: list[dict[str, Any]],
    *,
    overrides: dict[str, Any] | None = None,
    config_path: Path | None = None,
    run_output_dir: Path | None = None,
) -> list[dict[str, Any]]:
    env = build_env_for_plugin(
        plugin_name,
        schema,
        overrides,
        config_path=config_path,
        run_output_dir=run_output_dir,
    )
    requests: list[dict[str, Any]] = []
    seen: set[str] = set()
    for spec in binaries:
        record = dict(spec)
        name = str(record.get("name") or "").strip()
        if not name:
            continue
        record["name"] = name.format(**env)
        signature = _binary_request_signature(record)
        if signature in seen:
            continue
        seen.add(signature)
        requests.append(record)
    return requests


def apply_runtime_path_defaults(
    config: dict[str, Any] | None = None,
    *,
    run_output_dir: Path | None = None,
) -> dict[str, Any]:
    """Return config with LIB_DIR-derived runtime paths filled in coherently."""
    merged: dict[str, Any] = dict(config or {})
    env_like = {key: _serialize_value(value) for key, value in merged.items() if value is not None}
    base_path = env_like.get("PATH") or os.environ.get("PATH", "")
    if base_path:
        env_like["PATH"] = base_path
    runtime_path_keys = {
        "LIB_BIN_DIR",
        "PIP_HOME",
        "PIP_BIN_DIR",
        "NPM_HOME",
        "NODE_MODULES_DIR",
        "NODE_PATH",
        "NPM_BIN_DIR",
        "PUPPETEER_CACHE_DIR",
    }
    explicit_keys = {
        key
        for key, value in env_like.items()
        if (key == "PATH" and "PATH" in merged)
        or (key not in {"PATH", *runtime_path_keys} or value != _serialize_value(GLOBAL_DEFAULTS.get(key)))
    }
    merged.update(_derive_runtime_paths(env_like, explicit_keys=explicit_keys, run_output_dir=run_output_dir))
    return merged


def _serialize_value(value: Any) -> str:
    """Serialize value to string for environment variable."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "True" if value else "False"
    elif isinstance(value, list):
        return json.dumps(value)
    return str(value)
