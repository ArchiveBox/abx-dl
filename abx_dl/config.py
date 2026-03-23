"""
Configuration management for abx-dl.

Loads config from environment variables and persistent config file.
"""

import json
import os
import platform
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any


def get_arch() -> str:
    """Get architecture string like arm64-darwin or x86_64-linux."""
    machine = platform.machine().lower()
    system = platform.system().lower()
    return f"{machine}-{system}"


# Paths
CONFIG_DIR = Path(os.environ.get("CONFIG_DIR", Path.home() / ".config" / "abx"))
CONFIG_FILE = CONFIG_DIR / "config.env"
DATA_DIR = Path(os.environ.get("DATA_DIR", Path.cwd()))
LIB_DIR = Path(os.environ.get("LIB_DIR", CONFIG_DIR / "lib" / get_arch()))
PERSONAS_DIR = Path(os.environ.get("PERSONAS_DIR", CONFIG_DIR / "personas"))
TMP_DIR = Path(os.environ.get("TMP_DIR", tempfile.mkdtemp(prefix="abx-dl-")))

# Ensure directories exist
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
LIB_DIR.mkdir(parents=True, exist_ok=True)
PERSONAS_DIR.mkdir(parents=True, exist_ok=True)


def ensure_default_persona_dir() -> Path:
    """Ensure the default persona directory exists and return its path."""
    default_persona_dir = PERSONAS_DIR / "Default"
    default_persona_dir.mkdir(parents=True, exist_ok=True)
    return default_persona_dir


def load_config_file() -> dict[str, str]:
    """Load config from ~/.config/abx/config.env file."""
    config = {}
    if CONFIG_FILE.exists():
        for line in CONFIG_FILE.read_text().splitlines():
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


def get_config(*keys: str, plugin_schemas: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """
    Get config values. Returns all config if no keys specified.

    If plugin_schemas is provided (dict of {plugin_name: schema}), includes plugin config.
    Returns dict grouped as: {'GLOBAL': {...}, 'plugins/name': {...}, ...}
    If plugin_schemas is None, returns flat dict of global config only.
    """
    # Build global config
    global_config: dict[str, Any] = {}
    global_config.update(GLOBAL_DEFAULTS)

    # Override with persistent config (parse JSON values)
    for key, value in load_config_file().items():
        try:
            global_config[key] = json.loads(value)
        except json.JSONDecodeError:
            global_config[key] = value

    # Override with environment variables
    for key in list(global_config.keys()):
        if key in os.environ:
            try:
                global_config[key] = json.loads(os.environ[key])
            except json.JSONDecodeError:
                global_config[key] = os.environ[key]

    if plugin_schemas is None:
        # No plugins - return flat global config
        if keys:
            return {k: global_config.get(k) for k in keys}
        return dict(sorted(global_config.items()))

    # Build grouped config with plugins
    result: dict[str, dict[str, Any]] = {"GLOBAL": dict(sorted(global_config.items()))}

    for plugin_name, schema in sorted(plugin_schemas.items()):
        plugin_config: dict[str, Any] = {}
        for key in schema:
            plugin_config[key] = get_config_value(key, schema)
        if plugin_config:
            result[f"plugins/{plugin_name}"] = plugin_config

    if keys:
        # Search all sections for requested keys
        flat = {}
        for section_config in result.values():
            for k in keys:
                if k in section_config:
                    flat[k] = section_config[k]
        return flat

    return result


def resolve_alias(key: str, plugin_schemas: dict[str, dict[str, Any]] | None = None) -> str:
    """
    Resolve an alias to its canonical config key.
    Returns the canonical key if found, otherwise returns the original key.
    """
    if plugin_schemas is None:
        return key

    for schema in plugin_schemas.values():
        # Check if key is a canonical key
        if key in schema:
            return key
        # Check if key is an alias for any canonical key
        for canonical_key, prop in schema.items():
            if key in prop.get("x-aliases", []):
                return canonical_key

    return key


def set_config(plugin_schemas: dict[str, dict[str, Any]] | None = None, **kwargs: Any) -> dict[str, Any]:
    """
    Set config values persistently in ~/.config/abx/config.env.
    Resolves aliases to canonical keys if plugin_schemas provided.
    Returns dict of {canonical_key: value} that was actually saved.
    """
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing config
    config = load_config_file()

    # Resolve aliases and update values (store as JSON)
    saved = {}
    for key, value in kwargs.items():
        canonical_key = resolve_alias(key, plugin_schemas)
        config[canonical_key] = json.dumps(value)
        saved[canonical_key] = value

    # Write back
    lines = [f"{k}={v}" for k, v in sorted(config.items())]
    CONFIG_FILE.write_text("\n".join(lines) + "\n")

    return saved


def unset_config(*keys: str) -> list[str]:
    """Remove config keys from ~/.config/abx/config.env if present."""
    if not keys:
        return []

    config = load_config_file()
    removed: list[str] = []
    for key in keys:
        if key in config:
            removed.append(key)
            config.pop(key, None)

    lines = [f"{k}={v}" for k, v in sorted(config.items())]
    CONFIG_FILE.write_text(("\n".join(lines) + "\n") if lines else "")
    return removed


# Load persistent config into environment (lower priority than actual env vars)
_persistent_config = load_config_file()
for _key, _value in _persistent_config.items():
    if _key not in os.environ:
        os.environ[_key] = _value

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
    # abx-dl handles Chromium installation via on_Binary__12_puppeteer_install instead.
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


def get_env(key: str, default: str = "") -> str:
    """Get environment variable value."""
    return os.environ.get(key, default).strip()


def get_env_bool(key: str, default: bool = False) -> bool:
    """Get boolean from environment variable."""
    val = get_env(key, "").lower()
    if val in ("true", "1", "yes", "on"):
        return True
    if val in ("false", "0", "no", "off"):
        return False
    return default


def get_env_int(key: str, default: int = 0) -> int:
    """Get integer from environment variable."""
    try:
        return int(get_env(key, str(default)))
    except ValueError:
        return default


def get_env_array(key: str, default: list[str] | None = None) -> list[str]:
    """Get array from environment variable (JSON format)."""
    val = get_env(key, "")
    if not val:
        return default if default is not None else []
    try:
        result = json.loads(val)
        if isinstance(result, list):
            return [str(item) for item in result]
    except json.JSONDecodeError:
        pass
    return default if default is not None else []


def get_config_value(key: str, schema: dict[str, Any]) -> Any:
    """
    Get a config value from environment with fallback chain:
    1. Direct environment variable
    2. Aliased environment variables (x-aliases)
    3. Fallback environment variable (x-fallback)
    4. Schema default
    5. Global default
    """
    prop = schema.get(key, {})
    prop_type = prop.get("type", "string")

    # Check direct env var
    env_val = os.environ.get(key)
    if env_val is not None:
        return _parse_value(env_val, prop_type)

    # Check aliases
    for alias in prop.get("x-aliases", []):
        env_val = os.environ.get(alias)
        if env_val is not None:
            return _parse_value(env_val, prop_type)

    # Check fallback
    fallback_key = prop.get("x-fallback")
    if fallback_key:
        env_val = os.environ.get(fallback_key)
        if env_val is not None:
            return _parse_value(env_val, prop_type)
        # Check global default for fallback key
        if fallback_key in GLOBAL_DEFAULTS:
            return GLOBAL_DEFAULTS[fallback_key]

    # Schema default
    if "default" in prop:
        return prop["default"]

    # Global default
    return GLOBAL_DEFAULTS.get(key)


def _parse_value(val: str, prop_type: str) -> Any:
    """Parse string value to appropriate type."""
    if prop_type == "boolean":
        return val.lower() in ("true", "1", "yes", "on")
    elif prop_type == "integer":
        try:
            return int(val)
        except ValueError:
            return 0
    elif prop_type == "array":
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return []
    return val


def _derive_runtime_paths(
    env: dict[str, str],
    explicit_keys: set[str],
    run_output_dir: Path | None = None,
) -> dict[str, str]:
    """
    Derive path defaults that depend on LIB_DIR and per-run output location.
    Explicit env/config values always win over these derived defaults.
    """
    lib_dir = Path(env.get("LIB_DIR", str(LIB_DIR)))
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
        run_dir = str(run_output_dir)
        if "CRAWL_DIR" not in explicit_keys:
            derived["CRAWL_DIR"] = run_dir
        if "SNAP_DIR" not in explicit_keys:
            derived["SNAP_DIR"] = run_dir

    path_dirs: list[str] = []
    current_path = env.get("PATH", "")
    if current_path:
        path_dirs = [part for part in current_path.split(":") if part]

    derived_path = current_path
    runtime_bin_dirs = [
        str(Path(sys.executable).parent),
    ]
    resolved_uv = shutil.which("uv", path=current_path or None)
    if resolved_uv:
        runtime_bin_dirs.append(str(Path(resolved_uv).resolve().parent))

    for extra_dir in (*runtime_bin_dirs, str(pip_bin_dir), str(npm_bin_dir)):
        if extra_dir and extra_dir not in path_dirs:
            derived_path = f"{extra_dir}:{derived_path}" if derived_path else extra_dir
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
            key for key, value in overrides.items() if key not in blocked_env_keys and _runtime_override_is_explicit(key, value, overrides)
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
    for key, value in GLOBAL_DEFAULTS.items():
        if key not in env:
            env[key] = _serialize_value(value)

    # Add plugin config values from schema defaults
    for key, prop in schema.items():
        if key not in env:
            value = get_config_value(key, schema)
            if value is not None:
                env[key] = _serialize_value(value)

    # Apply overrides
    if overrides:
        for key, value in overrides.items():
            if key in blocked_env_keys:
                continue
            env[key] = _serialize_value(value)

    # Keep path values coherent with the effective LIB_DIR and current run dir.
    env.update(_derive_runtime_paths(env, explicit_keys=explicit_keys, run_output_dir=run_output_dir))

    return env


def _serialize_value(value: Any) -> str:
    """Serialize value to string for environment variable."""
    if isinstance(value, bool):
        return "True" if value else "False"
    elif isinstance(value, list):
        return json.dumps(value)
    return str(value)
