"""Configuration management for abx-dl.

``config.env`` stores only user-provided values.
``derived.env`` stores runtime-derived cache entries (e.g. resolved binary paths).
Only user config participates in ``get_initial_env()``. Runtime code reads user
and sparse derived state through ``get_config()`` and never blindly merges
derived cache into user config.
"""

import json
import os
import re
import tempfile
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Self, cast
from collections.abc import Mapping

from abxbus import EventBus
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .events import MachineEvent
from .models import Plugin, PluginConfig, PluginEnv, RequiredBinary
from abx_plugins.plugins.base import utils as plugin_utils


class BootstrapConfig(BaseSettings):
    """Minimal settings needed to locate config files before full settings load."""

    CONFIG_DIR: Path = Field(default_factory=lambda: Path.home() / ".config" / "abx")
    DATA_DIR: Path = Field(default_factory=Path.cwd)

    model_config = SettingsConfigDict(
        env_prefix="",
        extra="allow",
        validate_default=True,
    )


BOOTSTRAP_CONFIG = BootstrapConfig()

# Paths
CONFIG_DIR = BOOTSTRAP_CONFIG.CONFIG_DIR
CONFIG_FILE = CONFIG_DIR / "config.env"
DERIVED_CONFIG_FILE = CONFIG_DIR / "derived.env"
DATA_DIR = BOOTSTRAP_CONFIG.DATA_DIR


@lru_cache(maxsize=1)
def _default_tmp_dir() -> Path:
    """Create the process-local fallback temp dir only if config asks for it."""
    return Path(tempfile.mkdtemp(prefix="abx-dl-"))


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
        env_file=CONFIG_FILE,
        extra="allow",
        validate_default=True,
    )

    def __init__(self, **values: Any) -> None:
        values.setdefault("_env_file", BootstrapConfig().CONFIG_DIR / "config.env")
        super().__init__(**values)

    @field_validator("CHROME_SANDBOX", mode="before")
    @classmethod
    def validate_chrome_sandbox(cls, value: Any) -> Any:
        """Normalize boolean inputs back to the string form hooks expect."""
        if isinstance(value, bool):
            return "true" if value else "false"
        return value

    @field_validator(
        "LIB_DIR",
        "LIB_BIN_DIR",
        "PERSONAS_DIR",
        "CRAWL_DIR",
        "SNAP_DIR",
        "TMP_DIR",
        "PIP_HOME",
        "PIP_BIN_DIR",
        "NPM_HOME",
        "NODE_MODULES_DIR",
        "NODE_PATH",
        "NPM_BIN_DIR",
        "PUPPETEER_CACHE_DIR",
        mode="before",
    )
    @classmethod
    def empty_optional_path_to_none(cls, value: Any) -> Any:
        """Treat empty optional runtime path settings as unset, not as cwd."""
        if isinstance(value, str) and value.strip() == "":
            return None
        return value

    @model_validator(mode="after")
    def derive_runtime_paths(self) -> Self:
        """Fill runtime path defaults from CONFIG_DIR / DATA_DIR once, centrally."""
        default_lib_dir = self.CONFIG_DIR / "lib"
        if self.LIB_DIR is None:
            self.LIB_DIR = default_lib_dir
        default_lib_bin_dir = default_lib_dir / "bin"
        default_pip_home = default_lib_dir / "pip"
        default_pip_bin_dir = default_pip_home / "venv" / "bin"
        default_npm_home = default_lib_dir / "npm"
        default_node_modules_dir = default_npm_home / "node_modules"
        default_npm_bin_dir = default_node_modules_dir / ".bin"
        default_puppeteer_cache_dir = default_lib_dir / "puppeteer"
        lib_dir_changed = self.LIB_DIR != default_lib_dir
        if self.LIB_BIN_DIR is None or (lib_dir_changed and self.LIB_BIN_DIR == default_lib_bin_dir):
            self.LIB_BIN_DIR = self.LIB_DIR / "bin"
        if self.PERSONAS_DIR is None:
            self.PERSONAS_DIR = self.CONFIG_DIR / "personas"
        if self.CRAWL_DIR is None:
            self.CRAWL_DIR = self.DATA_DIR
        if self.SNAP_DIR is None:
            self.SNAP_DIR = self.DATA_DIR
        if self.TMP_DIR is None:
            self.TMP_DIR = _default_tmp_dir()
        if self.PIP_HOME is None or (lib_dir_changed and self.PIP_HOME == default_pip_home):
            self.PIP_HOME = self.LIB_DIR / "pip"
        if self.PIP_BIN_DIR is None or (lib_dir_changed and self.PIP_BIN_DIR == default_pip_bin_dir):
            self.PIP_BIN_DIR = self.PIP_HOME / "venv" / "bin"
        if self.NPM_HOME is None or (lib_dir_changed and self.NPM_HOME == default_npm_home):
            self.NPM_HOME = self.LIB_DIR / "npm"
        if self.NODE_MODULES_DIR is None or (lib_dir_changed and self.NODE_MODULES_DIR == default_node_modules_dir):
            self.NODE_MODULES_DIR = self.NPM_HOME / "node_modules"
        if self.NODE_PATH is None or (lib_dir_changed and self.NODE_PATH == str(default_node_modules_dir)):
            self.NODE_PATH = str(self.NODE_MODULES_DIR)
        if self.NPM_BIN_DIR is None or (lib_dir_changed and self.NPM_BIN_DIR == default_npm_bin_dir):
            self.NPM_BIN_DIR = self.NODE_MODULES_DIR / ".bin"
        if self.PUPPETEER_CACHE_DIR is None or (lib_dir_changed and self.PUPPETEER_CACHE_DIR == default_puppeteer_cache_dir):
            self.PUPPETEER_CACHE_DIR = self.LIB_DIR / "puppeteer"
        return self

    def __getitem__(self, key: str) -> Any:
        if key in type(self).model_fields:
            return self.__dict__[key]
        if self.__pydantic_extra__ and key in self.__pydantic_extra__:
            return self.__pydantic_extra__[key]
        raise KeyError(key)

    def __contains__(self, key: str) -> bool:
        return key in type(self).model_fields or bool(self.__pydantic_extra__ and key in self.__pydantic_extra__)


@dataclass(frozen=True)
class RuntimeConfig:
    user: GlobalConfig
    derived: dict[str, Any]


def _config_file(settings: GlobalConfig | None = None) -> Path:
    runtime_settings = settings or GlobalConfig()
    return runtime_settings.CONFIG_DIR / "config.env"


def _derived_config_file(settings: GlobalConfig | None = None) -> Path:
    runtime_settings = settings or GlobalConfig()
    return runtime_settings.CONFIG_DIR / "derived.env"


def ensure_default_persona_dir() -> Path:
    """Ensure the default persona directory exists and return its path."""
    default_persona_dir = cast(Path, GlobalConfig().PERSONAS_DIR) / "Default"
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
    """Write env-style config back to disk in stable key order."""
    lines = [f"{k}={v}" for k, v in sorted(config.items())]
    path.write_text(("\n".join(lines) + "\n") if lines else "")


def is_path_like_env_value(value: Any) -> bool:
    """Return True when a config/env value names a filesystem path."""
    text = str(value or "").strip()
    return bool(text) and (text.startswith(("~/", "./", "/")) or "/" in text or "\\" in text)


def _load_plugin_config_model(
    plugin: Plugin,
    *,
    user_env: GlobalConfig | Mapping[str, Any] | None = None,
    derived_env: GlobalConfig | Mapping[str, Any] | None = None,
    hydrate_binaries: bool = True,
) -> Any:
    """Resolve one plugin's typed config model from the final effective env.

    ``x-fallback`` should see the same effective values hooks see, so derived
    ``*_BINARY`` cache is overlaid here before schema resolution.
    """
    global_config = user_env.model_dump(mode="json") if isinstance(user_env, BaseSettings) else dict(user_env or get_initial_env())
    for key, value in list(global_config.items()):
        if key in GlobalConfig.model_fields:
            continue
        if isinstance(value, (dict, list)):
            global_config[key] = dump_to_dotenv_format(value)
    if derived_env:
        if isinstance(derived_env, BaseSettings):
            derived_keys = set(derived_env.model_fields_set)
            if derived_env.__pydantic_extra__:
                derived_keys.update(derived_env.__pydantic_extra__)
            effective_derived_env = {key: value for key, value in derived_env.model_dump(mode="json").items() if key in derived_keys}
        else:
            effective_derived_env = dict(derived_env)
        for key, value in effective_derived_env.items():
            if value is None:
                continue
            if key in {"DATA_DIR", "CRAWL_DIR", "SNAP_DIR"} and global_config.get(key) != value:
                continue
            if key.endswith("_BINARY"):
                user_value = str(global_config.get(key, "")).strip()
                if user_value and is_path_like_env_value(user_value):
                    continue
                configured_value = str(global_config.get(key, "")).strip()
                derived_value = str(value).strip()
                if not derived_value:
                    continue
                if not is_path_like_env_value(derived_value):
                    continue
                derived_path = Path(derived_value).expanduser()
                if not derived_path.exists():
                    continue
                if configured_value and derived_path.name != configured_value:
                    continue
            global_config[key] = value
    serialized_user_config = {key: dump_to_dotenv_format(value) for key, value in global_config.items() if value is not None}
    user_config = {**os.environ, **serialized_user_config}
    environ: dict[str, str] = {}
    config_path = plugin.path / "config.json"
    if not config_path.exists():
        return PluginEnv()
    resolved_config = plugin_utils.load_config(
        config_path,
        global_config=global_config,
        user_config=user_config,
        environ=environ,
        hydrate_binaries=hydrate_binaries,
    )
    return resolved_config


async def get_config(bus: EventBus | None = None, *, include_derived: bool = True) -> RuntimeConfig:
    """Return the current runtime config state from one canonical derivation path.

    ``MachineEvent`` history is returned newest-first by ``bus.filter()``, so
    state replay must apply it oldest-to-newest. ``derived`` intentionally stays
    sparse: it only contains runtime-emitted cache values like resolved binaries,
    never default-filled ``GlobalConfig`` paths.
    """
    current_user_config: dict[str, Any] = {} if bus is not None else get_initial_env()
    current_derived_config: dict[str, Any] = {} if bus is not None else get_derived_config(current_user_config)
    if bus is not None:
        for candidate in reversed(await bus.filter(MachineEvent, past=True)):
            target_config = current_derived_config if candidate.config_type == "derived" else current_user_config
            if candidate.config is not None:
                if candidate.config_type == "derived" and not include_derived:
                    continue
                target_config.update(candidate.config)
                continue
            key = candidate.key.removeprefix("config/")
            if not key:
                continue
            if candidate.config_type == "derived" and not include_derived:
                continue
            if candidate.method == "update":
                target_config[key] = candidate.value
                continue
            if candidate.method == "unset":
                target_config.pop(key, None)
    if not include_derived:
        current_derived_config = {}
    return RuntimeConfig(user=GlobalConfig(**current_user_config), derived=current_derived_config)


async def get_plugin_env(
    bus: EventBus,
    *,
    plugin: Plugin,
    run_output_dir: Path,
    include_derived: bool = True,
    extra_context: dict[str, Any] | None = None,
    config: RuntimeConfig | None = None,
) -> PluginEnv:
    """Build the flat runtime env model for one plugin on the current bus.

    User config stays authoritative. Derived config is only overlaid here for
    recoverable runtime cache such as resolved ``*_BINARY`` paths.
    """
    runtime_config = config or await get_config(bus, include_derived=include_derived)
    plugin_config = _load_plugin_config_model(
        plugin,
        user_env=runtime_config.user,
        derived_env=runtime_config.derived if include_derived else None,
    )
    return PluginEnv.from_config(plugin_config, run_output_dir=run_output_dir, extra_context=extra_context)


def get_initial_env(*keys: str, plugin_schemas: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """Load persisted user config before a bus exists.

    This is bootstrap-only state from ``config.env``. It intentionally excludes
    ``derived.env`` because derived cache is reconstructed separately at runtime.
    """
    settings = GlobalConfig()
    all_config = dict(settings.model_dump(mode="json"))
    global_config = {key: all_config[key] for key in GLOBAL_DEFAULT_KEYS if key in all_config}
    raw_user_config = _load_env_file(_config_file(settings))
    user_config = {key: plugin_utils._parse_config_value(value) for key, value in raw_user_config.items()}
    flat_config = dict(global_config)
    flat_config.update(user_config)

    if plugin_schemas is None:
        if keys:
            return {k: flat_config.get(k) for k in keys}
        return dict(sorted(flat_config.items()))

    result: dict[str, dict[str, Any]] = {"GLOBAL": dict(sorted(global_config.items()))}
    resolved_plugins = plugin_utils.resolve_plugin_configs(
        plugin_schemas,
        global_config=global_config,
        user_config=raw_user_config,
        environ=os.environ,
    )
    for plugin_name, schema in sorted(plugin_schemas.items()):
        plugin_config = resolved_plugins[plugin_name] if plugin_name in resolved_plugins else {}
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


def set_user_config(plugin_schemas: dict[str, dict[str, Any]] | None = None, **kwargs: Any) -> dict[str, Any]:
    """Validate and persist user-owned config updates into ``config.env``."""
    settings = GlobalConfig()
    config_file = _config_file(settings)
    config_file.parent.mkdir(parents=True, exist_ok=True)

    # Load existing user config
    config = _load_env_file(config_file)
    global_config = {key: value for key, value in dict(settings.model_dump(mode="json")).items() if key in GLOBAL_DEFAULT_KEY_SET}

    # Resolve aliases and update values (store as JSON)
    saved: dict[str, Any] = {}
    for key, value in kwargs.items():
        canonical_key = plugin_utils.resolve_alias(key, plugin_schemas)
        validated_value: Any = None

        if canonical_key in GLOBAL_DEFAULT_KEY_SET:
            validated_value = GlobalConfig(**{canonical_key: value}).model_dump(mode="json")[canonical_key]
        else:
            if plugin_schemas is None:
                raise KeyError(f"Unknown config key: {canonical_key}")

            validation_user_config = dict(config)
            validation_user_config[canonical_key] = value if isinstance(value, str) else json.dumps(value)
            resolved_plugins = plugin_utils.resolve_plugin_configs(
                plugin_schemas,
                global_config=global_config,
                user_config=validation_user_config,
                environ={},
            )
            plugin_value_found = False
            for plugin_config in resolved_plugins.values():
                if canonical_key in plugin_config:
                    validated_value = plugin_config[canonical_key]
                    plugin_value_found = True
                    break
            if not plugin_value_found:
                raise KeyError(f"Unknown config key: {canonical_key}")

        config[canonical_key] = json.dumps(validated_value)
        saved[canonical_key] = validated_value

    _write_env_file(config_file, config)

    return saved


def unset_user_config(*keys: str) -> list[str]:
    """Remove user config keys from ~/.config/abx/config.env if present."""
    if not keys:
        return []

    config_file = _config_file()
    config = _load_env_file(config_file)
    removed: list[str] = []
    for key in keys:
        if key in config:
            removed.append(key)
            config.pop(key, None)

    _write_env_file(config_file, config)
    return removed


def get_derived_config(current_config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Load persisted derived runtime cache from ``derived.env``."""
    raw = _load_env_file(_derived_config_file(GlobalConfig(**(current_config or {}))))
    if not raw:
        return {}

    parsed: dict[str, Any] = {}
    for key, value in raw.items():
        parsed[key] = plugin_utils._parse_config_value(value)
    return parsed


def set_derived_config(current_config: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
    """Persist derived runtime cache values into ``derived.env``."""
    derived_config_file = _derived_config_file(GlobalConfig(**(current_config or {})))
    derived_config_file.parent.mkdir(parents=True, exist_ok=True)
    config = _load_env_file(derived_config_file)

    saved = {}
    for key, value in kwargs.items():
        if value is None:
            continue
        config[key] = json.dumps(value)
        saved[key] = value

    _write_env_file(derived_config_file, config)
    return saved


def unset_derived_config(*keys: str, current_config: dict[str, Any] | None = None) -> list[str]:
    """Remove derived cache keys from ``derived.env`` if present."""
    if not keys:
        return []

    derived_config_file = _derived_config_file(GlobalConfig(**(current_config or {})))
    config = _load_env_file(derived_config_file)
    removed: list[str] = []
    for key in keys:
        if key in config:
            removed.append(key)
            config.pop(key, None)

    _write_env_file(derived_config_file, config)
    return removed


GLOBAL_DEFAULT_KEYS = (
    "DATA_DIR",
    "ABX_RUNTIME",
    "DRY_RUN",
    "TIMEOUT",
    "USER_AGENT",
    "CHECK_SSL_VALIDITY",
    "COOKIES_FILE",
    "LIB_DIR",
    "LIB_BIN_DIR",
    "PERSONAS_DIR",
    "CRAWL_DIR",
    "SNAP_DIR",
    "TMP_DIR",
    "PIP_HOME",
    "PIP_BIN_DIR",
    "NPM_HOME",
    "NODE_MODULES_DIR",
    "NODE_PATH",
    "NPM_BIN_DIR",
    "PUPPETEER_SKIP_DOWNLOAD",
    "PUPPETEER_CACHE_DIR",
    "CHROME_SANDBOX",
)
GLOBAL_DEFAULT_KEY_SET = frozenset(GLOBAL_DEFAULT_KEYS)


def load_plugin_schema(plugin_dir: Path) -> dict[str, Any]:
    """Load the raw ``properties`` schema block from one plugin's config.json."""
    config_file = plugin_dir / "config.json"
    if not config_file.exists():
        return {}

    schema = PluginConfig.model_validate_json(config_file.read_text())
    return schema.properties


def get_required_binary_requests(
    plugin: Plugin,
    binaries: list[RequiredBinary],
    *,
    overrides: GlobalConfig | Mapping[str, Any] | None = None,
    derived_overrides: GlobalConfig | Mapping[str, Any] | None = None,
    run_output_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Hydrate one plugin's ``required_binaries`` into BinaryRequest payloads."""
    plugin_config = _load_plugin_config_model(
        plugin,
        user_env=overrides,
        derived_env=derived_overrides,
    )
    # For the cache-key signature we want the binary's *logical* name (e.g.
    # ``chromium``) regardless of where it's installed on this machine, so the
    # ``ABX_INSTALL_CACHE`` key matches across runs and installs. Replace any
    # resolved ``*_BINARY`` overrides with their plugin-schema defaults so
    # ``{X_BINARY}`` name templates hydrate to ``"chromium"``/``"node"``/etc.
    # rather than the local abspath. Stripping the keys outright would let
    # ``plugin_utils.load_config``'s chrome auto-detect re-inject Google Chrome
    # Canary's abspath; setting an explicit default short-circuits that path
    # (its ``has_explicit_browser`` check looks for the key, not the value).
    if isinstance(overrides, BaseSettings):
        request_name_overrides: dict[str, Any] = overrides.model_dump(mode="json")
    else:
        request_name_overrides = dict(overrides or {})
    schema_binary_defaults: dict[str, Any] = {}
    for prop_key, prop in plugin.config.properties.items():
        if not prop_key.endswith("_BINARY"):
            continue
        default = prop.get("default") if isinstance(prop, dict) else None
        if isinstance(default, str) and default:
            schema_binary_defaults[prop_key] = default
    request_name_overrides = {key: value for key, value in request_name_overrides.items() if not key.endswith("_BINARY")}
    request_name_overrides.update(schema_binary_defaults)
    request_name_config = _load_plugin_config_model(
        plugin,
        user_env=request_name_overrides,
        derived_env=None,
        hydrate_binaries=False,
    )
    env = PluginEnv.from_config(
        plugin_config,
        run_output_dir=run_output_dir or Path.cwd(),
    ).to_env()
    request_name_env = PluginEnv.from_config(
        request_name_config,
        run_output_dir=run_output_dir or Path.cwd(),
    ).to_env()
    requests: list[dict[str, Any]] = []
    seen: set[str] = set()
    for spec in binaries:
        record = spec.model_dump(mode="json")
        name_template = record.get("name")

        def hydrate(value: Any, source_env: dict[str, str]) -> Any:
            if isinstance(value, str):
                return value.format(**source_env)
            if isinstance(value, list):
                return [hydrate(item, source_env) for item in value]
            if isinstance(value, dict):
                return {key: hydrate(nested_value, source_env) for key, nested_value in value.items()}
            return value

        record = hydrate(record, env)
        if isinstance(name_template, str):
            record["name"] = hydrate(name_template, request_name_env)
        signature = json.dumps(record, sort_keys=True, default=str)
        if signature in seen:
            continue
        seen.add(signature)
        requests.append(record)
    return requests


def dump_to_dotenv_format(value: Any) -> str:
    """Serialize value to string for environment variable."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "True" if value else "False"
    elif isinstance(value, (list, dict)):
        return json.dumps(value)
    return str(value)
