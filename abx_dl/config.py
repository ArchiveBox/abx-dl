"""
Configuration management for abx-dl.

Loads config exclusively from environment variables.
"""

import json
import os
import platform
import tempfile
from pathlib import Path
from typing import Any


def get_arch() -> str:
    """Get architecture string like arm64-darwin or x86_64-linux."""
    machine = platform.machine().lower()
    system = platform.system().lower()
    return f"{machine}-{system}"


# Paths
DATA_DIR = Path(os.environ.get('DATA_DIR', Path.cwd()))
LIB_DIR = Path(os.environ.get('LIB_DIR', Path.home() / '.config' / 'abx' / 'lib' / get_arch()))
TMP_DIR = Path(os.environ.get('TMP_DIR', tempfile.mkdtemp(prefix='abx-dl-')))

# Ensure directories exist
LIB_DIR.mkdir(parents=True, exist_ok=True)

# Derived paths for package managers
PIP_HOME = LIB_DIR / 'pip'
NPM_HOME = LIB_DIR / 'npm'
NODE_MODULES_DIR = NPM_HOME / 'node_modules'
NPM_BIN_DIR = NODE_MODULES_DIR / '.bin'

# Global config defaults
GLOBAL_DEFAULTS = {
    'TIMEOUT': 60,
    'USER_AGENT': 'Mozilla/5.0 (compatible; abx-dl/1.0; +https://github.com/ArchiveBox/abx-dl)',
    'CHECK_SSL_VALIDITY': True,
    'COOKIES_FILE': '',
    'LIB_DIR': str(LIB_DIR),
    'TMP_DIR': str(TMP_DIR),
    'PIP_HOME': str(PIP_HOME),
    'NPM_HOME': str(NPM_HOME),
    'NODE_MODULES_DIR': str(NODE_MODULES_DIR),
    'NPM_BIN_DIR': str(NPM_BIN_DIR),
}


def load_plugin_schema(plugin_dir: Path) -> dict[str, Any]:
    """Load config schema from a plugin's config.json."""
    config_file = plugin_dir / 'config.json'
    if not config_file.exists():
        return {}

    try:
        schema = json.loads(config_file.read_text())
        return schema.get('properties', {})
    except json.JSONDecodeError:
        return {}


def get_env(key: str, default: str = '') -> str:
    """Get environment variable value."""
    return os.environ.get(key, default).strip()


def get_env_bool(key: str, default: bool = False) -> bool:
    """Get boolean from environment variable."""
    val = get_env(key, '').lower()
    if val in ('true', '1', 'yes', 'on'):
        return True
    if val in ('false', '0', 'no', 'off'):
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
    val = get_env(key, '')
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
    prop_type = prop.get('type', 'string')

    # Check direct env var
    env_val = os.environ.get(key)
    if env_val is not None:
        return _parse_value(env_val, prop_type)

    # Check aliases
    for alias in prop.get('x-aliases', []):
        env_val = os.environ.get(alias)
        if env_val is not None:
            return _parse_value(env_val, prop_type)

    # Check fallback
    fallback_key = prop.get('x-fallback')
    if fallback_key:
        env_val = os.environ.get(fallback_key)
        if env_val is not None:
            return _parse_value(env_val, prop_type)
        # Check global default for fallback key
        if fallback_key in GLOBAL_DEFAULTS:
            return GLOBAL_DEFAULTS[fallback_key]

    # Schema default
    if 'default' in prop:
        return prop['default']

    # Global default
    return GLOBAL_DEFAULTS.get(key)


def _parse_value(val: str, prop_type: str) -> Any:
    """Parse string value to appropriate type."""
    if prop_type == 'boolean':
        return val.lower() in ('true', '1', 'yes', 'on')
    elif prop_type == 'integer':
        try:
            return int(val)
        except ValueError:
            return 0
    elif prop_type == 'array':
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return []
    return val


def build_env_for_plugin(plugin_name: str, schema: dict[str, Any], overrides: dict[str, Any] | None = None) -> dict[str, str]:
    """
    Build environment variables dict for running a plugin.
    Exports all config as environment variables.
    """
    env = os.environ.copy()

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
            env[key] = _serialize_value(value)

    return env


def _serialize_value(value: Any) -> str:
    """Serialize value to string for environment variable."""
    if isinstance(value, bool):
        return 'True' if value else 'False'
    elif isinstance(value, list):
        return json.dumps(value)
    return str(value)
