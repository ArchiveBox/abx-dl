"""
Dependency management for abx-dl using abxpkg.
"""

from typing import Any, cast

from abxpkg import (
    Binary,
    BinaryOverrides,
    BinProvider,
    EnvProvider,
    PipProvider,
    PnpmProvider,
    DEFAULT_PROVIDER_NAMES,
    PROVIDER_CLASS_BY_NAME,
)  # DO NOT REMOVE UNUSED IMPORT, critical for pydantic circular reference fix
from abxpkg.binprovider import env_flag_is_true

from .config import GlobalConfig


def get_default_providers(config: GlobalConfig | None = None) -> list[BinProvider]:
    """Build providers from the current runtime config."""
    runtime_config = config or GlobalConfig()
    providers: list[BinProvider] = []
    for provider_name in DEFAULT_PROVIDER_NAMES:
        provider_class = PROVIDER_CLASS_BY_NAME[provider_name]
        try:
            if provider_name == "env":
                providers.append(EnvProvider())
            elif provider_name == "pip":
                providers.append(PipProvider(install_root=runtime_config.PIP_HOME))
            elif provider_name == "pnpm":
                providers.append(PnpmProvider(install_root=runtime_config.PNPM_HOME))
            else:
                providers.append(provider_class())
        except Exception:
            pass
    return providers


def _providers_for_spec(spec: dict[str, Any], *, config: GlobalConfig | None = None) -> list[BinProvider]:
    providers_str = spec.get("binproviders", "env")
    requested_names = providers_str.split(",")
    return [provider for provider in get_default_providers(config) if provider.name in requested_names]


def load_binary(spec: dict[str, Any]) -> Binary:
    """Load a binary from a spec dict."""
    providers = _providers_for_spec(spec)
    overrides = spec.get("overrides", {})
    if isinstance(overrides, dict):
        overrides = {provider: ({"install_args": value} if isinstance(value, list) else value) for provider, value in overrides.items()}
    overrides = cast(BinaryOverrides, overrides)
    min_version = spec.get("min_version") or None

    binary = Binary(
        name=spec["name"],
        min_version=min_version,
        binproviders=providers,
        overrides=overrides,
    )

    try:
        return binary.load()
    except Exception:
        return binary


def install_binary(spec: dict[str, Any]) -> Binary:
    """Load or install a binary from a spec dict."""
    providers = _providers_for_spec(spec)
    overrides = spec.get("overrides", {})
    if isinstance(overrides, dict):
        overrides = {provider: ({"install_args": value} if isinstance(value, list) else value) for provider, value in overrides.items()}
    overrides = cast(BinaryOverrides, overrides)
    min_version = spec.get("min_version") or None

    binary = Binary(
        name=spec["name"],
        min_version=min_version,
        binproviders=providers,
        overrides=overrides,
    )

    try:
        return binary.install(no_cache=env_flag_is_true("ABXPKG_NO_CACHE"))
    except Exception:
        return binary
