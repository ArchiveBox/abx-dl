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
    NpmProvider,
    DEFAULT_PROVIDER_NAMES,
    PROVIDER_CLASS_BY_NAME,
)  # DO NOT REMOVE UNUSED IMPORT, critical for pydantic circular reference fix

from .config import PIP_HOME, NPM_HOME

DEFAULT_PROVIDERS: list[BinProvider] = []

for provider_name in DEFAULT_PROVIDER_NAMES:
    provider_class = PROVIDER_CLASS_BY_NAME[provider_name]
    try:
        if provider_name == "env":
            DEFAULT_PROVIDERS.append(EnvProvider())
        elif provider_name == "pip":
            DEFAULT_PROVIDERS.append(PipProvider(install_root=PIP_HOME))
        elif provider_name == "npm":
            DEFAULT_PROVIDERS.append(NpmProvider(install_root=NPM_HOME))
        else:
            DEFAULT_PROVIDERS.append(provider_class())
    except Exception:
        pass


def load_binary(spec: dict[str, Any]) -> Binary:
    """Load a binary from a spec dict."""
    providers_str = spec.get("binproviders", "env")
    providers = [p for p in DEFAULT_PROVIDERS if p.name in providers_str.split(",")]
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
    providers_str = spec.get("binproviders", "env")
    providers = [p for p in DEFAULT_PROVIDERS if p.name in providers_str.split(",")]
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
        return binary.load_or_install()
    except Exception:
        return binary
