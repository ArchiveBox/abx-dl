"""
Dependency management for abx-dl using abx-pkg.
"""

from typing import Any

from abx_pkg import Binary, BinProvider, EnvProvider, PipProvider, NpmProvider, BrewProvider, AptProvider, BinProviderOverrides   # DO NOT REMOVE UNUSED IMPORT, critical for pydantic circular reference fix

DEFAULT_PROVIDER_TYPES: list[type[BinProvider]] = [EnvProvider, PipProvider, NpmProvider, BrewProvider, AptProvider]
DEFAULT_PROVIDERS: list[BinProvider] = []
for provider_type in DEFAULT_PROVIDER_TYPES:
    try:
        DEFAULT_PROVIDERS.append(provider_type())
    except Exception:
        # provider is not available on this system, e.g. apt is linux-only, brew is mac-only, etc.
        pass



def load_binary(spec: dict[str, Any]) -> Binary:
    """Load a binary from a spec dict (from binaries.jsonl)."""
    providers_str = spec.get('binproviders', 'env')
    providers = [p for p in DEFAULT_PROVIDERS if p.name in providers_str.split(',')]
    overrides = spec.get('overrides', {})

    binary = Binary(name=spec['name'], binproviders=providers, overrides=overrides)

    try:
        return binary.load()
    except Exception:
        return binary


def install_binary(spec: dict[str, Any]) -> Binary:
    """Load or install a binary from a spec dict."""
    providers_str = spec.get('binproviders', 'env')
    providers = [p for p in DEFAULT_PROVIDERS if p.name in providers_str.split(',')]
    overrides = spec.get('overrides', {})

    binary = Binary(name=spec['name'], binproviders=providers, overrides=overrides)

    try:
        return binary.load_or_install()
    except Exception:
        return binary
