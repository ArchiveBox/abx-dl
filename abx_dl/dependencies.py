"""
Dependency management for abx-dl using abx-pkg.
"""

from typing import Any

from abx_pkg import Binary, BinProvider, EnvProvider, PipProvider, NpmProvider, BrewProvider, AptProvider


DEFAULT_PROVIDERS: list[BinProvider] = [EnvProvider(), PipProvider(), NpmProvider(), BrewProvider(), AptProvider()]


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
