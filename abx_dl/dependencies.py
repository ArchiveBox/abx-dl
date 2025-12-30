"""
Dependency management for abx-dl using abx-pkg.

Handles detection and installation of binary dependencies for plugins.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from abx_pkg import Binary, BinProvider, EnvProvider

# Try to import optional providers
try:
    from abx_pkg import PipProvider
    HAS_PIP = True
except ImportError:
    HAS_PIP = False

try:
    from abx_pkg import NpmProvider
    HAS_NPM = True
except ImportError:
    HAS_NPM = False

try:
    from abx_pkg import BrewProvider
    HAS_BREW = True
except ImportError:
    HAS_BREW = False

try:
    from abx_pkg import AptProvider
    HAS_APT = True
except ImportError:
    HAS_APT = False


import platform

def get_arch() -> str:
    """Get architecture string like arm64-darwin or x86_64-linux."""
    machine = platform.machine().lower()
    system = platform.system().lower()
    return f"{machine}-{system}"

# Shared lib directory for installed dependencies
LIB_DIR = Path.home() / '.config' / 'abx-dl' / 'lib' / get_arch()


@dataclass
class BinaryInfo:
    """Information about a detected/installed binary."""
    name: str
    abspath: str | None
    version: str | None
    provider: str | None
    is_available: bool


class DependencyManager:
    """Manages binary dependencies for plugins."""

    def __init__(self, lib_dir: Path | None = None):
        self.lib_dir = lib_dir or LIB_DIR
        self.lib_dir.mkdir(parents=True, exist_ok=True)
        self._providers: dict[str, BinProvider] = {}
        self._cache: dict[str, BinaryInfo] = {}
        self._init_providers()

    def _init_providers(self):
        """Initialize available binary providers."""
        # EnvProvider always available - checks existing PATH
        self._providers['env'] = EnvProvider()

        # Optional providers
        if HAS_PIP:
            try:
                pip_venv = self.lib_dir / 'pip' / 'venv'
                self._providers['pip'] = PipProvider(pip_venv=pip_venv)
            except Exception:
                pass

        if HAS_NPM:
            try:
                npm_prefix = self.lib_dir / 'npm'
                self._providers['npm'] = NpmProvider(npm_prefix=npm_prefix)
            except Exception:
                pass

        if HAS_BREW:
            try:
                self._providers['brew'] = BrewProvider()
            except Exception:
                pass

        if HAS_APT:
            try:
                self._providers['apt'] = AptProvider()
            except Exception:
                pass

    def get_providers(self, provider_names: list[str] | None = None) -> list[BinProvider]:
        """Get list of providers by name, or all if none specified."""
        if not provider_names:
            return list(self._providers.values())

        providers = []
        for name in provider_names:
            name_lower = name.lower().strip()
            if name_lower in self._providers:
                providers.append(self._providers[name_lower])

        return providers or [self._providers['env']]

    def detect(self, bin_name: str, provider_names: list[str] | None = None) -> BinaryInfo:
        """Detect if a binary is available."""
        cache_key = f"{bin_name}:{','.join(provider_names or [])}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        providers = self.get_providers(provider_names)

        try:
            binary = Binary(name=bin_name, binproviders_supported=providers)
            loaded = binary.load()

            if loaded and loaded.loaded_abspath:
                info = BinaryInfo(
                    name=bin_name,
                    abspath=str(loaded.loaded_abspath),
                    version=str(loaded.loaded_version) if loaded.loaded_version else None,
                    provider=loaded.loaded_binprovider.name if loaded.loaded_binprovider else None,
                    is_available=True,
                )
            else:
                info = BinaryInfo(
                    name=bin_name,
                    abspath=None,
                    version=None,
                    provider=None,
                    is_available=False,
                )
        except Exception:
            info = BinaryInfo(
                name=bin_name,
                abspath=None,
                version=None,
                provider=None,
                is_available=False,
            )

        self._cache[cache_key] = info
        return info

    def install(self, bin_name: str, provider_names: list[str] | None = None,
                overrides: dict[str, Any] | None = None) -> BinaryInfo:
        """Install a binary using available providers."""
        providers = self.get_providers(provider_names)

        try:
            binary = Binary(
                name=bin_name,
                binproviders_supported=providers,
                overrides=overrides or {},
            )
            installed = binary.install()

            if installed and installed.loaded_abspath:
                info = BinaryInfo(
                    name=bin_name,
                    abspath=str(installed.loaded_abspath),
                    version=str(installed.loaded_version) if installed.loaded_version else None,
                    provider=installed.loaded_binprovider.name if installed.loaded_binprovider else None,
                    is_available=True,
                )
                cache_key = f"{bin_name}:{','.join(provider_names or [])}"
                self._cache[cache_key] = info
                return info
        except Exception:
            pass

        return BinaryInfo(
            name=bin_name,
            abspath=None,
            version=None,
            provider=None,
            is_available=False,
        )

    def ensure(self, bin_name: str, provider_names: list[str] | None = None,
               overrides: dict[str, Any] | None = None) -> BinaryInfo:
        """Ensure a binary is available, installing if needed."""
        info = self.detect(bin_name, provider_names)
        if info.is_available:
            return info
        return self.install(bin_name, provider_names, overrides)

    def check_plugin_dependencies(self, binaries: list[dict[str, Any]]) -> dict[str, BinaryInfo]:
        """Check all binary dependencies for a plugin."""
        results = {}

        for binary_spec in binaries:
            bin_name = binary_spec.get('name')
            if not bin_name:
                continue

            providers_str = binary_spec.get('binproviders', 'env')
            provider_names = [p.strip() for p in providers_str.split(',')]

            results[bin_name] = self.detect(bin_name, provider_names)

        return results

    def install_plugin_dependencies(self, binaries: list[dict[str, Any]]) -> dict[str, BinaryInfo]:
        """Install all binary dependencies for a plugin."""
        results = {}

        for binary_spec in binaries:
            bin_name = binary_spec.get('name')
            if not bin_name:
                continue

            providers_str = binary_spec.get('binproviders', 'env')
            provider_names = [p.strip() for p in providers_str.split(',')]
            overrides = binary_spec.get('overrides', {})

            results[bin_name] = self.ensure(bin_name, provider_names, overrides)

        return results
