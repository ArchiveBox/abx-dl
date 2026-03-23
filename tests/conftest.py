"""Fixtures for test isolation — prevent config file pollution across tests."""

import os
from pathlib import Path

import pytest


# Keys that test hooks write via MachineEvent → set_config.
# Clean these from os.environ before each test so a previous test's side effects
# don't leak into subprocess env dicts built by build_env_for_plugin().
_TEST_CONFIG_KEYS = frozenset(
    {
        "DEMO_BINARY",
        "DEMO_TOOL_BINARY",
        "DEMO_FLAG",
        "HOOK_ORDER",
        "JAVA_VERSION_CONSTRAINT",
        "LIB_BIN_DIR",
        "LIB_DIR",
        "NPM_BIN_DIR",
        "NPM_HOME",
        "NODE_MODULES_DIR",
        "NODE_PATH",
        "PIP_BIN_DIR",
        "PIP_HOME",
        "PUPPETEER_CACHE_DIR",
    },
)


@pytest.fixture(autouse=True)
def isolated_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Point CONFIG_DIR/CONFIG_FILE at a temp dir so tests don't share state."""
    config_dir = tmp_path / ".config" / "abx"
    config_dir.mkdir(parents=True, exist_ok=True)
    lib_dir = config_dir / "lib"
    lib_bin_dir = lib_dir / "bin"
    pip_home = lib_dir / "pip"
    pip_bin_dir = pip_home / "venv" / "bin"
    npm_home = lib_dir / "npm"
    node_modules_dir = npm_home / "node_modules"
    npm_bin_dir = node_modules_dir / ".bin"
    lib_bin_dir.mkdir(parents=True, exist_ok=True)

    # Patch the config module's globals before anything writes/reads them.
    import abx_dl.config as config_mod

    monkeypatch.setattr(config_mod, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(config_mod, "CONFIG_FILE", config_dir / "config.env")
    monkeypatch.setattr(config_mod, "LIB_DIR", lib_dir)
    monkeypatch.setattr(config_mod, "LIB_BIN_DIR", lib_bin_dir)
    monkeypatch.setattr(config_mod, "PIP_HOME", pip_home)
    monkeypatch.setattr(config_mod, "PIP_BIN_DIR", pip_bin_dir)
    monkeypatch.setattr(config_mod, "NPM_HOME", npm_home)
    monkeypatch.setattr(config_mod, "NODE_MODULES_DIR", node_modules_dir)
    monkeypatch.setattr(config_mod, "NPM_BIN_DIR", npm_bin_dir)
    monkeypatch.setattr(
        config_mod,
        "GLOBAL_DEFAULTS",
        {
            **config_mod.GLOBAL_DEFAULTS,
            "CONFIG_DIR": str(config_dir),
            "LIB_DIR": str(lib_dir),
            "LIB_BIN_DIR": str(lib_bin_dir),
            "PIP_HOME": str(pip_home),
            "PIP_BIN_DIR": str(pip_bin_dir),
            "NPM_HOME": str(npm_home),
            "NODE_MODULES_DIR": str(node_modules_dir),
            "NODE_PATH": str(node_modules_dir),
            "NPM_BIN_DIR": str(npm_bin_dir),
            "PUPPETEER_CACHE_DIR": str(lib_dir / "puppeteer"),
        },
    )
    monkeypatch.setenv("CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("LIB_DIR", str(lib_dir))
    monkeypatch.setenv("LIB_BIN_DIR", str(lib_bin_dir))
    monkeypatch.setenv("PIP_HOME", str(pip_home))
    monkeypatch.setenv("PIP_BIN_DIR", str(pip_bin_dir))
    monkeypatch.setenv("NPM_HOME", str(npm_home))
    monkeypatch.setenv("NODE_MODULES_DIR", str(node_modules_dir))
    monkeypatch.setenv("NODE_PATH", str(node_modules_dir))
    monkeypatch.setenv("NPM_BIN_DIR", str(npm_bin_dir))
    monkeypatch.setenv("PUPPETEER_CACHE_DIR", str(lib_dir / "puppeteer"))

    # Remove any env vars leaked from prior tests.
    for key in _TEST_CONFIG_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key in list(os.environ):
        if key.endswith("_BINARY"):
            monkeypatch.delenv(key, raising=False)

    yield
