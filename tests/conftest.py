"""Fixtures for test isolation — prevent config/derived file pollution across tests."""

import os
from pathlib import Path

import pytest


# Keys that test hooks may mirror into runtime envs.
# Clean these from os.environ before each test so a previous test's side effects
# don't leak into subprocess env dicts built by build_env_for_plugin().
_TEST_CONFIG_KEYS = frozenset(
    {
        "CONFIG_DIR",
        "CRAWL_DIR",
        "DATA_DIR",
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
        "PERSONAS_DIR",
        "PIP_BIN_DIR",
        "PIP_HOME",
        "PUPPETEER_CACHE_DIR",
        "SNAP_DIR",
        "TMP_DIR",
    },
)


@pytest.fixture(autouse=True)
def isolated_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Point config.env and derived.env at a temp dir so tests don't share state."""
    home_dir = tmp_path / "home"
    config_dir = home_dir / ".config" / "abx"
    data_dir = tmp_path / "data"
    personas_dir = config_dir / "personas"
    tmp_dir = tmp_path / "tmp"
    config_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    personas_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
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
    monkeypatch.setattr(config_mod, "DERIVED_CONFIG_FILE", config_dir / "derived.env")
    monkeypatch.setattr(config_mod, "DATA_DIR", data_dir)
    monkeypatch.setattr(config_mod, "LIB_DIR", lib_dir)
    monkeypatch.setattr(config_mod, "LIB_BIN_DIR", lib_bin_dir)
    monkeypatch.setattr(config_mod, "PERSONAS_DIR", personas_dir)
    monkeypatch.setattr(config_mod, "TMP_DIR", tmp_dir)
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
            "DATA_DIR": str(data_dir),
            "CRAWL_DIR": str(data_dir),
            "SNAP_DIR": str(data_dir),
            "LIB_DIR": str(lib_dir),
            "LIB_BIN_DIR": str(lib_bin_dir),
            "PERSONAS_DIR": str(personas_dir),
            "TMP_DIR": str(tmp_dir),
            "PIP_HOME": str(pip_home),
            "PIP_BIN_DIR": str(pip_bin_dir),
            "NPM_HOME": str(npm_home),
            "NODE_MODULES_DIR": str(node_modules_dir),
            "NODE_PATH": str(node_modules_dir),
            "NPM_BIN_DIR": str(npm_bin_dir),
            "PUPPETEER_CACHE_DIR": str(lib_dir / "puppeteer"),
        },
    )

    # Remove any env vars leaked from prior tests before setting the isolated values.
    for key in _TEST_CONFIG_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key in list(os.environ):
        if key.endswith("_BINARY"):
            monkeypatch.delenv(key, raising=False)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("LIB_DIR", str(lib_dir))
    monkeypatch.setenv("PERSONAS_DIR", str(personas_dir))
    monkeypatch.setenv("TMP_DIR", str(tmp_dir))

    yield
