"""Fixtures for test isolation — prevent config/derived file pollution across tests."""

import importlib
import os
from pathlib import Path

import pytest
from platformdirs import user_config_path


# Keys that test hooks may mirror into runtime envs.
# Clean these from os.environ before each test so a previous test's side effects
# don't leak into subprocess env dicts built by PluginEnv.to_env().
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
        "ABXPKG_LIB_DIR",
        "ABXPKG_LIB_DIR",
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
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
    },
)


@pytest.fixture(autouse=True)
def isolated_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Point config.env and derived.env at a temp dir so tests don't share state."""
    # Remove any env vars leaked from prior tests before setting the isolated values.
    for key in _TEST_CONFIG_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key in list(os.environ):
        if key.endswith("_BINARY"):
            monkeypatch.delenv(key, raising=False)

    home_dir = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(home_dir / ".config"))
    config_dir = user_config_path("abx")
    data_dir = tmp_path / "data"
    personas_dir = config_dir / "personas"
    tmp_dir = tmp_path / "tmp"
    config_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    personas_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    lib_dir = config_dir / "lib"
    lib_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("ABXPKG_LIB_DIR", str(lib_dir))
    monkeypatch.setenv("PERSONAS_DIR", str(personas_dir))
    monkeypatch.setenv("TMP_DIR", str(tmp_dir))

    import abx_dl.config as config_mod
    import abx_dl.dependencies as dependencies_mod

    importlib.reload(config_mod)
    importlib.reload(dependencies_mod)

    yield
