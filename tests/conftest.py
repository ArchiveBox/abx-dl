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
def isolated_config(tmp_path: Path):
    """Point config.env and derived.env at a temp dir so tests don't share state."""
    original_cwd = Path.cwd()
    original_env = os.environ.copy()
    # Remove any env vars leaked from prior tests before setting the isolated values.
    for key in _TEST_CONFIG_KEYS:
        os.environ.pop(key, None)
    for key in list(os.environ):
        if key.endswith("_BINARY"):
            os.environ.pop(key, None)

    home_dir = tmp_path / "home"
    os.environ["HOME"] = str(home_dir)
    os.environ["XDG_CONFIG_HOME"] = str(home_dir / ".config")
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

    os.chdir(tmp_path)
    os.environ["CONFIG_DIR"] = str(config_dir)
    os.environ["DATA_DIR"] = str(data_dir)
    os.environ["ABXPKG_LIB_DIR"] = str(lib_dir)
    os.environ["PERSONAS_DIR"] = str(personas_dir)
    os.environ["TMP_DIR"] = str(tmp_dir)

    import abx_dl.config as config_mod
    import abx_dl.dependencies as dependencies_mod

    importlib.reload(config_mod)
    importlib.reload(dependencies_mod)

    try:
        yield
    finally:
        os.chdir(original_cwd)
        os.environ.clear()
        os.environ.update(original_env)
        importlib.reload(config_mod)
        importlib.reload(dependencies_mod)
