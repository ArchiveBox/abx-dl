"""Fixtures for test isolation — prevent config file pollution across tests."""

from pathlib import Path

import pytest


# Keys that test hooks write via MachineEvent → set_config.
# Clean these from os.environ before each test so a previous test's side effects
# don't leak into subprocess env dicts built by build_env_for_plugin().
_TEST_CONFIG_KEYS = frozenset({
    'DEMO_BINARY', 'DEMO_FLAG', 'HOOK_ORDER',
    'NODE_MODULES_DIR', 'NODE_PATH',
})


@pytest.fixture(autouse=True)
def isolated_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Point CONFIG_DIR/CONFIG_FILE at a temp dir so tests don't share state."""
    config_dir = tmp_path / '.config' / 'abx'
    config_dir.mkdir(parents=True, exist_ok=True)

    # Patch the config module's globals before anything writes/reads them.
    import abx_dl.config as config_mod
    monkeypatch.setattr(config_mod, 'CONFIG_DIR', config_dir)
    monkeypatch.setattr(config_mod, 'CONFIG_FILE', config_dir / 'config.env')

    # Remove any env vars leaked from prior tests.
    for key in _TEST_CONFIG_KEYS:
        monkeypatch.delenv(key, raising=False)

    yield
