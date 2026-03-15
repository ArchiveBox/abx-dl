from pathlib import Path

from abx_dl.config import build_env_for_plugin


def test_build_env_for_plugin_sets_run_dirs_and_node_path(monkeypatch, tmp_path: Path) -> None:
    for key in (
        'CRAWL_DIR',
        'SNAP_DIR',
        'LIB_DIR',
        'NPM_HOME',
        'NODE_MODULES_DIR',
        'NODE_PATH',
        'NPM_BIN_DIR',
        'PIP_HOME',
        'PIP_BIN_DIR',
    ):
        monkeypatch.delenv(key, raising=False)

    env = build_env_for_plugin('demo', {}, run_output_dir=tmp_path)

    assert env['CRAWL_DIR'] == str(tmp_path)
    assert env['SNAP_DIR'] == str(tmp_path)
    assert env['NODE_PATH'] == env['NODE_MODULES_DIR']
    assert env['PIP_BIN_DIR'] in env['PATH'].split(':')
    assert env['NPM_BIN_DIR'] in env['PATH'].split(':')


def test_build_env_for_plugin_derives_puppeteer_cache_from_effective_lib_dir(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv('PUPPETEER_CACHE_DIR', raising=False)

    env = build_env_for_plugin('demo', {}, overrides={'LIB_DIR': str(tmp_path / 'lib')}, run_output_dir=tmp_path)

    assert env['PUPPETEER_CACHE_DIR'] == str(tmp_path / 'lib' / 'puppeteer')


def test_build_env_for_plugin_keeps_chrome_sandbox_enabled_by_default(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv('CHROME_SANDBOX', raising=False)

    env = build_env_for_plugin('demo', {}, run_output_dir=tmp_path)

    assert env['CHROME_SANDBOX'] == 'true'
