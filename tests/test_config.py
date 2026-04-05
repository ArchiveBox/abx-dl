import asyncio
from pathlib import Path
from typing import Any

from abx_dl.config import GlobalConfig, get_plugin_env
from abx_dl.models import PluginEnv, discover_plugins
from abx_dl.orchestrator import create_bus, install_plugins


def assemble_env(*, overrides: dict[str, Any] | None = None, run_output_dir: Path) -> dict[str, str]:
    config = GlobalConfig(**(overrides or {})).model_dump(mode="json")
    return PluginEnv.from_config(config, run_output_dir=run_output_dir).to_env()


def test_plugin_env_sets_run_dirs_and_node_path(monkeypatch, tmp_path: Path) -> None:
    for key in (
        "CRAWL_DIR",
        "SNAP_DIR",
        "LIB_DIR",
        "LIB_BIN_DIR",
        "NPM_HOME",
        "NODE_MODULES_DIR",
        "NODE_PATH",
        "NPM_BIN_DIR",
        "PIP_HOME",
        "PIP_BIN_DIR",
        "VIRTUAL_ENV",
    ):
        monkeypatch.delenv(key, raising=False)

    env = assemble_env(run_output_dir=tmp_path)

    assert env["CRAWL_DIR"] == str(tmp_path)
    assert env["SNAP_DIR"] == str(tmp_path)
    assert env["LIB_BIN_DIR"] == str(Path(env["LIB_DIR"]) / "bin")
    assert env["NODE_PATH"] == env["NODE_MODULES_DIR"]
    assert env["PIP_BIN_DIR"] in env["PATH"].split(":")
    assert env["NPM_BIN_DIR"] in env["PATH"].split(":")
    assert "VIRTUAL_ENV" not in env


def test_plugin_env_exports_shared_runtime_paths_after_real_install_phase(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("VIRTUAL_ENV", raising=False)
    run_output_dir = tmp_path / "run"
    plugins = discover_plugins()
    bus = create_bus(total_timeout=300.0, name=f"test_config_shared_runtime_{tmp_path.name}")
    try:
        asyncio.run(
            install_plugins(
                plugin_names=["ytdlp", "puppeteer"],
                plugins=plugins,
                output_dir=run_output_dir,
                bus=bus,
            ),
        )
        ytdlp_env = get_plugin_env(
            bus,
            plugin=plugins["ytdlp"],
            run_output_dir=run_output_dir,
        ).to_env()
    finally:
        asyncio.run(bus.stop())

    lib_dir = Path(ytdlp_env["LIB_DIR"])
    pip_venv = lib_dir / "pip" / "venv"
    npm_prefix = lib_dir / "npm"
    npm_bin_dir = npm_prefix / "node_modules" / ".bin"

    assert ytdlp_env["VIRTUAL_ENV"] == str(pip_venv)
    assert ytdlp_env["PIP_BIN_DIR"] == str(pip_venv / "bin")
    assert (Path(ytdlp_env["YTDLP_BINARY"]).expanduser()).is_file()
    assert Path(ytdlp_env["YTDLP_BINARY"]).is_relative_to(pip_venv)
    assert (pip_venv / "pyvenv.cfg").is_file()
    assert ytdlp_env["NPM_HOME"] == str(npm_prefix)
    assert ytdlp_env["NODE_MODULES_DIR"] == str(npm_prefix / "node_modules")
    assert ytdlp_env["NODE_PATH"] == str(npm_prefix / "node_modules")
    assert ytdlp_env["NPM_BIN_DIR"] == str(npm_bin_dir)
    assert (npm_bin_dir / "puppeteer").is_file()
    assert npm_bin_dir.is_dir()
    assert ytdlp_env["NPM_BIN_DIR"] in ytdlp_env["PATH"].split(":")


def test_plugin_env_derives_puppeteer_cache_from_effective_lib_dir(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("PUPPETEER_CACHE_DIR", raising=False)

    env = assemble_env(overrides={"LIB_DIR": str(tmp_path / "lib")}, run_output_dir=tmp_path)

    assert env["PUPPETEER_CACHE_DIR"] == str(tmp_path / "lib" / "puppeteer")
    assert env["LIB_BIN_DIR"] == str(tmp_path / "lib" / "bin")


def test_plugin_env_keeps_chrome_sandbox_enabled_by_default(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("CHROME_SANDBOX", raising=False)

    env = assemble_env(run_output_dir=tmp_path)

    assert env["CHROME_SANDBOX"] == "true"


def test_plugin_env_strips_uv_recursion_depth_from_env_and_overrides(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("UV_RUN_RECURSION_DEPTH", "1")

    env = assemble_env(overrides={"UV_RUN_RECURSION_DEPTH": True}, run_output_dir=tmp_path)

    assert "UV_RUN_RECURSION_DEPTH" not in env


def test_plugin_env_run_output_dir_overrides_default_shared_snap_dirs(tmp_path: Path) -> None:
    env = assemble_env(
        overrides={
            "DATA_DIR": str(tmp_path / "data"),
            "CRAWL_DIR": str(tmp_path / "data"),
            "SNAP_DIR": str(tmp_path / "data"),
        },
        run_output_dir=tmp_path / "run",
    )

    assert env["CRAWL_DIR"] == str(tmp_path / "run")
    assert env["SNAP_DIR"] == str(tmp_path / "run")


def test_plugin_env_preserves_explicit_shared_snap_dir_override(tmp_path: Path) -> None:
    explicit_snap_dir = tmp_path / "explicit-snap"
    env = assemble_env(
        overrides={
            "DATA_DIR": str(tmp_path / "data"),
            "CRAWL_DIR": str(explicit_snap_dir),
            "SNAP_DIR": str(explicit_snap_dir),
        },
        run_output_dir=tmp_path / "run",
    )

    assert env["CRAWL_DIR"] == str(explicit_snap_dir)
    assert env["SNAP_DIR"] == str(explicit_snap_dir)


def test_plugin_env_omits_none_runtime_overrides(tmp_path: Path) -> None:
    env = assemble_env(
        overrides={
            "SNAPSHOT_DEPTH": None,
            "PARENT_SNAPSHOT_ID": None,
        },
        run_output_dir=tmp_path,
    )

    assert "SNAPSHOT_DEPTH" not in env
    assert "PARENT_SNAPSHOT_ID" not in env
