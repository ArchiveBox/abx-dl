import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pytest
from abxpkg.binary_service import BinaryEvent
from abx_dl.config import GlobalConfig, RuntimeConfig, get_config, get_initial_env, get_plugin_env
from abx_dl.events import MachineEvent
from abx_dl.models import PluginEnv, discover_plugins
from abx_dl.orchestrator import create_bus, install_plugins
from abx_dl.services.binary_service import build_plugin_process_env
from platformdirs import user_config_path


def assemble_env(*, overrides: dict[str, Any] | None = None, run_output_dir: Path) -> dict[str, str]:
    config = GlobalConfig(**(overrides or {})).model_dump(mode="json")
    return PluginEnv.from_config(config, run_output_dir=run_output_dir).to_env()


@pytest.mark.parametrize("_fixture_case", range(2))
def test_isolated_config_shares_managed_binaries_but_isolates_mutable_state(
    _fixture_case: int,
    tmp_path: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    session_lib_dir = Path(os.environ["_ABX_DL_TEST_ABXPKG_LIB_DIR"])

    assert Path(os.environ["ABXPKG_LIB_DIR"]) == session_lib_dir
    assert Path(os.environ["CONFIG_DIR"]).is_relative_to(tmp_path)
    assert Path(os.environ["DATA_DIR"]).is_relative_to(tmp_path)
    assert Path(os.environ["PERSONAS_DIR"]).is_relative_to(tmp_path)
    assert Path(os.environ["TMP_DIR"]).is_relative_to(tmp_path)


def test_plugin_env_sets_run_dirs_without_projecting_binary_paths(tmp_path: Path) -> None:
    for key in (
        "CRAWL_DIR",
        "SNAP_DIR",
        "ABXPKG_LIB_DIR",
        "PNPM_HOME",
        "PNPM_BIN_DIR",
        "NPM_HOME",
        "NODE_MODULES_DIR",
        "NODE_PATH",
        "NPM_BIN_DIR",
        "PIP_HOME",
        "PIP_BIN_DIR",
        "VIRTUAL_ENV",
    ):
        os.environ.pop(key, None)

    env = assemble_env(run_output_dir=tmp_path)
    expected_config_dir = user_config_path("abx")
    expected_lib_dir = expected_config_dir / "lib"

    assert Path(env["CONFIG_DIR"]) == expected_config_dir
    assert env["CRAWL_DIR"] == str(tmp_path)
    assert env["SNAP_DIR"] == str(tmp_path)
    assert Path(env["ABXPKG_LIB_DIR"]) == expected_lib_dir
    assert "NODE_MODULES_DIR" not in env
    assert "NODE_PATH" not in env
    assert "PIP_BIN_DIR" not in env
    assert "PNPM_BIN_DIR" not in env
    assert "NPM_BIN_DIR" not in env
    assert "PUPPETEER_CACHE_DIR" not in env
    assert "VIRTUAL_ENV" not in env
    assert env["PATH"] == os.environ["PATH"]


def test_plugin_timeout_defaults_only_yield_to_explicit_global_timeout(tmp_path: Path) -> None:
    plugin = discover_plugins()["claudecodecleanup"]
    default_user = GlobalConfig.__pydantic_validator__.validate_python({})
    override_user = GlobalConfig.__pydantic_validator__.validate_python({"TIMEOUT": 90})

    default_env = asyncio.run(
        get_plugin_env(
            None,
            plugin=plugin,
            run_output_dir=tmp_path,
            config=RuntimeConfig(user=default_user, derived={}),
        ),
    )
    override_env = asyncio.run(
        get_plugin_env(
            None,
            plugin=plugin,
            run_output_dir=tmp_path,
            config=RuntimeConfig(user=override_user, derived={}),
        ),
    )

    assert default_env.TIMEOUT == 60
    assert default_env["CLAUDECODE_TIMEOUT"] == 120
    assert default_env["CLAUDECODECLEANUP_TIMEOUT"] == 180
    assert override_env.TIMEOUT == 90
    assert override_env["CLAUDECODE_TIMEOUT"] == 90
    assert override_env["CLAUDECODECLEANUP_TIMEOUT"] == 90


def test_plugin_timeout_defaults_without_event_bus(tmp_path: Path) -> None:
    plugin = discover_plugins()["claudecodecleanup"]

    env = asyncio.run(
        get_plugin_env(
            None,
            plugin=plugin,
            run_output_dir=tmp_path,
        ),
    )

    assert env.TIMEOUT == 60
    assert env["CLAUDECODE_TIMEOUT"] == 120
    assert env["CLAUDECODECLEANUP_TIMEOUT"] == 180


def test_machine_event_config_rebuild_applies_events_oldest_to_newest(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=10.0, name=f"test_config_machine_event_order_{tmp_path.name}")

    async def run() -> RuntimeConfig:
        await bus.emit(
            MachineEvent(config={"FAVICON_PROVIDER": "first", "ABXPKG_LIB_DIR": str(tmp_path / "lib-a")}, config_type="user"),
        ).now()
        await bus.emit(MachineEvent(config={"FAVICON_PROVIDER": "second"}, config_type="user")).now()
        await bus.emit(MachineEvent(config={"ABXPKG_LIB_DIR": str(tmp_path / "lib-b")}, config_type="derived")).now()
        await bus.emit(MachineEvent(config={"ABXPKG_LIB_DIR": str(tmp_path / "lib-c")}, config_type="derived")).now()
        return await get_config(bus)

    try:
        runtime_config = asyncio.run(run())
    finally:
        asyncio.run(bus.wait_until_idle())

    assert runtime_config.user["FAVICON_PROVIDER"] == "second"
    assert runtime_config.derived["ABXPKG_LIB_DIR"] == str(tmp_path / "lib-c")


def test_plugin_env_exports_abxpkg_runtime_after_real_install_phase(tmp_path: Path) -> None:
    os.environ.pop("VIRTUAL_ENV", None)
    run_output_dir = tmp_path / "run"
    plugins = discover_plugins()
    bus = create_bus(total_timeout=300.0, name=f"test_config_shared_runtime_{tmp_path.name}")

    async def run() -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
        await install_plugins(
            plugin_names=["ytdlp"],
            plugins=plugins,
            output_dir=run_output_dir,
            bus=bus,
        )
        base_env = (
            await get_plugin_env(
                bus,
                plugin=plugins["ytdlp"],
                run_output_dir=run_output_dir,
            )
        ).to_env()
        process_env = await build_plugin_process_env(
            bus,
            plugins=plugins,
            plugin=plugins["ytdlp"],
            runtime_env=base_env,
        )
        binary_events = await bus.filter(
            BinaryEvent,
            past=True,
            where=lambda event: event.name == "yt-dlp",
        )
        assert binary_events
        return base_env, process_env, binary_events[-1].env

    try:
        base_env, process_env, binary_env = asyncio.run(run())
    finally:
        asyncio.run(bus.wait_until_idle())

    ytdlp_binary = shutil.which(process_env["YTDLP_BINARY"], path=process_env["PATH"])
    assert "VIRTUAL_ENV" not in base_env
    assert ytdlp_binary
    ytdlp_path = Path(ytdlp_binary)
    assert ytdlp_path.is_file()
    assert binary_env.get("PATH")
    assert str(ytdlp_path.parent) in process_env["PATH"].split(os.pathsep)


def test_plugin_env_keeps_chrome_sandbox_enabled_by_default(tmp_path: Path) -> None:
    env = assemble_env(run_output_dir=tmp_path)

    assert env["CHROME_SANDBOX"] == "true"


def test_plugin_env_strips_uv_recursion_depth_from_env_and_overrides(tmp_path: Path) -> None:
    os.environ["UV_RUN_RECURSION_DEPTH"] = "1"
    try:
        env = assemble_env(overrides={"UV_RUN_RECURSION_DEPTH": True}, run_output_dir=tmp_path)
    finally:
        os.environ.pop("UV_RUN_RECURSION_DEPTH")

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


def test_plugin_env_preserves_user_runtime_dirs_when_derived_config_has_defaults(
    tmp_path: Path,
) -> None:
    for key in (
        "ABXPKG_LIB_DIR",
        "PNPM_HOME",
        "PNPM_BIN_DIR",
        "NPM_HOME",
        "NODE_MODULES_DIR",
        "NODE_PATH",
        "NPM_BIN_DIR",
        "PUPPETEER_CACHE_DIR",
    ):
        os.environ.pop(key, None)

    plugins = discover_plugins()
    bus = create_bus(total_timeout=60.0, name=f"test_config_derived_runtime_dirs_{tmp_path.name}")
    data_dir = tmp_path / "data"
    crawl_dir = tmp_path / "crawl"
    snap_dir = tmp_path / "snap"
    lib_dir = tmp_path / "data" / "lib" / "runtime"
    run_dir = tmp_path / "run"

    async def emit_runtime_config() -> None:
        await bus.emit(
            MachineEvent(
                config={
                    "DATA_DIR": str(data_dir),
                    "CRAWL_DIR": str(crawl_dir),
                    "SNAP_DIR": str(snap_dir),
                    "ABXPKG_LIB_DIR": str(lib_dir),
                },
                config_type="user",
            ),
        ).now()
        await bus.emit(MachineEvent(config={}, config_type="derived")).now()

    try:
        asyncio.run(emit_runtime_config())
        env = asyncio.run(
            get_plugin_env(
                bus,
                plugin=plugins["chrome"],
                run_output_dir=run_dir,
            ),
        ).to_env()
    finally:
        asyncio.run(bus.wait_until_idle())

    assert env["DATA_DIR"] == str(data_dir)
    assert env["CRAWL_DIR"] == str(crawl_dir)
    assert env["SNAP_DIR"] == str(snap_dir)
    assert env["ABXPKG_LIB_DIR"] == str(lib_dir)
    assert "PNPM_HOME" not in env
    assert "NPM_HOME" not in env
    assert env["NODE_MODULES_DIR"] == ""
    assert env["NODE_PATH"] == ""
    assert "PUPPETEER_CACHE_DIR" not in env


def test_plugin_env_ignores_direct_chrome_profile_env(
    tmp_path: Path,
) -> None:
    configured_profile = str(tmp_path / "stale" / "chrome_profile")

    plugins = discover_plugins()
    bus = create_bus(total_timeout=60.0, name=f"test_config_archivebox_runtime_{tmp_path.name}")

    async def emit_runtime_config() -> None:
        await bus.emit(
            MachineEvent(
                config={
                    "ABX_RUNTIME": "archivebox",
                    "DATA_DIR": str(tmp_path / "data"),
                    "CHROME_USER_DATA_DIR": configured_profile,
                },
                config_type="user",
            ),
        ).now()

    try:
        asyncio.run(emit_runtime_config())
        env = asyncio.run(
            get_plugin_env(
                bus,
                plugin=plugins["chrome"],
                run_output_dir=tmp_path / "crawl",
            ),
        ).to_env()
    finally:
        asyncio.run(bus.wait_until_idle())

    assert "CHROME_USER_DATA_DIR" not in env
    assert env["PERSONAS_DIR"] == str(tmp_path / "crawl" / ".persona")
    assert env["ACTIVE_PERSONA"] == "Default"


def test_plugin_env_preserves_explicit_personas_dir(tmp_path: Path) -> None:
    explicit_personas_dir = tmp_path / "personas"

    env = assemble_env(
        overrides={"PERSONAS_DIR": str(explicit_personas_dir)},
        run_output_dir=tmp_path / "crawl",
    )

    assert env["PERSONAS_DIR"] == str(explicit_personas_dir)


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


def test_plugin_env_accepts_structured_extra_context_from_machine_events(tmp_path: Path) -> None:
    plugins = discover_plugins()
    bus = create_bus(total_timeout=60.0, name=f"test_config_extra_context_{tmp_path.name}")

    async def emit_runtime_config() -> None:
        await bus.emit(MachineEvent(config=get_initial_env(), config_type="user")).now()
        await bus.emit(
            MachineEvent(
                config={"EXTRA_CONTEXT": {"snapshot_id": "test-snapshot", "snapshot_depth": 0}},
                config_type="user",
            ),
        ).now()

    try:
        asyncio.run(emit_runtime_config())
        env = asyncio.run(
            get_plugin_env(
                bus,
                plugin=plugins["favicon"],
                run_output_dir=tmp_path,
                extra_context={"plugin": "favicon", "hook_name": "on_Snapshot__37_favicon.finite.bg"},
            ),
        ).to_env()
    finally:
        asyncio.run(bus.wait_until_idle())

    extra_context = json.loads(env["EXTRA_CONTEXT"])
    assert extra_context["snapshot_id"] == "test-snapshot"
    assert extra_context["snapshot_depth"] == 0
    assert extra_context["plugin"] == "favicon"
