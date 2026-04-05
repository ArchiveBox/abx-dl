import asyncio
from datetime import datetime, timezone
from pathlib import Path

from abx_dl.config import get_initial_env
from abx_dl.events import BinaryRequestEvent, InstallEvent, MachineEvent
from abx_dl.models import Snapshot, discover_plugins
from abx_dl.orchestrator import create_bus
from abx_dl.services.binary_service import BinaryService


def test_install_event_does_not_skip_stale_cached_binary_requests(tmp_path: Path) -> None:
    plugins = discover_plugins()
    plugin = plugins["ytdlp"]
    snapshot = Snapshot(url="")
    run_dir = tmp_path / "run"
    managed_lib_dir = tmp_path / "lib"
    stale_binary = managed_lib_dir / "pip" / "venv" / "bin" / "yt-dlp"
    bus = create_bus(total_timeout=60.0, name=f"install_phase_stale_cache_{tmp_path.name}")
    BinaryService(
        bus,
        plugins={"ytdlp": plugin},
        auto_install=False,
        install_plugins=[plugin],
        output_dir=run_dir,
        snapshot=snapshot,
    )
    request_events: list[BinaryRequestEvent] = []
    machine_events: list[MachineEvent] = []

    async def on_BinaryRequestEvent(event: BinaryRequestEvent) -> None:
        request_events.append(event)

    async def on_MachineEvent(event: MachineEvent) -> None:
        machine_events.append(event)

    bus.on(BinaryRequestEvent, on_BinaryRequestEvent)
    bus.on(MachineEvent, on_MachineEvent)

    async def run() -> None:
        await bus.emit(
            MachineEvent(
                config={
                    **get_initial_env(),
                    "LIB_DIR": str(managed_lib_dir),
                },
                config_type="user",
            ),
        )
        await bus.emit(
            MachineEvent(
                config={
                    "ABX_INSTALL_CACHE": {
                        "yt-dlp": datetime.now(timezone.utc).isoformat(),
                    },
                    "YTDLP_BINARY": str(stale_binary),
                },
                config_type="derived",
            ),
        )
        await bus.emit(
            InstallEvent(
                url="",
                snapshot_id=snapshot.id,
                output_dir=str(run_dir),
            ),
        )

    try:
        asyncio.run(run())
    finally:
        asyncio.run(bus.stop())

    assert any(event.plugin_name == "ytdlp" and event.name == "yt-dlp" for event in request_events)
    assert any(
        event.config_type == "derived" and event.method == "unset" and event.key == "config/YTDLP_BINARY" for event in machine_events
    )
    cache_update = next(
        event
        for event in reversed(machine_events)
        if event.config_type == "derived"
        and event.method == "update"
        and event.key == "config/ABX_INSTALL_CACHE"
        and isinstance(event.value, dict)
    )
    assert "yt-dlp" not in cache_update.value
