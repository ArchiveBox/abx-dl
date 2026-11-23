"""Loss of a real snapshot-isolated browser leaves its sibling runnable."""

import asyncio
from pathlib import Path

import psutil
from abxpkg.binary_service import BinaryService
from pytest_httpserver import HTTPServer

from abx_dl.catalog import PluginCatalog
from abx_dl.config import GlobalConfig, RuntimeConfig, get_explicit_user_env
from abx_dl.events import (
    CrawlEvent,
    CrawlStartEvent,
    MachineEvent,
    ProcessCompletedEvent,
    ProcessStartedEvent,
    SnapshotCompletedEvent,
    SnapshotEvent,
)
from abx_dl.models import Snapshot
from abx_dl.orchestrator import create_bus, install_plugins
from abx_dl.services.archive_result_service import ArchiveResultService
from abx_dl.services.binary_service import PluginBinaryEnvService
from abx_dl.services.crawl_lifecycle_service import CrawlLifecycleService, SnapshotAttemptError
from abx_dl.services.crawl_service import CrawlService
from abx_dl.services.process_service import ProcessService
from abx_dl.services.snapshot_service import SnapshotService


def test_required_snapshot_browser_loss_settles_only_that_snapshot(tmp_path: Path, httpserver: HTTPServer) -> None:
    for index in range(2):
        httpserver.expect_request(f"/{index}").respond_with_data(
            f"<!doctype html><title>Queued article {index}</title>",
            content_type="text/html",
        )
    catalog = PluginCatalog.discover().select(["title", "chrome"])
    assert "chrome" in catalog["title"].config.required_plugins
    output_dir = tmp_path / "crawl"
    snapshot = Snapshot(url=httpserver.url_for("/0"))
    config = {
        **get_explicit_user_env(),
        "CRAWL_DIR": str(output_dir),
        "TIMEOUT": 10,
        "CHROME_TIMEOUT": 5,
        "CHROME_HEADLESS": True,
        "CHROME_ISOLATION": "snapshot",
    }
    bus = create_bus(total_timeout=120.0, name=f"snapshot_resource_loss_{tmp_path.name}")
    terminal = []

    async def run_snapshots(event: CrawlStartEvent) -> None:
        for index in range(2):
            assert not await lifecycle.should_abort()
            item = Snapshot(url=httpserver.url_for(f"/{index}"))
            snapshot_dir = output_dir / str(index)
            service = SnapshotService(
                bus,
                url=item.url,
                snapshot=item,
                output_dir=snapshot_dir,
                catalog=catalog,
                config=RuntimeConfig(user=GlobalConfig(**config), derived={}),
                snapshot_phase_timeout=60.0,
                snapshot_cleanup_phase_timeout=60.0,
            )
            loss = asyncio.create_task(lose_browser(snapshot_dir)) if index == 0 else None
            try:
                attempt = event.emit(
                    SnapshotEvent(url=item.url, snapshot_id=item.id, output_dir=str(snapshot_dir), event_timeout=60.0),
                )
                await attempt.now()
                await attempt.wait()
                if loss is not None:
                    await loss
                completed = await bus.find(SnapshotCompletedEvent, child_of=attempt, past=True, future=False)
                assert completed is not None
                await completed.wait()
                await completed.event_results_list()
                terminal.append(completed)
            finally:
                if loss is not None and not loss.done():
                    loss.cancel()
                    await asyncio.gather(loss, return_exceptions=True)
                service.close()

    hook_name = "on_Snapshot__00_chrome_launch.daemon.bg"

    async def lose_browser(snapshot_dir):
        navigation = await bus.find(ProcessStartedEvent, hook_name="on_Snapshot__30_chrome_navigate", past=True, future=30.0)
        assert navigation is not None and navigation.subprocess.returncode is None
        launch = await bus.find(ProcessStartedEvent, hook_name=hook_name, past=True, future=False)
        assert launch is not None and launch.subprocess.returncode is None
        owner = psutil.Process(launch.pid)
        browser_pid = int((snapshot_dir / "chrome/chrome.pid").read_text())
        children = owner.children(recursive=True)
        assert browser_pid in {child.pid for child in children}
        owner.kill()
        for child in reversed(children):
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        _, alive = await asyncio.to_thread(psutil.wait_procs, children, timeout=5.0)
        assert not alive
        completed = await bus.find(ProcessCompletedEvent, hook_name=hook_name, pid=launch.pid, past=True, future=15)
        assert completed is not None and completed.exit_code == -9 and not completed.cancelled
        await completed.wait()
        await completed.event_results_list()

    lifecycle = CrawlLifecycleService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        crawl_setup_phase_timeout=60.0,
        snapshot_phase_timeout=60.0,
        crawl_cleanup_phase_timeout=60.0,
        run_snapshots=run_snapshots,
    )

    async def run() -> None:
        install_bus = create_bus(total_timeout=120.0, name=f"browser_install_{tmp_path.name}")
        try:
            await install_plugins(catalog, config=config, output_dir=output_dir, bus=install_bus)
        finally:
            await install_bus.destroy(clear=False)
        PluginBinaryEnvService(bus, catalog=catalog)
        BinaryService(bus, auto_install=True)
        processes = ProcessService(bus, emit_jsonl=False, interactive_tty=False)
        ArchiveResultService(bus, emit_jsonl=False)
        CrawlService(bus, url=snapshot.url, snapshot=snapshot, output_dir=output_dir, catalog=catalog)
        try:
            await bus.emit(MachineEvent(config=config, config_type="user")).now()
            root = bus.emit(
                CrawlEvent(url=snapshot.url, snapshot_id=snapshot.id, output_dir=str(output_dir), event_timeout=120.0),
            )
            await root.now()
            await root.wait()
            crawl_error = None
            try:
                await root.event_results_list()
            except SnapshotAttemptError as error:
                crawl_error = error
            assert len(terminal) == 2
            assert not terminal[1].error
            assert (output_dir / "1/title/title.txt").read_text().strip() == "Queued article 1"
            assert "Required snapshot resource chrome:" + hook_name in terminal[0].error
            assert crawl_error is not None
            assert not (output_dir / "0/title/title.txt").exists()
            attempted = await bus.filter(ProcessStartedEvent, child_of=root, past=True, future=False)
            assert not any(row.plugin_name == "title" and Path(row.output_dir).parent == output_dir / "0" for row in attempted)
        finally:
            await processes.stop()
            await bus.destroy(clear=False)

    asyncio.run(run())
