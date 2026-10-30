"""Loss of an owned shared browser must not consume the rest of a crawl queue."""

import asyncio
import json
from pathlib import Path

import psutil
import pytest
from abxpkg.binary_service import BinaryService
from pytest_httpserver import HTTPServer

from abx_dl.catalog import PluginCatalog
from abx_dl.config import GlobalConfig, RuntimeConfig, get_explicit_user_env
from abx_dl.events import (
    ArchiveResultEvent,
    CrawlCompletedEvent,
    CrawlEvent,
    CrawlSetupEvent,
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
from abx_dl.services.crawl_lifecycle_service import CrawlLifecycleService
from abx_dl.services.crawl_service import CrawlService
from abx_dl.services.process_service import ProcessService
from abx_dl.services.snapshot_service import SnapshotService


@pytest.mark.parametrize(
    "resource_plugin,loss_phase",
    [("chrome", "during_snapshot"), ("chrome", "before_admission"), ("chrome_screencast", "during_snapshot")],
)
def test_required_crawl_browser_loss_stops_new_snapshot_admission(
    tmp_path: Path,
    httpserver: HTTPServer,
    resource_plugin: str,
    loss_phase: str,
) -> None:
    for index in range(2):
        httpserver.expect_request(f"/{index}").respond_with_data(
            f"<!doctype html><title>Queued article {index}</title><p>Original article {index}</p>",
            content_type="text/html",
        )
    catalog = PluginCatalog.discover().select(["title", resource_plugin])
    assert "chrome" in catalog["title"].config.required_plugins
    required = resource_plugin == "chrome"
    launch_hook = "on_CrawlSetup__90_chrome_launch.daemon.bg" if required else "on_Snapshot__02_chrome_screencast.daemon.bg"
    if not required:
        assert all(resource_plugin not in plugin.config.required_plugins for plugin in catalog.values())
    output_dir = tmp_path / "crawl"
    root_snapshot = Snapshot(url=httpserver.url_for("/0"))
    config = {
        **get_explicit_user_env(),
        "CRAWL_DIR": str(output_dir),
        "TIMEOUT": 10,
        "CHROME_TIMEOUT": 5,
        "CHROME_HEADLESS": True,
        "CHROME_ISOLATION": "crawl",
    }
    bus = create_bus(total_timeout=120.0, name=f"required_browser_loss_{tmp_path.name}")
    admitted = []
    killed = []

    async def run_snapshots(event: CrawlStartEvent) -> None:
        for index in range(2):
            # Embedders already consult the owner's admission signal. A failed
            # required resource must reach that boundary before another URL.
            if await lifecycle.should_abort():
                break
            snapshot = Snapshot(url=httpserver.url_for(f"/{index}"))
            snapshot_dir = output_dir / str(index)
            service = SnapshotService(
                bus,
                url=snapshot.url,
                snapshot=snapshot,
                output_dir=snapshot_dir,
                catalog=catalog,
                config=RuntimeConfig(user=GlobalConfig(**config), derived={}),
                snapshot_phase_timeout=60.0,
                snapshot_cleanup_phase_timeout=60.0,
            )
            admitted.append(snapshot.id)
            try:
                attempt = event.emit(
                    SnapshotEvent(url=snapshot.url, snapshot_id=snapshot.id, output_dir=str(snapshot_dir), event_timeout=60.0),
                )
                await attempt.now()
                await attempt.wait()
                terminal = await bus.find(SnapshotCompletedEvent, child_of=attempt, past=True, future=False)
                assert terminal is not None
                await terminal.wait()
                await terminal.event_results_list()
                if not terminal.error:
                    await attempt.event_results_list()
            finally:
                service.close()

    lifecycle = CrawlLifecycleService(
        bus,
        url=root_snapshot.url,
        snapshot=root_snapshot,
        output_dir=output_dir,
        crawl_setup_phase_timeout=60.0,
        snapshot_phase_timeout=60.0,
        crawl_cleanup_phase_timeout=60.0,
        run_snapshots=run_snapshots,
    )

    async def lose_owned_browser() -> None:
        # A real navigation can start only after the shared browser published
        # readiness. Kill its actual owning daemon/tree, as an RSS guard would;
        # never touch a browser found through a global process-name search.
        if loss_phase == "during_snapshot":
            navigation = await bus.find(ProcessStartedEvent, hook_name="on_Snapshot__30_chrome_navigate", past=True, future=30.0)
            assert navigation is not None and navigation.subprocess.returncode is None
        launch = await bus.find(ProcessStartedEvent, hook_name=launch_hook, past=True, future=False)
        assert launch is not None and launch.pid and launch.subprocess.returncode is None
        owner = psutil.Process(launch.pid)
        browser_pid = int((output_dir / "chrome/chrome.pid").read_text())
        children = owner.children(recursive=True)
        assert (browser_pid in {child.pid for child in children}) == required
        killed.extend([owner, *children])
        owner.kill()
        for child in reversed(children):
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        # SIGKILL delivery is asynchronous. Complete the test's actual process
        # fault before checking cleanup; do not race the kernel's child exits.
        _, alive = await asyncio.to_thread(psutil.wait_procs, children, timeout=5.0)
        assert not alive, [(child.pid, child.status()) for child in alive]

    async def lose_ready_browser_before_admission(event: CrawlSetupEvent) -> None:
        # Registered after real setup: the browser has fully initialized, but
        # no snapshot has been admitted. Observe its real completion barrier.
        await lose_owned_browser()
        completed = await bus.find(ProcessCompletedEvent, hook_name=launch_hook, past=True, future=10.0)
        assert completed is not None
        await completed.wait()
        await completed.event_results_list()

    async def run() -> None:
        install_bus = create_bus(total_timeout=120.0, name=f"browser_loss_install_{tmp_path.name}")
        try:
            await install_plugins(catalog, config=config, output_dir=output_dir, bus=install_bus)
        finally:
            await install_bus.destroy(clear=False)
        PluginBinaryEnvService(bus, catalog=catalog)
        BinaryService(bus, auto_install=True)
        processes = ProcessService(bus, emit_jsonl=False, interactive_tty=False)
        ArchiveResultService(bus, emit_jsonl=False)
        CrawlService(bus, url=root_snapshot.url, snapshot=root_snapshot, output_dir=output_dir, catalog=catalog)
        if loss_phase == "before_admission":
            bus.on(CrawlSetupEvent, lose_ready_browser_before_admission)
        loss = asyncio.create_task(lose_owned_browser()) if loss_phase == "during_snapshot" else None
        try:
            await bus.emit(MachineEvent(config=config, config_type="user")).now()
            root = bus.emit(
                CrawlEvent(url=root_snapshot.url, snapshot_id=root_snapshot.id, output_dir=str(output_dir), event_timeout=120.0),
            )
            await root.now()
            await root.wait()
            if loss is not None:
                await loss
            failure = None
            try:
                await root.event_results_list()
            except Exception as error:
                failure = error
            started = await bus.filter(ProcessStartedEvent, child_of=root, past=True, future=False)
            completed = await bus.filter(SnapshotCompletedEvent, child_of=root, past=True, future=False)
            crawl_completed = await bus.filter(CrawlCompletedEvent, child_of=root, past=True, future=False)
            launch = await bus.find(ProcessCompletedEvent, hook_name=launch_hook, pid=killed[0].pid, past=True, future=False)
            assert launch is not None and launch.status == "failed" and launch.exit_code == -9 and not launch.cancelled
            assert all(event.subprocess.returncode is not None for event in started)
            assert all(not child.is_running() or child.status() == psutil.STATUS_ZOMBIE for child in killed)
            (tmp_path / "lifecycle-evidence.json").write_text(
                json.dumps(
                    {
                        "admitted": admitted,
                        "snapshots": [{"id": event.snapshot_id, "error": event.error} for event in completed],
                        "crawl_errors": [event.error for event in crawl_completed],
                        "exception": str(failure),
                        "launch_exit": launch.exit_code,
                    },
                    indent=2,
                ),
            )
            expected = (1 if loss_phase == "during_snapshot" else 0) if required else 2
            assert len(admitted) == expected, "Losing the shared browser admitted another URL against its dead session"
            if not required:
                # Screencast's CrawlSetup hook legitimately exits after readiness;
                # its Snapshot monitor runs until cleanup. Neither its successful
                # finite setup nor a failed optional monitor invalidates Chrome.
                assert failure is None
                assert len(completed) == 2 and all(not item.error for item in completed)
                assert len(crawl_completed) == 1 and not crawl_completed[0].error
                for index in range(2):
                    assert (output_dir / str(index) / "title/title.txt").read_text().strip() == f"Queued article {index}"
                return
            assert not (output_dir / "1").exists()
            assert len(completed) == expected
            if expected:
                assert completed[0].snapshot_id == admitted[0] and completed[0].error
                # Losing a required browser is not a user's choice to skip the
                # foreground hook. Its interrupted attempt must remain failed.
                navigation = await bus.find(ProcessCompletedEvent, hook_name="on_Snapshot__30_chrome_navigate", past=True, future=False)
                assert navigation is not None and navigation.status == "failed"
                assert not navigation.cancelled, navigation.stderr
                assert "Hook interrupted by user" not in navigation.stderr
                assert "Required crawl resource" in navigation.stderr
                results = await bus.filter(ArchiveResultEvent, snapshot_id=admitted[0], past=True, future=False)
                navigation_results = [item for item in results if item.hook_name == navigation.hook_name]
                assert navigation_results and navigation_results[-1].status == "failed"
                assert "Required crawl resource" in navigation_results[-1].error
            else:
                assert not (output_dir / "0").exists()
            assert len(crawl_completed) == 1 and crawl_completed[0].error
            assert launch.hook_name in crawl_completed[0].error
            assert failure is not None and launch.hook_name in str(failure)
        finally:
            if loss is not None and not loss.done():
                loss.cancel()
                await asyncio.gather(loss, return_exceptions=True)
            await processes.stop()
            await bus.destroy(clear=False)

    asyncio.run(run())
