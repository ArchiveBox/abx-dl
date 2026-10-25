"""Embedding supplies work; the downloader owns the same crawl lifecycle."""

import asyncio
import json
import socket
import subprocess
import sys
from pathlib import Path

import pytest
from abxpkg.binary_service import BinaryService
from pytest_httpserver import HTTPServer

from abx_dl.catalog import PluginCatalog
from abx_dl.config import GlobalConfig, RuntimeConfig, get_explicit_user_env
from abx_dl.events import (
    CrawlAbortEvent,
    CrawlCleanupEvent,
    CrawlCompletedEvent,
    CrawlEvent,
    CrawlSetupEvent,
    CrawlStartEvent,
    MachineEvent,
    ProcessCompletedEvent,
    ProcessEvent,
    ProcessStartedEvent,
    SnapshotCompletedEvent,
    SnapshotEvent,
)
from abx_dl.models import Snapshot, write_jsonl
from abx_dl.orchestrator import create_bus, download, install_plugins
from abx_dl.services.archive_result_service import ArchiveResultService
from abx_dl.services.binary_service import PluginBinaryEnvService
from abx_dl.services.crawl_lifecycle_service import CrawlLifecycleService, SnapshotAttemptError
from abx_dl.services.crawl_service import CrawlService
from abx_dl.services.process_service import ProcessService
from abx_dl.services.snapshot_service import SnapshotService


@pytest.mark.parametrize(
    "mode",
    [
        "two_snapshots",
        "filesystem_error",
        "user_abort",
        "takeover",
        "pre_abort",
        "cancelled",
        "setup_failure",
        "preparation_failure",
        "disabled_setup",
        "consumer_failure",
        "preparation_consumer_failure",
    ],
)
def test_external_scheduler_uses_shared_crawl_lifecycle(tmp_path: Path, httpserver: HTTPServer, mode: str) -> None:
    for index in range(2):
        httpserver.expect_request(f"/{index}").respond_with_data(
            f"<!doctype html><title>Queued article {index}</title><p>Saved article {index}</p>",
            content_type="text/html",
        )
    catalog = PluginCatalog.discover().select(["title", "parse_txt_urls"])
    output_dir = tmp_path / "crawl"
    root_snapshot = Snapshot(url=httpserver.url_for("/0"))
    config = {**get_explicit_user_env(), "CRAWL_DIR": str(output_dir), "TIMEOUT": 30}
    if mode == "setup_failure":
        config["CHROME_TAB_TIMEOUT"] = 5
    if mode == "disabled_setup":
        config["CHROME_ENABLED"] = False
    bus = create_bus(total_timeout=120.0, name=f"external_scheduler_{mode}_{tmp_path.name}")
    callback_events: list[CrawlStartEvent] = []
    completed_output = tmp_path / "process-completed.json"
    completed_output.mkdir()

    async def persist_completed_process(event: ProcessCompletedEvent) -> None:
        if mode == "consumer_failure" and event.output_dir == str(output_dir / "0/title"):
            # Actual first-attempt persistence failure must remain visible while
            # the next independent snapshot still saves its own valid article.
            completed_output.write_text(event.model_dump_json())

    bus.on(ProcessCompletedEvent, persist_completed_process)

    async def write_snapshot_manifest(event: SnapshotEvent) -> None:
        if mode == "preparation_consumer_failure":
            snapshot_dir = Path(event.output_dir)
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            if snapshot_dir.name == "0":
                (snapshot_dir / "index.jsonl").mkdir()
            write_jsonl(snapshot_dir / "index.jsonl", Snapshot(id=event.snapshot_id, url=event.url), also_print=False)

    bus.on(SnapshotEvent, write_snapshot_manifest)

    async def lose_browser_control_file(event: ProcessEvent) -> None:
        if mode == "setup_failure" and event.hook_name == "on_CrawlSetup__91_chrome_wait":
            # Remove a real launch artifact before the real foreground wait
            # hook starts. Its nonzero exit must not skip independent parsers.
            (output_dir / "chrome/cdp_url.txt").unlink()

    bus.on(ProcessEvent, lose_browser_control_file)

    async def run_snapshots(event: CrawlStartEvent) -> None:
        callback_events.append(event)
        if mode == "filesystem_error":
            # A real queue persistence failure must still shut down the browser
            # already started by CrawlSetup, without publishing CrawlCompleted.
            output_dir.write_text("queue checkpoint")
        elif mode in {"user_abort", "takeover"}:
            await event.emit(CrawlAbortEvent(user_initiated=mode == "user_abort")).now()
        elif mode == "cancelled":
            # Cancel the actual scheduler task, without translating shutdown
            # into a user-abort/completion event or changing durable outcomes.
            owner = asyncio.current_task()
            assert owner is not None
            owner.cancel()
            await asyncio.sleep(0)
        else:
            for index in range(2):
                url = httpserver.url_for(f"/{index}")
                snapshot_catalog = catalog.select(["title"])
                if mode in {"setup_failure", "disabled_setup"}:
                    source = tmp_path / f"source-{index}.txt"
                    source.write_text(url)
                    url = source.as_uri()
                    snapshot_catalog = catalog.select(["parse_txt_urls"])
                snapshot = Snapshot(url=url)
                snapshot_dir = output_dir / str(index)
                if mode == "preparation_failure" and index == 0:
                    snapshot_dir.mkdir()
                    (snapshot_dir / "title").write_text("existing output must survive")
                service = SnapshotService(
                    bus,
                    url=snapshot.url,
                    snapshot=snapshot,
                    output_dir=snapshot_dir,
                    catalog=snapshot_catalog,
                    config=RuntimeConfig(user=GlobalConfig(**config), derived={}),
                    snapshot_phase_timeout=60.0,
                    snapshot_cleanup_phase_timeout=60.0,
                )
                try:
                    snapshot_event = event.emit(
                        SnapshotEvent(url=snapshot.url, snapshot_id=snapshot.id, output_dir=str(snapshot_dir), event_timeout=60.0),
                    )
                    await snapshot_event.now()
                    await snapshot_event.wait()
                    terminal = await bus.find(SnapshotCompletedEvent, child_of=snapshot_event, past=True, future=False)
                    assert terminal is not None
                    await terminal.wait()
                    await terminal.event_results_list()
                    if not terminal.error:
                        await snapshot_event.event_results_list()
                    if mode == "preparation_failure" and index == 0:
                        # Match a database embedder releasing already-projected
                        # history; final CLI failure must survive that release.
                        for recorded in await bus.filter("*", child_of=snapshot_event, past=True, future=False):
                            recorded.event_ttl = 0
                        snapshot_event.event_ttl = 0
                finally:
                    service.close()

    async def run() -> None:
        install_bus = create_bus(total_timeout=120.0, name=f"scheduler_install_{mode}_{tmp_path.name}")
        try:
            await install_plugins(catalog, config=config, output_dir=output_dir, bus=install_bus)
        finally:
            await install_bus.destroy(clear=False)
        PluginBinaryEnvService(bus, catalog=catalog)
        BinaryService(bus, auto_install=True)
        processes = ProcessService(bus, emit_jsonl=False, interactive_tty=False)
        ArchiveResultService(bus, emit_jsonl=False)
        CrawlService(bus, url=root_snapshot.url, snapshot=root_snapshot, output_dir=output_dir, catalog=catalog)
        CrawlLifecycleService(
            bus,
            url=root_snapshot.url,
            snapshot=root_snapshot,
            output_dir=output_dir,
            crawl_setup_phase_timeout=60.0,
            snapshot_phase_timeout=60.0,
            crawl_cleanup_phase_timeout=60.0,
            run_snapshots=run_snapshots,
        )
        try:
            await bus.emit(MachineEvent(config=config, config_type="user")).now()
            root = CrawlEvent(url=root_snapshot.url, snapshot_id=root_snapshot.id, output_dir=str(output_dir), event_timeout=120.0)
            if mode == "pre_abort":
                await bus.emit(CrawlAbortEvent(event_parent_id=root.event_id)).now()
            await bus.emit(root).now()
            await root.wait()
            if mode == "filesystem_error":
                with pytest.raises(IsADirectoryError):
                    await root.event_results_list()
            elif mode == "cancelled":
                with pytest.raises(asyncio.CancelledError):
                    await root.event_results_list()
            elif mode == "setup_failure":
                with pytest.raises(RuntimeError, match="Crawl setup hook chrome:on_CrawlSetup__91_chrome_wait failed"):
                    await root.event_results_list()
            elif mode in {"preparation_failure", "consumer_failure", "preparation_consumer_failure"}:
                with pytest.raises(SnapshotAttemptError, match="1 snapshot attempt\\(s\\) failed"):
                    await root.event_results_list()
            else:
                await root.event_results_list()
            started = await bus.filter(ProcessStartedEvent, past=True, future=False)
            assert all(event.subprocess.returncode is not None for event in started)
            if mode == "disabled_setup":
                assert all(event.plugin_name != "chrome" for event in started)
            assert len(callback_events) == (0 if mode == "pre_abort" else 1)
            assert len(await bus.filter(CrawlSetupEvent, child_of=root, past=True, future=False)) == (0 if mode == "pre_abort" else 1)
            assert len(await bus.filter(CrawlCleanupEvent, child_of=root, past=True, future=False)) == 1
            assert len(await bus.filter(CrawlCompletedEvent, child_of=root, past=True, future=False)) == (
                0 if mode in {"filesystem_error", "takeover", "cancelled"} else 1
            )
            completed = await bus.filter(SnapshotCompletedEvent, child_of=root, past=True, future=False)
            assert len(completed) == (
                2
                if mode in {"two_snapshots", "setup_failure", "disabled_setup", "consumer_failure", "preparation_consumer_failure"}
                else 1
                if mode == "preparation_failure"
                else 0
            )
            if mode == "preparation_consumer_failure":
                failed = [event for event in completed if event.error]
                assert len(failed) == 1
                assert "IsADirectoryError" in failed[0].error and "index.jsonl" in failed[0].error
                assert (output_dir / "0/index.jsonl").is_dir()
                assert not (output_dir / "0/title").exists()
                assert all(not event.output_dir.startswith(str(output_dir / "0") + "/") for event in started)
                assert (output_dir / "1/title/title.txt").read_text().strip() == "Queued article 1"
            if mode == "consumer_failure":
                failed = [event for event in completed if event.error]
                assert len(failed) == 1
                assert "IsADirectoryError" in failed[0].error and str(completed_output) in failed[0].error
                assert all(event.error == "" for event in completed if event is not failed[0])
            if mode in {"two_snapshots", "consumer_failure"}:
                for index in range(2):
                    assert (output_dir / str(index) / "title/title.txt").read_text().strip() == f"Queued article {index}"
            elif mode == "setup_failure":
                failed = await bus.find(ProcessCompletedEvent, hook_name="on_CrawlSetup__91_chrome_wait", past=True, future=False)
                assert failed is not None and failed.pid and failed.exit_code != 0 and failed.status == "failed"
                for index in range(2):
                    saved = json.loads((output_dir / str(index) / "parse_txt_urls/urls.jsonl").read_text())
                    assert saved["url"] == httpserver.url_for(f"/{index}")
            elif mode == "preparation_failure":
                assert (output_dir / "0/title").read_text() == "existing output must survive"
                assert (output_dir / "1/title/title.txt").read_text().strip() == "Queued article 1"
                assert await bus.find(ProcessCompletedEvent, plugin_name="title", pid=0, past=True, future=False) is None
            elif mode == "disabled_setup":
                for index in range(2):
                    saved = json.loads((output_dir / str(index) / "parse_txt_urls/urls.jsonl").read_text())
                    assert saved["url"] == httpserver.url_for(f"/{index}")
        finally:
            await processes.stop_background_hooks()
            await bus.destroy(clear=False)

    asyncio.run(run())


def test_cli_reports_preparation_failure_after_saving_independent_outputs(tmp_path: Path) -> None:
    source = tmp_path / "source.html"
    source.write_text('<a href="https://archivebox.io/tests/">ArchiveBox test</a>')
    output_dir = tmp_path / "capture"
    output_dir.mkdir()
    blocked = output_dir / "parse_html_urls"
    blocked.write_bytes(b"existing output must survive")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "abx_dl",
            "dl",
            "--plugins=parse_html_urls,parse_txt_urls,hashes",
            "--dir",
            str(output_dir),
            source.as_uri(),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "1 snapshot attempt(s) failed" in result.stderr
    assert blocked.read_bytes() == b"existing output must survive"
    records = [json.loads(line) for line in (output_dir / "index.jsonl").read_text().splitlines()]
    results = {record["plugin"]: record for record in records if record.get("type") == "ArchiveResult"}
    assert results["parse_html_urls"]["status"] == "failed"
    assert "FileExistsError" in results["parse_html_urls"]["error"]
    assert results["parse_txt_urls"]["status"] == results["hashes"]["status"] == "succeeded"
    assert json.loads((output_dir / "parse_txt_urls/urls.jsonl").read_text())["url"] == "https://archivebox.io/tests/"


def test_started_hook_failure_is_not_a_preparation_failure(tmp_path: Path) -> None:
    bus = create_bus(total_timeout=60.0, name=f"provider_failure_{tmp_path.name}")
    completed: list[ProcessCompletedEvent] = []
    bus.on(ProcessCompletedEvent, lambda event: completed.append(event))
    # A bound socket without listen() gives a real refused connection while
    # reserving the port. This exercises wget's nonzero exit, not a mock hook.
    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        url = f"http://127.0.0.1:{unavailable.getsockname()[1]}/"

        async def run() -> None:
            try:
                await download(
                    url,
                    PluginCatalog.discover().select(["wget"]),
                    tmp_path / "capture",
                    config={**get_explicit_user_env(), "TIMEOUT": 10},
                    bus=bus,
                    emit_jsonl=False,
                    interactive_tty=False,
                )
                assert len(await bus.filter(CrawlCompletedEvent, past=True, future=False)) == 1
            finally:
                await bus.destroy(clear=False)

        asyncio.run(run())
    wget = next(event for event in completed if event.plugin_name == "wget")
    assert wget.pid and wget.exit_code != 0 and wget.status == "failed"
