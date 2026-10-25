"""Real recorder shutdown must survive a consumer's filesystem failure."""

import asyncio
from pathlib import Path

import pytest
from pytest_httpserver import HTTPServer

from abx_dl.catalog import PluginCatalog
from abx_dl.config import get_explicit_user_env
from abx_dl.events import CrawlCompletedEvent, ProcessCompletedEvent, ProcessStartedEvent, SnapshotCleanupEvent, SnapshotCompletedEvent
from abx_dl.orchestrator import create_bus, download
from abx_dl.services.process_service import ProcessService


@pytest.mark.parametrize("failing_plugin,phase", [("consolelog", "Snapshot"), ("wget", "Snapshot"), ("chrome_screencast", "CrawlSetup")])
def test_cleanup_listener_failure_still_stops_browser(tmp_path: Path, httpserver: HTTPServer, failing_plugin: str, phase: str) -> None:
    httpserver.expect_request("/").respond_with_data(
        "<!doctype html><title>Recorder cleanup</title><script>console.log('saved console evidence')</script>",
        content_type="text/html",
    )
    output_dir = tmp_path / "capture"
    completed_output = tmp_path / "process-completed.json"
    completed_output.mkdir()
    bus = create_bus(total_timeout=120.0, name=f"cleanup_listener_failure_{tmp_path.name}")
    process_services: list[ProcessService] = []
    terminal_events: list[SnapshotCompletedEvent] = []
    crawl_completed: list[CrawlCompletedEvent] = []

    async def persist_completed_process(event: ProcessCompletedEvent) -> None:
        if event.plugin_name == failing_plugin and event.hook_name.startswith(f"on_{phase}__"):
            # A real consumer cannot persist into a directory. This must not
            # prevent the later Chrome dependency wave from stopping its tab.
            completed_output.write_text(event.model_dump_json())

    async def persist_completed_snapshot(event: SnapshotCompletedEvent) -> None:
        terminal_events.append(event)

    async def wait_for_finite_download(event: SnapshotCleanupEvent) -> None:
        if failing_plugin == "wget":
            # Exercise the other boundary: this real finite download and its
            # failed consumer have finished before cleanup scans live hooks.
            completed = await bus.find(ProcessCompletedEvent, plugin_name="wget", past=True, future=30.0)
            assert completed is not None
            await completed.wait()

    bus.on(ProcessCompletedEvent, persist_completed_process)
    bus.on(SnapshotCompletedEvent, persist_completed_snapshot)
    bus.on(CrawlCompletedEvent, lambda event: crawl_completed.append(event))
    bus.on(SnapshotCleanupEvent, wait_for_finite_download)

    async def run() -> None:
        try:
            with pytest.raises(IsADirectoryError):
                await download(
                    httpserver.url_for("/"),
                    PluginCatalog.discover().select(["consolelog", failing_plugin]),
                    output_dir,
                    config={**get_explicit_user_env(), "TIMEOUT": 30},
                    bus=bus,
                    emit_jsonl=False,
                    interactive_tty=False,
                    on_process_service_created=process_services.append,
                )
            started = await bus.filter(ProcessStartedEvent, past=True, future=False)
            assert any(event.plugin_name == "chrome" and event.is_background for event in started)
            assert all(event.subprocess.returncode is not None for event in started)
            assert len(terminal_events) == (1 if phase == "CrawlSetup" else 0)
            assert crawl_completed == []
            assert "saved console evidence" in (output_dir / "consolelog/console.jsonl").read_text()
        finally:
            # Test hygiene is deliberately after the assertions: the fallback
            # cannot make broken phase cleanup appear to have stopped Chrome.
            for service in process_services:
                await service.stop_background_hooks()
            await bus.destroy(clear=False)

    asyncio.run(run())


def test_snapshot_directory_failure_preserves_error_and_stops_recorders(tmp_path: Path, httpserver: HTTPServer) -> None:
    httpserver.expect_request("/").respond_with_data("<!doctype html><title>Directory failure</title>", content_type="text/html")
    output_dir = tmp_path / "capture"
    output_dir.mkdir()
    (output_dir / "headers").symlink_to("headers")
    bus = create_bus(total_timeout=120.0, name=f"cleanup_directory_failure_{tmp_path.name}")
    services: list[ProcessService] = []
    completed_output = tmp_path / "process-completed.json"
    completed_output.mkdir()

    async def persist_completed_process(event: ProcessCompletedEvent) -> None:
        if event.plugin_name == "consolelog":
            completed_output.write_text(event.model_dump_json())

    bus.on(ProcessCompletedEvent, persist_completed_process)

    async def run() -> None:
        try:
            with pytest.raises(RuntimeError, match="Background hook on_Snapshot__27_headers.*did not start") as failure:
                await download(
                    httpserver.url_for("/"),
                    PluginCatalog.discover().select(["consolelog", "headers"]),
                    output_dir,
                    config={**get_explicit_user_env(), "TIMEOUT": 30, "HEADERS_TIMEOUT": 5},
                    bus=bus,
                    emit_jsonl=False,
                    interactive_tty=False,
                    on_process_service_created=services.append,
                )
            # The execution error remains primary even when cleanup also has
            # a real persistence error; neither failure is silently discarded.
            assert any(str(completed_output) in note for note in failure.value.__notes__)
            started = await bus.filter(ProcessStartedEvent, past=True, future=False)
            assert any(event.plugin_name == "consolelog" for event in started)
            assert all(event.subprocess.returncode is not None for event in started)
            assert await bus.find(SnapshotCompletedEvent, past=True, future=False) is None
        finally:
            for service in services:
                await service.stop_background_hooks()
            await bus.destroy(clear=False)

    asyncio.run(run())
