"""Real recorder shutdown must survive a consumer's filesystem failure."""

import asyncio
from pathlib import Path

import pytest
from pytest_httpserver import HTTPServer

from abx_dl.catalog import PluginCatalog
from abx_dl.config import get_explicit_user_env
from abx_dl.events import (
    CrawlAbortEvent,
    CrawlCleanupEvent,
    CrawlCompletedEvent,
    ProcessCompletedEvent,
    ProcessStartedEvent,
    SnapshotCleanupEvent,
    SnapshotCompletedEvent,
)
from abx_dl.orchestrator import create_bus, download
from abx_dl.services.process_service import ProcessService
from abx_dl.services.crawl_lifecycle_service import SnapshotAttemptError


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
            with pytest.raises(SnapshotAttemptError if phase == "Snapshot" else IsADirectoryError) as failure:
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
            assert len(terminal_events) == 1
            if phase == "Snapshot":
                assert "IsADirectoryError" in terminal_events[0].error
                assert str(completed_output) in terminal_events[0].error
                assert any(terminal_events[0].error in note for note in failure.value.__notes__)
                cleanup = await bus.find(SnapshotCleanupEvent, past=True, future=False)
                assert cleanup is not None
                assert "IsADirectoryError" in cleanup.error and str(completed_output) in cleanup.error
            else:
                assert terminal_events[0].error == ""
            assert len(crawl_completed) == (1 if phase == "Snapshot" else 0)
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
            with pytest.raises(SnapshotAttemptError, match="1 snapshot attempt") as failure:
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
            terminal = await bus.find(SnapshotCompletedEvent, past=True, future=False)
            assert terminal is not None
            assert "RuntimeError: Background hook on_Snapshot__27_headers" in terminal.error
            assert "did not start" in terminal.error
            assert str(completed_output) in terminal.error
        finally:
            for service in services:
                await service.stop_background_hooks()
            await bus.destroy(clear=False)

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["cancelled", "takeover", "user_abort"])
def test_interrupted_snapshot_does_not_publish_failed_completion(tmp_path: Path, httpserver: HTTPServer, mode: str) -> None:
    httpserver.expect_request("/").respond_with_data("<!doctype html><title>Interrupted capture</title>", content_type="text/html")
    bus = create_bus(total_timeout=120.0, name=f"snapshot_interrupt_{mode}_{tmp_path.name}")
    started = asyncio.Event()
    services: list[ProcessService] = []

    async def observe_started(event: ProcessStartedEvent) -> None:
        if event.plugin_name == "consolelog":
            started.set()

    bus.on(ProcessStartedEvent, observe_started)

    async def run() -> None:
        task = asyncio.create_task(
            download(
                httpserver.url_for("/"),
                PluginCatalog.discover().select(["consolelog"]),
                tmp_path / "capture",
                config={**get_explicit_user_env(), "TIMEOUT": 30},
                bus=bus,
                emit_jsonl=False,
                interactive_tty=False,
                on_process_service_created=services.append,
            ),
        )
        try:
            await asyncio.wait_for(started.wait(), timeout=60)
            if mode == "cancelled":
                # The outer download waiter does not own bus processing. Cancel
                # the actual running snapshot owner to exercise its exception
                # boundary while its real recorder is still alive.
                owners = [
                    owner
                    for owner in asyncio.all_tasks()
                    if getattr(owner.get_coro(), "__qualname__", "") == "SnapshotService.on_SnapshotEvent"
                ]
                assert len(owners) == 1
                owners[0].cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                # Cancellation returns to the caller while the bus-owned
                # crawl handler may still be draining its browser. Verify the
                # actual cleanup phase, not an assumed synchronous barrier.
                cleanup = await bus.find(CrawlCleanupEvent, past=True, future=30.0)
                assert cleanup is not None
                await cleanup.wait()
                await cleanup.event_results_list()
            else:
                await bus.emit(CrawlAbortEvent(user_initiated=mode == "user_abort")).now()
                await task
            processes = await bus.filter(ProcessStartedEvent, past=True, future=False)
            assert any(event.plugin_name == "consolelog" for event in processes)
            # Abrupt owner cancellation can leave a writer alive; correctness
            # must not rely on finally completing. It leaves no terminal fact
            # and a later owner starts a fresh attempt, never a persisted pause.
            # Explicit abort/takeover still exercise real graceful shutdown.
            if mode != "cancelled":
                assert all(event.subprocess.returncode is not None for event in processes), [
                    (event.plugin_name, event.hook_name, event.subprocess.returncode) for event in processes
                ]
            completed = await bus.filter(SnapshotCompletedEvent, past=True, future=False)
            assert len(completed) == (1 if mode == "user_abort" else 0)
            if completed:
                assert completed[0].error == ""
            assert len(await bus.filter(CrawlCompletedEvent, past=True, future=False)) == (1 if mode == "user_abort" else 0)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            for service in services:
                await service.stop_background_hooks()
            await bus.destroy(clear=False)

    asyncio.run(run())
