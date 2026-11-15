import asyncio
import json
from pathlib import Path

from abx_dl.catalog import PluginCatalog
from abx_dl.events import ProcessCompletedEvent, ProcessKillEvent, ProcessStartedEvent
from abx_dl.orchestrator import create_bus, download


def test_snapshot_cleanup_closes_tab_before_its_browser(tmp_path: Path, httpserver):
    """Observe real hook teardown: a tab cannot flush after its browser is gone."""
    httpserver.expect_request("/").respond_with_data(
        "<html><title>Cleanup order</title><body>Saved page</body></html>",
        content_type="text/html",
    )
    httpserver.expect_request("/favicon.ico").respond_with_data("", status=404)
    output_dir = tmp_path / "capture"
    bus = create_bus(total_timeout=300.0, name=f"cleanup_order_{tmp_path.name}")
    tab_hook = "on_Snapshot__01_chrome_tab.daemon.bg"
    browser_hook = "on_Snapshot__00_chrome_launch.daemon.bg"
    tab_completed_before_browser = []

    async def on_ProcessKillEvent(event: ProcessKillEvent) -> None:
        if event.hook_name == browser_hook:
            # This subscriber only observes the real lifecycle. It neither
            # delays nor replaces the production process-kill handler.
            completed = await bus.find(ProcessCompletedEvent, hook_name=tab_hook, past=True, future=False)
            tab_completed_before_browser.append(completed is not None)

    bus.on(ProcessKillEvent, on_ProcessKillEvent)

    async def run() -> None:
        try:
            await download(
                httpserver.url_for("/"),
                PluginCatalog.discover().select(["chrome"]),
                output_dir,
                config={"CHROME_ISOLATION": "snapshot", "CHROME_HEADLESS": True, "CHROME_DELAY": 0},
                bus=bus,
                emit_jsonl=False,
                interactive_tty=False,
            )
            for hook in (tab_hook, browser_hook):
                completed = await bus.find(ProcessCompletedEvent, hook_name=hook, past=True, future=False)
                assert isinstance(completed, ProcessCompletedEvent), hook
                assert completed.exit_code == 0, completed.stderr
        finally:
            await bus.destroy(clear=False)

    asyncio.run(run())
    assert tab_completed_before_browser == [True]
    records = [json.loads(line) for line in (output_dir / "index.jsonl").read_text().splitlines()]
    for hook in (tab_hook, browser_hook, "on_Snapshot__30_chrome_navigate"):
        processes = [record for record in records if record.get("type") == "Process" and record.get("hook_name") == hook]
        assert len(processes) == 1, processes
        assert processes[0]["exit_code"] == 0, processes
    # Session markers are intentionally removed at cleanup; durable capture
    # evidence remains, with terminal process records for all three owners.
    assert not (output_dir / "chrome" / "navigation.json").exists()
    assert (output_dir / "chrome" / "random_seed.json").is_file()


def test_cleanup_releases_recorder_before_independent_download_finishes(tmp_path: Path, httpserver):
    """A completed dependent must not hold a recorder behind unrelated I/O."""
    # Wget's real rate limiter keeps its finite transfer active after Chrome
    # has loaded the same ordinary page. No hook or process event is replaced.
    page = "<!doctype html><title>Independent cleanup</title><p>Retained page</p><!--" + "saved content " * 75000 + "-->"
    httpserver.expect_request("/").respond_with_data(page, content_type="text/html")
    httpserver.expect_request("/favicon.ico").respond_with_data("", status=404)
    output_dir = tmp_path / "capture"
    bus = create_bus(total_timeout=120.0, name=f"independent_cleanup_{tmp_path.name}")
    wget_active_at_recorder_stop = []

    async def observe_stop(event: ProcessKillEvent) -> None:
        if event.plugin_name == "responses":
            started = await bus.find(ProcessStartedEvent, plugin_name="wget", past=True, future=False)
            completed = await bus.find(ProcessCompletedEvent, plugin_name="wget", past=True, future=False)
            wget_active_at_recorder_stop.append(started is not None and started.subprocess.returncode is None and completed is None)

    bus.on(ProcessKillEvent, observe_stop)

    async def run() -> None:
        try:
            await download(
                httpserver.url_for("/"),
                PluginCatalog.discover().select(["wget", "responses", "readability"]),
                output_dir,
                config={
                    "CHROME_ISOLATION": "snapshot",
                    "CHROME_HEADLESS": True,
                    "CHROME_DELAY": 0,
                    "READABILITY_ENABLED": False,
                    "WGET_ARGS_EXTRA": ["--limit-rate=64k"],
                },
                bus=bus,
                emit_jsonl=False,
                interactive_tty=False,
            )
            for plugin in ("wget", "responses"):
                completed = await bus.find(ProcessCompletedEvent, plugin_name=plugin, past=True, future=False)
                assert completed is not None
                assert completed.exit_code == 0, completed.stderr
        finally:
            await bus.destroy(clear=False)

    asyncio.run(run())
    assert wget_active_at_recorder_stop == [True]
    assert "Retained page" in next((output_dir / "wget").rglob("index.html")).read_text()
    assert (output_dir / "responses/index.jsonl").stat().st_size > 0
