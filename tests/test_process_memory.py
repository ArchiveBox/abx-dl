"""Memory admission observes real native hook processes without replacing hooks."""

import asyncio
import json
import os
import shutil
from pathlib import Path

import psutil
from pytest_httpserver import HTTPServer

from abx_dl.catalog import PluginCatalog
from abx_dl.events import ProcessEvent, ProcessStartedEvent, ProcessCompletedEvent
from abx_dl.execution import execute_hook
from abx_dl.orchestrator import create_bus
from abx_dl.services.process_service import ProcessService


def test_hook_memory_limit_preserves_parent_and_next_capture(tmp_path: Path) -> None:
    hook = PluginCatalog.discover()["parse_txt_urls"].filter_hooks("Snapshot")[0]
    source = tmp_path / "links.txt"
    source.write_text("https://example.com/preserved\n" * 100_000)

    async def run():
        sibling = await asyncio.create_subprocess_exec("sleep", "30")
        bus = create_bus(total_timeout=30, name=f"memory_followup_{tmp_path.name}")
        try:
            failed = await execute_hook(
                hook,
                output_dir=tmp_path / "limited" / hook.plugin_name,
                env={**os.environ, "SNAP_DIR": str(tmp_path / "limited"), "MEMORY_LIMIT": "1"},
                arguments={"url": source.as_uri()},
                bus=bus,
            )
            assert failed.exit_code != 0, failed
            assert failed.status == "failed"
            assert "memory limit exceeded" in failed.stderr
            assert not failed.cancelled
            assert sibling.returncode is None
            source.write_text("https://example.com/preserved\n")
            for overrides in (
                {},
                {"MEMORY_LIMIT": "1", "PARSE_TXT_URLS_MEMORY_LIMIT": "0"},
                {"MEMORY_LIMIT": "1", "PARSE_TXT_URLS_MEMORY_LIMIT": str(2 * 1024**3)},
            ):
                succeeded = await execute_hook(
                    hook,
                    output_dir=tmp_path / "normal" / hook.plugin_name,
                    env={**os.environ, "SNAP_DIR": str(tmp_path / "normal"), **overrides},
                    arguments={"url": source.as_uri()},
                    bus=bus,
                )
                assert succeeded.exit_code == 0, succeeded.stderr
                assert sibling.returncode is None
        finally:
            sibling.terminate()
            await sibling.wait()
            await bus.destroy(clear=False)
        return failed

    failed = asyncio.run(run())
    logs = list(Path(failed.output_dir).glob("*.stderr.log"))
    assert len(logs) == 1
    assert "memory limit exceeded" in logs[0].read_text()
    rows = (tmp_path / "normal" / hook.plugin_name / "urls.jsonl").read_text().splitlines()
    assert [json.loads(row)["url"] for row in rows] == ["https://example.com/preserved"]


def test_background_hook_memory_limit_kills_real_downloader_child(tmp_path: Path, httpserver: HTTPServer) -> None:
    wget = shutil.which("wget")
    assert wget, "Real wget must be installed for native subprocess coverage"
    httpserver.expect_request("/large.txt").respond_with_data("saved content\n" * 10_000)
    hook = PluginCatalog.discover()["wget"].filter_hooks("Snapshot")[0]
    bus = create_bus(total_timeout=30, name=f"memory_children_{tmp_path.name}")
    service = ProcessService(bus, emit_jsonl=False, interactive_tty=False)
    children = []

    async def observe_child(event: ProcessStartedEvent) -> None:
        root = psutil.Process(event.pid)
        deadline = asyncio.get_running_loop().time() + 5
        while asyncio.get_running_loop().time() < deadline:
            children[:] = [child for child in root.children(recursive=True) if "wget" in child.name()]
            if children:
                return
            await asyncio.sleep(0.01)
        raise AssertionError("Real wget child did not start")

    bus.on(ProcessStartedEvent, observe_child)

    async def run():
        event = bus.emit(
            ProcessEvent(
                plugin_name=hook.plugin_name,
                hook_name=hook.name,
                hook_path=str(hook.path),
                hook_args=[f"--url={httpserver.url_for('/large.txt')}"],
                is_background=hook.is_background,
                output_dir=str(tmp_path / "wget"),
                env={
                    **os.environ,
                    "SNAP_DIR": str(tmp_path),
                    "MEMORY_LIMIT": "1",
                    "WGET_BINARY": wget,
                    "WGET_ARGS_EXTRA": '["--limit-rate=1024"]',
                    "WGET_WARC_ENABLED": "false",
                },
                timeout=20,
            ),
        )
        await event.now()
        await service.wait_for_background_completions()
        completed = await bus.find(ProcessCompletedEvent, child_of=event, past=True, future=False)
        assert completed is not None
        assert completed.exit_code != 0
        assert "memory limit exceeded" in completed.stderr
        assert children
        for child in children:
            assert not child.is_running() or child.status() == psutil.STATUS_ZOMBIE
        await bus.destroy(clear=False)

    asyncio.run(run())
