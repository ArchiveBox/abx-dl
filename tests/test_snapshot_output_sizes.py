"""A shared captured file consumes the snapshot byte budget only once."""

import asyncio
from pathlib import Path

from pytest_httpserver import HTTPServer

from abx_dl.catalog import PluginCatalog
from abx_dl.config import get_explicit_user_env
from abx_dl.events import ArchiveResultEvent, ProcessCompletedEvent
from abx_dl.orchestrator import create_bus, download
from abx_dl.services.snapshot_service import SnapshotService


def test_reused_response_counts_once_in_snapshot_budget(tmp_path: Path, httpserver: HTTPServer) -> None:
    payload = b'<svg xmlns="http://www.w3.org/2000/svg"><!--' + b"capture " * 131072 + b'--><rect width="20" height="20"/></svg>'
    httpserver.expect_request("/image.svg").respond_with_data(payload, content_type="image/svg+xml")
    httpserver.expect_request("/favicon.ico").respond_with_data(b"", status=204)
    output_dir = tmp_path / "capture"
    catalog = PluginCatalog.discover().select(["responses", "staticfile"])

    async def run() -> None:
        bus = create_bus(name=f"shared_output_budget_{tmp_path.name}")
        try:
            await download(
                httpserver.url_for("/image.svg"),
                catalog,
                output_dir,
                bus=bus,
                emit_jsonl=False,
                interactive_tty=False,
                config={**get_explicit_user_env(), "CHROME_HEADLESS": True, "SNAPSHOT_MAX_SIZE": "10mb"},
            )
            results = await bus.filter(ArchiveResultEvent, past=True, future=False)
            reused = next(event for event in reversed(results) if event.plugin == "staticfile")
            assert reused.status == "succeeded"
            assert (output_dir / reused.output_str).read_bytes() == payload
            assert any(item.size == len(payload) and getattr(item, "root_relative", False) for item in reused.output_files)
            completed = await bus.filter(ProcessCompletedEvent, past=True, future=False)
            response = next(event for event in completed if event.plugin_name == "responses")
            assert any(item.size == len(payload) for item in response.output_files)
            # Inspect the actual service that owned this completed capture;
            # constructing/replaying synthetic events would miss publication order.
            service = next(
                owner for entry in bus.handlers.values() if isinstance(owner := getattr(entry.handler, "__self__", None), SnapshotService)
            )
            counted_payload_bytes = sum(size for size in service._output_sizes.values() if size == len(payload))
            assert counted_payload_bytes == len(payload), "Both results reference the same saved response, not two files"
        finally:
            await bus.destroy(clear=False)

    asyncio.run(run())
