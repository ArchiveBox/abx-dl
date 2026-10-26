"""Native download diagnostics stay complete without one bus turn per progress line."""

import asyncio
import hashlib
import io
import shutil

from pytest_httpserver import HTTPServer
from rich.console import Console

from abx_dl.catalog import PluginCatalog
from abx_dl.cli import LiveBusUI, _LiveProcessRecord
from abx_dl.events import ProcessCompletedEvent, ProcessStderrEvent
from abx_dl.orchestrator import create_bus, download


def test_real_wget_batches_diagnostics_without_losing_output(tmp_path, httpserver: HTTPServer):
    wget = shutil.which("wget")
    assert wget, "Real wget must be installed"
    payload = bytes(range(256)) * (8 * 1024 * 1024 // 256)
    httpserver.expect_request("/payload.bin").respond_with_data(payload, content_type="application/octet-stream")
    diagnostics = []
    summaries = []
    console_output = io.StringIO()

    async def run():
        bus = create_bus(total_timeout=30, name=f"wget_stderr_{tmp_path.name}")
        ui = LiveBusUI(bus, total_hooks=1, timeout_seconds=20, ui_console=Console(file=console_output, width=240), interactive_tty=False)

        def observe(event: ProcessStderrEvent):
            diagnostics.append(event.line)
            rows = [
                row
                for row in ui.live_results.values()
                if isinstance(row, _LiveProcessRecord) and row.plugin == "wget" and row.hook_name.startswith("on_Snapshot")
            ]
            assert len(rows) == 1
            summaries.append(rows[0].latest_stderr)

        bus.on(ProcessStderrEvent, observe)
        try:
            await download(
                httpserver.url_for("/payload.bin"),
                PluginCatalog.discover().select(["wget"]),
                tmp_path / "capture",
                auto_install=False,
                config={"WGET_BINARY": wget, "WGET_WARC_ENABLED": False, "TIMEOUT": 20},
                bus=bus,
                emit_jsonl=False,
                interactive_tty=False,
            )
            completed = await bus.find(ProcessCompletedEvent, plugin_name="wget", past=True, future=False)
            assert completed is not None and completed.exit_code == 0
            assert "succeeded" in console_output.getvalue()
            assert "payload.bin" in console_output.getvalue()
            return completed
        finally:
            await bus.destroy(clear=False)

    completed = asyncio.run(run())
    saved = list((tmp_path / "capture" / "wget").rglob("payload.bin"))
    assert len(saved) == 1
    assert hashlib.sha256(saved[0].read_bytes()).digest() == hashlib.sha256(payload).digest()
    assert len(completed.stderr.splitlines()) > 100, "Exercise a real verbose download, not a tiny empty response"
    # A buffered native downloader writes many completed progress lines at once.
    # Deliver that burst as diagnostics, not hundreds of serial event dispatches.
    assert len(diagnostics) < len(completed.stderr.splitlines()) // 8
    assert "".join(diagnostics) == completed.stderr
    for chunk, summary in zip(diagnostics, summaries, strict=True):
        visible = [line.strip() for line in chunk.split("\n") if line.strip() and not line.lstrip().startswith("{")]
        assert summary == visible[-1]
