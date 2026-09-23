"""Real parser throughput and discovered-record preservation."""

import asyncio
import json
from pathlib import Path

from abx_dl.catalog import PluginCatalog
from abx_dl.events import ArchiveResultEvent, SnapshotDiscoveredEvent
from abx_dl.models import Snapshot
from abx_dl.orchestrator import create_bus, download as execute_download


def test_real_parser_discovery_keeps_all_records_without_per_line_history_search(tmp_path: Path) -> None:
    """Bulk output must not rescan a growing crawl history for every URL."""
    import cProfile

    source = tmp_path / "input.jsonl"
    expected_urls = {f"https://example.com/article/{index}" for index in range(1000)}
    source.write_text("".join(json.dumps({"url": url, "depth": 999}) + "\n" for url in sorted(expected_urls)))
    selected = PluginCatalog.discover().select(["parse_jsonl_urls"])
    bus = create_bus(total_timeout=60.0, name=f"bulk_parser_{tmp_path.name}")
    snapshot = Snapshot(url=source.as_uri())
    profiler = cProfile.Profile()

    async def run() -> None:
        try:
            profiler.enable()
            await execute_download(
                source.as_uri(),
                selected,
                tmp_path / "output",
                auto_install=False,
                bus=bus,
                snapshot=snapshot,
                emit_jsonl=False,
                interactive_tty=False,
            )
            profiler.disable()
            discoveries = await bus.filter(SnapshotDiscoveredEvent, past=True, future=False)
            assert len(discoveries) == len(expected_urls)
            assert {event.snapshot.url for event in discoveries} == expected_urls
            assert {event.snapshot.depth for event in discoveries} == {1}
            assert len({event.snapshot.id for event in discoveries}) == len(expected_urls)
            results = await bus.filter(ArchiveResultEvent, past=True, future=False)
            assert len(results) == 1
            assert results[0].status == "succeeded"
        finally:
            profiler.disable()
            await bus.destroy(clear=True)

    asyncio.run(run())
    # Count real history queries instead of imposing a machine-dependent wall
    # clock threshold. Startup/finalization need a handful; URL count must not
    # multiply them. No monkeypatched bus or abbreviated handler path is used.
    history_queries = sum(
        entry.callcount
        for entry in profiler.getstats()
        if not isinstance(entry.code, str) and entry.code.co_name == "filter" and entry.code.co_filename.endswith("abxbus/event_history.py")
    )
    assert history_queries < len(expected_urls) // 10, history_queries
