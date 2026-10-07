import asyncio
import json
from pathlib import Path
import sqlite3
import os

import pytest

from abx_dl.catalog import PluginCatalog
from abx_dl.events import ProcessEvent
from abx_dl.models import Snapshot
from abx_dl.orchestrator import create_bus, download


@pytest.mark.parametrize("filter_env", [False, True])
def test_import_hook_environment_projection_is_optional_and_does_not_mutate_parent(tmp_path: Path, filter_env: bool):
    from abx_dl.events import ProcessStartedEvent
    from abx_dl.orchestrator import parse_input

    os.environ["ABX_TEST_PARENT_ONLY"] = "parent-value"
    os.environ["ABX_TEST_TOOL_SETTING"] = "tool-value"
    catalog = PluginCatalog.discover().select(["parse_txt_urls"])
    bus = create_bus(name=f"import_env_projection_{filter_env}", total_timeout=60)
    started = []
    bus.on(ProcessStartedEvent, lambda event: started.append(event))
    source = "https://example.com/real-import"
    snapshots = asyncio.run(
        parse_input(
            source,
            catalog,
            tmp_path / "import",
            auto_install=False,
            bus=bus,
            env_filter=(lambda env: {key: value for key, value in env.items() if key != "ABX_TEST_PARENT_ONLY"}) if filter_env else None,
        ),
    )
    assert [snapshot.url for snapshot in snapshots] == [source]
    assert (tmp_path / "import/staticfile/stdin.txt").read_text() == source
    assert started
    for event in started:
        assert event.env["ABX_TEST_TOOL_SETTING"] == "tool-value"
        assert ("ABX_TEST_PARENT_ONLY" in event.env) is not filter_env
    assert os.environ["ABX_TEST_PARENT_ONLY"] == "parent-value"


def test_download_passes_real_hook_inputs_not_context(tmp_path: Path):
    output_dir = tmp_path / "run"
    (output_dir / "title").mkdir(parents=True)
    title = "界" * 70000 + " literal $(touch injected) `touch injected`"
    (output_dir / "title" / "title.txt").write_text(title)
    source = tmp_path / "urls.txt"
    source.write_text("https://example.com/child")
    snapshot = Snapshot(url=source.as_uri(), id="explicit-snapshot", depth=4, title=title)
    catalog = PluginCatalog.discover().select(["parse_txt_urls", "search_backend_sqlite"])
    bus = create_bus(name="explicit_hook_inputs", total_timeout=60)
    processes = []
    bus.on(ProcessEvent, lambda event: processes.append(event))
    asyncio.run(
        download(
            snapshot.url,
            catalog,
            output_dir,
            auto_install=False,
            snapshot=snapshot,
            config={
                "DATA_DIR": str(output_dir),
                "CRAWL_DIR": str(output_dir),
                "SEARCH_BACKEND_SQLITE_ENABLED": True,
                "CRAWL_MAX_DEPTH": 5,
                "EXTRA_CONTEXT": {"snapshot_depth": "opaque-not-a-depth", "trace_id": [1, 2]},
            },
            runtime="archivebox",
            bus=bus,
            emit_jsonl=False,
            interactive_tty=False,
        ),
    )
    assert {event.plugin_name for event in processes} == {"parse_txt_urls", "search_backend_sqlite"}
    for event in processes:
        assert f"--url={snapshot.url}" in event.hook_args
        assert "--snapshot-id=explicit-snapshot" in event.hook_args
        assert "--depth=4" in event.hook_args
        context = json.loads(event.env["EXTRA_CONTEXT"])
        assert context["snapshot_depth"] == "opaque-not-a-depth"
        assert context["trace_id"] == [1, 2]
        assert "snapshot_title" not in context
        assert title not in str(event.env)
    records = [json.loads(line) for line in (output_dir / "index.jsonl").read_text().splitlines()]
    results = [record for record in records if record["type"] == "ArchiveResult"]
    assert len(results) == 2
    assert all(record["status"] == "succeeded" for record in results)
    assert all(record["snapshot_id"] == snapshot.id for record in results)
    with sqlite3.connect(output_dir / "search.sqlite3") as conn:
        assert conn.execute("SELECT snapshot_id, title FROM search_index").fetchall() == [(snapshot.id, title)]
    assert (output_dir / "title" / "title.txt").read_text() == title
    assert not list(tmp_path.rglob("injected"))
