import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import threading

from abx_dl.catalog import PluginCatalog
from abx_dl.events import ProcessStdoutEvent
from abx_dl.execution import execute_hook
from abx_dl.orchestrator import create_bus


def test_local_hook_output_is_not_queued_behind_archive_writes(tmp_path: Path):
    hook = PluginCatalog.discover()["parse_txt_urls"].filter_hooks("Snapshot")[0]
    source = tmp_path / "input.txt"
    source.write_text("https://example.com/discovered\n")
    delayed_source = tmp_path / "delayed.txt"
    os.mkfifo(delayed_source)
    source_open = threading.Event()
    release_source = threading.Event()

    def provide_source():
        with delayed_source.open("w") as stream:
            source_open.set()
            release_source.wait(20)
            stream.write(source.read_text())

    provider = threading.Thread(target=provide_source, daemon=True)
    provider.start()
    indexes = []
    index_records = []
    for index in range(4):
        path = tmp_path / f"blocked-{index}" / "index.jsonl"
        path.parent.mkdir()
        os.mkfifo(path)
        indexes.append(path)
    readers = []

    def drain(path):
        with path.open("rb") as stream:
            index_records.extend(json.loads(line) for line in stream.read().splitlines())

    observed = []

    def result_seen():
        return any('"type": "ArchiveResult"' in event.line for event in observed)

    async def run():
        loop = asyncio.get_running_loop()
        # A supported small executor makes contention reproducible without
        # starting dozens of subprocesses; every occupied worker is doing the
        # runtime's real final index append, not a synthetic blocking callback.
        loop.set_default_executor(ThreadPoolExecutor(max_workers=4))
        bus = create_bus(total_timeout=30, name=f"stdout_contention_{tmp_path.name}")
        bus.on(ProcessStdoutEvent, lambda event: observed.append(event))
        buses = [bus]
        target = asyncio.create_task(
            execute_hook(
                hook,
                output_dir=tmp_path / "target" / hook.plugin_name,
                env={**os.environ, "MEMORY_LIMIT": "2147483648", "SNAP_DIR": str(tmp_path / "target")},
                arguments={"url": delayed_source.as_uri()},
                bus=bus,
            ),
        )
        tasks = []
        try:
            async with asyncio.timeout(10):
                while not source_open.is_set():
                    if target.done():
                        raise AssertionError((await target).stderr)
                    await asyncio.sleep(0.01)
            for path in indexes:
                archive_bus = create_bus(total_timeout=30, name=f"archive_write_{len(tasks)}_{tmp_path.name}")
                buses.append(archive_bus)
                tasks.append(
                    asyncio.create_task(
                        execute_hook(
                            hook,
                            output_dir=path.parent / hook.plugin_name,
                            env={**os.environ, "MEMORY_LIMIT": "2147483648", "SNAP_DIR": str(path.parent)},
                            arguments={"url": source.as_uri()},
                            bus=archive_bus,
                        ),
                    ),
                )
            async with asyncio.timeout(10):
                while not all((path.parent / hook.plugin_name / "urls.jsonl").is_file() for path in indexes):
                    await asyncio.sleep(0.01)
            # This independent local task can only queue once all archival
            # appends occupy the pool. It is an observation, not the workload.
            await asyncio.sleep(0.2)
            probe = asyncio.create_task(asyncio.to_thread(lambda: None))
            await asyncio.sleep(0.2)
            assert not probe.done(), "The real archive writes did not occupy the executor"
            release_source.set()
            async with asyncio.timeout(5):
                while not (tmp_path / "target" / hook.plugin_name / "urls.jsonl").is_file():
                    await asyncio.sleep(0.01)
            until = loop.time() + 0.75
            while not result_seen() and loop.time() < until:
                await asyncio.sleep(0.01)
            assert result_seen(), "Completed native hook stdout waited behind unrelated archive index writes"
        finally:
            release_source.set()
            for path in indexes[: len(tasks)]:
                reader = threading.Thread(target=drain, args=(path,), daemon=True)
                reader.start()
                readers.append(reader)
            completed = await asyncio.gather(target, *tasks)
            assert all(result.exit_code == 0 for result in completed), [result.stderr for result in completed]
            for observed_bus in buses:
                await observed_bus.destroy(clear=False)

    try:
        asyncio.run(run())
    finally:
        release_source.set()
        provider.join(5)
        for reader in readers:
            reader.join(5)
    assert len(index_records) == 4
    assert all(record["status"] == "succeeded" and record["hook_name"] == hook.name for record in index_records)
    for directory in [tmp_path / "target", *(path.parent for path in indexes)]:
        records = (directory / hook.plugin_name / "urls.jsonl").read_text().splitlines()
        assert [json.loads(line)["url"] for line in records] == ["https://example.com/discovered"]
