import asyncio
import json
import os
from pathlib import Path

import pytest

from abx_dl.catalog import PluginCatalog
from abx_dl.events import ProcessStartedEvent
from abx_dl.execution import execute_hook
from abx_dl.orchestrator import create_bus


@pytest.mark.parametrize("missing_input", [False, True])
def test_hook_control_files_use_local_tmp_and_preserve_failed_logs(tmp_path: Path, missing_input: bool):
    hook = PluginCatalog.discover()["parse_txt_urls"].filter_hooks("Snapshot")[0]
    source = tmp_path / "input.txt"
    if not missing_input:
        source.write_text("https://archivebox.io/tests/\n")
    output = tmp_path / "archive" / hook.plugin_name
    runtime = tmp_path / "local-tmp"
    started = []
    observations = []
    bus = create_bus(total_timeout=30, name=f"local_process_files_{tmp_path.name}")

    async def observe_start(event: ProcessStartedEvent):
        started.append(event)
        observations.append((event.pid_file.read_text().strip(), list(output.glob("*.log")), list(output.glob("*.pid"))))

    bus.on(ProcessStartedEvent, observe_start)
    completed = asyncio.run(
        execute_hook(
            hook,
            output_dir=output,
            env={**os.environ, "TMP_DIR": str(runtime), "SNAP_DIR": str(output.parent)},
            arguments={"url": source.as_uri()},
            bus=bus,
        ),
    )
    assert len(started) == 1
    event = started[0]
    assert observations == [(str(event.pid), [], [])]
    assert all(path.parent == runtime / "process" for path in (event.stdout_file, event.stderr_file, event.pid_file))
    assert not event.pid_file.exists()
    assert not event.stdout_file.exists()
    assert not event.stderr_file.exists()
    assert not list((runtime / "process").iterdir())
    records = [json.loads(line) for line in completed.stdout.splitlines() if line.startswith("{")]
    result = next(record for record in records if record.get("type") == "ArchiveResult")
    if missing_input:
        assert completed.exit_code != 0
        assert result["status"] == "failed"
        assert (output / event.stdout_file.name).read_text() == completed.stdout
        assert (output / event.stderr_file.name).read_text() == completed.stderr
    else:
        assert completed.exit_code == 0, completed.stderr
        assert result["status"] == "succeeded"
        urls = [json.loads(line)["url"] for line in (output / "urls.jsonl").read_text().splitlines()]
        assert urls == ["https://archivebox.io/tests/"]
        assert not list(output.glob("*.log"))
