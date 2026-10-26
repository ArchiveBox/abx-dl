"""Real hook output blocked in native I/O must not block sibling asyncio work."""

import asyncio
import json
import os
import threading
from pathlib import Path

import pytest

from abx_dl.catalog import PluginCatalog
from abx_dl.events import ProcessStartedEvent, ProcessStderrEvent, ProcessStdoutEvent
from abx_dl.models import Process
from abx_dl.orchestrator import create_bus
from abx_dl.services.process_service import ProcessService, _OutputStreamState


@pytest.mark.parametrize("blocked_stream", ["stdout", "stderr"])
@pytest.mark.parametrize("cancel_count", [0, 1, 2])
@pytest.mark.parametrize("final_drain", [False, True])
def test_native_hook_log_read_keeps_loop_live_and_drains_before_cancellation(
    tmp_path: Path,
    blocked_stream: str,
    cancel_count: int,
    final_drain: bool,
):
    hook = PluginCatalog.discover()["parse_txt_urls"].filter_hooks("Snapshot")[0]
    source = tmp_path / "input.txt"
    source.write_text("https://example.com/discovered\n")
    output = tmp_path / "snapshot" / hook.plugin_name
    output.mkdir(parents=True)
    paths = {name: output / f"{name}.log" for name in ("stdout", "stderr")}
    os.mkfifo(paths[blocked_stream])
    # A retained native writer makes read() wait for EOF even after the real
    # parser exits. This is kernel backpressure, not a replacement reader.
    held_fd = os.open(paths[blocked_stream], os.O_RDWR)
    released = threading.Event()

    def release_io():
        os.close(held_fd)
        released.set()

    release = threading.Timer(1.5, release_io)

    async def run():
        bus = create_bus(total_timeout=20.0, name=f"native_log_io_{tmp_path.name}")
        service = ProcessService(bus, emit_jsonl=False, interactive_tty=False)
        observed_stdout: list[str] = []
        observed_stderr: list[str] = []

        async def record_stdout(event: ProcessStdoutEvent):
            observed_stdout.append(event.line)

        async def record_stderr(event: ProcessStderrEvent):
            observed_stderr.append(event.line)

        bus.on(ProcessStdoutEvent, record_stdout)
        bus.on(ProcessStderrEvent, record_stderr)
        env = {**os.environ, "SNAP_DIR": str(output.parent)}
        args = [f"--url={source.as_uri()}"]
        with paths["stdout"].open("wb") as out, paths["stderr"].open("wb") as err:
            child = await asyncio.create_subprocess_exec(str(hook.path), *args, cwd=output, env=env, stdout=out, stderr=err)
            assert await child.wait() == 0
        event = ProcessStartedEvent(
            plugin_name=hook.plugin_name,
            hook_name=hook.name,
            hook_path=str(hook.path),
            hook_args=args,
            output_dir=str(output),
            env=env,
            timeout=20,
            pid=child.pid,
            subprocess=child,
            stdout_file=paths["stdout"],
            stderr_file=paths["stderr"],
            pid_file=output / "hook.pid",
            files_before=set(),
        )
        # Production retains the started event until reader ownership ends.
        with bus.event_history.retain(event):
            await bus.emit(event).now()
            proc = Process(cmd=[str(hook.path), *args], pwd=str(output), timeout=20)
            stdout_state, stderr_state = _OutputStreamState(), _OutputStreamState()
            with paths["stdout"].open() as stdout, paths["stderr"].open() as stderr:
                stream = None
                if final_drain:
                    completion = asyncio.create_task(
                        service._emit_new_output_lines(
                            event=event,
                            proc=proc,
                            stdout_reader=stdout if blocked_stream == "stdout" else stderr,
                            state=stdout_state if blocked_stream == "stdout" else stderr_state,
                            emit_partial=True,
                            event_class=ProcessStdoutEvent if blocked_stream == "stdout" else ProcessStderrEvent,
                        ),
                    )
                else:
                    stream = asyncio.create_task(
                        service._stream_output(
                            event=event,
                            proc=proc,
                            stdout_reader=stdout,
                            state=stdout_state,
                            stderr_reader=stderr,
                            stderr_state=stderr_state,
                        ),
                    )
                    completion = asyncio.create_task(service._finish_stream_output(stream))
                release.start()
                loop = asyncio.get_running_loop()
                last_tick, longest_tick = loop.time(), 0.0
                await asyncio.sleep(0.05)
                longest_tick = max(longest_tick, loop.time() - last_tick)
                stdout_state.stop_requested.set()
                if cancel_count:
                    for _ in range(cancel_count):
                        completion.cancel()
                        await asyncio.sleep(0.05)
                while not completion.done():
                    last_tick = loop.time()
                    await asyncio.sleep(0.01)
                    longest_tick = max(longest_tick, loop.time() - last_tick)
                if cancel_count:
                    with pytest.raises(asyncio.CancelledError):
                        await completion
                else:
                    await completion
                assert released.is_set(), "Completion must retain ownership until native read returns"
                if stream is not None:
                    assert stream.done(), "Reader ownership must finish before its file can close"
                    assert not stream.cancelled()
                # A cancelled direct final drain retains bytes already consumed by
                # its worker. The next drain must emit them once, without loss.
                await service._emit_new_output_lines(event=event, proc=proc, stdout_reader=stdout, state=stdout_state, emit_partial=True)
                await service._emit_new_output_lines(
                    event=event,
                    proc=proc,
                    stdout_reader=stderr,
                    state=stderr_state,
                    emit_partial=True,
                    event_class=ProcessStderrEvent,
                )
                assert observed_stdout == [line.strip() for line in stdout_state.stdout_lines]
                assert "".join(observed_stderr) == "".join(stderr_state.stdout_lines)
                records = [json.loads(line) for line in stdout_state.stdout_lines if line.startswith("{")]
                assert any(record.get("type") == "ArchiveResult" and record.get("status") == "succeeded" for record in records)
                assert longest_tick < 0.5, longest_tick
        await bus.wait_until_idle()
        await bus.destroy(clear=False)

    try:
        asyncio.run(run())
    finally:
        release.cancel()
        if release.ident is not None:
            release.join()
        else:
            os.close(held_fd)
    records = [json.loads(line) for line in (output / "urls.jsonl").read_text().splitlines()]
    assert [record["url"] for record in records] == ["https://example.com/discovered"]


@pytest.mark.parametrize("cancel_phase", ["stream_finish", "final_stdout"])
def test_real_hook_cancellation_finishes_output_consumers_and_completion(tmp_path: Path, cancel_phase: str):
    """Cancellation during a real consumer must not drop later hook records."""
    from abx_dl.events import ProcessCompletedEvent
    from abx_dl.execution import execute_hook

    hook = PluginCatalog.discover()["parse_txt_urls"].filter_hooks("Snapshot")[0]
    source = tmp_path / "input.txt"
    os.mkfifo(source)
    output = tmp_path / "snapshot" / hook.plugin_name
    output.mkdir(parents=True)

    async def run():
        bus = create_bus(total_timeout=20.0, name=f"cancel_log_completion_{tmp_path.name}")
        service = ProcessService(bus, emit_jsonl=False, interactive_tty=False)
        first_line = asyncio.Event()
        release_first = asyncio.Event()
        final_line = asyncio.Event()
        release_final = asyncio.Event()
        observed: list[str] = []
        completions: list[ProcessCompletedEvent] = []

        async def record_completion(event: ProcessCompletedEvent):
            completions.append(event)

        bus.on(ProcessCompletedEvent, record_completion)

        async def consume(event: ProcessStdoutEvent):
            if event.line.startswith("parsing "):
                first_line.set()
                await release_first.wait()
            elif '"type": "Snapshot"' in event.line:
                final_line.set()
                await release_final.wait()
            observed.append(event.line)

        bus.on(ProcessStdoutEvent, consume)
        hook_task = asyncio.create_task(
            execute_hook(
                hook,
                output_dir=output,
                env={**os.environ, "SNAP_DIR": str(output.parent), "TMP_DIR": str(tmp_path / "runtime"), "PYTHONUNBUFFERED": "1"},
                arguments={"url": source.as_uri()},
                bus=bus,
                attach_process_service=False,
            ),
        )
        try:
            await asyncio.wait_for(first_line.wait(), 10)
            started = await bus.find(ProcessStartedEvent, past=True, future=False)
            assert isinstance(started, ProcessStartedEvent)
            # Actual FIFO input lets the real parser exit only after the first
            # stdout consumer is waiting. Completion then stops streaming and
            # drains the remaining real Snapshot/ArchiveResult lines.
            await asyncio.to_thread(source.write_text, "https://example.com/discovered\n")
            assert await asyncio.wait_for(started.subprocess.wait(), 10) == 0
            owner = next(iter(service._active_process_event_tasks.values()))
            if cancel_phase == "final_stdout":
                release_first.set()
                await asyncio.wait_for(final_line.wait(), 10)
            else:
                # Let the process-exit waiter enter its stream completion wait.
                await asyncio.sleep(0.05)
            owner.cancel()
            await asyncio.sleep(0.05)
            owner.cancel()
            await asyncio.sleep(0.05)
            release_first.set()
            release_final.set()
            await asyncio.gather(hook_task, return_exceptions=True)
            assert owner.cancelled(), "Finalization must preserve owner cancellation"
            await bus.wait_until_idle()
            assert len(completions) == 1, "Cancelled owner lost its final completion"
            completed = completions[0]
            assert completed.cancelled
            records = [json.loads(line) for line in observed if line.startswith("{")]
            assert [record["status"] for record in records if record.get("type") == "ArchiveResult"] == ["succeeded"]
            assert observed == completed.stdout.splitlines()
            assert [json.loads(line)["url"] for line in (output / "urls.jsonl").read_text().splitlines()] == [
                "https://example.com/discovered",
            ]
        finally:
            release_first.set()
            release_final.set()
            if not hook_task.done():
                hook_task.cancel()
                await asyncio.gather(hook_task, return_exceptions=True)
            await bus.destroy(clear=False)

    asyncio.run(run())


def test_cancellation_during_real_index_write_preserves_finished_hook(tmp_path: Path):
    """A completed hook's native persistence finishes even if its owner stops."""
    from abx_dl.events import ProcessCompletedEvent
    from abx_dl.execution import execute_hook

    hook = PluginCatalog.discover()["parse_txt_urls"].filter_hooks("Snapshot")[0]
    source = tmp_path / "input.txt"
    source.write_text("https://example.com/discovered\n")
    output = tmp_path / "snapshot" / hook.plugin_name
    output.mkdir(parents=True)
    index = output.parent / "index.jsonl"
    os.mkfifo(index)
    persisted: list[str] = []

    def read_index():
        persisted.extend(index.read_text().splitlines())

    # The real index writer cannot open until this independent reader starts.
    reader = threading.Timer(1.5, read_index)

    async def run():
        bus = create_bus(total_timeout=20.0, name=f"cancel_index_write_{tmp_path.name}")
        service = ProcessService(bus, emit_jsonl=False, interactive_tty=False)
        result_seen = asyncio.Event()
        observed: list[str] = []
        completions: list[ProcessCompletedEvent] = []

        async def consume(event: ProcessStdoutEvent):
            observed.append(event.line)
            if '"type": "ArchiveResult"' in event.line:
                result_seen.set()

        async def completed(event: ProcessCompletedEvent):
            completions.append(event)

        bus.on(ProcessStdoutEvent, consume)
        bus.on(ProcessCompletedEvent, completed)
        task = asyncio.create_task(
            execute_hook(
                hook,
                output_dir=output,
                env={**os.environ, "SNAP_DIR": str(output.parent), "TMP_DIR": str(tmp_path / "runtime")},
                arguments={"url": source.as_uri()},
                bus=bus,
                attach_process_service=False,
            ),
        )
        await asyncio.wait_for(result_seen.wait(), 10)
        started = await bus.find(ProcessStartedEvent, past=True, future=False)
        assert isinstance(started, ProcessStartedEvent)
        assert await started.subprocess.wait() == 0
        reader.start()
        await asyncio.sleep(0.1)
        owner = next(iter(service._active_process_event_tasks.values()))
        owner.cancel()
        await asyncio.sleep(0.05)
        owner.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert owner.cancelled(), "Persistence must finish before cancellation propagates"
        await bus.wait_until_idle()
        assert len(completions) == 1
        result = completions[0]
        assert result.exit_code == 0
        assert result.status == "succeeded"
        assert not result.cancelled
        assert result.stdout.splitlines() == observed
        assert not list((tmp_path / "runtime" / "process").iterdir())
        await bus.destroy(clear=False)

    try:
        asyncio.run(run())
    finally:
        reader.cancel()
        if reader.ident is not None:
            reader.join()
    assert len(persisted) == 1
    record = json.loads(persisted[0])
    assert record["exit_code"] == 0
    assert record["status"] == "succeeded"
