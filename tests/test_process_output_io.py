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
                assert observed_stderr == [line.strip() for line in stderr_state.stdout_lines]
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
