import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path

import psutil
import pytest

from abx_dl.process_utils import graceful_kill_by_pid_file, validate_pid_file, write_pid_file_with_mtime


def test_pid_cleanup_terminates_the_matching_real_process_without_a_shell_file(tmp_path: Path) -> None:
    async def run() -> None:
        command = [sys.executable, "-m", "http.server", "0", "--bind", "127.0.0.1"]
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        pid_file = tmp_path / "server.pid"
        try:
            write_pid_file_with_mtime(pid_file, process.pid, psutil.Process(process.pid).create_time())
            identity = psutil.Process(process.pid).as_dict(attrs=["pid", "cmdline", "create_time", "status"])
            identity["pid_file_mtime"] = pid_file.stat().st_mtime
            # A refused identity check deliberately leaves the process running.
            # Assert that result before waiting for exit, so a refusal reports
            # its evidence and reaches finally instead of deadlocking in gather.
            killed = await graceful_kill_by_pid_file(pid_file, expected_argv=command)
            assert killed is True, {
                "expected_argv": command,
                "identity_before_cleanup": identity,
                "returncode": process.returncode,
            }
            exit_code = await process.wait()
            assert exit_code == -signal.SIGTERM
            assert not list(tmp_path.glob("*.sh"))
        finally:
            if process.returncode is None:
                process.kill()
            await process.wait()

    asyncio.run(run())


@pytest.mark.parametrize("mismatch", ["start_time", "executable", "entrypoint"])
def test_pid_validation_uses_process_metadata_and_rejects_mismatches(tmp_path: Path, mismatch: str) -> None:
    command = [sys.executable, "-m", "http.server", "0", "--bind", "127.0.0.1"]
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    pid_file = tmp_path / "server.pid"
    try:
        started_at = psutil.Process(process.pid).create_time()
        write_pid_file_with_mtime(pid_file, process.pid, started_at)
        assert validate_pid_file(pid_file, expected_argv=command)
        assert not list(tmp_path.glob("*.sh"))

        recorded_command = command.copy()
        if mismatch == "start_time":
            os.utime(pid_file, (started_at - 60, started_at - 60))
        elif mismatch == "executable":
            recorded_command[0] = str(tmp_path / "different-executable")
        else:
            recorded_command[1] = str(tmp_path / "different-entrypoint.py")

        assert not validate_pid_file(pid_file, expected_argv=recorded_command)
        assert not asyncio.run(graceful_kill_by_pid_file(pid_file, expected_argv=recorded_command))
        assert process.poll() is None
    finally:
        process.terminate()
        process.wait(timeout=10)
