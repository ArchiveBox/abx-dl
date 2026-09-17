"""Exercise the installed ripgrep CLI and its real child-process lifecycle."""

import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from uuid import uuid4

import psutil
import pytest

from abx_dl.execution import iter_plugin_command
from abx_dl.models import PluginCommand
from abx_plugins.plugins.search_backend_ripgrep import search as ripgrep


@pytest.mark.parametrize("finish", ["close", "cancel", "timeout"])
def test_search_streams_and_reaps_ripgrep(tmp_path, finish):
    rg = shutil.which("rg")
    assert rg, "Install the real ripgrep binary before running this test"
    snapshot_id = str(uuid4())
    root = tmp_path / "snapshots" / snapshot_id
    root.mkdir(parents=True)
    query = "needle" + uuid4().hex
    (root / "a.txt").write_text(query + "\n")
    # A sparse real archive file keeps the search busy without allocating RAM or
    # writing gigabytes. --no-mmap makes cancellation independent of page faults.
    with (root / "z.txt").open("wb") as large:
        large.truncate(8 * 1024**3)
        for offset in range(0, 8 * 1024**3, 1024**2):
            large.seek(offset)
            large.write(b"\n")
    command = PluginCommand(
        name="search",
        plugin_name="search_backend_ripgrep",
        path=Path(sys.executable),
        args=[str(Path(ripgrep.__file__)), "search"],
    )
    env = {
        **os.environ,
        "SNAP_DIR": str(tmp_path / "snapshots"),
        "RIPGREP_BINARY": rg,
        "RIPGREP_ARGS_EXTRA": '["--no-mmap", "--text", "--sort", "path"]',
        "RIPGREP_TIMEOUT": "30",
    }
    stop = threading.Event()
    iterator = iter_plugin_command(command, arguments={"query": query}, env=env, timeout=2, stop_event=stop if finish == "cancel" else None)
    children = []
    try:
        assert next(iterator) == snapshot_id
        children = [child for child in psutil.Process().children(recursive=True) if query in " ".join(child.cmdline())]
        assert len(children) == 2, "Expected the real Python search CLI and its rg child"
        # Suspend the real engine to make a silent, still-running search deterministic.
        # The executor must cancel/timeout it, including while no stdout arrives.
        for child in children:
            if Path(child.exe()).name == Path(rg).name:
                child.send_signal(signal.SIGSTOP)
        started = time.monotonic()
        if finish == "close":
            iterator.close()
        elif finish == "cancel":
            timer = threading.Timer(0.1, stop.set)
            timer.start()
            try:
                assert list(iterator) == []
            finally:
                timer.cancel()
        else:
            with pytest.raises(subprocess.TimeoutExpired):
                list(iterator)
        assert time.monotonic() - started < 3
        _, alive = psutil.wait_procs(children, timeout=2)
        assert not alive, [(p.pid, p.status()) for p in alive]
    finally:
        iterator.close()
        # Preserve test isolation even on the old broken executor's red run.
        for child in psutil.Process().children(recursive=True):
            try:
                if query in " ".join(child.cmdline()):
                    child.kill()
            except psutil.NoSuchProcess:
                pass
