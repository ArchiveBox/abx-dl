"""Persist a crawl heartbeat so stale Chrome sessions can be identified safely."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path


class CrawlHeartbeat:
    """Write and refresh one ``.heartbeat.json`` file for a live crawl."""

    def __init__(
        self,
        crawl_dir: Path,
        *,
        runtime: str,
        crawl_id: str,
        kill_after_seconds: int = 180,
        update_interval_seconds: int = 5,
    ) -> None:
        self.crawl_dir = crawl_dir
        self.runtime = runtime
        self.crawl_id = crawl_id
        self.kill_after_seconds = kill_after_seconds
        self.update_interval_seconds = update_interval_seconds
        self.path = self.crawl_dir / ".heartbeat.json"
        self.owner_pid = os.getpid()
        self._task: asyncio.Task[None] | None = None

    def _payload(self) -> dict[str, object]:
        return {
            "runtime": self.runtime,
            "crawl_id": self.crawl_id,
            "owner_pid": self.owner_pid,
            "last_alive_at": time.time(),
            "kill_after_seconds": self.kill_after_seconds,
        }

    def _write(self) -> None:
        self.crawl_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        tmp_path.write_text(json.dumps(self._payload(), separators=(",", ":"), sort_keys=True))
        tmp_path.replace(self.path)

    async def start(self) -> None:
        """Write the initial heartbeat and start refreshing it in the background."""
        self._write()

        async def refresh() -> None:
            while True:
                await asyncio.sleep(self.update_interval_seconds)
                self._write()

        self._task = asyncio.create_task(refresh())

    async def stop(self) -> None:
        """Stop refreshing and remove the heartbeat file."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self.path.unlink(missing_ok=True)
