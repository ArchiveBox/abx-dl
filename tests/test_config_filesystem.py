import asyncio
import json
import os
import threading
from pathlib import Path

from abx_dl.catalog import PluginCatalog
from abx_dl.config import GlobalConfig, RuntimeConfig, get_plugin_env


def test_plugin_metadata_read_does_not_block_other_coroutines(tmp_path: Path) -> None:
    browser_file = tmp_path / "chrome" / "browser.json"
    browser_file.parent.mkdir()
    os.mkfifo(browser_file)
    identity = "Mozilla/5.0 Chrome/136.0.0.0 Safari/537.36"
    reader_connected = threading.Event()
    release_metadata = threading.Event()
    producer_released: list[bool] = []

    def write_metadata() -> None:
        # A real pipe keeps the metadata read pending until the independent
        # coroutine runs. The deadline only releases a broken implementation
        # so its failure cannot leave the test blocked indefinitely.
        with browser_file.open("w") as stream:
            reader_connected.set()
            producer_released.append(release_metadata.wait(2))
            json.dump({"userAgentOverride": {"userAgent": identity}}, stream)

    producer = threading.Thread(target=write_metadata, daemon=True)
    producer.start()
    catalog = PluginCatalog.discover()
    config = RuntimeConfig(user=GlobalConfig(SNAP_DIR=str(tmp_path), USER_AGENT="", WGET_TIMEOUT=73), derived={})

    async def run():
        async def independent_work() -> None:
            assert await asyncio.to_thread(reader_connected.wait, 5)
            release_metadata.set()

        independent = asyncio.create_task(independent_work())
        try:
            return await get_plugin_env(None, plugin=catalog["wget"], run_output_dir=tmp_path, config=config)
        finally:
            await independent

    try:
        result = asyncio.run(run())
    finally:
        release_metadata.set()
        producer.join(5)
    assert not producer.is_alive()
    assert producer_released == [True], "Plugin metadata read blocked the event loop until the producer's guard deadline"
    resolved = result.model_dump()
    assert resolved["USER_AGENT"] == identity
    assert resolved["WGET_USER_AGENT"] == identity
    assert resolved["WGET_TIMEOUT"] == 73
    assert resolved["SNAP_DIR"] == str(tmp_path)
