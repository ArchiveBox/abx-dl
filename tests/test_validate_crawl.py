import importlib.util
from pathlib import Path

import pytest


VALIDATOR_PATH = Path(__file__).parents[1] / "bin" / "validate_crawl.py"
SPEC = importlib.util.spec_from_file_location("validate_crawl", VALIDATOR_PATH)
assert SPEC is not None and SPEC.loader is not None
validate_crawl = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validate_crawl)


def _isolation_records(*, ublock_reason: str = "CHROME_ISOLATION=crawl"):
    return [
        {
            "type": "ArchiveResult",
            "plugin": "chrome",
            "hook_name": "on_Snapshot__00_chrome_launch.daemon.bg",
            "status": "skipped",
            "output_str": "CHROME_ISOLATION=crawl",
        },
        {
            "type": "ArchiveResult",
            "plugin": "ublock",
            "hook_name": "on_Snapshot__11_ublock_config",
            "status": "skipped",
            "output_str": ublock_reason,
        },
        {
            "type": "Process",
            "plugin": "chrome",
            "hook_name": "on_CrawlSetup__90_chrome_launch.daemon.bg",
            "status": "succeeded",
            "exit_code": 0,
            "stdout": '{"succeeded":true,"skipped":false}\n',
            "stderr": "",
        },
        {
            "type": "Process",
            "plugin": "chrome",
            "hook_name": "on_CrawlSetup__91_chrome_wait",
            "status": "succeeded",
            "exit_code": 0,
            "stdout": "chrome ready pid=123",
            "stderr": "",
        },
        {
            "type": "Process",
            "plugin": "ublock",
            "hook_name": "on_CrawlSetup__95_ublock_config",
            "status": "succeeded",
            "exit_code": 0,
            "stdout": "",
            "stderr": "[+] Disabled uBlock top-level strict blocking; subresource filtering remains enabled\n",
        },
    ]


def test_crawl_isolation_accepts_only_the_two_complementary_snapshot_noops():
    """Crawl-owned Chrome and uBlock each require one explicit Snapshot no-op.

    WHY: the Snapshot uBlock hook is intentionally complementary to CrawlSetup,
    not optional work. Accepting its skip is safe only when its exact reason and
    the successful browser-global CrawlSetup configuration are both present.
    """
    validate_crawl.validate_crawl_isolation(_isolation_records(), snapshot_count=1)


@pytest.mark.parametrize("reason", ["", "CHROME_ISOLATION=snapshot"])
def test_crawl_isolation_rejects_unexplained_ublock_snapshot_skips(reason):
    with pytest.raises(SystemExit, match="Unexpected skipped ArchiveResult"):
        validate_crawl.validate_crawl_isolation(
            _isolation_records(ublock_reason=reason),
            snapshot_count=1,
        )


def test_crawl_isolation_rejects_ublock_noop_without_successful_crawl_setup():
    """A typed no-op must not mask a missing or failed owner hook."""
    records = [record for record in _isolation_records() if record.get("hook_name") != "on_CrawlSetup__95_ublock_config"]
    with pytest.raises(SystemExit, match="uBlock process"):
        validate_crawl.validate_crawl_isolation(records, snapshot_count=1)
