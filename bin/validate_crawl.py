#!/usr/bin/env -S uv run python
"""Validate that a real all-plugin crawl executed its complete intended surface."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from abx_dl.catalog import PluginCatalog


SUCCESS_STATUSES = {"succeeded", "noresult", "noresults"}
CHROME_SNAPSHOT_OWNER = ("chrome", "on_Snapshot__00_chrome_launch.daemon.bg")
CHROME_CRAWL_SETUP_HOOK = "on_CrawlSetup__90_chrome_launch.daemon.bg"
CHROME_CRAWL_WAIT_HOOK = "on_CrawlSetup__91_chrome_wait"
UBLOCK_SNAPSHOT_CONFIG = ("ublock", "on_Snapshot__11_ublock_config")
UBLOCK_CRAWL_SETUP_HOOK = "on_CrawlSetup__95_ublock_config"
CRAWL_ISOLATION_NOOP = "CHROME_ISOLATION=crawl"
UBLOCK_CONFIGURED_MESSAGE = "Disabled uBlock top-level strict blocking"


def load_records(index_path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for line_number, line in enumerate(index_path.read_text(errors="replace").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise SystemExit(f"Invalid JSONL at {index_path}:{line_number}: {error}") from error
        if not isinstance(record, dict):
            raise SystemExit(f"Expected an object at {index_path}:{line_number}")
        records.append(record)
    return records


def validate_crawl_isolation(
    records: list[dict[str, object]],
    *,
    snapshot_count: int,
) -> None:
    """Validate the ownership handoff for crawl-scoped browser setup."""
    results = [record for record in records if record.get("type") == "ArchiveResult"]
    unexpected_skips = []
    chrome_owner_skips = []
    ublock_config_skips = []
    for record in results:
        if record.get("status") != "skipped":
            continue
        owner = (str(record.get("plugin")), str(record.get("hook_name")))
        if owner == CHROME_SNAPSHOT_OWNER and record.get("output_str") == CRAWL_ISOLATION_NOOP:
            chrome_owner_skips.append(record)
            continue
        if owner == UBLOCK_SNAPSHOT_CONFIG and record.get("output_str") == CRAWL_ISOLATION_NOOP:
            ublock_config_skips.append(record)
            continue
        unexpected_skips.append((*owner, record.get("output_str")))
    if unexpected_skips:
        raise SystemExit(f"Unexpected skipped ArchiveResult records: {unexpected_skips}")
    if len(chrome_owner_skips) != snapshot_count:
        raise SystemExit(
            "Expected exactly one crawl-owned Chrome Snapshot no-op per Snapshot, "
            f"got {len(chrome_owner_skips)} skips for {snapshot_count} Snapshots",
        )
    if len(ublock_config_skips) != snapshot_count:
        raise SystemExit(
            "Expected exactly one crawl-owned uBlock Snapshot no-op per Snapshot, "
            f"got {len(ublock_config_skips)} skips for {snapshot_count} Snapshots",
        )

    processes = [record for record in records if record.get("type") == "Process"]

    def require_process(
        plugin_name: str,
        hook_name: str,
        label: str,
    ) -> dict[str, object]:
        matches = [record for record in processes if record.get("plugin") == plugin_name and record.get("hook_name") == hook_name]
        if len(matches) != 1:
            raise SystemExit(
                f"Expected exactly one successful {label} process for {hook_name}, got {len(matches)}",
            )
        process = matches[0]
        if process.get("status") != "succeeded" or process.get("exit_code") != 0:
            raise SystemExit(
                f"{label} process did not succeed: {hook_name}: "
                f"status={process.get('status')} exit_code={process.get('exit_code')} "
                f"stderr={process.get('stderr')}",
            )
        return process

    setup_process = require_process("chrome", CHROME_CRAWL_SETUP_HOOK, "Chrome")
    setup_records = []
    for line in str(setup_process.get("stdout", "")).splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            setup_records.append(record)
    if {"succeeded": True, "skipped": False} not in setup_records:
        raise SystemExit(
            "Crawl-owned Chrome daemon did not report successful non-skipped cleanup",
        )
    wait_process = require_process("chrome", CHROME_CRAWL_WAIT_HOOK, "Chrome")
    if " ready pid=" not in str(wait_process.get("stdout", "")):
        raise SystemExit("Crawl-owned Chrome readiness hook did not observe a ready CDP session")

    ublock_process = require_process("ublock", UBLOCK_CRAWL_SETUP_HOOK, "uBlock")
    if UBLOCK_CONFIGURED_MESSAGE not in str(ublock_process.get("stderr", "")):
        raise SystemExit(
            "Crawl-owned uBlock setup did not confirm strict-blocking configuration",
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("index", type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--plugins-dir", required=True, type=Path)
    parser.add_argument("--config", default=".github/configs/ci-tooling.json", type=Path)
    args = parser.parse_args()

    index_path = args.index.resolve()
    output_dir = args.output_dir.resolve()
    plugins = PluginCatalog.discover(args.plugins_dir.resolve(), runtime="abx-dl")
    disabled_plugins = {
        plugin.name
        for plugin in plugins.values()
        if plugin.enabled_key in plugin.config.properties
        and str(
            os.environ.get(
                plugin.enabled_key,
                plugin.config.properties[plugin.enabled_key].get("default", True),
            ),
        )
        .strip()
        .lower()
        in {"", "0", "false", "no", "off", "none", "null"}
    }
    validation = json.loads(args.config.read_text())["crawl_validation"]
    required_plugin_outputs: dict[str, list[str]] = validation["required_plugin_outputs"]

    records = load_records(index_path)
    snapshots = [record for record in records if record.get("type") == "Snapshot"]
    if not snapshots:
        raise SystemExit("Crawl manifest has no Snapshot record")

    results = [record for record in records if record.get("type") == "ArchiveResult"]
    actual_hooks = {(str(record.get("plugin")), str(record.get("hook_name"))) for record in results}
    expected_hooks = {
        (plugin.name, hook.name)
        for plugin in plugins.values()
        if plugin.name not in disabled_plugins
        for hook in plugin.filter_hooks("Snapshot")
    }
    missing_hooks = sorted(expected_hooks - actual_hooks)
    if missing_hooks:
        raise SystemExit(f"Snapshot hooks missing ArchiveResult records: {missing_hooks}")

    failures = [
        (record.get("plugin"), record.get("hook_name"), record.get("status"), record.get("output_str"))
        for record in results
        if record.get("status") not in SUCCESS_STATUSES | {"skipped"}
    ]
    if failures:
        raise SystemExit(f"Unsuccessful ArchiveResult records: {failures}")

    validate_crawl_isolation(records, snapshot_count=len(snapshots))

    for plugin_name, relative_paths in required_plugin_outputs.items():
        plugin_results = [record for record in results if record.get("plugin") == plugin_name and record.get("status") == "succeeded"]
        if not plugin_results:
            raise SystemExit(f"Required plugin did not succeed: {plugin_name}")
        for relative_path in relative_paths:
            output_paths = list(output_dir.glob(relative_path))
            if not output_paths or any(not path.is_file() or path.stat().st_size == 0 for path in output_paths):
                raise SystemExit(f"Required non-empty output is missing: {output_dir / relative_path}")

    print(
        json.dumps(
            {
                "archive_results": len(results),
                "expected_snapshot_hooks": len(expected_hooks),
                "plugins": sorted({plugin for plugin, _ in expected_hooks}),
            },
            indent=2,
        ),
    )


if __name__ == "__main__":
    main()
