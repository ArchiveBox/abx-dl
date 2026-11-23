"""Monitoring follows real CLI configuration, independently of capture selection."""

import gzip
import json
import subprocess
import sys
from io import BytesIO

from test_cli import _cli_env, _run_cli


def test_disabled_bus_does_not_import_monitoring(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, asyncio; from abx_dl.orchestrator import create_bus; "
            "bus = create_bus(); assert not bus.middlewares; asyncio.run(bus.destroy()); "
            "assert 'abx_plugins.plugins.sentry.runtime' not in sys.modules; "
            "assert not any(n.startswith(('sentry_sdk', 'opentelemetry')) for n in sys.modules)",
        ],
        env=_cli_env(tmp_path),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_cli_sends_native_bus_traces_with_selected_capture_plugins(tmp_path, httpserver):
    httpserver.expect_request("/api/1/envelope/", method="POST").respond_with_json({})
    httpserver.expect_request("/urls.txt").respond_with_data("https://example.com/sentry-plugin\n", content_type="text/plain")
    installed = _run_cli(tmp_path, "install", "sentry")
    assert installed.returncode == 0, installed.stdout + installed.stderr
    dsn = httpserver.url_for("/1").replace("http://", "http://public@")
    for setting in (f"SENTRY_DSN={dsn}", "SENTRY_TRACES_SAMPLE_RATE=1", "SENTRY_ENABLE_METRICS=True", "SENTRY_ENABLED=True"):
        configured = _run_cli(tmp_path, "config", "--set", setting)
        assert configured.returncode == 0, configured.stdout + configured.stderr
    output = tmp_path / "capture"
    result = _run_cli(tmp_path, "dl", "--plugins=parse_txt_urls", "--dir", str(output), httpserver.url_for("/urls.txt"))
    assert result.returncode == 0, result.stdout + result.stderr
    records = [json.loads(line) for line in (output / "index.jsonl").read_text().splitlines()]
    assert any(r.get("type") == "ArchiveResult" and r.get("plugin") == "parse_txt_urls" and r["status"] == "succeeded" for r in records)
    assert "https://example.com/sentry-plugin" in (output / "parse_txt_urls/urls.jsonl").read_text()
    transactions = []
    for request, response in httpserver.log:
        if request.path != "/api/1/envelope/":
            continue
        assert response.status_code == 200
        data = gzip.decompress(request.data) if request.headers.get("Content-Encoding") == "gzip" else request.data
        envelope = BytesIO(data)
        json.loads(envelope.readline())
        while header := envelope.readline():
            metadata = json.loads(header)
            payload = json.loads(envelope.read(metadata["length"]))
            envelope.readline()
            if metadata["type"] == "transaction":
                transactions.append(payload)
    assert any("SnapshotEvent" in t["transaction"] for t in transactions)
    assert any("ProcessEvent" in t["transaction"] for t in transactions)
