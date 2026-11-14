"""CI recognizes unavailable paid credentials using real provider responses."""

import importlib.util
import json
import os
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "bin/check_ai_credentials.py"


@pytest.mark.parametrize(
    ("key", "reason"),
    [
        ("", "ANTHROPIC_API_KEY is not configured"),
        ("invalid-key-for-authentication-rejection", "Anthropic rejected the API credentials"),
    ],
)
def test_unavailable_credentials_disable_only_paid_analysis(tmp_path, key, reason):
    github_env = tmp_path / "github-env"
    result = subprocess.run(
        ["uv", "run", "--no-project", "python", str(SCRIPT)],
        env={**os.environ, "ANTHROPIC_API_KEY": key, "GITHUB_ENV": str(github_env)},
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "noresults", "reason": reason}
    assert github_env.read_text().splitlines() == [
        "AUTOTAG_ENABLED=False",
        "AUTOSUMMARY_ENABLED=False",
        "CLAUDECHROME_ENABLED=False",
        f"CI_ANTHROPIC_UNAVAILABLE={reason}",
    ]


def test_unrelated_real_provider_error_remains_fatal():
    # A real unsupported API route is an HTTP error, not an unavailable account.
    with pytest.raises(urllib.error.HTTPError) as rejected:
        urllib.request.urlopen("https://api.anthropic.com/v1/archivebox-invalid-route", timeout=30)
    response = json.loads(rejected.value.read())
    assert rejected.value.code == 404
    spec = importlib.util.spec_from_file_location("check_ai_credentials", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.unavailable_reason(rejected.value.code, response) == ""
