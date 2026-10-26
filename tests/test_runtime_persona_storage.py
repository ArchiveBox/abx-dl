"""Runtime browser databases must stay on local temporary storage."""

import json
import os
from pathlib import Path

import pytest

from abx_dl.catalog import PluginCatalog
from test_config import assemble_env
from test_executor import _run_download


def test_default_runtime_personas_use_tmp_dir(tmp_path):
    output = tmp_path / "archive" / "capture"
    env = assemble_env(run_output_dir=output)
    assert Path(env["PERSONAS_DIR"]).is_relative_to(Path(env["TMP_DIR"]))
    assert not Path(env["PERSONAS_DIR"]).is_relative_to(output)
    assert assemble_env(run_output_dir=output)["PERSONAS_DIR"] == env["PERSONAS_DIR"]
    assert assemble_env(run_output_dir=output.parent / "other")["PERSONAS_DIR"] != env["PERSONAS_DIR"]


def test_runtime_preserves_host_persona_symlink(tmp_path):
    output = tmp_path / "archive" / "capture"
    local_personas = tmp_path / "local" / "personas"
    local_personas.mkdir(parents=True)
    output.mkdir(parents=True)
    (output / ".persona").symlink_to(local_personas, target_is_directory=True)
    env = assemble_env(run_output_dir=output)
    assert Path(env["PERSONAS_DIR"]).resolve() == local_personas


@pytest.mark.parametrize("isolation", ["crawl", "snapshot"])
def test_real_browser_profile_stays_outside_archive(tmp_path, httpserver, isolation):
    httpserver.expect_request("/").respond_with_data("<title>Local runtime</title>", content_type="text/html")
    output = tmp_path / "archive" / "capture"
    catalog = PluginCatalog.discover().select(["title", "hashes"])
    results = _run_download(
        httpserver.url_for("/"),
        catalog,
        output,
        config_overrides={"CHROME_HEADLESS": True, "CHROME_ISOLATION": isolation},
        auto_install=True,
        emit_jsonl=False,
    )
    assert {r.plugin for r in results if r.status == "succeeded"} >= {"title", "hashes"}
    assert list(Path(os.environ["TMP_DIR"]).rglob("chrome_profile/Default/Preferences"))
    assert not list(output.rglob("chrome_profile"))
    assert not list(output.rglob("archivebox-extensions"))
    manifest = json.loads((output / "hashes" / "hashes.json").read_text())
    assert not any("chrome_profile" in row["path"] or ".persona" in row["path"] for row in manifest["files"])
