import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from abx_dl.config import get_initial_env, get_required_binary_requests
from abx_dl.models import discover_plugins, filter_plugins, parse_hook_filename


def test_parse_hook_filename_marks_bg_hooks() -> None:
    assert parse_hook_filename("on_Snapshot__66_papersdl.finite.bg.py") == ("Snapshot", 66, True)
    assert parse_hook_filename("on_Snapshot__9_chrome_wait.js") == ("Snapshot", 9, False)
    assert parse_hook_filename("on_Snapshot__chrome_wait.js") == ("Snapshot", 0, False)


def test_discover_plugins_marks_papersdl_as_background() -> None:
    plugins = discover_plugins()
    papersdl_hooks = plugins["papersdl"].hooks

    papersdl_hook = next(hook for hook in papersdl_hooks if "Snapshot" in hook.name and hook.order == 66)

    assert papersdl_hook.is_background is True
    assert papersdl_hook.path.parent == plugins["papersdl"].path
    assert "papersdl" in papersdl_hook.path.name


def test_discover_plugins_extension_plugins_declare_required_binaries() -> None:
    plugins = discover_plugins()

    expected = ["ublock", "istilldontcareaboutcookies", "singlefile", "twocaptcha", "claudechrome"]

    for plugin_name in expected:
        assert plugins[plugin_name].config.required_binaries


def test_discover_plugins_extends_packaged_plugins_with_runtime_plugin_dir(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "runtime_only"
    plugin_dir.mkdir()
    title_hook = discover_plugins()["title"].hooks[0]
    hook = plugin_dir / title_hook.path.name
    shutil.copy2(title_hook.path, hook)

    env = os.environ.copy()
    env["ABX_PLUGINS_DIR"] = str(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json; "
                "from abx_dl.models import discover_plugins; "
                "plugins = discover_plugins(runtime='archivebox'); "
                "print(json.dumps({name: [hook.name for hook in plugin.hooks] for name, plugin in plugins.items()}))"
            ),
        ],
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    discovered_hooks = json.loads(result.stdout)

    assert "wget" in discovered_hooks
    assert "parse_html_urls" in discovered_hooks
    assert discovered_hooks["runtime_only"] == [title_hook.name]


def test_filter_plugins_does_not_add_binary_providers_for_wget() -> None:
    plugins = discover_plugins()

    selected = filter_plugins(plugins, ["wget"], include_providers=True)

    assert "wget" in selected
    assert "env" not in selected
    assert "apt" not in selected
    assert "brew" not in selected
    assert "npm" not in selected
    assert "chromewebstore" not in selected


def test_filter_plugins_includes_required_plugins_without_binary_providers() -> None:
    plugins = discover_plugins()

    selected = filter_plugins(plugins, ["ublock"], include_providers=True)

    assert "ublock" in selected
    assert "chrome" in selected
    assert "puppeteer" not in selected
    assert "chromewebstore" not in selected
    assert "env" not in selected
    assert "apt" not in selected
    assert "brew" not in selected
    assert "npm" not in selected


def test_filter_plugins_prunes_plugins_with_disabled_required_plugins() -> None:
    plugins = discover_plugins(runtime="archivebox")

    selected = filter_plugins(plugins, ["wget", "accessibility", "ublock"], include_providers=True, disabled_names=["chrome"])

    assert "wget" in selected
    assert "chrome" not in selected
    assert "accessibility" not in selected
    assert "ublock" not in selected


def test_required_binary_requests_ignore_nonexistent_derived_binary_paths() -> None:
    plugins = discover_plugins()
    plugin = plugins["ytdlp"]

    requests = get_required_binary_requests(
        plugin,
        plugin.config.required_binaries,
        overrides=get_initial_env(),
        derived_overrides={
            "YTDLP_BINARY": "/does/not/exist/yt-dlp",
            "NODE_BINARY": "/does/not/exist/node",
            "FFMPEG_BINARY": "/does/not/exist/ffmpeg",
        },
        run_output_dir=Path.cwd(),
    )

    request_names = {request["name"] for request in requests}
    assert "yt-dlp" in request_names
    assert "node" in request_names
    assert "ffmpeg" in request_names
    assert "/does/not/exist/yt-dlp" not in request_names
    assert "/does/not/exist/node" not in request_names
    assert "/does/not/exist/ffmpeg" not in request_names
