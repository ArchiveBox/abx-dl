import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from abx_dl.catalog import PluginCatalog, PluginConfigResolver
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
        timeout=30,
        check=True,
    )
    discovered_hooks = json.loads(result.stdout)

    assert "wget" in discovered_hooks
    assert "parse_html_urls" in discovered_hooks
    assert discovered_hooks["runtime_only"] == [title_hook.name]


def test_plugin_catalog_exposes_complete_metadata_and_sorted_hooks() -> None:
    catalog = PluginCatalog.discover(runtime="archivebox")

    assert "wget" in catalog
    assert catalog["wget"].manifest["properties"]["WGET_ENABLED"]["type"] == "boolean"
    assert catalog.schemas["wget"]["title"] == catalog["wget"].config.title
    hooks = catalog.hooks("Snapshot", names=["title", "wget"])
    assert hooks == sorted(hooks, key=lambda item: item[1].sort_key)
    assert {plugin.name for plugin, _hook in hooks} == {"chrome", "title", "wget"}


def test_plugin_catalog_exposes_manifest_declared_commands() -> None:
    catalog = PluginCatalog.discover(runtime="archivebox")

    search = catalog.command("search_backend_ripgrep", "search")
    flush = catalog.command("search_backend_ripgrep", "flush")

    assert search is not None
    assert flush is not None
    assert search.path.name == "search.py"
    assert search.args == ["search"]
    assert flush.path == search.path
    assert flush.args == ["flush"]


def test_plugin_catalog_rejects_command_paths_outside_plugin(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "example"
    plugin_dir.mkdir()
    (plugin_dir / "config.json").write_text(
        '{"commands":{"escape":["../outside.py"]},"properties":{}}',
    )
    (tmp_path / "outside.py").write_text("#!/bin/sh\n")
    catalog = PluginCatalog.discover(plugins_dir=tmp_path)

    assert catalog.command("example", "escape") is None


def test_plugin_catalog_extra_dirs_override_packaged_plugins(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "wget"
    plugin_dir.mkdir()
    (plugin_dir / "config.json").write_text('{"title":"Custom Wget","properties":{}}')

    catalog = PluginCatalog.discover(extra_plugin_dirs=[tmp_path])

    assert catalog["wget"].path == plugin_dir
    assert catalog["wget"].manifest["title"] == "Custom Wget"


def test_default_selection_excludes_plugins_that_require_explicit_selection(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "explicit"
    plugin_dir.mkdir()
    (plugin_dir / "config.json").write_text('{"title":"Explicit","x-auto-run":false,"properties":{}}')
    catalog = PluginCatalog.discover(plugins_dir=tmp_path)

    assert "explicit" not in catalog.select()
    assert "explicit" in catalog.select(["explicit"])


def test_default_selection_can_be_empty() -> None:
    explicit = PluginCatalog.discover()["wget"].model_copy(
        update={"config": PluginCatalog.discover()["wget"].config.model_copy(update={"x_auto_run": False})},
    )

    assert not PluginCatalog({"explicit": explicit}).select()


def test_template_path_cannot_escape_plugin_templates(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "example"
    templates_dir = plugin_dir / "templates"
    templates_dir.mkdir(parents=True)
    (plugin_dir / "config.json").write_text('{"title":"Example","properties":{}}')
    (templates_dir / "details.html").write_text("details")
    outside = tmp_path / "outside.html"
    outside.write_text("outside")
    catalog = PluginCatalog.discover(plugins_dir=tmp_path)

    assert catalog.template_path("example", "details") == templates_dir / "details.html"
    assert catalog.template_path("example", "../../outside") is None
    assert catalog.template_path("example", str(outside.with_suffix(""))) is None


def test_plugin_config_resolver_uses_manifest_aliases_and_dependencies() -> None:
    resolver = PluginConfigResolver(PluginCatalog.discover(runtime="archivebox"))

    resolved = resolver.resolve(user_config={"SAVE_WGET": "False", "UBLOCK_ENABLED": "True"}, environ={})
    enabled = resolver.enabled_plugin_names(resolved=resolved)

    assert resolver.canonical_key("SAVE_WGET") == "WGET_ENABLED"
    assert "wget" not in enabled
    assert "ublock" in enabled
    assert "chrome" in enabled
    assert "wget" not in resolver.enabled_plugin_names_from_flat({"WGET_ENABLED": False})
    assert resolver.runtime_settings("wget", {"TIMEOUT": 91, "WGET_BINARY": "/bin/wget"}) == {
        "enabled": True,
        "timeout": 91,
        "binary": "/bin/wget",
    }


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
    assert list(selected).index("chrome") < list(selected).index("ublock")
    assert "puppeteer" not in selected
    assert "chromewebstore" not in selected
    assert "env" not in selected
    assert "apt" not in selected
    assert "brew" not in selected
    assert "npm" not in selected

    all_selected = filter_plugins(plugins, None, include_providers=True)
    assert list(all_selected).index("chrome") < list(all_selected).index("ublock")


def test_filter_plugins_prunes_plugins_with_disabled_required_plugins() -> None:
    plugins = discover_plugins(runtime="archivebox")

    selected = filter_plugins(plugins, ["wget", "accessibility", "ublock"], include_providers=True, disabled_names=["chrome"])

    assert "wget" in selected
    assert "chrome" not in selected
    assert "accessibility" not in selected
    assert "ublock" not in selected


def test_required_binary_requests_preserve_user_binary_overrides_and_ignore_stale_derived_paths() -> None:
    plugins = discover_plugins()
    plugin = plugins["ytdlp"]

    requests = get_required_binary_requests(
        plugin,
        plugin.config.required_binaries,
        overrides={**get_initial_env(), "YTDLP_BINARY": "/custom/tools/yt-dlp"},
        derived_overrides={
            "YTDLP_BINARY": "/does/not/exist/yt-dlp",
            "NODE_BINARY": "/does/not/exist/node",
            "FFMPEG_BINARY": "/does/not/exist/ffmpeg",
        },
        run_output_dir=Path.cwd(),
    )

    request_names = {request["name"] for request in requests}
    assert "/custom/tools/yt-dlp" in request_names
    assert "node" in request_names
    assert "ffmpeg" in request_names
    assert "yt-dlp" not in request_names
    assert "/does/not/exist/yt-dlp" not in request_names
    assert "/does/not/exist/node" not in request_names
    assert "/does/not/exist/ffmpeg" not in request_names
