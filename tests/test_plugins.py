from abx_dl.models import discover_plugins, parse_hook_filename


def test_parse_hook_filename_marks_bg_hooks() -> None:
    assert parse_hook_filename("on_Snapshot__66_papersdl.finite.bg.py") == ("Snapshot", 66, True)


def test_discover_plugins_marks_papersdl_as_background() -> None:
    plugins = discover_plugins()
    papersdl_hooks = plugins["papersdl"].hooks

    papersdl_hook = next(hook for hook in papersdl_hooks if "Snapshot" in hook.name and hook.order == 66)

    assert papersdl_hook.is_background is True
    assert papersdl_hook.path.parent == plugins["papersdl"].path
    assert "papersdl" in papersdl_hook.path.name


def test_discover_plugins_marks_extension_install_hooks_as_foreground() -> None:
    plugins = discover_plugins()

    expected = {
        "ublock": "on_Install__80_ublock_extension",
        "istilldontcareaboutcookies": "on_Install__81_istilldontcareaboutcookies_extension",
        "singlefile": "on_Install__82_singlefile",
        "twocaptcha": "on_Install__83_twocaptcha",
        "claudechrome": "on_Install__84_claudechrome",
    }

    for plugin_name, hook_name in expected.items():
        hook = next(hook for hook in plugins[plugin_name].get_install_hooks() if hook.name == hook_name)
        assert hook.is_background is False
        assert ".bg." not in hook.path.name
