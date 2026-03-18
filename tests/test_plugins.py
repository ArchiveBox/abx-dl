from pathlib import Path

from abx_dl.plugins import discover_plugins, parse_hook_filename


def test_parse_hook_filename_marks_bg_hooks() -> None:
    assert parse_hook_filename('on_Snapshot__66_papersdl.finite.bg.py') == ('Snapshot', 6, 6, True)


def test_discover_plugins_marks_papersdl_as_background() -> None:
    plugins = discover_plugins()
    papersdl_hooks = plugins['papersdl'].hooks

    papersdl_hook = next(
        hook for hook in papersdl_hooks
        if 'Snapshot' in hook.name and hook.step == 6 and hook.priority == 6
    )

    assert papersdl_hook.is_background is True
    assert papersdl_hook.path.parent == plugins['papersdl'].path
    assert 'papersdl' in papersdl_hook.path.name
