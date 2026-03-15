from pathlib import Path

from abx_dl.plugins import discover_plugins, parse_hook_filename


def test_parse_hook_filename_marks_bg_hooks() -> None:
    assert parse_hook_filename('on_Snapshot__66_papersdl.bg.py') == ('Snapshot', 6, 6, True, 'py')


def test_discover_plugins_marks_papersdl_as_background() -> None:
    plugins = discover_plugins()

    papersdl_hook = next(
        hook
        for hook in plugins['papersdl'].hooks
        if hook.path.name == 'on_Snapshot__66_papersdl.bg.py'
    )

    assert papersdl_hook.is_background is True
    assert papersdl_hook.name == 'on_Snapshot__66_papersdl.bg'
    assert papersdl_hook.path == Path(
        '/Users/squash/Local/Code/archiveboxes/new/abx-plugins/abx_plugins/plugins/papersdl/on_Snapshot__66_papersdl.bg.py'
    )
