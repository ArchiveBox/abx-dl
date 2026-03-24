from __future__ import annotations

import tomllib
from pathlib import Path


def test_base_install_does_not_publish_server_console_script() -> None:
    pyproject = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())

    assert pyproject["project"]["scripts"] == {"abx-dl": "abx_dl:main"}
