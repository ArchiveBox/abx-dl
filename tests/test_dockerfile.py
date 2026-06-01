from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _plugin_install_targets(dockerfile: Path) -> set[str]:
    text = dockerfile.read_text()
    commands = re.findall(r"abx-dl plugins --install((?: \\\n|[^\n])*)", text)
    targets: set[str] = set()
    for command in commands:
        command_text = command.replace("\\", " ")
        targets.update(re.findall(r"\b[a-z][a-z0-9_]*\b", command_text))
    return targets


def test_dockerfile_skips_optional_twocaptcha_extension_preinstall() -> None:
    targets = _plugin_install_targets(REPO_ROOT / "Dockerfile")

    assert "archivewebpage" in targets
    assert "ublock" in targets
    assert "istilldontcareaboutcookies" in targets
    assert "twocaptcha" not in targets


def test_dockerfile_build_installs_disable_release_age_gate() -> None:
    text = (REPO_ROOT / "Dockerfile").read_text()

    assert "ABXPKG_POSTINSTALL_SCRIPTS=True" in text
    assert "ABXPKG_MIN_RELEASE_AGE=0" in text
