import json
import os
import subprocess
import textwrap
from pathlib import Path

import pytest


@pytest.mark.parametrize("platform", ["linux", "macos"])
def test_ci_matrix_runs_every_file_on_linux_and_preserves_mac_coverage(tmp_path, platform):
    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github/workflows/ci.yml").read_text()
    discovery = workflow.split("      - name: Build test matrix\n", 1)[1]
    script = textwrap.dedent(discovery.split("python - <<'PY'\n", 1)[1].split("\n          PY", 1)[0])
    output = tmp_path / "github-output"
    subprocess.run(
        ["uv", "run", "--no-project", "python", "-c", script],
        cwd=root,
        env={**os.environ, "CI_TEST_PLATFORM": platform, "UGNAS_CI_MAX_JOBS": "3", "GITHUB_OUTPUT": str(output)},
        check=True,
        capture_output=True,
        text=True,
    )
    matrix = json.loads(output.read_text().removeprefix("tests="))
    files = sorted(
        str(path.relative_to(root)) for directory in (root / "tests", root / "server/tests") for path in directory.glob("test_*.py")
    )
    expected = files if platform == "linux" else [path for index, path in enumerate(files) if index % 6 >= 3]
    assert [item["file"] for item in matrix] == expected
    assert len(expected) == len(set(expected))
    assert {item["os"] for item in matrix} == {"ubuntu-24.04" if platform == "linux" else "macos-15"}
    assert {item["python"] for item in matrix} == {"3.12", "3.13", "3.14"}
    assert sum(item["ugnas"] for item in matrix) == (3 if platform == "linux" else 0)
    for item in matrix:
        if item["ugnas"]:
            assert "# ci-runner: hosted" not in (root / item["file"]).read_text().splitlines()[:5]
