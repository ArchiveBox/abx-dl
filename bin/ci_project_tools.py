#!/usr/bin/env -S uv run --no-project python
"""Project host-first CI tools into ABXPKG_LIB_DIR/env/bin."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tomllib
from pathlib import Path


def abxpkg_version(project_file: Path) -> str:
    dependencies = tomllib.loads(project_file.read_text())["project"]["dependencies"]
    for dependency in dependencies:
        match = re.fullmatch(r"abxpkg==([^; ]+)", dependency)
        if match:
            return match.group(1)
    raise SystemExit(f"Exact abxpkg dependency not found in {project_file}")


def append_line(path: Path, line: str) -> None:
    with path.open("a", encoding="utf-8") as output:
        output.write(f"{line}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("section")
    parser.add_argument("--lib", required=True, type=Path)
    parser.add_argument("--project", default="pyproject.toml", type=Path)
    parser.add_argument("--config", default=".github/configs/ci-tooling.json", type=Path)
    args = parser.parse_args()

    lib_dir = args.lib.resolve()
    env_bin = lib_dir / "env" / "bin"
    env_bin.mkdir(parents=True, exist_ok=True)

    command = [
        "uv",
        "run",
        "--no-project",
        "--with",
        f"abxpkg=={abxpkg_version(args.project)}",
        "abxpkg",
        "env",
        "--install",
        "--json",
        f"--lib={lib_dir}",
        f"--deps-from={args.config.resolve()}:{args.section}",
    ]
    command_env = dict(os.environ)
    command_env["ABXPKG_LIB_DIR"] = str(lib_dir)
    command_env["PATH"] = os.pathsep.join((str(env_bin), command_env["PATH"]))
    resolved = json.loads(subprocess.check_output(command, env=command_env, text=True))

    github_env = os.environ.get("GITHUB_ENV")
    github_path = os.environ.get("GITHUB_PATH")
    if github_env and github_path:
        append_line(Path(github_env), f"ABXPKG_LIB_DIR={lib_dir}")
        append_line(Path(github_path), str(env_bin))
        for key, value in resolved.items():
            if key != "PATH":
                append_line(Path(github_env), f"{key}={value}")

    print(json.dumps(resolved, sort_keys=True))


if __name__ == "__main__":
    main()
