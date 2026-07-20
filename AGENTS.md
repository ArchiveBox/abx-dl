# abx-dl Agent Guide

`abx-dl` is the standalone downloader/extractor CLI that runs ArchiveBox plugin hooks without a full ArchiveBox collection. Keep this repo on `main`.

## Shared Standards

- Use `uv` and `uv run` for Python commands. Do not use system `python`, direct `.venv/bin/python`, or `pip` commands.
- Prefer existing repo patterns, helper APIs, fixtures, scripts, and command surfaces.
- Keep edits focused and minimal. Do not add wrappers, shims, aliases, or extra abstraction layers unless the current code path requires them.
- Do not weaken assertions, skip tests, xfail tests, or accept flaky behavior.
- No mocks, monkeypatches, fakes, simulated handlers, fake binaries, fake hooks, fake buses, or direct shortcuts around user-facing flows.
- Tests and verification should use real CLI commands, real hooks, real installs, real subprocesses, real DB/config rows where present, real files, real URLs or `pytest-httpserver`, and existing fixtures.
- Assertions must verify real correctness: exit codes, returned values, event records, filesystem contents, field values, output files, and side effects.
- Start behavior fixes with a red failing test when a test is requested or practical.
- Trace root causes from observed behavior. Do not paper over failures with retries, wider timeouts, broad fallbacks, or looser assertions.
- Read `README.md` for the full CLI, config, plugin, and release surface.

## Development Setup

<!--
```bash
export UV_PROJECT_ENVIRONMENT="$(mktemp -d)/.venv"
unset VIRTUAL_ENV
```
-->
<!--pytest-codeblocks:cont-->
```bash
set -Eeuo pipefail
uv sync
help_output="$(uv run abx-dl --help)"
grep -q 'Usage:' <<<"$help_output"
```

## Plugin Inspection

```bash
set -Eeuo pipefail
plugin_info="$(uv run abx-dl plugins wget)"
grep -q 'WGET_BINARY=wget' <<<"$plugin_info"
grep -q 'on_Snapshot__06_wget' <<<"$plugin_info"
```

Docker:

<!--pytest.mark.docker_required-->
```bash
set -Eeuo pipefail
output_dir="$(mktemp -d)"
trap 'rm -rf "$output_dir"' EXIT
docker run --rm -v "$output_dir:/out" "${ABXDL_IMAGE:-archivebox/abx-dl:latest}" --no-install --max-urls=1 --plugins=title,wget 'https://example.com'
test -s "$output_dir/index.jsonl"
test -s "$output_dir/title/title.txt"
test -s "$output_dir/wget/example.com/index.html"
grep -q 'Example Domain' "$output_dir/title/title.txt"
grep -q 'Example Domain' "$output_dir/wget/example.com/index.html"
```

## Basic Usage

```text
uv run abx-dl dl --plugins=title,wget,screenshot,pdf 'https://example.com'
uv run abx-dl dl --output=html,json,txt,pdf,image 'https://example.com'
uv run abx-dl install wget ytdlp chrome
uv run abx-dl config
uv run abx-dl config --get TIMEOUT
```

## Verification

Use targeted tests and real user-facing commands:

```bash
set -Eeuo pipefail
uv run pytest tests/test_cli.py::test_readme_install_command_runs_real_install_pipeline -q
```

The skill's live wget check covers extractor output. The main test workflow runs
the exhaustive repository suite and the separate Prek job runs every hook.
