---
name: abx-dl
description: Use this when working on the standalone ArchiveBox downloader CLI, extractor orchestration, plugin execution, config, and output verification.
---

# abx-dl

## Purpose

`abx-dl` runs ArchiveBox plugin hooks against URLs without a full ArchiveBox collection.

## Shared Rules

- Keep this repo on branch `main`.
- Use `uv` and `uv run` for Python commands.
- Do not use system `python`, direct `.venv/bin/python`, or `pip` commands.
- Use real CLI commands, real hooks, real installs, real subprocesses, real files, and real URLs or `pytest-httpserver`.
- Do not mock, monkeypatch, fake, simulate, skip, xfail, or weaken tests.
- Verify `index.jsonl`, hook statuses, output files, config, and filesystem side effects.
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
version_output="$(uv run abx-dl version)"
grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' <<<"$version_output"
```

## Configuration Inspection

```bash
set -Eeuo pipefail
timeout_config="$(uv run abx-dl config --get TIMEOUT)"
grep -Eq '^TIMEOUT=[0-9]+$' <<<"$timeout_config"
```

## Basic Usage

```bash
set -Eeuo pipefail
output_dir="$(mktemp -d)"
trap 'rm -rf "$output_dir"' EXIT
uv run abx-dl dl --plugins=wget --dir "$output_dir" 'https://example.com'
test -s "$output_dir/index.jsonl"
test -s "$output_dir/wget/example.com/index.html"
grep -q 'Example Domain' "$output_dir/wget/example.com/index.html"
grep -q '"plugin": "wget".*"status": "succeeded"' "$output_dir/index.jsonl"
```

```text
uv run abx-dl dl --plugins=title,wget,screenshot,pdf 'https://example.com'
uv run abx-dl dl --output=html,json,txt,pdf,image 'https://example.com'
uv run abx-dl install wget ytdlp chrome
uv run abx-dl config --get TIMEOUT
```

## Verification

```bash
set -Eeuo pipefail
uv run pytest tests/test_plugins.py::test_filter_plugins_does_not_add_binary_providers_for_wget -q
```

The basic-usage check above performs the live extractor run and verifies its
manifest and downloaded HTML. Read `README.md` for install and multi-extractor
examples.
