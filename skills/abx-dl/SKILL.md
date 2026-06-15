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
uv sync
uv run abx-dl --help
uv run abx-dl plugins wget
```

## User-Facing Setup

<!--
```bash
cd "$(mktemp -d)"
exec >stdout.log
```
-->
<!--pytest-codeblocks:cont-->
```bash
uvx abx-dl dl --plugins=title,wget 'https://example.com'
```

<!--pytest-codeblocks:cont-->
<!--
```bash
test -s index.jsonl
test -s title/title.txt
test -s wget/example.com/index.html
```
-->

## Basic Usage

<!--
```bash
cd "$(mktemp -d)"
exec >stdout.log
```
-->
<!--pytest-codeblocks:cont-->
```bash
uv run abx-dl dl --plugins=title,wget --dir ./downloads 'https://example.com'
```

<!--pytest-codeblocks:cont-->
<!--
```bash
test -s downloads/index.jsonl
test -s downloads/title/title.txt
test -s downloads/wget/example.com/index.html
grep -q 'Example Domain' downloads/title/title.txt
```
-->

```text
uv run abx-dl dl --plugins=title,wget,screenshot,pdf 'https://example.com'
uv run abx-dl dl --output=html,json,txt,pdf,image 'https://example.com'
uv run abx-dl install wget ytdlp chrome
uv run abx-dl config --get TIMEOUT
```

## Verification

<!--pytest.mark.skip(reason="pytest invocation")-->
```bash
uv run pytest tests/test_cli.py -q
uv run prek run --all-files
```

For live extractor checks:

```bash
repo="$(pwd)"
cd "$(mktemp -d)"
exec >stdout.log
uv run --project "$repo" abx-dl dl --plugins=title,wget 'https://example.com'
find . -maxdepth 3 -type f | sort
```
