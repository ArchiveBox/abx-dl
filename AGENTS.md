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

Docker:

```console
docker run -it -v "$PWD:/out" archivebox/abxdl 'https://example.com'
```

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
uv run abx-dl config
uv run abx-dl config --get TIMEOUT
```

## Verification

Use targeted tests and real user-facing commands:

```console
uv run pytest tests/test_cli.py -q
uv run prek run --all-files
```

For extractor behavior, run in a clean directory and inspect `index.jsonl` plus plugin output files:

```bash
cd "$(mktemp -d)"
exec >stdout.log
uv run --project /path/to/abx-dl abx-dl dl --plugins=title,wget 'https://example.com'
find . -maxdepth 3 -type f | sort
```
