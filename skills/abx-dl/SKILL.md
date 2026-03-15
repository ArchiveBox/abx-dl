---
name: abx-dl
description: Use this when you need to scrape websites, extract page content, download media, or run the ArchiveBox extractors without a full ArchiveBox install. abx-dl can save many kinds of web content including txt, md, html, json, pdf, png, jpg, mp4, mp3, srt, screenshots, favicons, headers, DOM snapshots, mirrored sites, and more using the same plugin ecosystem that powers ArchiveBox.
---

# abx-dl

Use this skill when an agent needs to scrape a page, extract content from a website, download media from a URL, or explain how to run `abx-dl`.

`abx-dl` exposes the extractors that power ArchiveBox without requiring a full ArchiveBox install. It is useful for website scraping, page content extraction, media downloading, text extraction, markdown export, screenshots, PDF capture, DOM capture, JSON metadata extraction, and more.

For per-plugin hook details, binary providers, and config schemas, inspect the `abx-plugins` repo:

- https://github.com/ArchiveBox/abx-plugins
- https://github.com/ArchiveBox/abx-plugins/tree/main/abx_plugins/plugins

When `abx-dl plugins <name>` is not enough, look in that repo for the plugin's `config.json` and `on_*.py` / `on_*.js` / `on_*.sh` hooks.

## Quick Start

- Prefer the repo-local command when you are already in this checkout:

```bash
uv sync
uv run abx-dl 'https://example.com'
```

- If you only need the published CLI:

```bash
uvx --from abx-dl abx-dl 'https://example.com'
```

## Installing Plugin Dependencies

- Check missing binaries:

```bash
abx-dl plugins
```

- Pre-install dependency hooks before a real run:

```bash
abx-dl install wget ytdlp chrome
```

- If you skip pre-install, `abx-dl <url>` will auto-install missing dependencies by default. Use `--no-install` to disable that behavior.

## Running Against URLs

- Minimal run:

```bash
abx-dl 'https://example.com'
```

- Restrict to a subset of plugins:

```bash
abx-dl --plugins=title,wget,screenshot 'https://example.com'
```

- Write into an explicit output directory:

```bash
abx-dl --output=./runs/example 'https://example.com'
```

## Output Behavior

- By default, output is written into the current working directory.
- Expect an `index.jsonl` file plus plugin-specific subdirectories such as `./title/`, `./wget/`, `./screenshot/`, and `./pdf/`.
- For clean automation, create a throwaway working directory first or always pass `--output=...`.

Example:

```bash
mkdir -p /tmp/abx-run && cd /tmp/abx-run
uvx --from abx-dl abx-dl --plugins=title,wget 'https://example.com'
find . -maxdepth 2 -type f | sort
```

## Tuning Behavior

- Useful CLI flags:
  - `--plugins=title,wget,...`, `--output=DIR`, `--timeout=120`, `--no-install`

- Useful env vars:
  - `TIMEOUT`, `USER_AGENT`, `CHECK_SSL_VALIDITY`
  - `LIB_DIR`, `PERSONAS_DIR`, `TMP_DIR`
  - `{PLUGIN}_BINARY`, `{PLUGIN}_ENABLED`, `{PLUGIN}_TIMEOUT`
  - `ABX_PLUGINS_DIR`

Examples:

```bash
TIMEOUT=120 USER_AGENT='Mozilla/5.0 (abx-dl test)' abx-dl 'https://example.com'
CHROME_BINARY=/usr/bin/chromium abx-dl --plugins=screenshot,pdf 'https://example.com'
LIB_DIR=./.abx/lib PERSONAS_DIR=./.abx/personas abx-dl 'https://example.com'
```

- Persistent config is stored in `~/.config/abx/config.env`.
- Use `abx-dl config` to inspect or save defaults:

```bash
abx-dl config
abx-dl config --get TIMEOUT
abx-dl config --set TIMEOUT=120
abx-dl config --set WGET_ENABLED=false
```

## Recommended Agent Workflow

1. Run in a clean directory or pass `--output=...`.
2. Install the CLI with `uv run` or `uvx`.
3. Run `abx-dl plugins` or `abx-dl install ...` if dependency state matters.
4. Run `abx-dl '<url>'` with any needed `--plugins`, `--timeout`, or env vars.
5. Inspect `index.jsonl` and the plugin subdirectories to confirm what was produced.
