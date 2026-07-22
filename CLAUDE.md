# Claude Code Instructions for abx-dl

Use [`AGENTS.md`](./AGENTS.md) as the canonical repository guide. Keep this
checkout on `main`, use `uv` for Python commands, and exercise real CLI,
plugin, subprocess, network, and filesystem paths in tests. Do not add skips,
xfails, retries, fake hooks, fake binaries, fake buses, or fallback binary
resolution.

## Development setup

```bash
set -Eeuo pipefail
uv sync --locked --dev --no-sources
help_output="$(uv run --no-sync --no-sources abx-dl --help)"
grep -q 'Usage:' <<<"$help_output"
```

## Inspect the installed plugin surface

```bash
set -Eeuo pipefail
plugin_info="$(uv run --no-sync --no-sources abx-dl plugins wget)"
grep -q 'WGET_BINARY=wget' <<<"$plugin_info"
grep -q 'on_Snapshot__06_wget' <<<"$plugin_info"
```

## Verification

The normal CI workflow runs the complete repository suite on Linux and macOS
with every supported Python minor version, every documentation snippet, every
test directory from the pinned `abx-plugins` release, and a real all-plugin
crawl. The Docker workflow builds and tests both amd64 and arm64 images and
runs every snippet marked `docker_required`.

Run the repository's complete static verification before publishing changes:

```bash
set -Eeuo pipefail
uv run --no-sync --no-sources prek run --all-files
```

## Releases

Releases are automatic from `main`. Bump the version and its lockfile, commit,
and push. The release workflow waits for the exact commit's test workflow,
creates the matching GitHub release and tag, waits for the exact Docker build,
and publishes the package through PyPI trusted publishing. Do not create or
push tags locally.

## Architecture

- Plugin discovery comes from the installed `abx-plugins` package or the
  explicit `ABX_PLUGINS_DIR` override.
- Required binaries are emitted as `BinaryRequestEvent` records and resolved
  only by `abxpkg`; host binaries are considered before managed providers.
- Resolved host and managed binaries are projected into
  `ABXPKG_LIB_DIR/env/bin`. `ABXPKG_LIB_DIR/bin` is only a human convenience
  surface and must never be used programmatically.
- Crawl setup and snapshot hooks run through the unified event state machine.
  Chrome readiness uses its plugin-owned file mechanism rather than a generic
  bus-level readiness event.
