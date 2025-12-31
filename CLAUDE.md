# Claude Code Instructions for abx-dl

## Project Overview

abx-dl is a CLI tool for downloading URLs using ArchiveBox plugins. Plugins are symlinked from `/Users/squash/Code/ArchiveBox/archivebox/plugins` during development.

## Development Setup

```bash
# Install dependencies
uv sync --extra dev

# Activate venv (or use uv run prefix)
source .venv/bin/activate
```

## Running Tests

### Manual testing in a temp directory
```bash
cd /tmp && rm -rf abx-dl-test && mkdir abx-dl-test && cd abx-dl-test
abx-dl dl --plugins=chrome,title 'https://example.com'

# Or test all plugins
abx-dl dl 'https://example.com'

# Check output
ls -la
cat title/title.txt
cat index.jsonl | jq -s '.'
```

### Running pytest
```bash
uv run pytest tests/
uv run pytest -xvs tests/  # verbose with output
```

## Linting

```bash
uv run ruff check abx_dl/
uv run ruff format abx_dl/
uv run mypy abx_dl/
```

## Rolling a New Release

1. **Bump version in pyproject.toml**
   ```bash
   # Edit version = "X.Y.Z" in pyproject.toml
   ```

2. **Commit, tag, and push**
   ```bash
   git add pyproject.toml
   git commit -m "Bump to vX.Y.Z"
   git push
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```

3. **Create GitHub release**
   ```bash
   gh release create vX.Y.Z --title "vX.Y.Z" --notes "Release notes here"
   ```

4. **PyPI publish** happens automatically via GitHub Actions when a tag is pushed (requires trusted publisher configured on PyPI)

## CI/CD Workflows

- `.github/workflows/test.yml` - Parallel plugin tests on PR/push to main
- `.github/workflows/publish.yml` - PyPI publishing on tags/releases

## Key Architecture Notes

- **Plugins with Crawl hooks** (like chrome) self-install their dependencies - the executor skips pre-checking binaries for these
- **Background hooks** (`.bg.js` suffix) run as daemons and are cleaned up via PID files at the end
- **Config propagation**: Crawl hooks can output `{"type": "Machine", "_method": "update", "key": "config/KEY", "value": "..."}` JSONL to set config for subsequent hooks
- **Binary discovery**: Hooks output `{"type": "Binary", "name": "...", "abspath": "..."}` to register binary paths

## Common Issues

- **Chrome plugin skipped**: Fixed in v1.0.4 - plugins with Crawl hooks skip dependency pre-check
- **Hook file conflicts**: Each hook writes to `{hook_name}.stdout.log` etc. to avoid conflicts when multiple hooks run in same plugin dir
