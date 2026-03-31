# ⬇️ `abx-dl`

> A simple all-in-one CLI tool to auto-detect and download *everything* available from a URL.

```bash
uvx abx-dl 'https://example.com'
```
---

✨ *Ever wish you could `yt-dlp`, `gallery-dl`, `wget`, `curl`, `puppeteer`, etc. all in one command?*

`abx-dl` is an all-in-one CLI tool for downloading URLs "by any means necessary".

It's useful for scraping, downloading, OSINT, digital preservation, and more.
`abx-dl` provides a simpler one-shot CLI interface to the [ArchiveBox](https://github.com/ArchiveBox/ArchiveBox) plugin ecosystem.

<img width="1000" height="1082" alt="Screenshot 2026-03-11 at 6 53 03 AM" src="https://github.com/user-attachments/assets/4e19d985-1a93-4f65-9970-2565be16b718" />


---

<br/>

#### 🍜 What does it save?

```bash
abx-dl --plugins=wget,title,screenshot,pdf,readability,git 'https://example.com'
```

`abx-dl` runs all plugins by default (and auto installs dependencies). You can specify `--plugins=wget,favicon,title` or filters like `--output=html,pdf,ico,text/` to limit plugin selection.
- HTML, JS, CSS, images, etc. rendered with a headless browser
- title, favicon, headers, outlinks, and other metadata
- audio, video, subtitles, playlists, comments
- snapshot of the page as a PDF, screenshot, and [Singlefile](https://github.com/gildas-lormeau/single-file-cli) HTML
- article text, `git` source code
- [and much more](https://github.com/ArchiveBox/abx-dl#All-Outputs)...

<br/>

#### 🧩 How does it work?

`abx-dl` uses the **[Plugin Library](https://archivebox.github.io/abx-plugins/)** (shared with [ArchiveBox](https://github.com/ArchiveBox/ArchiveBox)) to run a collection of downloading and scraping tools.

Plugins are loaded from the installed [`abx-plugins`](https://pypi.org/project/abx-plugins/) package (or from `ABX_PLUGINS_DIR` if you override it) and execute in distinct phases:
1. **Install phase** runner reads plugins `config.json`: `required_binaries` and emits `BinaryRequestEvent`s ➡️ which go to **BinaryRequest hooks** (`on_BinaryRequest__*`) that answer requests using apt/brew/env/etc.
2. **CrawlSetup hooks** (`on_CrawlSetup__*`) launch/configure expensive crawl-scoped processes like chrome, or trigger side effects. they emit no stdout JSONL records.
4. **Snapshot hooks** (`on_Snapshot__*`) run per URL to extract content and emit only `ArchiveResult`, `Snapshot`, and `Tag` records


<br/>

#### ⚙️ Configuration

Configuration is handled via environment variables plus a user config file (`~/.config/abx/config.env`). Runtime-derived cache entries such as resolved binary paths are stored separately in `~/.config/abx/derived.env`:

```bash
abx-dl config                        # show all config (global + per-plugin)
abx-dl config --get WGET_TIMEOUT     # get a specific value
abx-dl config --set TIMEOUT=120      # set persistently (resolves aliases)
```

Output is grouped by section:
```bash
# GLOBAL
TIMEOUT=60
USER_AGENT="Mozilla/5.0 ..."
...

# plugins/wget
WGET_BINARY="wget"
WGET_TIMEOUT=60
...

# plugins/chrome
CHROME_BINARY="chromium"
...
```

Common options:
- `TIMEOUT=60` - default timeout for hooks
- `USER_AGENT` - default user agent string
- `{PLUGIN}_BINARY` - path or name of the binary to use (e.g. `WGET_BINARY=wget` or `CHROME_BINARY=/usr/bin/chromium`)
- `{PLUGIN}_ENABLED=True/False` - enable/disable specific plugins
- `{PLUGIN}_TIMEOUT=120` - per-plugin timeout overrides

Aliases are automatically resolved (e.g. `--set USE_WGET=false` saves as `WGET_ENABLED=false`).

The config split matters:
- `config.env` stores only user-provided values, typically written by `abx-dl config --set ...`
- `derived.env` stores runtime-derived cache entries such as resolved `*_BINARY` paths and the `ABX_INSTALL_CACHE` install timestamp map
- `MachineService` keeps user config and derived cache separate for the whole run
- plugin config hydration uses user/default config only; `derived.env` is consulted separately by the binary-resolution layer and is never blindly merged into user config

Binary resolution uses those layers differently:
- a user-provided path-like `*_BINARY` in `config.env` is authoritative and will hard-fail if broken
- a bare binary name in user/default config can reuse a cached abspath from `derived.env`
- if that derived abspath is stale, `abx-dl` ignores it and runs the normal `BinaryRequest` provider flow for the same binary name

When embedded in ArchiveBox, the equivalent derived cache comes from persisted `machine_binary` rows in the DB.

One-off tuning is often easiest via env vars or CLI args:

```bash
env \
  CHROME_BINARY=/usr/bin/chromium \
  TIMEOUT=120 \
  USER_AGENT='Mozilla/5.0 (abx-dl smoke test)' \
  abx-dl 'https://example.com'  'https://example.com'

# CLI args
abx-dl --dir=/tmp/test --plugins=wget,title,favicon --output=html,txt,css,js,ico,png,pdf --timeout=90 'https://example.com'
```

<br/>

---

<br/>

### 📦 Install

```bash
pip install abx-dl
abx-dl 'https://example.com'

# Or run the published CLI without installing it globally
uvx abx-dl 'https://example.com'

# Pre-install dependencies to avoid having to wait for them to install on first-run
uvx abx-dl@latest install
```

<br/>

### 🔠 Usage

```bash
# Default command - a bare URL archives with all enabled plugins:
abx-dl 'https://example.com'

# Select plugins by output type (mimetypes, categories, or file extensions):
abx-dl --output=html,pdf,video/ 'https://example.com'
abx-dl -o text -o image -o mp4 'https://example.com'

# Limit work to a subset of plugins by name:
abx-dl --plugins=wget,title,screenshot,pdf 'https://example.com'

# Skip auto-installing missing dependencies (emit warnings instead):
abx-dl --no-install 'https://example.com'

# Specify output directory:
abx-dl --dir=./downloads 'https://example.com'

# Set timeout:
abx-dl --timeout=120 'https://example.com'
```

#### Commands

```bash
abx-dl <url>                              # Download URL (default shorthand)
abx-dl plugins                            # Check + show info for all plugins
abx-dl plugins wget ytdlp git             # Check + show info for specific plugins
abx-dl install wget ytdlp git             # Pre-install plugin dependencies
abx-dl config                             # Show all config values
abx-dl config --get TIMEOUT               # Get a specific config value
abx-dl config --set TIMEOUT=120           # Set a config value persistently
```

#### Installing Dependencies

Many plugins require external binaries (e.g., `wget`, `chrome`, `yt-dlp`, `single-file`).

By default, `abx-dl` lazily installs missing dependencies as needed when you download a URL.
Use `--no-install` to skip plugins with missing dependencies instead. `install` runs only the pre-run dependency pipeline (`required_binaries` → `BinaryRequestEvent` → `BinaryEvent`) without starting crawl setup or snapshot extraction:

```bash
# Auto-installs missing deps on-the-fly (default behavior)
abx-dl 'https://example.com'

# Skip plugins with missing deps, emit warnings instead
abx-dl --no-install 'https://example.com'

# Install dependencies for specific plugins only
abx-dl install wget singlefile ytdlp

# Check which dependencies are available/missing
abx-dl plugins
```

Successful preflight installs are cached for 24 hours in `derived.env` under `ABX_INSTALL_CACHE`, keyed by binary name. If a binary was installed successfully recently, `abx-dl` skips re-running the install preflight for that binary. Cached abspaths are still validated at use time, and stale cache entries fall back to the normal provider resolution path.

The normal runtime flow after dependency preflight is:
- `CrawlEvent` (internal lifecycle root)
- `CrawlSetupEvent` → plugin `on_CrawlSetup__*` hooks
- `CrawlStartEvent` → `SnapshotEvent`
- `SnapshotEvent` → plugin `on_Snapshot__*` hooks
- `SnapshotCleanupEvent` / `CrawlCleanupEvent`

Hook output contract:
- `on_BinaryRequest__*` hooks emit only `Binary`
- `on_CrawlSetup__*` hooks emit no stdout JSONL records
- `on_Snapshot__*` hooks emit only `ArchiveResult`, `Snapshot`, and `Tag`
- the TUI and services consume structured events derived from those hook records

Dependencies are installed to `~/.config/abx/lib/{arch}/` using the appropriate package manager:
- **pip packages** → `~/.config/abx/lib/{arch}/pip/venv/`
- **npm packages** → `~/.config/abx/lib/{arch}/npm/`
- **brew/apt packages** → system locations

You can override the install location with `LIB_DIR=/path/to/lib abx-dl install wget`.

<br/>

---

<br/>

### Output Structure

By default, `abx-dl` writes results into the current working directory. Each run creates an `index.jsonl` manifest plus one subdirectory per plugin that produced output. If you want to keep runs isolated, `cd` into a scratch directory first or pass `--dir=/path/to/run`.

```bash
mkdir -p /tmp/abx-run && cd /tmp/abx-run
uvx --from abx-dl abx-dl --plugins=title,wget 'https://example.com'
```

```
./
├── index.jsonl             # Snapshot metadata and results (JSONL format)
├── title/
│   └── title.txt
├── favicon/
│   └── favicon.ico
├── screenshot/
│   └── screenshot.png
├── pdf/
│   └── output.pdf
├── dom/
│   └── output.html
├── wget/
│   └── example.com/
│       └── index.html
├── singlefile/
│   └── output.html
└── ...
```

<br/>

### All Outputs

- `index.jsonl` - snapshot metadata and plugin results (JSONL format, ArchiveBox-compatible)
- `title/title.txt` - page title
- `favicon/favicon.ico` - site favicon
- `screenshot/screenshot.png` - full page screenshot (Chrome)
- `pdf/output.pdf` - page as PDF (Chrome)
- `dom/output.html` - rendered DOM (Chrome)
- `wget/example.com/...` - mirrored site files
- `singlefile/output.html` - single-file HTML snapshot
- ... and more via plugin library ...

---

### Available Plugins

See the [`abx-plugins` marketplace](https://github.com/ArchiveBox/abx-plugins).

#### Snapshot / Extraction Plugins

- `ytdlp` - downloads media plus sidecars: audio, video, images/thumbnails, subtitles (`.srt`, `.vtt`), JSON metadata, and text descriptions.
- `gallerydl` - downloads gallery/media sets as images, videos, JSON sidecars, text sidecars, and ZIP archives.
- `forumdl` - exports forum/thread archives as JSONL, WARC, and mailbox-style message archives.
- `git` - clones repository contents including text, binaries, images, audio, video, fonts, and other tracked files.
- `wget` - mirrors pages and requisites as HTML, WARC, images, CSS, JavaScript, fonts, audio, and video.
- `archivedotorg` - saves a Wayback Machine archive link as plain text.
- `favicon` - saves site favicons and touch icons as image files.
- `modalcloser` - setup helper only; no direct archive files.
- `consolelog` - saves browser console events as JSONL.
- `dns` - saves observed DNS activity as JSONL.
- `ssl` - saves TLS certificate/connection metadata as JSONL.
- `responses` - saves HTTP response metadata as JSONL and can record referenced text, images, audio, video, apps, and fonts.
- `redirects` - saves redirect chains as JSONL.
- `staticfile` - saves non-HTML direct file responses such as PDF, EPUB, images, audio, video, JSON, XML, CSV, ZIP, and generic binary files.
- `headers` - saves main-document HTTP headers as JSON.
- `chrome` - manages shared browser state and emits plain-text and JSON runtime metadata.
- `seo` - saves SEO metadata such as meta tags and Open Graph fields as JSON.
- `accessibility` - saves the browser accessibility tree as JSON.
- `infiniscroll` - page-expansion helper only; no direct archive files.
- `claudechrome` - saves Claude-computer-use interaction results as JSON plus PNG screenshots.
- `singlefile` - saves a full self-contained page snapshot as HTML.
- `screenshot` - saves rendered page screenshots as PNG.
- `pdf` - saves rendered pages as PDF.
- `dom` - saves fully rendered DOM output as HTML.
- `title` - saves the final page title as plain text.
- `readability` - extracts article HTML, plain text, and JSON metadata.
- `defuddle` - extracts cleaned article HTML, plain text, and JSON metadata.
- `mercury` - extracts article HTML, plain text, and JSON metadata.
- `claudecodeextract` - generates cleaned Markdown from other extractor outputs.
- `htmltotext` - converts archived HTML into plain text.
- `trafilatura` - extracts article content as plain text, Markdown, HTML, CSV, JSON, and XML/TEI.
- `papersdl` - downloads academic papers as PDF.
- `parse_html_urls` - emits discovered links from HTML as JSONL records.
- `parse_txt_urls` - emits discovered links from text files as JSONL records.
- `parse_rss_urls` - emits discovered feed entry URLs from RSS/Atom as JSONL records.
- `parse_netscape_urls` - emits discovered bookmark URLs from Netscape bookmark exports as JSONL records.
- `parse_jsonl_urls` - emits discovered bookmark URLs from JSONL exports as JSONL records.
- `parse_dom_outlinks` - emits crawlable rendered-DOM outlinks as JSONL records.
- `search_backend_sqlite` - writes a searchable SQLite FTS index database.
- `search_backend_sonic` - pushes content into Sonic search; no local archive files declared.
- `claudecodecleanup` - writes cleanup/deduplication results as plain text.
- `hashes` - writes file hash manifests as JSON.
- and more via the [`abx-plugins` marketplace](https://github.com/ArchiveBox/abx-plugins)...

---

### AI Skill

This repo includes an `abx-dl` skill for coding agents that need to run the standalone ArchiveBox extractor pipeline without a full ArchiveBox install.

- Skill source: [`skills/abx-dl/SKILL.md`](./skills/abx-dl/SKILL.md)
- skills.sh page: https://skills.sh/archivebox/abx-dl/abx-dl

---

### Architecture

`abx-dl` is built on these components:

- **`abx_dl/plugins.py`** - Plugin discovery from `abx-plugins` or `ABX_PLUGINS_DIR`
- **`abx_dl/executor.py`** - Hook execution engine with config propagation
- **`abx_dl/config.py`** - Environment variable configuration
- **`abx_dl/cli.py`** - Rich CLI with live progress display

---

For more advanced use with collections, parallel downloading, a Web UI + REST API, etc.
See: [`ArchiveBox/ArchiveBox`](https://github.com/ArchiveBox/ArchiveBox)
