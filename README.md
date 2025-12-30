# ⬇️ `abx-dl`

> A simple all-in-one CLI tool to auto-detect and download *everything* available from a URL.

```bash
pip install abx-dl
abx-dl 'https://example.com/page/to/download'
```

---

✨ *Ever wish you could `yt-dlp`, `gallery-dl`, `wget`, `curl`, `puppeteer`, etc. all in one command?*

`abx-dl` is an all-in-one CLI tool for downloading URLs "by any means necessary".

It's useful for scraping, downloading, OSINT, digital preservation, and more.
`abx-dl` provides a simpler one-shot CLI interface to the [ArchiveBox](https://github.com/ArchiveBox/ArchiveBox) plugin ecosystem.

---

<br/>

#### 🍜 What does it save?

```bash
abx-dl --plugins=title,favicon,headers,wget,singlefile,screenshot,pdf,dom,readability,git,... 'https://example.com'
```

`abx-dl` runs all plugins by default, or you can specify `--plugins=...` for specific methods:
- HTML, JS, CSS, images, etc. rendered with a headless browser
- title, favicon, headers, outlinks, and other metadata
- audio, video, subtitles, playlists, comments
- snapshot of the page as a PDF, screenshot, and [Singlefile](https://github.com/gildas-lormeau/single-file-cli) HTML
- article text, `git` source code
- [and much more](https://github.com/ArchiveBox/abx-dl#All-Outputs)...

<br/>

#### 🧩 How does it work?

`abx-dl` uses the **[ABX Plugin Library](https://docs.sweeting.me/s/archivebox-plugin-ecosystem-announcement)** (shared with [ArchiveBox](https://github.com/ArchiveBox/ArchiveBox)) to run a collection of downloading and scraping tools.

Plugins are discovered from the `plugins/` directory and execute hooks in order:
1. **Crawl hooks** run first (setup/install dependencies like Chrome)
2. **Snapshot hooks** run per-URL to extract content

Each plugin can output:
- Files to its output directory
- JSONL records for status reporting
- Config updates that propagate to subsequent plugins

<br/>

#### ⚙️ Configuration

Configuration is handled via environment variables:

- `CHROME_BINARY`, `WGET_BINARY`, etc. - binary paths
- `TIMEOUT=60` - default timeout for hooks
- `{PLUGIN}_ENABLED=true/false` - enable/disable specific plugins
- `{PLUGIN}_TIMEOUT=120` - per-plugin timeout overrides

<br/>

---

<br/>

### 📦 Install

```bash
pip install abx-dl
abx-dl plugins --install   # optional: pre-install plugin dependencies
```

<br/>

### 🔠 Usage

```bash
# Basic usage - download URL with all plugins:
abx-dl 'https://example.com'

# Download with specific plugins only:
abx-dl --plugins=wget,ytdlp,git,screenshot 'https://example.com'

# Skip auto-installing missing dependencies (emit warnings instead):
abx-dl --no-install 'https://example.com'

# Specify output directory:
abx-dl --output=./downloads 'https://example.com'

# Set timeout:
abx-dl --timeout=120 'https://example.com'
```

#### Commands

```bash
abx-dl <url>                              # Download URL (default command)
abx-dl plugins                            # Check + show info for all plugins
abx-dl plugins wget ytdlp git             # Check + show info for specific plugins
abx-dl plugins --install                  # Install all plugin dependencies
abx-dl plugins --install wget ytdlp git   # Install specific plugin dependencies
```

#### Installing Dependencies

Many plugins require external binaries (e.g., `wget`, `chrome`, `yt-dlp`, `single-file`).

By default, `abx-dl` lazily auto-installs missing dependencies as needed when you download a URL.
Use `--no-install` to skip plugins with missing dependencies instead:

```bash
# Auto-installs missing deps on-the-fly (default behavior)
abx-dl 'https://example.com'

# Skip plugins with missing deps, emit warnings instead
abx-dl --no-install 'https://example.com'

# Pre-install all plugin dependencies
abx-dl plugins --install

# Install dependencies for specific plugins only
abx-dl plugins --install wget singlefile ytdlp

# Check which dependencies are available/missing
abx-dl plugins
```

Dependencies are installed to `~/.config/abx/lib/{arch}/` using the appropriate package manager:
- **pip packages** → `~/.config/abx/lib/{arch}/pip/venv/`
- **npm packages** → `~/.config/abx/lib/{arch}/npm/`
- **brew/apt packages** → system locations

You can override the install location with `LIB_DIR=/path/to/lib abx-dl install`.

<br/>

---

<br/>

### Output Structure

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

### Architecture

`abx-dl` is built on these components:

- **`abx_dl/plugins.py`** - Plugin discovery from `plugins/` directory
- **`abx_dl/executor.py`** - Hook execution engine with config propagation
- **`abx_dl/config.py`** - Environment variable configuration
- **`abx_dl/cli.py`** - Rich CLI with live progress display

Plugins are symlinked from [ArchiveBox](https://github.com/ArchiveBox/ArchiveBox)'s plugin directory.

---

For more advanced use with collections, parallel downloading, a Web UI + REST API, etc.
See: [`ArchiveBox/ArchiveBox`](https://github.com/ArchiveBox/ArchiveBox)
