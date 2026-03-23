Strict Rename / Phase Split Plan
================================

Goals
-----
- No compatibility layer.
- No legacy aliases.
- No fallback parsing of old names.
- Rename everything directly across `abx-plugins`, `abx-dl`, and `archivebox`.
- Replace fake `archivebox://install` crawl bootstrapping with a real install phase.

Core Renames
------------
- `Binary` record -> `BinaryRequest`
- resolved provider-emitted `Binary` record -> `Binary`
- `BinaryEvent` -> `BinaryRequestEvent`
- `BinaryEvent` -> `BinaryEvent`
- `BinaryProcessEvent` -> `BinaryRequestProcessEvent`
- `on_Binary__*` -> `on_BinaryRequest__*`
- `on_Crawl__*install*` -> `on_Install__*`
- `on_Crawl__{setup|launch|config|wait|start|...}` -> `on_CrawlSetup__*`
- keep all `on_Snapshot__*` unchanged
- remove `archivebox://install`
- remove `INSTALL_URL`
- add real `InstallEvent`

Behavior Rules
--------------
- `BinaryRequest` never carries resolved metadata like `abspath`, `version`, `sha256`.
- `Binary` always carries resolved metadata.
- `on_Install__*` emits `BinaryRequest`, `Machine`, and `Process` only.
- `on_CrawlSetup__*` emits `Machine` and `Process` only, never `ArchiveResult`.
- `on_BinaryRequest__*` emits `Binary`.
- `download()` emits `InstallEvent` before `CrawlEvent`.
- `abx-dl install` and `archivebox install` emit `InstallEvent` directly.
- cache hits from `config.env` / DB should respond to `BinaryRequestEvent` by emitting synthetic `BinaryEvent`.

Hook File Renames
-----------------
Provider hooks:
- env/on_Binary__00_env_discover.py -> env/on_BinaryRequest__00_env.py
- npm/on_Binary__10_npm_install.py -> npm/on_BinaryRequest__10_npm.py
- pip/on_Binary__11_pip_install.py -> pip/on_BinaryRequest__11_pip.py
- brew/on_Binary__12_brew_install.py -> brew/on_BinaryRequest__12_brew.py
- cargo/on_Binary__12_cargo_install.py -> cargo/on_BinaryRequest__12_cargo.py
- puppeteer/on_Binary__12_puppeteer_install.py -> puppeteer/on_BinaryRequest__12_puppeteer.py
- apt/on_Binary__13_apt_install.py -> apt/on_BinaryRequest__13_apt.py
- custom/on_Binary__14_custom_install.py -> custom/on_BinaryRequest__14_custom.py

Install hooks:
- npm/on_Crawl__01_npm_install.py -> npm/on_Install__01_npm.py
- git/on_Crawl__05_git_install.finite.bg.py -> git/on_Install__05_git.finite.bg.py
- wget/on_Crawl__10_wget_install.finite.bg.py -> wget/on_Install__10_wget.finite.bg.py
- ytdlp/on_Crawl__15_ytdlp_install.finite.bg.py -> ytdlp/on_Install__15_ytdlp.finite.bg.py
- gallerydl/on_Crawl__20_gallerydl_install.finite.bg.py -> gallerydl/on_Install__20_gallerydl.finite.bg.py
- forumdl/on_Crawl__25_forumdl_install.finite.bg.py -> forumdl/on_Install__25_forumdl.finite.bg.py
- papersdl/on_Crawl__30_papersdl_install.finite.bg.py -> papersdl/on_Install__30_papersdl.finite.bg.py
- claudecode/on_Crawl__35_claudecode_install.finite.bg.py -> claudecode/on_Install__35_claudecode.finite.bg.py
- readability/on_Crawl__35_readability_install.finite.bg.py -> readability/on_Install__35_readability.finite.bg.py
- mercury/on_Crawl__40_mercury_install.finite.bg.py -> mercury/on_Install__40_mercury.finite.bg.py
- defuddle/on_Crawl__41_defuddle_install.finite.bg.py -> defuddle/on_Install__41_defuddle.finite.bg.py
- trafilatura/on_Crawl__41_trafilatura_install.finite.bg.py -> trafilatura/on_Install__41_trafilatura.finite.bg.py
- opendataloader/on_Crawl__42_opendataloader_install.finite.bg.py -> opendataloader/on_Install__42_opendataloader.finite.bg.py
- liteparse/on_Crawl__43_liteparse_install.finite.bg.py -> liteparse/on_Install__43_liteparse.finite.bg.py
- singlefile/on_Crawl__45_singlefile_install.finite.bg.py -> singlefile/on_Install__45_singlefile.finite.bg.py
- search_backend_ripgrep/on_Crawl__50_ripgrep_install.finite.bg.py -> search_backend_ripgrep/on_Install__50_ripgrep.finite.bg.py
- search_backend_sonic/on_Crawl__50_sonic_install.finite.bg.py -> search_backend_sonic/on_Install__50_sonic.finite.bg.py
- puppeteer/on_Crawl__60_puppeteer_install.py -> puppeteer/on_Install__60_puppeteer.py
- chrome/on_Crawl__70_chrome_install.finite.bg.py -> chrome/on_Install__70_chrome.finite.bg.py
- ublock/on_Crawl__80_install_ublock_extension.js -> ublock/on_Install__80_ublock_extension.js
- istilldontcareaboutcookies/on_Crawl__81_install_istilldontcareaboutcookies_extension.js -> istilldontcareaboutcookies/on_Install__81_istilldontcareaboutcookies_extension.js
- singlefile/on_Crawl__82_singlefile_install.js -> singlefile/on_Install__82_singlefile.js
- twocaptcha/on_Crawl__83_twocaptcha_install.js -> twocaptcha/on_Install__83_twocaptcha.js
- claudechrome/on_Crawl__84_claudechrome_install.js -> claudechrome/on_Install__84_claudechrome.js

Crawl setup hooks:
- search_backend_sonic/on_Crawl__55_sonic_start.py -> search_backend_sonic/on_CrawlSetup__55_sonic_start.py
- chrome/on_Crawl__90_chrome_launch.daemon.bg.js -> chrome/on_CrawlSetup__90_chrome_launch.daemon.bg.js
- chrome/on_Crawl__91_chrome_wait.js -> chrome/on_CrawlSetup__91_chrome_wait.js
- twocaptcha/on_Crawl__95_twocaptcha_config.js -> twocaptcha/on_CrawlSetup__95_twocaptcha_config.js
- claudechrome/on_Crawl__96_claudechrome_config.js -> claudechrome/on_CrawlSetup__96_claudechrome_config.js

abx-dl Work
-----------
- Add exact hook event parsing to `Hook`.
- Partition hooks by exact event type, not substring.
- Add `InstallEvent`.
- Rename all Binary* event classes and usages.
- Rename process routing from `type=Binary` to `type=BinaryRequest` / `type=Binary`.
- Make install CLI path emit `InstallEvent` directly.
- Remove `INSTALL_URL` and all fake-install crawl logic.
- Update live UI labels and install table handling to the new event names.
- Update tests.

archivebox Work
---------------
- Rename projector/service imports to the new event names.
- Update runner logic to emit `InstallEvent` instead of fake install crawl.
- Update binary projection to listen for `BinaryRequestEvent` and `BinaryEvent`.
- Update CLI code and tests.
- Remove all `INSTALL_URL` / fake-crawl install references.

Execution Plan
--------------
1. Audit sibling-repo write access and request escalation if required.
2. Parallelize:
   - abx-plugins hook renames and emitted record rename
   - abx-dl event/type rename and install-phase split
   - archivebox projector/runner rename
3. Integrate and run focused tests.
4. Run broader cross-repo tests until green.
