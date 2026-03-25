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
- `on_Binary__*` -> `on_BinaryRequest__*`
- `config.json > required_binaries` + `InstallEvent` preflight replace old install-hook bootstrapping
- `on_Crawl__{setup|launch|config|wait|start|...}` -> `on_CrawlSetup__*`
- keep all `on_Snapshot__*` unchanged
- remove `archivebox://install`
- remove `INSTALL_URL`
- add real `InstallEvent`

Behavior Rules
--------------
- `BinaryRequest` never carries resolved metadata like `abspath`, `version`, `sha256`.
- `Binary` always carries resolved metadata.
- `InstallEvent` is orchestrator-only; provider plugins participate via `on_BinaryRequest__*`.
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
