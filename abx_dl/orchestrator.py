"""
Event-driven orchestrator for abx-dl.

This is the main entry point for downloading a URL. It wires up all services
on a caller-provided abxbus EventBus and emits the phase root events that drive
the entire lifecycle. Everything else is driven by services reacting to events.

Full event tree for a typical run::

    InstallEvent                                # emitted here first by download()
    └── BinaryRequestEvent × N                  # emitted from config.json required_binaries
        └── provider hooks → BinaryEvent

    CrawlEvent                                  # internal lifecycle root
    ├── CrawlSetupEvent                         # plugin on_CrawlSetup hooks run here
    │   ├── ProcessEvent  (bg: chrome_launch)
    │   ├── ProcessEvent  (FG: chrome_wait)
    │   │   └── ProcessCompletedEvent
    │   └── ...
    │
    ├── CrawlStartEvent                         # triggers snapshot phase
    │   └── SnapshotEvent (depth=0)
    │       ├── ProcessEvent  (on_Snapshot hooks)
    │       │   ├── ProcessStdoutEvent
    │       │   │   ├── SnapshotEvent (depth>0, ignored by abx-dl)
    │       │   │   ├── TagEvent
    │       │   │   └── ArchiveResultEvent (from hook JSONL)
    │       │   └── ProcessCompletedEvent
    │       │       └── ArchiveResultEvent (synthetic, only if hook didn't report one / exited with an error)
    │       ├── SnapshotCleanupEvent
    │       │   └── ProcessKillEvent × N
    │       └── SnapshotCompletedEvent
    │
    ├── CrawlCleanupEvent                       # SIGTERMs bg crawl daemons
    │   └── ProcessKillEvent × N
    │
    └── CrawlCompletedEvent                     # informational

Result collection:
- ArchiveResultEvents are only emitted during the snapshot phase (under
  CrawlStartEvent → SnapshotEvent).
- Install preflight resolves ``required_binaries`` through provider plugins'
  ``on_BinaryRequest__*`` hooks.
- Provider hooks emit ``Binary`` only. Crawl setup hooks emit no stdout JSONL
  records. Snapshot hooks emit ``ArchiveResult`` and may also emit ``Snapshot``
  and ``Tag``.
- ArchiveResultService emits ArchiveResultEvents in two cases: directly from
  hook JSONL output (inline), or as a synthetic fallback on ProcessCompletedEvent
  when the hook didn't report one itself (failed, or succeeded with output files).
  The orchestrator collects all of them.
- Any external bus consumers should be attached up front during bus setup.

Key abxbus concepts used:

- **event_concurrency='parallel'**: child events of a parent can process
  concurrently. This is what lets bg ProcessEvents (fire-and-forget) run
  alongside the parent CrawlEvent's serial handler chain.

- **Serial handler execution** (default): handlers on the same event run one
  at a time in registration order. This preserves hook ordering — fg hooks
  see config updates from all prior hooks.

- **Queue-jump** (``await bus.emit(...)``): the emitted event and ALL its
  descendants complete synchronously before the await returns. This is how
  config propagation works: InstallEvent emits BinaryRequestEvent →
  provider hooks resolve/install it → BinaryEvent updates runtime binary state,
  and snapshot hook stdout records like ``ArchiveResult`` / ``Snapshot`` / ``Tag``
  are also fully routed before the next stdout line is read.

- **Fire-and-forget** (``bus.emit(...)`` without await): the event becomes a
  concurrent child of the current event. It runs in the background and is
  subject to the parent event's timeout.

- **max_history_drop=True**: when the event history buffer fills up, old
  entries are dropped instead of rejecting new events. A full plugin run
  generates hundreds of events, easily exceeding the default 100.
"""

from __future__ import annotations

import sys
import json
from collections.abc import Sequence
from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from abxbus import EventBus, EventBusMiddleware, EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode

from .config import ensure_default_persona_dir, get_initial_env, get_derived_config
from .events import (
    CrawlEvent,
    InstallEvent,
    MachineEvent,
    slow_warning_timeout,
)
from .heartbeat import CrawlHeartbeat
from .models import Snapshot, write_jsonl
from .models import Hook, Plugin, discover_plugins, filter_plugins
from .services import ArchiveResultService, BinaryService, CrawlService, MachineService, ProcessService, SnapshotService, TagService


def setup_services(
    bus: EventBus,
    *,
    plugins: dict[str, Plugin],
    url: str | None = None,
    snapshot: Snapshot | None = None,
    output_dir: Path | None = None,
    install_enabled: bool = True,
    crawl_setup_enabled: bool = True,
    crawl_start_enabled: bool = True,
    snapshot_cleanup_enabled: bool = True,
    crawl_cleanup_enabled: bool = True,
    crawl_setup_phase_timeout: float = 300.0,
    snapshot_phase_timeout: float = 300.0,
    snapshot_cleanup_phase_timeout: float = 300.0,
    crawl_cleanup_phase_timeout: float = 300.0,
    persist_derived: bool = True,
    auto_install: bool = True,
    emit_jsonl: bool = True,
    interactive_tty: bool | None = None,
    MachineService: type[MachineService] | None = MachineService,
    BinaryService: type[BinaryService] | None = BinaryService,
    ProcessService: type[ProcessService] | None = ProcessService,
    ArchiveResultService: type[ArchiveResultService] | None = ArchiveResultService,
    TagService: type[TagService] | None = TagService,
    CrawlService: type[CrawlService] | None = CrawlService,
    SnapshotService: type[SnapshotService] | None = SnapshotService,
) -> None:
    """Attach the shared abx-dl services to an existing bus.

    This is the public entrypoint for embedding abx-dl as an event-driven
    runtime without immediately starting a crawl via ``download()``.
    """
    if interactive_tty is None:
        interactive_tty = sys.stdout.isatty() or sys.stderr.isatty()

    if MachineService is not None:
        MachineService(bus, persist_derived=persist_derived)

    if BinaryService is not None:
        install_plugins = get_install_plugins(plugins)
        BinaryService(
            bus,
            plugins=plugins,
            auto_install=auto_install,
            install_plugins=install_plugins,
            output_dir=output_dir,
            snapshot=snapshot,
        )

    if ProcessService is not None:
        ProcessService(
            bus,
            emit_jsonl=emit_jsonl,
            interactive_tty=bool(interactive_tty),
        )

    if ArchiveResultService is not None:
        ArchiveResultService(
            bus,
            emit_jsonl=emit_jsonl,
        )

    if TagService is not None:
        TagService(bus)

    if (
        CrawlService is not None
        and url is not None
        and snapshot is not None
        and output_dir is not None
        and (crawl_setup_enabled or crawl_start_enabled or crawl_cleanup_enabled)
    ):
        CrawlService(
            bus,
            url=url,
            snapshot=snapshot,
            output_dir=output_dir,
            plugins=plugins,
            crawl_setup_enabled=crawl_setup_enabled,
            crawl_start_enabled=crawl_start_enabled,
            crawl_cleanup_enabled=crawl_cleanup_enabled,
            crawl_setup_phase_timeout=crawl_setup_phase_timeout,
            snapshot_phase_timeout=snapshot_phase_timeout,
            crawl_cleanup_phase_timeout=crawl_cleanup_phase_timeout,
        )
        if SnapshotService is not None and (crawl_start_enabled or snapshot_cleanup_enabled):
            SnapshotService(
                bus,
                url=url,
                snapshot=snapshot,
                output_dir=output_dir,
                plugins=plugins,
                snapshot_phase_timeout=snapshot_phase_timeout,
                snapshot_cleanup_enabled=snapshot_cleanup_enabled,
                snapshot_cleanup_phase_timeout=snapshot_cleanup_phase_timeout,
            )

    return None


def get_install_plugins(plugins: dict[str, Plugin]) -> list[Plugin]:
    """Return plugins that declare required binaries for the install phase."""
    return [plugin for plugin in plugins.values() if plugin.config.required_binaries]


def compute_install_phase_timeout(plugins: list[Plugin], config: dict[str, Any] | None = None) -> float:
    """Sum per-plugin timeouts across plugins that declare required binaries."""
    total = sum(get_plugin_timeout(plugin, config) for plugin in plugins)
    return max(float(total), 60.0)


async def install_plugins(
    plugin_names: Sequence[str] | None = None,
    *,
    plugins: dict[str, Plugin] | None = None,
    output_dir: Path | None = None,
    config_overrides: dict[str, Any] | None = None,
    derived_config_overrides: dict[str, Any] | None = None,
    emit_jsonl: bool = False,
    bus: EventBus | None = None,
    dry_run: bool = False,
    MachineService: type[MachineService] | None = MachineService,
    BinaryService: type[BinaryService] | None = BinaryService,
    ProcessService: type[ProcessService] | None = ProcessService,
):
    """Run only the dependency preflight on an existing bus or a temporary one.

    This emits InstallEvent, which resolves enabled plugins'
    ``config.json > required_binaries`` through provider plugins'
    ``on_BinaryRequest__*`` hooks, without starting the later
    ``on_CrawlSetup__*`` or ``on_Snapshot__*`` plugin phases.
    """
    all_plugins = plugins or discover_plugins()
    selected = filter_plugins(all_plugins, list(plugin_names), include_providers=True) if plugin_names else all_plugins
    if not selected:
        return []

    merged_config = dict(config_overrides or {})
    if dry_run:
        merged_config["DRY_RUN"] = True
    initial_user_config = get_initial_env()
    initial_user_config.update(merged_config)
    initial_derived_config = get_derived_config(initial_user_config)
    if derived_config_overrides:
        initial_derived_config.update(derived_config_overrides)

    install_output_dir = output_dir
    temp_dir_ctx = nullcontext(output_dir) if output_dir is not None else TemporaryDirectory(prefix="abx-dl-install-")

    with temp_dir_ctx as temp_dir:
        install_output_dir = install_output_dir or Path(temp_dir)
        install_output_dir.mkdir(parents=True, exist_ok=True)
        owns_bus = bus is None
        bus = bus or create_bus(total_timeout=60.0)
        snapshot = Snapshot(url="")
        install_plugins_for_phase = get_install_plugins(selected)

        setup_services(
            bus,
            plugins=selected,
            url="",
            snapshot=snapshot,
            output_dir=install_output_dir,
            install_enabled=True,
            crawl_setup_enabled=False,
            crawl_start_enabled=False,
            snapshot_cleanup_enabled=False,
            crawl_cleanup_enabled=False,
            crawl_setup_phase_timeout=60.0,
            snapshot_phase_timeout=60.0,
            snapshot_cleanup_phase_timeout=60.0,
            crawl_cleanup_phase_timeout=60.0,
            persist_derived=True,
            auto_install=True,
            emit_jsonl=emit_jsonl,
            interactive_tty=sys.stdout.isatty() or sys.stderr.isatty(),
            MachineService=MachineService,
            BinaryService=BinaryService,
            ProcessService=ProcessService,
            ArchiveResultService=None,
            TagService=None,
        )
        await bus.emit(
            MachineEvent(
                config=initial_user_config,
                config_type="user",
            ),
        )
        if initial_derived_config:
            await bus.emit(
                MachineEvent(
                    config=initial_derived_config,
                    config_type="derived",
                ),
            )
        install_phase_timeout = compute_install_phase_timeout(install_plugins_for_phase, merged_config or None)
        try:
            await bus.emit(
                InstallEvent(
                    url="",
                    snapshot_id=snapshot.id,
                    output_dir=str(install_output_dir),
                    event_timeout=install_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(install_phase_timeout),
                ),
            )
        finally:
            if owns_bus:
                await bus.stop()


def get_plugin_timeout(plugin: Plugin, config: dict[str, Any] | None = None) -> int:
    """Resolve a plugin's timeout from config overrides and schema defaults.

    Checks (in priority order):
    1. ``{PLUGIN_NAME}_TIMEOUT`` in *config*
    2. ``TIMEOUT`` in *config*
    3. ``{PLUGIN_NAME}_TIMEOUT`` default from the plugin's config properties
    4. Global default (60s)
    """
    name_upper = plugin.name.upper()
    cfg = config or {}
    # Check config overrides first
    if f"{name_upper}_TIMEOUT" in cfg:
        return int(cfg[f"{name_upper}_TIMEOUT"])
    if "TIMEOUT" in cfg:
        return int(cfg["TIMEOUT"])
    # Check plugin schema defaults
    schema_key = f"{name_upper}_TIMEOUT"
    schema_def = plugin.config.properties[schema_key] if schema_key in plugin.config.properties else {}
    if isinstance(schema_def, dict) and "default" in schema_def:
        return int(schema_def["default"])
    return 60


def compute_phase_timeout(hooks: list[tuple[Plugin, Hook]], config: dict[str, Any] | None = None) -> float:
    """Sum per-plugin timeouts across all hooks in a phase.

    Each hook contributes its plugin's timeout (from ``get_plugin_timeout``).
    This gives an accurate ceiling: slow plugins (e.g. YTDLP_TIMEOUT=120)
    contribute more than fast ones (e.g. TITLE_TIMEOUT=10).

    Returns at least 60.0 (minimum phase timeout).
    """
    total = sum(get_plugin_timeout(plugin, config) for plugin, _hook in hooks)
    return max(float(total), 60.0)


def create_bus(
    *,
    total_timeout: float = 60.0,
    name: str | None = "AbxDl",
    event_concurrency: EventConcurrencyMode | str | None = EventConcurrencyMode.PARALLEL,
    event_handler_concurrency: EventHandlerConcurrencyMode | str = EventHandlerConcurrencyMode.SERIAL,
    event_handler_completion: EventHandlerCompletionMode | str = EventHandlerCompletionMode.ALL,
    max_history_size: int | None = 100000,
    max_history_drop: bool = True,
    event_timeout: float | None = None,
    event_slow_timeout: float | None = None,
    event_handler_slow_timeout: float | None = 60.0,
    event_handler_detect_file_paths: bool = True,
    warn_on_duplicate_handler_names: bool = False,
    max_handler_recursion_depth: int = 6,
    middlewares: Sequence[EventBusMiddleware | type[EventBusMiddleware]] | None = None,
    id: str | None = None,
) -> EventBus:
    """Create a configured EventBus for a download run.

    Any external consumers should be attached during bus setup before the run starts.

    Args:
        total_timeout: Total timeout for the entire run (sum of all phase
            timeouts). Computed by ``compute_phase_timeout`` in download().
        name: Optional EventBus instance name.
        middlewares: Optional EventBus middlewares.
    """
    bus = EventBus(
        name=name,
        # parallel event concurrency lets bg ProcessEvents (fire-and-forget
        # children) process concurrently with the parent event's serial handlers
        event_concurrency=event_concurrency,
        event_handler_concurrency=event_handler_concurrency,
        event_handler_completion=event_handler_completion,
        # A full plugin run generates hundreds of events; drop old history
        # entries instead of rejecting new events when the buffer fills
        max_history_size=max_history_size,
        max_history_drop=max_history_drop,
        # Total timeout covers both crawl and snapshot phases.
        # Individual hooks set their own timeouts via event_handler_timeout
        # on their ProcessEvent.
        event_timeout=total_timeout if event_timeout is None else event_timeout,
        event_slow_timeout=(total_timeout * 0.8) if event_slow_timeout is None else event_slow_timeout,
        event_handler_slow_timeout=event_handler_slow_timeout,
        event_handler_detect_file_paths=event_handler_detect_file_paths,
        warn_on_duplicate_handler_names=warn_on_duplicate_handler_names,
        # Normal abx-dl processing legitimately nests several queue-jumped
        # handler chains (stdout -> typed event -> follow-up process/event work).
        max_handler_recursion_depth=max_handler_recursion_depth,
        middlewares=middlewares,
        id=id,
    )
    return bus


async def download(
    url: str,
    plugins: dict[str, Plugin],
    output_dir: Path,
    selected_plugins: list[str] | None = None,
    config_overrides: dict[str, Any] | None = None,
    derived_config_overrides: dict[str, Any] | None = None,
    auto_install: bool = True,
    *,
    bus: EventBus | None = None,
    emit_jsonl: bool | None = None,
    interactive_tty: bool | None = None,
    install_enabled: bool = True,
    crawl_setup_enabled: bool = True,
    crawl_start_enabled: bool = True,
    snapshot_cleanup_enabled: bool = True,
    crawl_cleanup_enabled: bool = True,
    dry_run: bool = False,
    MachineService: type[MachineService] | None = MachineService,
    BinaryService: type[BinaryService] | None = BinaryService,
    ProcessService: type[ProcessService] | None = ProcessService,
    ArchiveResultService: type[ArchiveResultService] | None = ArchiveResultService,
    TagService: type[TagService] | None = TagService,
    CrawlService: type[CrawlService] | None = CrawlService,
    SnapshotService: type[SnapshotService] | None = SnapshotService,
):
    """Download a URL using plugins, coordinated through a abxbus EventBus.

    This is the only public function in the orchestrator. It:
    1. Discovers and sorts hooks from selected plugins
    2. Wires up all services on the bus
    3. Emits InstallEvent for dependency preflight, then CrawlEvent as the
       internal lifecycle root for the CrawlSetup → CrawlStart → Snapshot →
       SnapshotCleanup → CrawlCleanup sequence (unless phase flags request a subset)
    4. Leaves all result collection to bus subscribers attached during setup

    Args:
        url: The URL to download/archive.
        plugins: All discovered plugins (from discover_plugins()).
        output_dir: Where to write output files and index.jsonl.
        selected_plugins: If set, only use these plugins (with dependency resolution).
        config_overrides: Extra config values (e.g. TIMEOUT) merged into user_config.
        auto_install: Whether to auto-install missing binaries.
        bus: Pre-configured EventBus to run against. If None, a default bus is
            created via create_bus().
        emit_jsonl: Whether to print JSONL to stdout. Defaults to True if not a TTY.

    """

    config_overrides = dict(config_overrides or {})
    if dry_run:
        config_overrides["DRY_RUN"] = True
    initial_user_config = get_initial_env()
    initial_user_config.update(config_overrides)
    initial_derived_config = get_derived_config(initial_user_config)
    if derived_config_overrides:
        initial_derived_config.update(derived_config_overrides)
    ensure_default_persona_dir()
    output_dir = output_dir or Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "index.jsonl"
    stdout_is_tty = sys.stdout.isatty()
    if emit_jsonl is None:
        emit_jsonl = not stdout_is_tty
    if interactive_tty is None:
        interactive_tty = stdout_is_tty or sys.stderr.isatty()

    # Filter plugins for runtime phases (includes provider plugins so their
    # on_BinaryRequest hooks are available during install resolution).
    if selected_plugins:
        plugins = filter_plugins(plugins, selected_plugins)

    # Create snapshot record and write it as the first line of index.jsonl
    snapshot_payload: dict[str, Any] = {"url": url}
    if config_overrides.get("EXTRA_CONTEXT"):
        extra_context = config_overrides["EXTRA_CONTEXT"]
        if isinstance(extra_context, str):
            extra_context = json.loads(extra_context)
        if not isinstance(extra_context, dict):
            raise TypeError("EXTRA_CONTEXT must be an object")
        if "snapshot_id" in extra_context:
            snapshot_payload["id"] = str(extra_context["snapshot_id"])
        if "snapshot_depth" in extra_context:
            snapshot_payload["depth"] = int(extra_context["snapshot_depth"])
        if "crawl_id" in extra_context:
            snapshot_payload["crawl_id"] = str(extra_context["crawl_id"])
    snapshot = Snapshot(**snapshot_payload)
    write_jsonl(index_path, snapshot, also_print=emit_jsonl)

    # Collect and sort hooks by (order, name) so execution order matches
    # the numeric prefix in hook filenames (e.g. __10, __41, __70, __90, __91)
    install_plugins_for_phase = get_install_plugins(plugins)
    crawl_setup_hooks: list[tuple[Plugin, Hook]] = []
    snapshot_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in plugins.values():
        for hook in plugin.filter_hooks("CrawlSetup"):
            crawl_setup_hooks.append((plugin, hook))
        for hook in plugin.filter_hooks("Snapshot"):
            snapshot_hooks.append((plugin, hook))
    crawl_setup_hooks.sort(key=lambda x: x[1].sort_key)
    snapshot_hooks.sort(key=lambda x: x[1].sort_key)

    # Compute per-phase timeouts from plugin-specific settings
    install_phase_timeout = compute_install_phase_timeout(install_plugins_for_phase, config_overrides or None)
    crawl_setup_phase_timeout = compute_phase_timeout(crawl_setup_hooks, config_overrides or None)
    snapshot_phase_timeout = compute_phase_timeout(snapshot_hooks, config_overrides or None)
    snapshot_cleanup_phase_timeout = snapshot_phase_timeout
    crawl_cleanup_phase_timeout = crawl_setup_phase_timeout
    total_timeout = (
        (install_phase_timeout if install_enabled else 0.0)
        + (crawl_setup_phase_timeout if crawl_setup_enabled else 0.0)
        + (snapshot_phase_timeout if crawl_start_enabled else 0.0)
        + (snapshot_cleanup_phase_timeout if snapshot_cleanup_enabled else 0.0)
        + (crawl_cleanup_phase_timeout if crawl_cleanup_enabled else 0.0)
    )

    owns_bus = bus is None
    if bus is None:
        bus = create_bus(total_timeout=total_timeout)
    assert bus is not None

    setup_services(
        bus,
        plugins=plugins,
        url=url,
        snapshot=snapshot,
        output_dir=output_dir,
        install_enabled=install_enabled,
        crawl_setup_enabled=crawl_setup_enabled,
        crawl_start_enabled=crawl_start_enabled,
        snapshot_cleanup_enabled=snapshot_cleanup_enabled,
        crawl_cleanup_enabled=crawl_cleanup_enabled,
        crawl_setup_phase_timeout=crawl_setup_phase_timeout,
        snapshot_phase_timeout=snapshot_phase_timeout,
        snapshot_cleanup_phase_timeout=snapshot_cleanup_phase_timeout,
        crawl_cleanup_phase_timeout=crawl_cleanup_phase_timeout,
        persist_derived=True,
        auto_install=auto_install,
        emit_jsonl=emit_jsonl,
        interactive_tty=interactive_tty,
        MachineService=MachineService,
        BinaryService=BinaryService,
        ProcessService=ProcessService,
        ArchiveResultService=ArchiveResultService,
        TagService=TagService,
        CrawlService=CrawlService,
        SnapshotService=SnapshotService,
    )
    await bus.emit(
        MachineEvent(
            config=initial_user_config,
            config_type="user",
        ),
    )
    if initial_derived_config:
        await bus.emit(
            MachineEvent(
                config=initial_derived_config,
                config_type="derived",
            ),
        )

    heartbeat = None
    if crawl_setup_enabled or crawl_start_enabled or crawl_cleanup_enabled:
        heartbeat = CrawlHeartbeat(
            output_dir,
            runtime=str(initial_user_config.get("ABX_RUNTIME", "abx-dl")),
            crawl_id=snapshot.crawl_id or snapshot.id,
        )
        await heartbeat.start()

    try:
        if install_enabled:
            await bus.emit(
                InstallEvent(
                    url=url,
                    snapshot_id=snapshot.id,
                    output_dir=str(output_dir),
                    event_timeout=install_phase_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(install_phase_timeout),
                ),
            )
        if crawl_setup_enabled or crawl_start_enabled or crawl_cleanup_enabled:
            crawl_event_timeout = (
                (crawl_setup_phase_timeout if crawl_setup_enabled else 0.0)
                + (snapshot_phase_timeout if crawl_start_enabled else 0.0)
                + (crawl_cleanup_phase_timeout if crawl_cleanup_enabled else 0.0)
            )
            crawl_event = CrawlEvent(
                url=url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_timeout=crawl_event_timeout,
                event_handler_slow_timeout=slow_warning_timeout(crawl_event_timeout),
            )
            await bus.emit(crawl_event)
    finally:
        if heartbeat is not None:
            await heartbeat.stop()
        if owns_bus:
            await bus.stop()
