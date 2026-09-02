"""
Event-driven orchestrator for abx-dl.

This is the main entry point for downloading a URL. It wires up all services
on a caller-provided abxbus EventBus and emits the phase root events that drive
the entire lifecycle. Everything else is driven by services reacting to events.

Full event tree for a typical run::

    InstallEvent                                # dependency preflight
    └── BinaryRequestEvent × N                  # config.json required_binaries

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
    │       │   │   ├── SnapshotDiscoveredEvent
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
- Install preflight resolves ``required_binaries`` through abxpkg and projects
  the resulting runtime environment before any hooks start.
- Crawl setup hooks emit no stdout JSONL records. Snapshot hooks emit
  ``ArchiveResult`` and may also emit ``Snapshot`` and ``Tag``.
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

- **Queue-jump** (``await bus.emit(...).now()``): the emitted event and ALL its
  descendants complete synchronously before the await returns. Snapshot hook
  stdout records like ``ArchiveResult`` / ``Snapshot`` / ``Tag`` are fully
  routed before the next stdout line is read.

- **Fire-and-forget** (``bus.emit(...)`` without await): the event becomes a
  concurrent child of the current event. It runs in the background and is
  subject to the parent event's timeout.

- **max_history_drop=True**: when the event history buffer fills up, old
  entries are dropped instead of rejecting new events. A full plugin run
  generates hundreds of events, easily exceeding the default 100.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Sequence
from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from abxbus import EventBus, EventBusMiddleware, EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode
from abxpkg.binary_service import BinaryRequestEvent, BinaryService

from .config import GlobalConfig, RuntimeConfig, ensure_default_persona_dir
from .catalog import PluginCatalog
from .events import (
    CrawlEvent,
    InstallEvent,
    MachineEvent,
    SnapshotDiscoveredEvent,
    SnapshotEvent,
    slow_warning_timeout,
)
from .models import Hook, Plugin, RequiredBinary, Snapshot, write_jsonl
from .services import (
    ArchiveResultService,
    CrawlLifecycleService,
    CrawlService,
    PluginBinariesService,
    PluginBinaryEnvService,
    ProcessService,
    SnapshotService,
    TagService,
)


def get_install_plugins(catalog: PluginCatalog) -> list[Plugin]:
    """Return plugins that declare required binaries for the install phase."""
    return [plugin for plugin in catalog.values() if plugin.config.required_binaries]


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def get_binary_request_install_timeout(record: RequiredBinary | dict[str, Any], config: dict[str, Any] | None = None) -> int:
    """Resolve the timeout budget needed for a required binary request."""
    cfg = config or {}
    record_data = record.model_dump(mode="json") if isinstance(record, RequiredBinary) else record
    default_event_timeout = _positive_int(BinaryRequestEvent.model_fields["event_timeout"].default) or 300
    candidates = [
        record_data.get("event_timeout"),
        record_data.get("install_timeout"),
        cfg.get("ABXPKG_INSTALL_TIMEOUT"),
        os.environ.get("ABXPKG_INSTALL_TIMEOUT"),
        default_event_timeout,
    ]
    return max(_positive_int(candidate) or 0 for candidate in candidates)


def compute_install_phase_timeout(plugins: list[Plugin], config: dict[str, Any] | None = None) -> float:
    """Return the largest timeout budget among concurrent per-binary lanes."""
    lane_budgets: dict[str, int] = {}
    for plugin in plugins:
        plugin_timeout = get_plugin_timeout(plugin, config)
        for record in plugin.config.required_binaries:
            request_budget = max(plugin_timeout, get_binary_request_install_timeout(record, config))
            lane_budgets[record.name] = lane_budgets.get(record.name, 0) + request_budget
    return max(float(max(lane_budgets.values(), default=0)), 60.0)


async def install_plugins(
    catalog: PluginCatalog,
    *,
    config: dict[str, Any] | None = None,
    derived_config: dict[str, Any] | None = None,
    runtime: str = "abx-dl",
    output_dir: Path | None = None,
    emit_jsonl: bool = False,
    bus: EventBus | None = None,
):
    """Run only the dependency preflight on an existing bus or a temporary one.

    This emits InstallEvent, which resolves enabled plugins'
    ``config.json > required_binaries`` through abxpkg, without starting the
    later ``on_CrawlSetup__*`` or ``on_Snapshot__*`` plugin phases.
    """
    if not catalog:
        return []

    user_config = dict(config or {})
    user_config["ABX_RUNTIME"] = runtime
    install_timeout = compute_install_phase_timeout(get_install_plugins(catalog), user_config)

    install_output_dir = output_dir
    temp_dir_ctx = nullcontext(output_dir) if output_dir is not None else TemporaryDirectory(prefix="abx-dl-install-")

    with temp_dir_ctx as temp_dir:
        install_output_dir = install_output_dir or Path(temp_dir)
        install_output_dir.mkdir(parents=True, exist_ok=True)
        bus = bus or create_bus(total_timeout=install_timeout)
        snapshot = Snapshot(url="")
        PluginBinaryEnvService(bus, catalog=catalog)
        BinaryService(bus, auto_install=True)
        PluginBinariesService(
            bus,
            catalog=catalog,
            auto_install=True,
            install_plugins=get_install_plugins(catalog),
            output_dir=install_output_dir,
            snapshot=snapshot,
        )
        ProcessService(
            bus,
            emit_jsonl=emit_jsonl,
            interactive_tty=sys.stdout.isatty() or sys.stderr.isatty(),
        )
        await bus.emit(MachineEvent(config=user_config, config_type="user")).now()
        if derived_config:
            await bus.emit(MachineEvent(config=dict(derived_config), config_type="derived")).now()
        try:
            install_event = bus.emit(
                InstallEvent(
                    url="",
                    snapshot_id=snapshot.id,
                    output_dir=str(install_output_dir),
                    event_timeout=install_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(install_timeout),
                ),
            )
            await install_event.now(timeout=install_timeout)
            await install_event.wait(timeout=install_timeout)
            await install_event.event_results_list()
        finally:
            await bus.wait_until_idle()


async def parse_input(
    source_text: str,
    catalog: PluginCatalog,
    output_dir: Path,
    *,
    config: dict[str, Any] | None = None,
    derived_config: dict[str, Any] | None = None,
    runtime: str = "abx-dl",
    auto_install: bool = True,
    bus: EventBus | None = None,
    emit_jsonl: bool = False,
) -> list[Snapshot]:
    """Parse imported text through opted-in snapshot hooks and return URL facts.

    The source is durably written to ``staticfile/stdin.txt``. Parser hooks run
    against that real file URL using an in-memory Snapshot context; no database,
    pseudo URL, crawl lifecycle, or persistent ingestion state is involved.
    Returned facts retain hook metadata and are normalized to depth zero so a
    collection application can persist them as initial crawl snapshots.
    """

    output_dir = output_dir.expanduser().resolve()
    input_path = output_dir / "staticfile" / "stdin.txt"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_input_path = input_path.with_name(f".{input_path.name}.{os.getpid()}.tmp")
    temporary_input_path.write_text(source_text, encoding="utf-8")
    temporary_input_path.replace(input_path)

    parser_catalog = PluginCatalog(
        {name: plugin for name, plugin in catalog.items() if plugin.config.x_accepts_internal_input and plugin.filter_hooks("Snapshot")},
    )
    if not parser_catalog:
        return []

    user_config = dict(config or {})
    user_config["ABX_RUNTIME"] = runtime
    runtime_config = RuntimeConfig(user=GlobalConfig(**user_config), derived=dict(derived_config or {}))
    snapshot = Snapshot(url=input_path.as_uri())
    install_timeout = compute_install_phase_timeout(get_install_plugins(parser_catalog), user_config)
    snapshot_hooks = [(plugin, hook) for plugin in parser_catalog.values() for hook in plugin.filter_hooks("Snapshot")]
    snapshot_timeout = compute_phase_timeout(snapshot_hooks, user_config)
    owns_bus = bus is None
    bus = bus or create_bus(total_timeout=install_timeout + (snapshot_timeout * 2), name=f"AbxDlInput_{snapshot.id}")

    PluginBinaryEnvService(bus, catalog=parser_catalog)
    BinaryService(bus, auto_install=auto_install)
    PluginBinariesService(
        bus,
        catalog=parser_catalog,
        auto_install=auto_install,
        install_plugins=get_install_plugins(parser_catalog),
        output_dir=output_dir,
        snapshot=snapshot,
    )
    ProcessService(bus, emit_jsonl=emit_jsonl, interactive_tty=False)
    ArchiveResultService(bus, emit_jsonl=emit_jsonl)
    TagService(bus)
    SnapshotService(
        bus,
        url=snapshot.url,
        snapshot=snapshot,
        output_dir=output_dir,
        catalog=parser_catalog,
        config=runtime_config,
        snapshot_phase_timeout=snapshot_timeout,
        snapshot_cleanup_phase_timeout=snapshot_timeout,
    )
    await bus.emit(MachineEvent(config=user_config, config_type="user")).now()
    if derived_config:
        await bus.emit(MachineEvent(config=dict(derived_config), config_type="derived")).now()

    try:
        install_event = bus.emit(
            InstallEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_timeout=install_timeout,
                event_handler_slow_timeout=slow_warning_timeout(install_timeout),
            ),
        )
        await install_event.now(timeout=install_timeout)
        await install_event.wait(timeout=install_timeout)
        await install_event.event_results_list()
        await bus.wait_until_idle()

        snapshot_event = bus.emit(
            SnapshotEvent(
                url=snapshot.url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                depth=0,
                event_timeout=snapshot_timeout,
                event_handler_slow_timeout=slow_warning_timeout(snapshot_timeout),
            ),
        )
        await snapshot_event.now(timeout=snapshot_timeout)
        await snapshot_event.wait(timeout=snapshot_timeout)
        await snapshot_event.event_results_list()
        await bus.wait_until_idle()
        discoveries = await bus.filter(
            SnapshotDiscoveredEvent,
            child_of=snapshot_event,
            past=True,
            future=False,
        )
        return [event.snapshot.model_copy(update={"depth": 0}) for event in reversed(discoveries)]
    finally:
        if owns_bus:
            await bus.wait_until_idle()


def get_plugin_timeout(plugin: Plugin, config: dict[str, Any] | None = None) -> int:
    """Resolve a plugin's timeout from runtime config and schema defaults.

    Checks (in priority order):
    1. ``{PLUGIN_NAME}_TIMEOUT`` in *config*
    2. ``{PLUGIN_NAME}_TIMEOUT`` in the process environment
    3. ``TIMEOUT`` in *config*
    4. ``TIMEOUT`` in the process environment
    5. ``{PLUGIN_NAME}_TIMEOUT`` default from the plugin's config properties
    6. Global default (60s)
    """
    name_upper = plugin.name.upper()
    cfg = config or {}
    # Check config overrides first
    if f"{name_upper}_TIMEOUT" in cfg:
        return int(cfg[f"{name_upper}_TIMEOUT"])
    if f"{name_upper}_TIMEOUT" in os.environ:
        return int(os.environ[f"{name_upper}_TIMEOUT"])
    if "TIMEOUT" in cfg:
        return int(cfg["TIMEOUT"])
    if "TIMEOUT" in os.environ:
        return int(os.environ["TIMEOUT"])
    # Check plugin schema defaults
    schema_key = f"{name_upper}_TIMEOUT"
    schema_def = plugin.config.properties.get(schema_key, {})
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
    catalog: PluginCatalog,
    output_dir: Path,
    auto_install: bool = True,
    *,
    config: dict[str, Any] | None = None,
    derived_config: dict[str, Any] | None = None,
    runtime: str = "abx-dl",
    bus: EventBus | None = None,
    emit_jsonl: bool | None = None,
    interactive_tty: bool | None = None,
):
    """Download a URL using plugins, coordinated through a abxbus EventBus.

    This is the only public function in the orchestrator. It:
    1. Discovers and sorts hooks from selected plugins
    2. Wires up all services on the bus
    3. Emits InstallEvent for dependency preflight, then CrawlEvent for the
       CrawlSetup → CrawlStart → Snapshot →
       SnapshotCleanup → CrawlCleanup sequence
    4. Leaves all result collection to bus subscribers attached during setup

    Args:
        url: The URL to download/archive.
        catalog: The selected plugins to execute.
        output_dir: Where to write output files and index.jsonl.
        auto_install: Whether to auto-install missing binaries.
        bus: Pre-configured EventBus to run against. If None, a default bus is
            created via create_bus().
        emit_jsonl: Whether to print JSONL to stdout. Defaults to True if not a TTY.

    """

    ensure_default_persona_dir()
    # Hook subprocesses run with cwd set to SNAP_DIR/<plugin>, while hook env
    # carries shared crawl/snapshot paths like SNAP_DIR and CRAWL_DIR. Keeping
    # those paths absolute here prevents JS/Python hooks from resolving the
    # same run directory differently after ProcessService enters a plugin cwd.
    output_dir = (output_dir or Path.cwd()).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "index.jsonl"
    stdout_is_tty = sys.stdout.isatty()
    if emit_jsonl is None:
        emit_jsonl = not stdout_is_tty
    if interactive_tty is None:
        interactive_tty = stdout_is_tty or sys.stderr.isatty()
    assert isinstance(interactive_tty, bool)

    user_config = dict(config or {})
    user_config["ABX_RUNTIME"] = runtime
    runtime_config = RuntimeConfig(user=GlobalConfig(**user_config), derived=dict(derived_config or {}))

    # Create snapshot record and write it as the first line of index.jsonl
    snapshot_payload: dict[str, Any] = {"url": url}
    if user_config.get("EXTRA_CONTEXT"):
        extra_context = user_config["EXTRA_CONTEXT"]
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

    crawl_setup_hooks = [(plugin, hook) for plugin in catalog.values() for hook in plugin.filter_hooks("CrawlSetup")]
    snapshot_hooks = [(plugin, hook) for plugin in catalog.values() for hook in plugin.filter_hooks("Snapshot")]
    install_phase_timeout = compute_install_phase_timeout(get_install_plugins(catalog), user_config)
    crawl_setup_phase_timeout = compute_phase_timeout(crawl_setup_hooks, user_config)
    snapshot_phase_timeout = compute_phase_timeout(snapshot_hooks, user_config)
    snapshot_cleanup_phase_timeout = snapshot_phase_timeout
    crawl_cleanup_phase_timeout = crawl_setup_phase_timeout
    total_timeout = (
        install_phase_timeout
        + crawl_setup_phase_timeout
        + snapshot_phase_timeout
        + snapshot_cleanup_phase_timeout
        + crawl_cleanup_phase_timeout
    )

    owns_bus = bus is None
    if bus is None:
        bus = create_bus(total_timeout=total_timeout)
    assert bus is not None

    PluginBinaryEnvService(bus, catalog=catalog)
    BinaryService(bus, auto_install=auto_install)
    PluginBinariesService(
        bus,
        catalog=catalog,
        auto_install=auto_install,
        install_plugins=get_install_plugins(catalog),
        output_dir=output_dir,
        snapshot=snapshot,
    )
    ProcessService(bus, emit_jsonl=emit_jsonl, interactive_tty=interactive_tty)
    ArchiveResultService(bus, emit_jsonl=emit_jsonl)
    TagService(bus)
    CrawlService(bus, url=url, snapshot=snapshot, output_dir=output_dir, catalog=catalog)
    SnapshotService(
        bus,
        url=url,
        snapshot=snapshot,
        output_dir=output_dir,
        catalog=catalog,
        config=runtime_config,
        snapshot_phase_timeout=snapshot_phase_timeout,
        snapshot_cleanup_phase_timeout=snapshot_cleanup_phase_timeout,
    )
    CrawlLifecycleService(
        bus,
        url=url,
        snapshot=snapshot,
        output_dir=output_dir,
        crawl_setup_phase_timeout=crawl_setup_phase_timeout,
        snapshot_phase_timeout=snapshot_phase_timeout,
        crawl_cleanup_phase_timeout=crawl_cleanup_phase_timeout,
    )
    await bus.emit(MachineEvent(config=user_config, config_type="user")).now()
    if derived_config:
        await bus.emit(MachineEvent(config=dict(derived_config), config_type="derived")).now()

    try:
        install_event = bus.emit(
            InstallEvent(
                url=url,
                snapshot_id=snapshot.id,
                output_dir=str(output_dir),
                event_timeout=install_phase_timeout,
                event_handler_slow_timeout=slow_warning_timeout(install_phase_timeout),
            ),
        )
        await install_event.now(timeout=install_phase_timeout)
        await install_event.wait(timeout=install_phase_timeout)
        await install_event.event_results_list()
        await bus.wait_until_idle()
        crawl_event_timeout = crawl_setup_phase_timeout + snapshot_phase_timeout + crawl_cleanup_phase_timeout
        crawl_event = CrawlEvent(
            url=url,
            snapshot_id=snapshot.id,
            output_dir=str(output_dir),
            event_timeout=crawl_event_timeout,
            event_handler_slow_timeout=slow_warning_timeout(crawl_event_timeout),
        )
        emitted_crawl_event = bus.emit(crawl_event)
        await emitted_crawl_event.now()
        await emitted_crawl_event.event_results_list()
    finally:
        if owns_bus:
            await bus.wait_until_idle()
