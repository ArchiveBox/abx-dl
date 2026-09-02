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
from collections.abc import Iterable, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
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
    slow_warning_timeout,
)
from .models import Hook, Plugin, RequiredBinary, Snapshot, write_jsonl
from .services import (
    ArchiveResultService,
    CrawlService,
    PluginBinariesService,
    PluginBinaryEnvService,
    ProcessService,
    SnapshotService,
    TagService,
)


def _attach_services(
    bus: EventBus,
    *,
    catalog: PluginCatalog,
    url: str | None = None,
    snapshot: Snapshot | None = None,
    output_dir: Path | None = None,
    runtime_config: RuntimeConfig,
    install_enabled: bool = True,
    crawl_setup_enabled: bool = True,
    crawl_start_enabled: bool = True,
    snapshot_cleanup_enabled: bool = True,
    emit_discovered_snapshot_events: bool = True,
    crawl_cleanup_enabled: bool = True,
    crawl_completed_enabled: bool = True,
    crawl_event_enabled: bool = True,
    crawl_setup_phase_timeout: float = 300.0,
    snapshot_phase_timeout: float = 300.0,
    snapshot_cleanup_phase_timeout: float = 300.0,
    crawl_cleanup_phase_timeout: float = 300.0,
    auto_install: bool = True,
    emit_jsonl: bool = True,
    interactive_tty: bool | None = None,
    abort_requested: Any | None = None,
    PluginBinariesService: type[PluginBinariesService] | None = PluginBinariesService,
    PluginBinaryEnvService: type[PluginBinaryEnvService] | None = PluginBinaryEnvService,
    BinaryService: type[BinaryService] | None = BinaryService,
    ProcessService: type[ProcessService] | None = ProcessService,
    ArchiveResultService: type[ArchiveResultService] | None = ArchiveResultService,
    TagService: type[TagService] | None = TagService,
    CrawlService: type[CrawlService] | None = CrawlService,
    SnapshotService: type[SnapshotService] | None = SnapshotService,
) -> None:
    """Attach the shared abx-dl services to an existing bus.

    ExecutionPlan is the public entrypoint for attaching this configured view
    to an application-owned event bus.
    """
    if interactive_tty is None:
        interactive_tty = sys.stdout.isatty() or sys.stderr.isatty()

    if PluginBinaryEnvService is not None:
        PluginBinaryEnvService(bus, catalog=catalog)

    if BinaryService is not None:
        BinaryService(
            bus,
            auto_install=auto_install,
        )

    if install_enabled and PluginBinariesService is not None:
        install_plugins = get_install_plugins(catalog)
        PluginBinariesService(
            bus,
            catalog=catalog,
            auto_install=auto_install,
            install_plugins=install_plugins,
            output_dir=output_dir,
            snapshot=snapshot,
            abort_requested=abort_requested,
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
            catalog=catalog,
            crawl_setup_enabled=crawl_setup_enabled,
            crawl_start_enabled=crawl_start_enabled,
            crawl_cleanup_enabled=crawl_cleanup_enabled,
            crawl_completed_enabled=crawl_completed_enabled,
            crawl_event_enabled=crawl_event_enabled,
            crawl_setup_phase_timeout=crawl_setup_phase_timeout,
            snapshot_phase_timeout=snapshot_phase_timeout,
            snapshot_cleanup_phase_timeout=snapshot_cleanup_phase_timeout,
            crawl_cleanup_phase_timeout=crawl_cleanup_phase_timeout,
            abort_requested=abort_requested,
        )
        if SnapshotService is not None and (crawl_start_enabled or snapshot_cleanup_enabled):
            if runtime_config is None:
                raise TypeError(
                    "runtime_config is required when ExecutionPlan attaches SnapshotService",
                )
            SnapshotService(
                bus,
                url=url,
                snapshot=snapshot,
                output_dir=output_dir,
                catalog=catalog,
                config=runtime_config,
                snapshot_phase_timeout=snapshot_phase_timeout,
                snapshot_cleanup_enabled=snapshot_cleanup_enabled,
                snapshot_cleanup_phase_timeout=snapshot_cleanup_phase_timeout,
                abort_requested=abort_requested,
                emit_discovered_snapshot_events=emit_discovered_snapshot_events,
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
    plan: ExecutionPlan,
    *,
    output_dir: Path | None = None,
    emit_jsonl: bool = False,
    bus: EventBus | None = None,
    PluginBinariesService: type[PluginBinariesService] | None = PluginBinariesService,
    PluginBinaryEnvService: type[PluginBinaryEnvService] | None = PluginBinaryEnvService,
    BinaryService: type[BinaryService] | None = BinaryService,
    ProcessService: type[ProcessService] | None = ProcessService,
):
    """Run only the dependency preflight on an existing bus or a temporary one.

    This emits InstallEvent, which resolves enabled plugins'
    ``config.json > required_binaries`` through abxpkg, without starting the
    later ``on_CrawlSetup__*`` or ``on_Snapshot__*`` plugin phases.
    """
    if not plan.catalog:
        return []

    install_output_dir = output_dir
    temp_dir_ctx = nullcontext(output_dir) if output_dir is not None else TemporaryDirectory(prefix="abx-dl-install-")

    with temp_dir_ctx as temp_dir:
        install_output_dir = install_output_dir or Path(temp_dir)
        install_output_dir.mkdir(parents=True, exist_ok=True)
        bus = bus or create_bus(total_timeout=plan.install_timeout)
        snapshot = Snapshot(url="")
        plan.attach_services(
            bus,
            url="",
            snapshot=snapshot,
            output_dir=install_output_dir,
            install_enabled=True,
            crawl_setup_enabled=False,
            crawl_start_enabled=False,
            snapshot_cleanup_enabled=False,
            crawl_cleanup_enabled=False,
            auto_install=True,
            emit_jsonl=emit_jsonl,
            interactive_tty=sys.stdout.isatty() or sys.stderr.isatty(),
            PluginBinariesService=PluginBinariesService,
            PluginBinaryEnvService=PluginBinaryEnvService,
            BinaryService=BinaryService,
            ProcessService=ProcessService,
            ArchiveResultService=None,
            TagService=None,
        )
        await plan.seed_config(bus)
        try:
            install_event = bus.emit(
                InstallEvent(
                    url="",
                    snapshot_id=snapshot.id,
                    output_dir=str(install_output_dir),
                    event_timeout=plan.install_timeout,
                    event_handler_slow_timeout=slow_warning_timeout(plan.install_timeout),
                ),
            )
            await install_event.now(timeout=plan.install_timeout)
            await install_event.wait(timeout=plan.install_timeout)
            await install_event.event_results_list()
        finally:
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


@dataclass(frozen=True)
class ExecutionPlan:
    """Framework-neutral description of one configured plugin execution.

    Embedders own persistence and scheduling.  The plan owns the shared plugin
    selection, config seed, and timeout calculations required to attach abx-dl
    services to their EventBus.
    """

    catalog: PluginCatalog
    config: dict[str, Any]
    derived_config: dict[str, Any]
    runtime: str
    install_timeout: float
    crawl_setup_timeout: float
    snapshot_timeout: float
    crawl_cleanup_timeout: float

    @classmethod
    def build(
        cls,
        catalog: PluginCatalog,
        *,
        selected_plugins: Iterable[str] | None = None,
        disabled_plugins: Iterable[str] = (),
        config: dict[str, Any] | None = None,
        derived_config: dict[str, Any] | None = None,
        runtime: str = "abx-dl",
    ) -> ExecutionPlan:
        selected = catalog.select(selected_plugins, disabled_names=disabled_plugins)
        runtime_config = dict(config or {})
        # The embedding application owns runtime identity. Do not let an env
        # value make hooks believe a standalone run is ArchiveBox (or vice
        # versa) after plugin discovery already used this explicit runtime.
        runtime_config["ABX_RUNTIME"] = runtime
        crawl_setup_hooks = [(plugin, hook) for plugin in selected.values() for hook in plugin.filter_hooks("CrawlSetup")]
        snapshot_hooks = [(plugin, hook) for plugin in selected.values() for hook in plugin.filter_hooks("Snapshot")]
        return cls(
            catalog=selected,
            config=runtime_config,
            derived_config=dict(derived_config or {}),
            runtime=runtime,
            install_timeout=compute_install_phase_timeout(get_install_plugins(selected), runtime_config),
            crawl_setup_timeout=compute_phase_timeout(crawl_setup_hooks, runtime_config),
            snapshot_timeout=compute_phase_timeout(snapshot_hooks, runtime_config),
            crawl_cleanup_timeout=compute_phase_timeout(crawl_setup_hooks, runtime_config),
        )

    @property
    def runtime_config(self) -> RuntimeConfig:
        return RuntimeConfig(
            user=GlobalConfig(**self.config),
            derived=dict(self.derived_config),
        )

    async def seed_config(self, bus: EventBus, *, parent_event: Any | None = None) -> None:
        """Publish the plan's config layers through the normal runtime contract."""
        user_event = MachineEvent(config=dict(self.config), config_type="user")
        if parent_event is not None:
            user_event.event_parent_id = parent_event.event_id
        await bus.emit(user_event).now()
        if self.derived_config:
            derived_event = MachineEvent(config=dict(self.derived_config), config_type="derived")
            if parent_event is not None:
                derived_event.event_parent_id = parent_event.event_id
            await bus.emit(derived_event).now()

    def attach_services(
        self,
        bus: EventBus,
        *,
        url: str | None = None,
        snapshot: Snapshot | None = None,
        output_dir: Path | None = None,
        **service_options: Any,
    ) -> None:
        """Attach shared services using this plan's single selected/configured view."""
        _attach_services(
            bus,
            catalog=self.catalog,
            url=url,
            snapshot=snapshot,
            output_dir=output_dir,
            runtime_config=self.runtime_config,
            crawl_setup_phase_timeout=self.crawl_setup_timeout,
            snapshot_phase_timeout=self.snapshot_timeout,
            snapshot_cleanup_phase_timeout=self.snapshot_timeout,
            crawl_cleanup_phase_timeout=self.crawl_cleanup_timeout,
            **service_options,
        )

    def attach_snapshot_service(
        self,
        bus: EventBus,
        *,
        url: str,
        snapshot: Snapshot,
        output_dir: Path,
        snapshot_service: type[SnapshotService] = SnapshotService,
        timeout_padding: float = 0.0,
        abort_requested: Any | None = None,
        selected_hooks_by_plugin: dict[str, set[str] | None] | None = None,
        emit_discovered_snapshot_events: bool = True,
    ) -> SnapshotService:
        """Attach one snapshot executor to an existing application-owned bus."""
        timeout = self.snapshot_timeout + timeout_padding
        return snapshot_service(
            bus,
            url=url,
            snapshot=snapshot,
            output_dir=output_dir,
            catalog=self.catalog,
            config=self.runtime_config,
            snapshot_phase_timeout=timeout,
            snapshot_cleanup_enabled=True,
            snapshot_cleanup_phase_timeout=timeout,
            abort_requested=abort_requested,
            selected_hooks_by_plugin=selected_hooks_by_plugin,
            emit_discovered_snapshot_events=emit_discovered_snapshot_events,
        )


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
    plan: ExecutionPlan,
    output_dir: Path,
    auto_install: bool = True,
    *,
    bus: EventBus | None = None,
    emit_jsonl: bool | None = None,
    interactive_tty: bool | None = None,
    crawl_setup_enabled: bool = True,
    crawl_start_enabled: bool = True,
    snapshot_cleanup_enabled: bool = True,
    crawl_cleanup_enabled: bool = True,
    crawl_completed_enabled: bool = True,
    crawl_event_enabled: bool = True,
    PluginBinariesService: type[PluginBinariesService] | None = PluginBinariesService,
    PluginBinaryEnvService: type[PluginBinaryEnvService] | None = PluginBinaryEnvService,
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
    3. Emits InstallEvent for dependency preflight, then CrawlEvent for the
       CrawlSetup → CrawlStart → Snapshot →
       SnapshotCleanup → CrawlCleanup sequence (unless phase flags request a subset)
    4. Leaves all result collection to bus subscribers attached during setup

    Args:
        url: The URL to download/archive.
        plan: The selected, configured plugin execution plan.
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

    # Create snapshot record and write it as the first line of index.jsonl
    snapshot_payload: dict[str, Any] = {"url": url}
    if plan.config.get("EXTRA_CONTEXT"):
        extra_context = plan.config["EXTRA_CONTEXT"]
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

    install_phase_timeout = plan.install_timeout
    crawl_setup_phase_timeout = plan.crawl_setup_timeout
    snapshot_phase_timeout = plan.snapshot_timeout
    snapshot_cleanup_phase_timeout = snapshot_phase_timeout
    crawl_cleanup_phase_timeout = plan.crawl_cleanup_timeout
    total_timeout = (
        install_phase_timeout
        + (crawl_setup_phase_timeout if crawl_setup_enabled else 0.0)
        + (snapshot_phase_timeout if crawl_start_enabled else 0.0)
        + (snapshot_cleanup_phase_timeout if snapshot_cleanup_enabled else 0.0)
        + (crawl_cleanup_phase_timeout if crawl_cleanup_enabled else 0.0)
    )

    owns_bus = bus is None
    if bus is None:
        bus = create_bus(total_timeout=total_timeout)
    assert bus is not None

    plan.attach_services(
        bus,
        url=url,
        snapshot=snapshot,
        output_dir=output_dir,
        install_enabled=True,
        crawl_setup_enabled=crawl_setup_enabled,
        crawl_start_enabled=crawl_start_enabled,
        snapshot_cleanup_enabled=snapshot_cleanup_enabled,
        crawl_cleanup_enabled=crawl_cleanup_enabled,
        crawl_completed_enabled=crawl_completed_enabled,
        crawl_event_enabled=crawl_event_enabled,
        auto_install=auto_install,
        emit_jsonl=emit_jsonl,
        interactive_tty=interactive_tty,
        PluginBinariesService=PluginBinariesService,
        PluginBinaryEnvService=PluginBinaryEnvService,
        BinaryService=BinaryService,
        ProcessService=ProcessService,
        ArchiveResultService=ArchiveResultService,
        TagService=TagService,
        CrawlService=CrawlService,
        SnapshotService=SnapshotService,
    )
    await plan.seed_config(bus)

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
            emitted_crawl_event = bus.emit(crawl_event)
            await emitted_crawl_event.now()
            await emitted_crawl_event.event_results_list()
    finally:
        if owns_bus:
            await bus.wait_until_idle()
