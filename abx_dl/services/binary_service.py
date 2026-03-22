"""BinaryService — resolves binary dependencies by broadcasting to provider hooks."""

import json
import shutil
from pathlib import Path
from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..config import unset_config
from ..events import BinaryEvent, BinaryInstalledEvent, BinaryProcessEvent, MachineEvent, ProcessStdoutEvent, slow_warning_timeout
from ..models import Hook, Plugin
from .base import BaseService, add_extra_context
from .machine_service import MachineService


def _binary_env_key(name: str) -> str:
    """Convert a binary name to its env var key, e.g. 'yt-dlp' → 'YT_DLP_BINARY'."""
    normalized = "".join(ch if ch.isalnum() else "_" for ch in name).upper()
    return f"{normalized}_BINARY"


def _ordered_binproviders(binproviders: str) -> list[str]:
    providers = str(binproviders or "").strip()
    if not providers or providers == "*":
        return []
    return [provider.strip() for provider in providers.split(",") if provider.strip()]


class BinaryService(BaseService):
    """Resolves binary dependencies emitted by hooks during installation.

    When a hook needs a binary, it outputs JSONL like::

        {"type": "Binary", "name": "chromium", "binproviders": "puppeteer",
         "overrides": {"puppeteer": ["chromium@latest", "--install-deps"]}}

    ProcessService routes this via ProcessStdoutEvent.
    on_ProcessStdoutEvent picks up type=Binary records and emits
    BinaryEvent, which triggers the provider hook chain.

    Handler registration order (all on BinaryEvent)::

        on_Binary__10_npm_...   — provider hook handler
        on_Binary__11_pip_...   — provider hook handler
        on_Binary__12_brew_...  — provider hook handler
        on_Binary__13_apt_...   — provider hook handler
        ...
        on_BinaryEvent          — runs last: emits BinaryInstalledEvent

    Provider hooks skip early if the binary is already resolved (abspath set on
    the event, or env key already in shared_config from a prior provider).
    on_BinaryEvent runs last and emits BinaryInstalledEvent with the resolved
    path (whether discovered by env or installed by another provider).

    abxbus detail: all handlers run serially in registration order, so the
    early-exit check works correctly.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessStdoutEvent, BinaryEvent, BinaryInstalledEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        BinaryEvent,
        MachineEvent,
        BinaryProcessEvent,
        BinaryInstalledEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        machine: MachineService,
        plugins: dict[str, Plugin],
        auto_install: bool,
    ):
        self.machine = machine
        self.auto_install = auto_install
        self.plugins = plugins
        # Pre-collect all binary hooks at init time (sorted by order across all plugins)
        self.binary_hooks: list[tuple[Plugin, Hook]] = sorted(
            [(plugin, hook) for plugin in plugins.values() for hook in plugin.get_binary_hooks()],
            key=lambda x: x[1].sort_key,
        )
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register handlers in correct order.

        1. ProcessStdoutEvent → BinaryEvent routing
        2. Provider hooks on BinaryEvent (try to resolve/install)
        3. on_BinaryEvent last (emits BinaryInstalledEvent)
        """
        self._register(ProcessStdoutEvent, self.on_ProcessStdoutEvent)

        for plugin, hook in self.binary_hooks:
            handler = self._make_provider_hook_handler(plugin, hook)
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self._register(BinaryEvent, handler)

        # on_BinaryEvent runs last — branches on outcome
        self._register(BinaryEvent, self.on_BinaryEvent)
        self._register(BinaryInstalledEvent, self.on_BinaryInstalledEvent)

    def _plugin_binary_config_keys(self, plugin_name: str, binary_name: str) -> list[str]:
        """Return plugin config keys whose default binary name matches the requested binary."""
        if plugin_name == "mercury" and binary_name == "postlight-parser":
            return ["MERCURY_BINARY"]

        plugin = self.plugins.get(plugin_name)
        if plugin is None:
            return []

        matching_keys: list[str] = []
        for key, prop in plugin.config_schema.items():
            if not key.endswith("_BINARY"):
                continue
            if prop.get("default") == binary_name:
                matching_keys.append(key)
        return matching_keys

    def _binary_config_keys(self, plugin_name: str, binary_name: str) -> list[str]:
        plugin_keys = self._plugin_binary_config_keys(plugin_name, binary_name)
        if plugin_keys:
            return plugin_keys
        return [_binary_env_key(binary_name)]

    def _get_registered_binary_path(self, plugin_name: str, binary_name: str) -> str:
        for config_key in self._binary_config_keys(plugin_name, binary_name):
            value = str(self.machine.shared_config.get(config_key) or "").strip()
            if value:
                return value
        return ""

    async def _register_binary_path(self, plugin_name: str, binary_name: str, abspath: str) -> None:
        """Persist the generic and plugin-specific binary env vars for a resolved binary."""
        for config_key in self._binary_config_keys(plugin_name, binary_name):
            await self.bus.emit(
                MachineEvent(
                    method="update",
                    key=f"config/{config_key}",
                    value=abspath,
                ),
            )
        if plugin_name == "mercury" and binary_name == "postlight-parser":
            self.machine.shared_config.pop("POSTLIGHT_PARSER_BINARY", None)
            unset_config("POSTLIGHT_PARSER_BINARY")

    def _make_provider_hook_handler(self, plugin: Plugin, hook: Hook):
        """Create an async handler that runs one binary provider hook.

        The handler reads args from the BinaryEvent at call time and checks
        early-exit (binary already resolved by a prior provider) before running.
        """

        async def handler(event: BinaryEvent, _plugin=plugin, _hook=hook) -> None:
            if not event.name or event.abspath:
                # Already resolved or no name — skip
                return
            ordered_providers = _ordered_binproviders(event.binproviders)
            if ordered_providers:
                if _plugin.name not in ordered_providers or _plugin.name != ordered_providers[0]:
                    return
            elif self._get_registered_binary_path(event.plugin_name, event.name):
                # For unordered provider sets, a prior provider or cached config
                # already resolved the binary, so don't re-run installers.
                return

            from ..models import uuid7

            binary_id = event.binary_id or uuid7()
            hook_args = [
                f"--name={event.name}",
            ]
            if event.binproviders:
                hook_args.append(f"--binproviders={event.binproviders}")
            if event.min_version:
                hook_args.append(f"--min-version={event.min_version}")
            if event.overrides is not None:
                hook_args.append(f"--overrides={json.dumps(event.overrides)}")
            if event.custom_cmd:
                hook_args.append(f"--custom-cmd={event.custom_cmd}")

            run_output_dir = Path(event.output_dir).parent if event.output_dir else Path.cwd()
            plugin_output_dir = run_output_dir / _plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)
            plugin_env = self.machine.get_env_for_plugin(
                _plugin,
                run_output_dir=run_output_dir,
            )
            plugin_env = add_extra_context(
                plugin_env,
                {
                    "binary_id": binary_id,
                    "hook_name": event.hook_name,
                    "machine_id": plugin_env.get("MACHINE_ID", ""),
                    "plugin_name": event.plugin_name,
                },
            )
            await self.bus.emit(
                BinaryProcessEvent(
                    plugin_name=_plugin.name,
                    hook_name=_hook.name,
                    hook_path=str(_hook.path),
                    hook_args=hook_args,
                    is_background=False,
                    output_dir=str(plugin_output_dir),
                    env=plugin_env,
                    timeout=300,
                    event_handler_timeout=330.0,
                    event_handler_slow_timeout=slow_warning_timeout(300),
                ),
            )

        return handler

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Route type=Binary records to BinaryEvent."""
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict) or record.pop("type", "") != "Binary":
            return

        from ..models import uuid7

        record.setdefault("binary_id", uuid7())

        await self.bus.emit(
            BinaryEvent(
                plugin_name=record.pop("plugin_name", event.plugin_name),
                hook_name=record.pop("hook_name", event.hook_name),
                output_dir=record.pop("output_dir", event.output_dir),
                **record,
            ),
        )

    async def on_BinaryEvent(self, event: BinaryEvent) -> None:
        """Handle binary resolution — runs after all provider hooks.

        If the binary has an abspath (either from the requesting hook or from
        a provider's nested BinaryEvent), registers it in config and emits
        BinaryInstalledEvent. If no abspath but a provider set the config key,
        emits BinaryInstalledEvent with the resolved path.
        """
        if not event.name:
            return

        inherited_binproviders = event.binproviders
        if not inherited_binproviders and event.binary_id:
            ancestor_request = await self.bus.find(
                BinaryEvent,
                past=True,
                future=False,
                where=lambda candidate: (
                    candidate.event_id != event.event_id
                    and candidate.binary_id == event.binary_id
                    and candidate.name == event.name
                    and bool(candidate.binproviders)
                ),
            )
            if ancestor_request is not None:
                inherited_binproviders = ancestor_request.binproviders
        ordered_providers = _ordered_binproviders(inherited_binproviders)

        binprovider = getattr(event, "binprovider", "") or ""
        if event.abspath:
            # Binary path provided — register in config and notify
            await self._register_binary_path(event.plugin_name, event.name, event.abspath)
            await self.bus.emit(
                BinaryInstalledEvent(
                    name=event.name,
                    plugin_name=event.plugin_name,
                    hook_name=event.hook_name,
                    abspath=event.abspath,
                    version=event.version,
                    sha256=event.sha256,
                    binproviders=inherited_binproviders,
                    binprovider=binprovider,
                    overrides=event.overrides,
                    binary_id=event.binary_id,
                    machine_id=event.machine_id,
                ),
            )
        else:
            existing_installed = await self.bus.find(
                BinaryInstalledEvent,
                child_of=event,
                past=True,
                future=False,
                name=event.name,
            )
            if existing_installed is not None:
                return
            if ordered_providers and len(ordered_providers) > 1:
                await self.bus.emit(
                    BinaryEvent(
                        name=event.name,
                        plugin_name=event.plugin_name,
                        hook_name=event.hook_name,
                        output_dir=event.output_dir,
                        version=event.version,
                        sha256=event.sha256,
                        min_version=event.min_version,
                        binary_id=event.binary_id,
                        machine_id=event.machine_id,
                        binproviders=",".join(ordered_providers[1:]),
                        binprovider=binprovider,
                        overrides=event.overrides,
                        custom_cmd=event.custom_cmd,
                    ),
                )
                return
            # No abspath — check if a provider resolved it via shared_config
            abspath = self._get_registered_binary_path(event.plugin_name, event.name)
            if abspath:
                await self._register_binary_path(event.plugin_name, event.name, abspath)
                resolved_event = await self.bus.find(
                    BinaryEvent,
                    child_of=event,
                    past=True,
                    future=False,
                    name=event.name,
                    abspath=abspath,
                )
                await self.bus.emit(
                    BinaryInstalledEvent(
                        name=event.name,
                        plugin_name=event.plugin_name,
                        hook_name=event.hook_name,
                        abspath=abspath,
                        version=(resolved_event.version if resolved_event else "") or event.version,
                        sha256=(resolved_event.sha256 if resolved_event else "") or event.sha256,
                        binproviders=(resolved_event.binproviders if resolved_event else "") or inherited_binproviders,
                        binprovider=(resolved_event.binprovider if resolved_event else "") or binprovider,
                        overrides=(resolved_event.overrides if resolved_event else None) or event.overrides,
                        binary_id=event.binary_id,
                        machine_id=event.machine_id,
                    ),
                )

    async def on_BinaryInstalledEvent(self, event: BinaryInstalledEvent) -> None:
        if not event.abspath:
            return
        self._link_installed_binary(event.name, event.abspath)

    def _link_installed_binary(self, binary_name: str, binary_abspath: str) -> None:
        lib_bin_dir_value = str(self.machine.shared_config.get("LIB_BIN_DIR") or "").strip()
        if not lib_bin_dir_value:
            return
        lib_bin_dir = Path(lib_bin_dir_value).expanduser()
        lib_bin_dir.mkdir(parents=True, exist_ok=True)

        target = Path(binary_abspath).expanduser()
        link_path = lib_bin_dir / binary_name

        if link_path.is_symlink() or link_path.is_file():
            link_path.unlink()
        elif link_path.exists():
            shutil.rmtree(link_path)

        link_path.symlink_to(target)
