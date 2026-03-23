"""BinaryService — resolves BinaryRequest records into resolved Binary events."""

import json
import os
import shutil
from pathlib import Path
from typing import Any, ClassVar, cast

from abx_pkg import HandlerDict, SemVer
from abxbus import BaseEvent, EventBus

from ..config import unset_config
from ..events import BinaryRequestEvent, BinaryEvent, BinaryRequestProcessEvent, MachineEvent, ProcessStdoutEvent, slow_warning_timeout
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


def _coerce_handler_dict(value: object) -> HandlerDict:
    handlers: HandlerDict = {}
    if not isinstance(value, dict):
        return handlers

    raw_handlers = cast(dict[str, Any], value)
    if "abspath" in raw_handlers:
        handlers["abspath"] = raw_handlers["abspath"]
    if "version" in raw_handlers:
        handlers["version"] = raw_handlers["version"]
    if "install_args" in raw_handlers:
        handlers["install_args"] = raw_handlers["install_args"]
    if "packages" in raw_handlers:
        handlers["packages"] = raw_handlers["packages"]
    if "install" in raw_handlers:
        handlers["install"] = raw_handlers["install"]
    if "update" in raw_handlers:
        handlers["update"] = raw_handlers["update"]
    if "uninstall" in raw_handlers:
        handlers["uninstall"] = raw_handlers["uninstall"]
    return handlers


class BinaryService(BaseService):
    """Resolves binary dependencies requested by install/setup/snapshot hooks.

    When a hook needs a binary, it outputs JSONL like::

        {"type": "BinaryRequest", "name": "chromium", "binproviders": "puppeteer",
         "overrides": {"puppeteer": ["chromium@latest", "--install-deps"]}}

    ProcessService routes this via ProcessStdoutEvent.
    on_ProcessStdoutEvent picks up type=BinaryRequest records and emits
    BinaryRequestEvent, which triggers the provider hook chain.

    Handler registration order (all on BinaryRequestEvent)::

        on_BinaryRequest__10_npm_...   — provider hook handler
        on_BinaryRequest__11_pip_...   — provider hook handler
        on_BinaryRequest__12_brew_...  — provider hook handler
        on_BinaryRequest__13_apt_...   — provider hook handler
        ...
        on_BinaryRequestEvent   — runs last: emits BinaryEvent

    Provider hooks skip early if the binary is already resolved in shared config
    or a prior provider already emitted BinaryEvent. on_BinaryRequestEvent runs
    last and emits the final resolved BinaryEvent with concrete metadata.

    abxbus detail: all handlers run serially in registration order, so the
    early-exit check works correctly.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessStdoutEvent, BinaryRequestEvent, BinaryEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        BinaryRequestEvent,
        MachineEvent,
        BinaryRequestProcessEvent,
        BinaryEvent,
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
            [(plugin, hook) for plugin in plugins.values() for hook in plugin.get_binary_request_hooks()],
            key=lambda x: x[1].sort_key,
        )
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register handlers in correct order.

        1. ProcessStdoutEvent → BinaryRequestEvent / BinaryEvent routing
        2. Provider hooks on BinaryRequestEvent (try providers in hook order)
        3. on_BinaryRequestEvent last (finalizes the winning BinaryEvent)
        """
        self._register(ProcessStdoutEvent, self.on_ProcessStdoutEvent)

        for plugin, hook in self.binary_hooks:
            handler = self._make_provider_hook_handler(plugin, hook)
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self._register(BinaryRequestEvent, handler)

        self._register(BinaryRequestEvent, self.on_BinaryRequestEvent)
        self._register(BinaryEvent, self.on_BinaryEvent)

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
        keys = [_binary_env_key(binary_name)]
        for plugin_key in self._plugin_binary_config_keys(plugin_name, binary_name):
            keys.insert(0, plugin_key)
        return list(dict.fromkeys(keys))

    def _iter_registered_binary_values(self, plugin_name: str, binary_name: str) -> list[str]:
        values: list[str] = []
        for config_key in self._binary_config_keys(plugin_name, binary_name):
            value = str(self.machine.shared_config.get(config_key) or "").strip()
            if value:
                values.append(value)

        candidate_dirs: list[Path] = []
        lib_bin_dir_value = str(self.machine.shared_config.get("LIB_BIN_DIR") or "").strip()
        if lib_bin_dir_value:
            candidate_dirs.append(Path(lib_bin_dir_value).expanduser())
        lib_dir_value = str(self.machine.shared_config.get("LIB_DIR") or "").strip()
        if lib_dir_value:
            candidate_dirs.append(Path(lib_dir_value).expanduser() / "bin")

        for candidate_dir in candidate_dirs:
            candidate = candidate_dir / binary_name
            if candidate.exists():
                values.append(str(candidate))

        return list(dict.fromkeys(values))

    def _get_registered_binary_path(self, plugin_name: str, binary_name: str) -> str:
        values = self._iter_registered_binary_values(plugin_name, binary_name)
        return values[0] if values else ""

    def _resolve_registered_binary(
        self,
        *,
        plugin_name: str,
        binary_name: str,
        registered_value: str,
        binproviders: str,
        overrides: dict[str, Any] | None,
        min_version: str | None,
    ) -> tuple[str, str, str, str]:
        """Resolve a config-backed binary name/path into concrete abspath/version/provider metadata."""
        registered_path = Path(registered_value).expanduser()
        if registered_path.is_absolute() and registered_path.exists() and os.access(registered_path, os.X_OK):
            if registered_path.parent.name == ".bin" and registered_path.parent.parent.name == "node_modules":
                resolved_version = ""
                resolved_target = registered_path.resolve()
                for candidate in (resolved_target, *resolved_target.parents):
                    package_json = candidate / "package.json"
                    if not package_json.exists():
                        continue
                    try:
                        package = json.loads(package_json.read_text())
                    except (OSError, json.JSONDecodeError):
                        break
                    resolved_version = str(package.get("version") or "").strip()
                    break
                return str(registered_path), resolved_version, "", "npm"
            if (
                registered_path.parent.name == "bin"
                and registered_path.parent.parent.name == "venv"
                and registered_path.parent.parent.parent.name == "pip"
            ):
                try:
                    from abx_pkg import Binary as AbxBinary, PipProvider

                    pip_provider = PipProvider(pip_venv=registered_path.parent.parent)
                    pip_overrides = _coerce_handler_dict((overrides or {}).get("pip"))
                    pip_overrides["abspath"] = str(registered_path)
                    loaded = AbxBinary(
                        name=binary_name,
                        binproviders=[pip_provider],
                        overrides={"pip": pip_overrides},
                        min_version=SemVer(min_version) if min_version else None,
                    ).load()
                    if loaded is not None and getattr(loaded, "abspath", None):
                        return (
                            str(getattr(loaded, "abspath", "") or registered_path),
                            str(getattr(loaded, "version", "") or ""),
                            str(getattr(loaded, "sha256", "") or ""),
                            "pip",
                        )
                except Exception:
                    pass
            try:
                from abx_pkg.semver import bin_version

                detected_version = bin_version(registered_path)
            except Exception:
                detected_version = None
            return str(registered_path), str(detected_version or ""), "", "env"
        try:
            from ..dependencies import load_binary

            merged_overrides = dict(overrides or {})
            effective_binproviders = binproviders or "env"
            if registered_path.is_absolute() or "/" in registered_value:
                env_overrides = dict(merged_overrides.get("env", {}))
                env_overrides["abspath"] = registered_value
                merged_overrides["env"] = env_overrides
                provider_order = ["env", *_ordered_binproviders(effective_binproviders)]
                effective_binproviders = ",".join(dict.fromkeys(provider_order))

            binary = load_binary(
                {
                    "name": binary_name,
                    "binproviders": effective_binproviders,
                    "overrides": merged_overrides,
                    "min_version": min_version,
                },
            )
            resolved_abspath = str(getattr(binary, "abspath", None) or "")
            resolved_version = str(getattr(binary, "version", None) or "")
            resolved_sha256 = str(getattr(binary, "sha256", None) or "")
            resolved_provider = str(getattr(getattr(binary, "loaded_binprovider", None), "name", "") or "")
            if resolved_abspath and not resolved_version:
                try:
                    from abx_pkg.semver import bin_version

                    detected_version = bin_version(Path(resolved_abspath).expanduser())
                except Exception:
                    detected_version = None
                if detected_version:
                    resolved_version = str(detected_version)
            if resolved_abspath:
                return resolved_abspath, resolved_version, resolved_sha256, resolved_provider
        except Exception:
            pass
        if registered_path.exists() and os.access(registered_path, os.X_OK):
            return str(registered_path), "", "", "env"
        return "", "", "", ""

    async def _emit_cached_binary_installed(self, event: BinaryRequestEvent, *, inherited_binproviders: str) -> bool:
        for registered_value in self._iter_registered_binary_values(event.plugin_name, event.name):
            resolved_abspath, resolved_version, resolved_sha256, resolved_provider = self._resolve_registered_binary(
                plugin_name=event.plugin_name,
                binary_name=event.name,
                registered_value=registered_value,
                binproviders=inherited_binproviders,
                overrides=event.overrides,
                min_version=event.min_version,
            )
            if not resolved_abspath:
                continue
            await self._register_binary_path(event.plugin_name, event.name, resolved_abspath)
            await self.bus.emit(
                BinaryEvent(
                    name=event.name,
                    plugin_name=event.plugin_name,
                    hook_name=event.hook_name,
                    abspath=resolved_abspath,
                    version=resolved_version,
                    sha256=resolved_sha256,
                    binproviders=inherited_binproviders or event.binproviders,
                    binprovider=resolved_provider or "env",
                    overrides=event.overrides,
                    binary_id=event.binary_id,
                    machine_id=event.machine_id,
                ),
            )
            return True
        return False

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

        The handler reads args from the BinaryRequestEvent at call time and checks
        early-exit (binary already resolved by a prior provider) before running.
        """

        async def handler(event: BinaryRequestEvent, _plugin=plugin, _hook=hook) -> None:
            if not event.name:
                return
            inherited_binproviders = event.binproviders
            if not inherited_binproviders and event.binary_id:
                ancestor_request = await self.bus.find(
                    BinaryRequestEvent,
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
            existing_installed = await self.bus.find(
                BinaryEvent,
                child_of=event,
                past=True,
                future=False,
                name=event.name,
            )
            if existing_installed is not None:
                return
            if await self._emit_cached_binary_installed(event, inherited_binproviders=inherited_binproviders):
                return
            ordered_providers = _ordered_binproviders(inherited_binproviders)
            if ordered_providers:
                if _plugin.name not in ordered_providers or _plugin.name != ordered_providers[0]:
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
                BinaryRequestProcessEvent(
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
        """Route hook stdout ``BinaryRequest`` / ``Binary`` JSONL to typed events."""
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict):
            return
        record_type = record.pop("type", "")

        from ..models import uuid7

        if record_type == "BinaryRequest":
            record.setdefault("binary_id", uuid7())
            await self.bus.emit(
                BinaryRequestEvent(
                    plugin_name=record.pop("plugin_name", event.plugin_name),
                    hook_name=record.pop("hook_name", event.hook_name),
                    output_dir=record.pop("output_dir", event.output_dir),
                    **record,
                ),
            )
            return

        if record_type == "Binary":
            inherited_binproviders = str(record.get("binproviders", "")).strip()
            binary_id = str(record.get("binary_id", "")).strip()
            if not inherited_binproviders and binary_id:
                ancestor_request = await self.bus.find(
                    BinaryRequestEvent,
                    past=True,
                    future=False,
                    where=lambda candidate: candidate.binary_id == binary_id and bool(candidate.binproviders),
                )
                if ancestor_request is not None:
                    inherited_binproviders = ancestor_request.binproviders
            record.setdefault("binary_id", uuid7())
            await self.bus.emit(
                BinaryEvent(
                    plugin_name=record.pop("plugin_name", event.plugin_name),
                    hook_name=record.pop("hook_name", event.hook_name),
                    binprovider=record.pop("binprovider", event.plugin_name),
                    binproviders=record.pop("binproviders", inherited_binproviders),
                    **record,
                ),
            )

    async def on_BinaryRequestEvent(self, event: BinaryRequestEvent) -> None:
        """Finalize a binary request after provider hooks had a chance to satisfy it."""
        if not event.name:
            return

        inherited_binproviders = event.binproviders
        if not inherited_binproviders and event.binary_id:
            ancestor_request = await self.bus.find(
                BinaryRequestEvent,
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

        existing_installed = await self.bus.find(
            BinaryEvent,
            child_of=event,
            past=True,
            future=False,
            name=event.name,
        )
        if existing_installed is not None:
            return
        if await self._emit_cached_binary_installed(event, inherited_binproviders=inherited_binproviders):
            return
        if ordered_providers and len(ordered_providers) > 1:
            await self.bus.emit(
                BinaryRequestEvent(
                    name=event.name,
                    plugin_name=event.plugin_name,
                    hook_name=event.hook_name,
                    output_dir=event.output_dir,
                    min_version=event.min_version,
                    binary_id=event.binary_id,
                    machine_id=event.machine_id,
                    binproviders=",".join(ordered_providers[1:]),
                    overrides=event.overrides,
                    custom_cmd=event.custom_cmd,
                ),
            )

    async def on_BinaryEvent(self, event: BinaryEvent) -> None:
        if not event.abspath:
            return
        await self._register_binary_path(event.plugin_name, event.name, event.abspath)
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
