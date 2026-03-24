"""BinaryService — resolves BinaryRequest records into resolved Binary events."""

import ast
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar, cast

from abx_pkg import HandlerDict, SemVer
from abxbus import BaseEvent, EventBus

from ..events import BinaryRequestEvent, BinaryEvent, BinaryRequestProcessEvent, MachineEvent, ProcessStdoutEvent, slow_warning_timeout
from ..models import Hook, Plugin
from .base import BaseService, add_extra_context
from .machine_service import MachineService


def _ordered_binproviders(binproviders: str) -> list[str]:
    providers = str(binproviders or "").strip()
    if not providers or providers == "*":
        return []
    return [provider.strip() for provider in providers.split(",") if provider.strip()]


def _is_absolute_binary_path(value: str) -> bool:
    return Path(str(value or "")).expanduser().is_absolute()


def _is_path_like_binary_value(value: str) -> bool:
    value = str(value or "").strip()
    if not value:
        return False
    return value.startswith(("~", ".", "/")) or "/" in value or "\\" in value


def _is_app_bundle_binary(path: Path) -> bool:
    parts = path.expanduser().parts
    try:
        app_index = next(index for index, part in enumerate(parts) if part.endswith(".app"))
    except StopIteration:
        return False
    return len(parts) > app_index + 2 and parts[app_index + 1 : app_index + 3] == ("Contents", "MacOS")


_TEMPLATE_NAME_RE = re.compile(r"^\{([A-Z0-9_]+)\}$")


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


def _sanitize_binary_overrides(overrides: dict[str, Any] | None) -> dict[str, HandlerDict]:
    if not isinstance(overrides, dict):
        return {}
    sanitized: dict[str, HandlerDict] = {}
    for provider_name, provider_overrides in overrides.items():
        handlers = _coerce_handler_dict(provider_overrides)
        if handlers:
            sanitized[str(provider_name)] = handlers
    return sanitized


class BinaryService(BaseService):
    """Resolves binary dependencies requested by install/setup/snapshot hooks.

    When a hook needs a binary, it outputs JSONL like::

        {"type": "BinaryRequest", "name": "chromium", "binproviders": "puppeteer",
         "overrides": {"puppeteer": ["chromium@latest", "--install-deps"]}}

    ProcessService routes this via ProcessStdoutEvent.
    on_ProcessStdoutEvent picks up type=BinaryRequest records and emits
    a single BinaryRequestEvent. BinaryService then tries the configured
    providers in order within that one event and emits one final BinaryEvent
    when the request resolves or exhausts all providers.
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
        self.binary_provider_hooks: dict[str, tuple[Plugin, Hook]] = {}
        for plugin, hook in self.binary_hooks:
            self.binary_provider_hooks.setdefault(plugin.name, (plugin, hook))
        super().__init__(bus)

    def _attach_handlers(self) -> None:
        """Register handlers in correct order.

        1. ProcessStdoutEvent → BinaryRequestEvent / BinaryEvent routing
        2. on_BinaryRequestEvent tries provider hooks in request order
        3. on_BinaryEvent persists successful binary paths
        """
        self._register(ProcessStdoutEvent, self.on_ProcessStdoutEvent)
        self._register(BinaryRequestEvent, self.on_BinaryRequestEvent)
        self._register(BinaryEvent, self.on_BinaryEvent)

    def _request_run_output_dir(self, output_dir: str, plugin_name: str) -> Path:
        path = Path(output_dir).expanduser() if output_dir else Path.cwd()
        return path.parent if plugin_name and path.name == plugin_name else path

    def _plugin_binary_config_keys(self, event: BinaryRequestEvent) -> list[str]:
        """Return config keys whose required_binaries template hydrates to this request name."""
        plugin = self.plugins.get(event.plugin_name)
        if plugin is None:
            return []

        plugin_env = self.machine.get_declared_env_for_plugin(
            plugin,
            run_output_dir=self._request_run_output_dir(event.output_dir, event.plugin_name),
        )
        matching_keys: list[str] = []
        for spec in plugin.binaries:
            template_name = str(spec.get("name") or "").strip()
            match = _TEMPLATE_NAME_RE.fullmatch(template_name)
            if match is None:
                continue
            key = match.group(1)
            try:
                hydrated_name = template_name.format(**plugin_env)
            except Exception:
                continue
            if hydrated_name == event.name:
                matching_keys.append(key)
        return matching_keys

    def _binary_config_keys(self, event: BinaryRequestEvent) -> list[str]:
        return list(dict.fromkeys(self._plugin_binary_config_keys(event)))

    def _invalidate_derived_binary(self, *config_keys: str) -> None:
        if config_keys:
            self.machine.unset_derived(*config_keys)

    def _iter_cached_binary_candidates(self, event: BinaryRequestEvent) -> list[tuple[tuple[str, ...], str, bool]]:
        request_name = str(event.name or "").strip()
        if not request_name:
            return []

        if _is_path_like_binary_value(request_name):
            return [(tuple(self._binary_config_keys(event)), request_name, True)]

        values: list[tuple[tuple[str, ...], str, bool]] = []
        for config_key in self._binary_config_keys(event):
            derived_value = str(self.machine.derived_config.get(config_key) or "").strip()
            if not derived_value:
                continue
            if not _is_path_like_binary_value(derived_value):
                self._invalidate_derived_binary(config_key)
                continue
            derived_path = Path(derived_value).expanduser()
            if derived_path.name != request_name:
                self._invalidate_derived_binary(config_key)
                continue
            values.append(((config_key,), derived_value, False))

        deduped: list[tuple[tuple[str, ...], str, bool]] = []
        seen_values: set[str] = set()
        for keys, value, authoritative in values:
            if value in seen_values:
                continue
            seen_values.add(value)
            deduped.append((keys, value, authoritative))
        return deduped

    def _resolve_registered_binary(
        self,
        *,
        registered_value: str,
        binproviders: str,
        overrides: dict[str, Any] | None,
        min_version: str | None,
    ) -> tuple[str, str, str, str]:
        """Resolve a cached binary path/name into concrete abspath/version/provider metadata."""
        registered_path = Path(registered_value).expanduser()
        lookup_name = registered_path.name if _is_path_like_binary_value(registered_value) else registered_value
        if registered_path.is_absolute() and registered_path.exists():
            target_path = registered_path.resolve()
            if registered_path.is_file() and not os.access(registered_path, os.X_OK):
                return str(registered_path), "", "", "env"
            if "node_modules" in target_path.parts:
                resolved_version = ""
                for candidate in (target_path, *target_path.parents):
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
                target_path.parent.name == "bin"
                and target_path.parent.parent.name == "venv"
                and target_path.parent.parent.parent.name == "pip"
            ):
                try:
                    from abx_pkg import Binary as AbxBinary, PipProvider

                    pip_provider = PipProvider(pip_venv=target_path.parent.parent)
                    pip_overrides = _coerce_handler_dict((overrides or {}).get("pip"))
                    pip_overrides["abspath"] = str(target_path)
                    loaded = AbxBinary(
                        name=lookup_name,
                        binproviders=[pip_provider],
                        overrides={"pip": pip_overrides},
                        min_version=SemVer(min_version) if min_version else None,
                    ).load()
                    if loaded is not None and loaded.abspath:
                        return (
                            str(loaded.abspath or registered_path),
                            str(loaded.version or ""),
                            str(loaded.sha256 or ""),
                            "pip",
                        )
                except Exception:
                    pass
        try:
            from ..dependencies import load_binary

            merged_overrides = _sanitize_binary_overrides(overrides)
            effective_binproviders = binproviders or "env"
            if registered_path.is_absolute() or "/" in registered_value:
                env_overrides = cast(HandlerDict, dict(merged_overrides.get("env", {})))
                env_overrides["abspath"] = registered_value
                merged_overrides["env"] = env_overrides
                provider_order = ["env", *_ordered_binproviders(effective_binproviders)]
                effective_binproviders = ",".join(dict.fromkeys(provider_order))

            binary = load_binary(
                {
                    "name": lookup_name,
                    "binproviders": effective_binproviders,
                    "overrides": merged_overrides,
                    "min_version": min_version,
                },
            )
            resolved_abspath = str(binary.abspath or "")
            resolved_version = str(binary.version or "")
            resolved_sha256 = str(binary.sha256 or "")
            resolved_provider = str(binary.loaded_binprovider.name if binary.loaded_binprovider is not None else "")
            if resolved_abspath and Path(resolved_abspath).expanduser().exists():
                return resolved_abspath, resolved_version, resolved_sha256, resolved_provider
        except Exception:
            pass
        return "", "", "", ""

    async def _emit_cached_binary_installed(self, event: BinaryRequestEvent, *, inherited_binproviders: str) -> tuple[bool, str]:
        for config_keys, registered_value, authoritative_override in self._iter_cached_binary_candidates(event):
            resolved_abspath, resolved_version, resolved_sha256, resolved_provider = self._resolve_registered_binary(
                registered_value=registered_value,
                binproviders=inherited_binproviders,
                overrides=event.overrides,
                min_version=event.min_version,
            )
            if not resolved_abspath:
                if config_keys:
                    self._invalidate_derived_binary(*config_keys)
                if authoritative_override:
                    return True, f"{event.name} not available at {registered_value}"
                continue
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
            return True, ""
        return False, ""

    async def _register_binary_path(self, request_event: BinaryRequestEvent, abspath: str) -> None:
        """Persist derived binary paths for future runs."""
        for config_key in self._binary_config_keys(request_event):
            await self.bus.emit(
                MachineEvent(
                    method="update",
                    key=f"config/{config_key}",
                    value=abspath,
                ),
            )

    async def _run_provider_hook(
        self,
        event: BinaryRequestEvent,
        *,
        provider_name: str,
        inherited_binproviders: str,
    ) -> None:
        provider = self.binary_provider_hooks.get(provider_name)
        if provider is None or not event.name:
            return
        plugin, hook = provider

        from ..models import uuid7

        binary_id = event.binary_id or uuid7()
        hook_args = [f"--name={event.name}"]
        if inherited_binproviders:
            hook_args.append(f"--binproviders={inherited_binproviders}")
        if event.min_version:
            hook_args.append(f"--min-version={event.min_version}")
        if event.overrides is not None:
            hook_args.append(f"--overrides={json.dumps(event.overrides)}")
        if event.custom_cmd:
            hook_args.append(f"--custom-cmd={event.custom_cmd}")

        run_output_dir = Path(event.output_dir).parent if event.output_dir else Path.cwd()
        plugin_output_dir = run_output_dir / plugin.name
        plugin_output_dir.mkdir(parents=True, exist_ok=True)
        plugin_env = self.machine.get_env_for_plugin(
            plugin,
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
                plugin_name=plugin.name,
                hook_name=hook.name,
                hook_path=str(hook.path),
                hook_args=hook_args,
                is_background=False,
                output_dir=str(plugin_output_dir),
                env=plugin_env,
                timeout=300,
                event_handler_timeout=330.0,
                event_handler_slow_timeout=slow_warning_timeout(300),
            ),
        )

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Route hook stdout BinaryRequest/Binary JSONL records."""
        stripped = event.line.strip()
        if not stripped:
            return

        try:
            payload = json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            try:
                payload = ast.literal_eval(stripped)
            except (SyntaxError, ValueError):
                return

        if not isinstance(payload, dict):
            return
        from ..models import uuid7

        record = dict(payload)
        record_type = record.pop("type", "")

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
        ordered_providers = _ordered_binproviders(inherited_binproviders or event.binproviders or "env")

        existing_installed = await self.bus.find(
            BinaryEvent,
            child_of=event,
            past=True,
            future=False,
            name=event.name,
        )
        if existing_installed is not None:
            return
        cached_terminal, cached_error = await self._emit_cached_binary_installed(
            event,
            inherited_binproviders=inherited_binproviders,
        )
        if cached_terminal:
            if cached_error:
                raise FileNotFoundError(cached_error)
            return

        for provider_name in ordered_providers:
            await self._run_provider_hook(
                event,
                provider_name=provider_name,
                inherited_binproviders=inherited_binproviders or event.binproviders,
            )
            existing_installed = await self.bus.find(
                BinaryEvent,
                child_of=event,
                past=True,
                future=False,
                name=event.name,
                where=lambda candidate: bool(candidate.abspath),
            )
            if existing_installed is not None:
                return
        return

    async def on_BinaryEvent(self, event: BinaryEvent) -> None:
        if not event.abspath:
            return
        install_cache = self.machine.derived_config.get("ABX_INSTALL_CACHE")
        if not isinstance(install_cache, dict):
            install_cache = {}
        if event.name:
            install_cache = dict(install_cache)
            install_cache[event.name] = datetime.now(timezone.utc).isoformat()
            self.machine.update_derived(ABX_INSTALL_CACHE=install_cache)
        persisted_abspath = self._link_installed_binary(event.name, event.abspath)
        request_event = await self.bus.find(
            BinaryRequestEvent,
            past=True,
            future=False,
            where=lambda candidate: (
                (event.binary_id and candidate.binary_id == event.binary_id)
                or (candidate.name == event.name and candidate.plugin_name == event.plugin_name)
            ),
        )
        if request_event is not None:
            await self._register_binary_path(request_event, persisted_abspath)
            return

    def _link_installed_binary(self, binary_name: str, binary_abspath: str) -> str:
        if _is_path_like_binary_value(binary_name):
            return binary_abspath
        lib_bin_dir_value = str(self.machine.shared_config.get("LIB_BIN_DIR") or "").strip()
        if not lib_bin_dir_value:
            return binary_abspath
        lib_bin_dir = Path(lib_bin_dir_value).expanduser()
        lib_bin_dir.mkdir(parents=True, exist_ok=True)

        target = Path(binary_abspath).expanduser()
        link_path = lib_bin_dir / binary_name

        if target == link_path:
            return str(link_path)

        # macOS app-bundle binaries rely on relative ../Frameworks paths and
        # break when executed through a detached symlink in LIB_BIN_DIR.
        if _is_app_bundle_binary(target):
            if link_path.is_symlink() or link_path.is_file():
                link_path.unlink()
            elif link_path.exists():
                shutil.rmtree(link_path)
            return str(target)

        if link_path.is_symlink() or link_path.is_file():
            link_path.unlink()
        elif link_path.exists():
            shutil.rmtree(link_path)

        link_path.symlink_to(target)
        return str(link_path)
