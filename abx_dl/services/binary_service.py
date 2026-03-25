"""BinaryService — resolves BinaryRequest records into resolved Binary events."""

import json
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ClassVar, Literal

from abxbus import BaseEvent, EventBus
from pydantic import BaseModel, ConfigDict

from ..config import get_derived_env, get_plugin_env, get_required_binary_requests, get_user_env, is_path_like_env_value
from ..events import BinaryEvent, BinaryRequestEvent, InstallEvent, MachineEvent, ProcessEvent, ProcessStdoutEvent, slow_warning_timeout
from ..models import Hook, Plugin, Snapshot, uuid7
from .base import BaseService


def _ordered_binproviders(binproviders: str) -> list[str]:
    providers = str(binproviders or "").strip()
    if not providers or providers == "*":
        return []
    return [provider.strip() for provider in providers.split(",") if provider.strip()]


def _is_app_bundle_binary(path: Path) -> bool:
    parts = path.expanduser().parts
    try:
        app_index = next(index for index, part in enumerate(parts) if part.endswith(".app"))
    except StopIteration:
        return False
    return len(parts) > app_index + 2 and parts[app_index + 1 : app_index + 3] == ("Contents", "MacOS")


_TEMPLATE_NAME_RE = re.compile(r"^\{([A-Z0-9_]+)\}$")


class EmittedBinaryRecord(BaseModel):
    """Typed shape for provider-emitted Binary records."""

    model_config = ConfigDict(extra="ignore")

    type: Literal["Binary"]
    name: str
    abspath: str
    version: str = ""
    sha256: str = ""
    binproviders: str = ""
    binprovider: str = ""
    overrides: dict[str, Any] | None = None


class BinaryService(BaseService):
    """Resolves binary dependencies requested during install preflight.

    BinaryRequestEvent comes from ``config.json > required_binaries`` during the
    install phase. Provider hooks only emit resolved ``Binary`` JSONL records.
    ProcessService routes those stdout lines via ProcessStdoutEvent, and
    BinaryService turns them into BinaryEvent objects.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [
        InstallEvent,
        ProcessStdoutEvent,
        BinaryRequestEvent,
        BinaryEvent,
    ]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        BinaryRequestEvent,
        ProcessEvent,
        MachineEvent,
        BinaryEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        plugins: dict[str, Plugin],
        auto_install: bool,
        install_plugins: list[Plugin] | None = None,
        output_dir: Path | None = None,
        snapshot: Snapshot | None = None,
    ):
        self.auto_install = auto_install
        self.plugins = plugins
        self.install_plugins = install_plugins or []
        self.output_dir = output_dir
        self.snapshot = snapshot
        # Pre-collect all binary hooks at init time (sorted by order across all plugins)
        self.binary_hooks: list[tuple[Plugin, Hook]] = sorted(
            [(plugin, hook) for plugin in plugins.values() for hook in plugin.filter_hooks("BinaryRequest")],
            key=lambda x: x[1].sort_key,
        )
        super().__init__(bus)
        self.bus.on(InstallEvent, self.on_InstallEvent)
        self.bus.on(ProcessStdoutEvent, self.on_ProcessStdoutEvent)

        # Register a handler for each on_BinaryRequest hook provided by plugins
        for plugin, hook in self.binary_hooks:

            async def on_BinaryRequest(
                event: BinaryRequestEvent,
                plugin=plugin,
                hook=hook,
                provider_name=plugin.name,
            ) -> None:
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

                cached_terminal, cached_error = await self._emit_cached_binary_if_already_installed(
                    event,
                    inherited_binproviders=inherited_binproviders,
                )
                if cached_terminal:
                    if cached_error:
                        raise FileNotFoundError(cached_error)
                    return

                if not self.auto_install:
                    return

                ordered_providers = _ordered_binproviders(inherited_binproviders or event.binproviders or "env")
                if provider_name not in ordered_providers:
                    return

                binary_id = event.binary_id or uuid7()
                hook_args = [f"--name={event.name}"]
                if inherited_binproviders:
                    hook_args.append(f"--binproviders={inherited_binproviders}")
                if event.min_version:
                    hook_args.append(f"--min-version={event.min_version}")
                if event.overrides:
                    hook_args.append(f"--overrides={json.dumps(event.overrides)}")

                run_output_dir = Path(event.output_dir).parent
                plugin_output_dir = run_output_dir / plugin.name
                plugin_output_dir.mkdir(parents=True, exist_ok=True)
                plugin_config = get_plugin_env(
                    self.bus,
                    plugin=plugin,
                    run_output_dir=run_output_dir,
                    extra_context={
                        "binary_id": binary_id,
                        "binproviders": inherited_binproviders or event.binproviders,
                        "hook_name": event.hook_name,
                        "machine_id": event.machine_id,
                        "plugin_name": event.plugin_name,
                    },
                )
                plugin_env = plugin_config.to_env()
                await self.bus.emit(
                    ProcessEvent(
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

            handler_name = f"on_BinaryRequestEvent__{plugin.name}__{hook.name.replace('.', '_')}"
            on_BinaryRequest.__name__ = handler_name
            on_BinaryRequest.__qualname__ = handler_name
            self.bus.on(BinaryRequestEvent, on_BinaryRequest)
        self.bus.on(BinaryEvent, self.on_BinaryEvent)

    async def on_InstallEvent(self, event: InstallEvent) -> None:
        """Emit BinaryRequestEvents for this run's enabled plugins."""
        if self.snapshot is None or self.output_dir is None:
            return
        if event.output_dir != str(self.output_dir):
            return

        current_user_config = get_user_env(self.bus)
        current_derived_config = get_derived_env(self.bus)
        install_cache: dict[str, str] = {}
        if "ABX_INSTALL_CACHE" in current_derived_config:
            install_cache_value = current_derived_config["ABX_INSTALL_CACHE"]
            if not isinstance(install_cache_value, dict):
                raise TypeError("ABX_INSTALL_CACHE must be a dict[str, str].")
            install_cache = {str(binary_name): str(cached_at) for binary_name, cached_at in install_cache_value.items()}
        now = datetime.now(timezone.utc)
        pruned_install_cache: dict[str, str] = {}
        for binary_name, cached_at in install_cache.items():
            try:
                cache_time = datetime.fromisoformat(str(cached_at))
            except ValueError:
                continue
            if cache_time.tzinfo is None:
                cache_time = cache_time.replace(tzinfo=timezone.utc)
            if now - cache_time < timedelta(hours=24):
                pruned_install_cache[str(binary_name)] = cache_time.isoformat()
        if pruned_install_cache != install_cache:
            await self.bus.emit(
                MachineEvent(
                    method="update",
                    key="config/ABX_INSTALL_CACHE",
                    value=pruned_install_cache,
                    config_type="derived",
                ),
            )

        seen: set[str] = set()
        for plugin in self.install_plugins:
            if plugin.enabled_key in plugin.config.properties:
                plugin_env = get_plugin_env(
                    self.bus,
                    plugin=plugin,
                    run_output_dir=self.output_dir,
                    include_derived=False,
                )
                if not plugin_env[plugin.enabled_key]:
                    continue
            plugin_output_dir = self.output_dir / plugin.name
            for record in get_required_binary_requests(
                plugin,
                plugin.config.required_binaries,
                overrides=current_user_config.model_dump(mode="json"),
                derived_overrides=current_derived_config.model_dump(mode="json"),
                run_output_dir=self.output_dir,
            ):
                signature = json.dumps(record, sort_keys=True, default=str)
                if signature in seen:
                    continue
                seen.add(signature)
                if record["name"].strip() in pruned_install_cache:
                    continue
                await self.bus.emit(
                    BinaryRequestEvent(
                        plugin_name=plugin.name,
                        output_dir=str(plugin_output_dir),
                        binary_id=uuid7(),
                        **record,
                    ),
                )

    def _request_run_output_dir(self, output_dir: str, plugin_name: str) -> Path:
        """Normalize a BinaryRequest output dir back to the run root for config lookup."""
        path = Path(output_dir).expanduser()
        return path.parent if plugin_name and path.name == plugin_name else path

    def _config_keys_for_binary_request(self, event: BinaryRequestEvent) -> list[str]:
        """Return ``*_BINARY`` config keys that correspond to one binary request."""
        plugin = self.plugins[event.plugin_name]

        runtime_env = get_plugin_env(
            self.bus,
            plugin=plugin,
            run_output_dir=self._request_run_output_dir(event.output_dir, event.plugin_name),
            include_derived=False,
        ).to_env()
        matching_keys: list[str] = []
        for spec in plugin.config.required_binaries:
            template_name = spec.name.strip()
            match = _TEMPLATE_NAME_RE.fullmatch(template_name)
            if match is None:
                continue
            key = match.group(1)
            try:
                hydrated_name = template_name.format(**runtime_env)
            except Exception:
                continue
            if hydrated_name == event.name:
                matching_keys.append(key)
        if matching_keys:
            return list(dict.fromkeys(matching_keys))
        for key, prop in plugin.config.properties.items():
            if not key.endswith("_BINARY"):
                continue
            configured_value = str(runtime_env[key] or prop.get("default") or "").strip()
            if not configured_value:
                continue
            if configured_value == event.name:
                matching_keys.append(key)
                continue
            if is_path_like_env_value(configured_value) and Path(configured_value).expanduser().name == event.name:
                matching_keys.append(key)
        return list(dict.fromkeys(matching_keys))

    def _iter_cached_binary_candidates(
        self,
        event: BinaryRequestEvent,
    ) -> tuple[list[tuple[tuple[str, ...], str, bool]], set[str]]:
        """Return cached binary paths plus any stale derived keys that should be cleared."""
        request_name = event.name
        current_derived_config = get_derived_env(self.bus)

        if is_path_like_env_value(request_name):
            return [(tuple(self._config_keys_for_binary_request(event)), request_name, True)], set()

        values: list[tuple[tuple[str, ...], str, bool]] = []
        stale_config_keys: set[str] = set()
        for config_key in self._config_keys_for_binary_request(event):
            if config_key not in current_derived_config:
                continue
            derived_value = str(current_derived_config[config_key]).strip()
            if not derived_value:
                continue
            if not is_path_like_env_value(derived_value):
                stale_config_keys.add(config_key)
                continue
            derived_path = Path(derived_value).expanduser()
            if derived_path.name != request_name:
                stale_config_keys.add(config_key)
                continue
            values.append(((config_key,), derived_value, False))

        deduped: list[tuple[tuple[str, ...], str, bool]] = []
        seen_values: set[str] = set()
        for keys, value, authoritative in values:
            if value in seen_values:
                continue
            seen_values.add(value)
            deduped.append((keys, value, authoritative))
        return deduped, stale_config_keys

    async def _emit_cached_binary_if_already_installed(self, event: BinaryRequestEvent, *, inherited_binproviders: str) -> tuple[bool, str]:
        """Reuse a cached binary path when it still exists, or surface a broken user path."""
        candidates, stale_config_keys = self._iter_cached_binary_candidates(event)
        for config_key in stale_config_keys:
            await self.bus.emit(
                MachineEvent(
                    method="unset",
                    key=f"config/{config_key}",
                    config_type="derived",
                ),
            )
        for config_keys, registered_value, authoritative_override in candidates:
            registered_path = Path(registered_value).expanduser()
            if not registered_path.exists():
                for config_key in config_keys:
                    await self.bus.emit(
                        MachineEvent(
                            method="unset",
                            key=f"config/{config_key}",
                            config_type="derived",
                        ),
                    )
                if authoritative_override:
                    return True, f"{event.name} not available at {registered_value}"
                continue
            await self.bus.emit(
                BinaryEvent(
                    name=event.name,
                    plugin_name=event.plugin_name,
                    hook_name=event.hook_name,
                    abspath=str(registered_path),
                    version="",
                    sha256="",
                    binproviders=inherited_binproviders or event.binproviders,
                    binprovider="",
                    overrides=event.overrides,
                    binary_id=event.binary_id,
                    machine_id=event.machine_id,
                ),
            )
            return True, ""
        return False, ""

    async def _persist_binary_abspath_in_config(self, request_event: BinaryRequestEvent, abspath: str) -> None:
        """Persist derived binary paths for future runs."""
        for config_key in self._config_keys_for_binary_request(request_event):
            await self.bus.emit(
                MachineEvent(
                    method="update",
                    key=f"config/{config_key}",
                    value=abspath,
                    config_type="derived",
                ),
            )

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Route provider hook stdout Binary JSONL records."""
        stripped = event.line.strip()
        if not stripped:
            return

        try:
            payload = json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            return

        if not isinstance(payload, dict):
            return
        try:
            binary_payload = EmittedBinaryRecord(**payload)
        except Exception:
            return

        request_event = await self.bus.find(
            BinaryRequestEvent,
            past=True,
            future=False,
            where=lambda candidate: candidate.name == binary_payload.name and self.bus.event_is_child_of(event, candidate),
        )
        assert request_event is not None

        binary_event = BinaryEvent(
            name=binary_payload.name,
            plugin_name=request_event.plugin_name,
            hook_name=request_event.hook_name,
            abspath=binary_payload.abspath,
            version=binary_payload.version,
            sha256=binary_payload.sha256,
            binproviders=binary_payload.binproviders or request_event.binproviders,
            binprovider=binary_payload.binprovider,
            overrides=binary_payload.overrides if binary_payload.overrides is not None else request_event.overrides,
            binary_id=request_event.binary_id,
            machine_id=request_event.machine_id,
        )

        await self.bus.emit(binary_event)

    async def on_BinaryEvent(self, event: BinaryEvent) -> None:
        """Persist successful binary installs into the install cache and derived config."""
        current_derived_config = get_derived_env(self.bus)
        install_cache: dict[str, str] = {}
        if "ABX_INSTALL_CACHE" in current_derived_config:
            install_cache_value = current_derived_config["ABX_INSTALL_CACHE"]
            if not isinstance(install_cache_value, dict):
                raise TypeError("ABX_INSTALL_CACHE must be a dict[str, str].")
            install_cache = {str(binary_name): str(cached_at) for binary_name, cached_at in install_cache_value.items()}
        install_cache[event.name] = datetime.now(timezone.utc).isoformat()
        await self.bus.emit(
            MachineEvent(
                method="update",
                key="config/ABX_INSTALL_CACHE",
                value=install_cache,
                config_type="derived",
            ),
        )
        self._link_installed_binary(event.name, event.abspath)
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
            await self._persist_binary_abspath_in_config(request_event, event.abspath)

    def _link_installed_binary(self, binary_name: str, binary_abspath: str) -> None:
        """Link a resolved binary into ``LIB_BIN_DIR`` when that indirection is safe."""
        if is_path_like_env_value(binary_name):
            return
        current_user_config = get_user_env(self.bus)
        lib_bin_dir = current_user_config.LIB_BIN_DIR
        if lib_bin_dir is None:
            return
        lib_bin_dir.mkdir(parents=True, exist_ok=True)

        target = Path(binary_abspath).expanduser()
        link_path = lib_bin_dir / binary_name

        if target == link_path:
            return

        # macOS app-bundle binaries rely on relative ../Frameworks paths and
        # break when executed through a detached symlink in LIB_BIN_DIR.
        if _is_app_bundle_binary(target):
            if link_path.is_symlink() or link_path.is_file():
                link_path.unlink()
            elif link_path.exists():
                shutil.rmtree(link_path)
            return

        if link_path.is_symlink() or link_path.is_file():
            link_path.unlink()
        elif link_path.exists():
            shutil.rmtree(link_path)

        link_path.symlink_to(target)
