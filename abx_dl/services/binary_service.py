"""Required binary request orchestration and abx-dl cache projection."""

from __future__ import annotations

import builtins
import json
import re
import shutil
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from inspect import isawaitable
from pathlib import Path
from typing import ClassVar

from abxbus import BaseEvent, EventBus
from abxpkg import Binary as AbxBinary
from abxpkg import BinProvider, PROVIDER_CLASS_BY_NAME
from abxpkg.binary_service import BinaryRequestEvent

from ..config import RuntimeConfig, get_config, get_plugin_env, get_required_binary_requests, is_path_like_env_value
from ..events import CrawlAbortEvent, InstallEvent, MachineEvent
from ..models import Plugin, Snapshot, uuid7
from .base import BaseService


_TEMPLATE_NAME_RE = re.compile(r"^\{([A-Z0-9_]+)\}$")


def _is_app_bundle_binary(path: Path) -> bool:
    parts = path.expanduser().parts
    try:
        app_index = next(index for index, part in enumerate(parts) if part.endswith(".app"))
    except StopIteration:
        return False
    return len(parts) > app_index + 2 and parts[app_index + 1 : app_index + 3] == ("Contents", "MacOS")


def _provider_names(binproviders: str | list[str] | None) -> list[str]:
    if isinstance(binproviders, str):
        raw_names = [part.strip() for part in binproviders.split(",")]
    elif binproviders:
        raw_names = [str(part).strip() for part in binproviders]
    else:
        raw_names = ["env"]
    names: list[str] = []
    for name in raw_names:
        if name and name not in names:
            names.append(name)
    return names or ["env"]


def _providers_for_names(names: list[str]) -> list[BinProvider]:
    providers: list[BinProvider] = []
    for name in names:
        provider_class = PROVIDER_CLASS_BY_NAME.get(name)
        if provider_class is not None:
            providers.append(provider_class())
    return providers


class PluginBinariesService(BaseService):
    """Emit abxpkg BinaryRequestEvents for enabled plugins' required binaries."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [InstallEvent, CrawlAbortEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [BinaryRequestEvent, MachineEvent]

    def __init__(
        self,
        bus: EventBus,
        *,
        plugins: dict[str, Plugin],
        auto_install: bool,
        install_plugins: list[Plugin] | None = None,
        output_dir: Path | None = None,
        snapshot: Snapshot | None = None,
        abort_requested: Callable[[], bool | Awaitable[bool]] | None = None,
    ):
        self.auto_install = auto_install
        self.plugins = plugins
        self.install_plugins = install_plugins or []
        self.output_dir = output_dir
        self.snapshot = snapshot
        self.abort_requested = False
        self.abort_requested_callback = abort_requested
        super().__init__(bus)
        self.bus.on(InstallEvent, self.on_InstallEvent)
        self.bus.on(CrawlAbortEvent, self.on_CrawlAbortEvent)

    async def should_abort(self) -> bool:
        if self.abort_requested:
            return True
        if self.abort_requested_callback is None:
            return False
        callback_result = self.abort_requested_callback()
        if isawaitable(callback_result):
            callback_result = await callback_result
        if bool(callback_result):
            self.abort_requested = True
            return True
        return False

    async def on_CrawlAbortEvent(self, event: CrawlAbortEvent) -> None:
        self.abort_requested = True

    async def on_InstallEvent(self, event: InstallEvent) -> None:
        """Emit BinaryRequestEvents for this run's enabled plugins."""
        if self.snapshot is None or self.output_dir is None:
            return
        if event.output_dir != str(self.output_dir):
            return
        if await self.should_abort():
            return

        current_config = await get_config(self.bus)
        current_user_config = current_config.user
        current_derived_config = current_config.derived
        install_cache = _install_cache_from_config(current_config)
        pruned_install_cache = _prune_install_cache(install_cache)
        install_cache_changed = pruned_install_cache != install_cache

        seen: set[str] = set()
        for plugin in self.install_plugins:
            if await self.should_abort():
                break
            if plugin.enabled_key in plugin.config.properties:
                plugin_env = await get_plugin_env(
                    self.bus,
                    plugin=plugin,
                    run_output_dir=self.output_dir,
                    include_derived=False,
                    config=current_config,
                )
                if not plugin_env[plugin.enabled_key]:
                    continue
            plugin_output_dir = self.output_dir / plugin.name
            for record in get_required_binary_requests(
                plugin,
                plugin.config.required_binaries,
                overrides=current_user_config.model_dump(mode="json"),
                derived_overrides=current_derived_config,
                run_output_dir=self.output_dir,
            ):
                if await self.should_abort():
                    break
                signature = json.dumps(record, sort_keys=True, default=str)
                if signature in seen:
                    continue
                seen.add(signature)
                install_cache_key = f"binary_request/{signature}"
                request_payload = {
                    key: value for key, value in record.items() if key in BinaryRequestEvent.model_fields and key != "extra_context"
                }
                request_event = BinaryRequestEvent(
                    **request_payload,
                    auto_install=self.auto_install,
                    lib_dir=current_user_config.LIB_DIR,
                    dry_run=current_user_config.DRY_RUN,
                    extra_context={
                        "plugin_name": plugin.name,
                        "hook_name": "",
                        "output_dir": str(plugin_output_dir),
                        "binary_id": uuid7(),
                        "machine_id": "",
                        "install_cache_key": install_cache_key,
                        "install_cache_hit": install_cache_key in pruned_install_cache,
                    },
                )
                emitted_request = event.emit(request_event)
                await (await emitted_request.now()).event_results_list(raise_if_none=False)
                if await self.should_abort():
                    break
            if await self.should_abort():
                break
        if install_cache_changed:
            await event.emit(
                MachineEvent(
                    method="update",
                    key="config/ABX_INSTALL_CACHE",
                    value=pruned_install_cache,
                    config_type="derived",
                ),
            ).now()


class AbxDlBinaryCacheBackend:
    """Project abxpkg Binary events onto abx-dl derived config and symlinks."""

    def __init__(self, bus: EventBus, *, plugins: dict[str, Plugin]):
        self.bus = bus
        self.plugins = plugins

    async def get(self, request: BinaryRequestEvent) -> AbxBinary | None:
        current_config = await get_config(self.bus)
        candidates, stale_config_keys = await self._iter_cached_binary_candidates(request, config=current_config)
        for config_key in stale_config_keys:
            await request.emit(
                MachineEvent(
                    method="unset",
                    key=f"config/{config_key}",
                    config_type="derived",
                ),
            ).now()
        for _config_keys, registered_value, _authoritative in candidates:
            registered_path = Path(registered_value).expanduser()
            if not registered_path.exists():
                continue
            return AbxBinary.model_validate(
                {
                    "name": request.name,
                    "description": request.description,
                    "binproviders": _providers_for_names(_provider_names(request.binproviders)),
                    "overrides": request.overrides or {},
                    "loaded_abspath": str(registered_path),
                    "loaded_version": None,
                    "loaded_sha256": None,
                    "loaded_binprovider": None,
                    "env": {},
                },
            )
        return None

    async def set(self, request: BinaryRequestEvent | None, binary: AbxBinary) -> None:
        current_config = await get_config(self.bus)
        install_cache = _install_cache_from_config(current_config)
        request_context = request.extra_context if request is not None else {}
        install_cache_key = str(request_context.get("install_cache_key") or binary.name)
        install_cache[install_cache_key] = datetime.now(timezone.utc).isoformat()
        if request is not None:
            await request.emit(
                MachineEvent(
                    method="update",
                    key="config/ABX_INSTALL_CACHE",
                    value=install_cache,
                    config_type="derived",
                ),
            ).now()
        if binary.loaded_abspath:
            await self._link_installed_binary(binary.name, str(binary.loaded_abspath), config=current_config)
            if request is not None:
                await self._persist_binary_abspath_in_config(request, str(binary.loaded_abspath), config=current_config)

    async def invalidate(self, request: BinaryRequestEvent, binary: AbxBinary, reason: str) -> None:
        current_config = await get_config(self.bus)
        for config_key in await self._config_keys_for_binary_request(request, config=current_config):
            await request.emit(
                MachineEvent(
                    method="unset",
                    key=f"config/{config_key}",
                    config_type="derived",
                ),
            ).now()

    def _request_run_output_dir(self, output_dir: str, plugin_name: str) -> Path:
        path = Path(output_dir).expanduser()
        return path.parent if plugin_name and path.name == plugin_name else path

    async def _config_keys_for_binary_request(
        self,
        request: BinaryRequestEvent,
        *,
        config: RuntimeConfig | None = None,
    ) -> list[str]:
        plugin_name = str(request.extra_context.get("plugin_name") or "")
        output_dir = str(request.extra_context.get("output_dir") or "")
        plugin = self.plugins.get(plugin_name)
        if plugin is None:
            return []

        runtime_env = (
            await get_plugin_env(
                self.bus,
                plugin=plugin,
                run_output_dir=self._request_run_output_dir(output_dir, plugin_name),
                include_derived=False,
                config=config,
            )
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
            if hydrated_name == request.name:
                matching_keys.append(key)
        if matching_keys:
            return list(dict.fromkeys(matching_keys))
        for key, prop in plugin.config.properties.items():
            if not key.endswith("_BINARY"):
                continue
            configured_value = str(runtime_env[key] or prop.get("default") or "").strip()
            if not configured_value:
                continue
            if configured_value == request.name:
                matching_keys.append(key)
                continue
            if is_path_like_env_value(configured_value) and Path(configured_value).expanduser().name == request.name:
                matching_keys.append(key)
        return list(dict.fromkeys(matching_keys))

    async def _iter_cached_binary_candidates(
        self,
        request: BinaryRequestEvent,
        *,
        config: RuntimeConfig | None = None,
    ) -> tuple[list[tuple[tuple[str, ...], str, bool]], builtins.set[str]]:
        request_name = request.name
        current_config = config or await get_config(self.bus)
        current_derived_config = current_config.derived

        if is_path_like_env_value(request_name):
            return [(tuple(await self._config_keys_for_binary_request(request, config=current_config)), request_name, True)], set()

        values: list[tuple[tuple[str, ...], str, bool]] = []
        stale_config_keys: set[str] = set()
        for config_key in await self._config_keys_for_binary_request(request, config=current_config):
            if config_key not in current_derived_config:
                continue
            derived_value = str(current_derived_config[config_key]).strip()
            if not derived_value:
                continue
            if not is_path_like_env_value(derived_value):
                stale_config_keys.add(config_key)
                continue
            derived_path = Path(derived_value).expanduser()
            if not derived_path.exists():
                stale_config_keys.add(config_key)
                continue
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

    async def _persist_binary_abspath_in_config(
        self,
        request: BinaryRequestEvent,
        abspath: str,
        *,
        config: RuntimeConfig | None = None,
    ) -> None:
        current_config = config or await get_config(self.bus)
        for config_key in await self._config_keys_for_binary_request(request, config=current_config):
            await request.emit(
                MachineEvent(
                    method="update",
                    key=f"config/{config_key}",
                    value=abspath,
                    config_type="derived",
                ),
            ).now()

    async def _link_installed_binary(
        self,
        binary_name: str,
        binary_abspath: str,
        *,
        config: RuntimeConfig | None = None,
    ) -> None:
        if is_path_like_env_value(binary_name):
            return
        current_user_config = (config or await get_config(self.bus)).user
        lib_bin_dir = current_user_config.LIB_BIN_DIR
        if lib_bin_dir is None:
            return
        lib_bin_dir.mkdir(parents=True, exist_ok=True)

        target = Path(binary_abspath).expanduser()
        link_path = lib_bin_dir / binary_name
        if target == link_path:
            return
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


def _install_cache_from_config(config: RuntimeConfig) -> dict[str, str]:
    install_cache: dict[str, str] = {}
    current_derived_config = config.derived
    if "ABX_INSTALL_CACHE" in current_derived_config:
        install_cache_value = current_derived_config["ABX_INSTALL_CACHE"]
        if not isinstance(install_cache_value, dict):
            raise TypeError("ABX_INSTALL_CACHE must be a dict[str, str].")
        install_cache = {str(binary_name): str(cached_at) for binary_name, cached_at in install_cache_value.items()}
    return install_cache


def _prune_install_cache(install_cache: dict[str, str]) -> dict[str, str]:
    now = datetime.now(timezone.utc)
    pruned_install_cache: dict[str, str] = {}
    for binary_name, cached_at in install_cache.items():
        if not binary_name.startswith("binary_request/"):
            continue
        try:
            cache_time = datetime.fromisoformat(str(cached_at))
        except ValueError:
            continue
        if cache_time.tzinfo is None:
            cache_time = cache_time.replace(tzinfo=timezone.utc)
        if now - cache_time < timedelta(hours=24):
            pruned_install_cache[str(binary_name)] = cache_time.isoformat()
    return pruned_install_cache
