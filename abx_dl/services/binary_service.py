"""Required binary request orchestration and abx-dl cache projection."""

from __future__ import annotations

import asyncio
import copy
import json
import re
import shlex
import shutil
from collections.abc import Awaitable, Callable, Mapping
from inspect import isawaitable
from pathlib import Path
from typing import Any, ClassVar

from abxbus import BaseEvent, EventBus
from abxpkg import Binary as AbxBinary
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


def _write_binary_wrapper(wrapper_path: Path, target: Path) -> None:
    target_abspath = target.expanduser().resolve(strict=False)
    wrapper_path.write_text(f'#!/bin/sh\nexec {shlex.quote(str(target_abspath))} "$@"\n')
    wrapper_path.chmod(0o755)


def _config_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "off", "none", "null"}
    return bool(value)


def _plugin_enabled_from_user_config(plugin: Plugin, user_config: RuntimeConfig) -> bool:
    if plugin.config.x_install_when_disabled:
        return True
    enabled_key = plugin.enabled_key
    if enabled_key not in plugin.config.properties:
        return True
    user_payload = user_config.user.model_dump(mode="json")
    if enabled_key in user_payload:
        return _config_bool(user_payload[enabled_key])
    prop = plugin.config.properties.get(enabled_key) or {}
    if isinstance(prop, Mapping) and "default" in prop:
        return _config_bool(prop["default"])
    return True


_ABXPKG_OVERRIDE_KEYS = {
    "PATH",
    "INSTALLER_BIN",
    "euid",
    "install_root",
    "bin_dir",
    "dry_run",
    "postinstall_scripts",
    "min_release_age",
    "install_timeout",
    "version_timeout",
    "abspath",
    "version",
    "install_args",
    "packages",
    "install",
    "update",
    "uninstall",
    "docs_url",
    "search",
}


def split_abxpkg_binary_request_overrides(
    overrides: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split abxpkg-native overrides from plugin-owned request metadata."""
    if not isinstance(overrides, Mapping):
        return {}, {}

    native: dict[str, Any] = {}
    provider_metadata: dict[str, Any] = {}
    raw_overrides = copy.deepcopy(dict(overrides))

    for provider_name, provider_overrides in overrides.items():
        provider_key = str(provider_name)
        if isinstance(provider_overrides, list):
            native[provider_key] = {"install_args": provider_overrides}
            continue
        if not isinstance(provider_overrides, Mapping):
            continue
        native_values = {str(key): value for key, value in provider_overrides.items() if str(key) in _ABXPKG_OVERRIDE_KEYS}
        metadata_values = {str(key): value for key, value in provider_overrides.items() if str(key) not in _ABXPKG_OVERRIDE_KEYS}
        if native_values:
            native[provider_key] = native_values
        if metadata_values:
            provider_metadata[provider_key] = metadata_values

    extra_context: dict[str, Any] = {}
    if provider_metadata:
        extra_context["provider_metadata"] = provider_metadata
    if provider_metadata or native != raw_overrides:
        extra_context["raw_overrides"] = raw_overrides
    return native, extra_context


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
        seen: set[str] = set()
        request_events: list[BinaryRequestEvent] = []
        for plugin in self.install_plugins:
            if await self.should_abort():
                break
            if not _plugin_enabled_from_user_config(plugin, current_config):
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
                request_payload = {
                    key: value for key, value in record.items() if key in BinaryRequestEvent.model_fields and key != "extra_context"
                }
                native_overrides, override_extra_context = split_abxpkg_binary_request_overrides(record.get("overrides"))
                if native_overrides:
                    request_payload["overrides"] = native_overrides
                else:
                    request_payload.pop("overrides", None)
                if current_user_config.ABXPKG_NO_CACHE:
                    request_payload["no_cache"] = True
                request_event = BinaryRequestEvent(
                    **request_payload,
                    auto_install=self.auto_install,
                    lib_dir=current_user_config.ABXPKG_LIB_DIR,
                    dry_run=current_user_config.DRY_RUN,
                    extra_context={
                        "plugin_name": plugin.name,
                        "hook_name": "",
                        "output_dir": str(plugin_output_dir),
                        "binary_id": uuid7(),
                        "machine_id": "",
                        **override_extra_context,
                    },
                )
                request_events.append(request_event)
            if await self.should_abort():
                break

        requests_by_binary: dict[str, list[BinaryRequestEvent]] = {}
        for request_event in request_events:
            requests_by_binary.setdefault(request_event.name, []).append(request_event)

        async def resolve_binary_requests(binary_requests: list[BinaryRequestEvent]) -> None:
            for request_event in binary_requests:
                emitted_request: BaseEvent = event.emit(request_event)
                completed_request = await emitted_request.now()
                await completed_request.event_results_list(raise_if_none=False)

        await asyncio.gather(*(resolve_binary_requests(binary_requests) for binary_requests in requests_by_binary.values()))


class AbxDlEnvConfigFileBinaryCacheBackend:
    """Project abxpkg Binary events onto abx-dl derived config and symlinks."""

    def __init__(self, bus: EventBus, *, plugins: dict[str, Plugin]):
        self.bus = bus
        self.plugins = plugins

    def get(self, request: BinaryRequestEvent) -> AbxBinary | None:
        return None

    async def set(self, request: BinaryRequestEvent | None, binary: AbxBinary) -> None:
        current_config = await get_config(self.bus)
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
        if current_user_config.ABXPKG_LIB_DIR is None:
            return
        lib_bin_dir = current_user_config.ABXPKG_LIB_DIR / "bin"
        lib_bin_dir.mkdir(parents=True, exist_ok=True)

        target = Path(binary_abspath).expanduser().resolve(strict=False)
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
        _write_binary_wrapper(link_path, target)
