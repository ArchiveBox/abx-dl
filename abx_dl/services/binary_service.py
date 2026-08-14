"""Required binary request orchestration and abx-dl cache projection."""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from inspect import isawaitable
from pathlib import Path
from typing import Any, ClassVar

from abxbus import BaseEvent, EventBus
from abxpkg import BinProvider, Binary as AbxBinary
from abxpkg.binary_service import BinaryEvent, BinaryRequestEvent

from ..config import RuntimeConfig, get_config, get_plugin_env, get_required_binary_requests, is_path_like_env_value
from ..events import CrawlAbortEvent, InstallEvent, MachineEvent
from ..models import Plugin, Snapshot, filter_plugins, uuid7
from .base import BaseService

_TEMPLATE_NAME_RE = re.compile(r"^\{([A-Z0-9_]+)\}$")


async def build_plugin_process_env(
    bus: EventBus,
    *,
    plugins: dict[str, Plugin],
    plugin: Plugin,
    runtime_env: dict[str, str],
) -> dict[str, str]:
    """Project resolved binary environments exactly as hook processes receive them."""
    env = runtime_env
    env_plugin_names = set(filter_plugins(plugins, [plugin.name], include_providers=True))
    binary_events = await bus.filter(
        BinaryEvent,
        past=True,
        where=lambda candidate: str(candidate.extra_context.get("plugin_name") or "") in env_plugin_names,
    )
    for binary_event in reversed(binary_events):
        if binary_event.env:
            env = BinProvider.build_exec_env(
                base_env=env,
                extra_env=binary_event.env,
            )
    return BinProvider.build_exec_env(base_env=env, extra_env=runtime_env)


def _config_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "off", "none", "null"}
    return bool(value)


def _plugin_enabled_from_user_config(plugin: Plugin, user_config: RuntimeConfig) -> bool:
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
        allowed_binproviders: Sequence[str] | None = None,
    ):
        self.auto_install = auto_install
        self.plugins = plugins
        self.install_plugins = install_plugins or []
        self.output_dir = output_dir
        self.snapshot = snapshot
        self.abort_requested = False
        self.abort_requested_callback = abort_requested
        self.allowed_binproviders = (
            {provider.strip().lower() for provider in allowed_binproviders if provider.strip()}
            if allowed_binproviders is not None
            else None
        )
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
            plugin_base_env = (
                await get_plugin_env(
                    self.bus,
                    plugin=plugin,
                    run_output_dir=self.output_dir,
                    config=current_config,
                    hydrate_binaries=False,
                )
            ).to_env()
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
                if self.allowed_binproviders is not None:
                    configured_value = record.get("binproviders", "")
                    configured = (
                        [provider.strip() for provider in configured_value.split(",") if provider.strip()]
                        if isinstance(configured_value, str)
                        else [str(provider).strip() for provider in configured_value if str(provider).strip()]
                    )
                    configured = [provider for provider in configured if provider.lower() in self.allowed_binproviders]
                    if not configured:
                        raise ValueError(
                            f"Binary {record.get('name')!r} has no configured providers allowed by {sorted(self.allowed_binproviders)!r}",
                        )
                    record["binproviders"] = ",".join(configured) if isinstance(configured_value, str) else configured
                signature = json.dumps(record, sort_keys=True, default=str)
                if signature in seen:
                    continue
                seen.add(signature)
                request_payload = {
                    key: value for key, value in record.items() if key in BinaryRequestEvent.model_fields and key != "extra_context"
                }
                if current_user_config.ABXPKG_NO_CACHE:
                    request_payload["no_cache"] = True
                request_event = BinaryRequestEvent(
                    **request_payload,
                    auto_install=self.auto_install,
                    lib_dir=current_user_config.ABXPKG_LIB_DIR,
                    base_env=plugin_base_env,
                    dry_run=current_user_config.DRY_RUN,
                    extra_context={
                        "plugin_name": plugin.name,
                        "hook_name": "",
                        "output_dir": str(plugin_output_dir),
                        "binary_id": uuid7(),
                        "machine_id": "",
                    },
                )
                request_events.append(request_event)
            if await self.should_abort():
                break

        for request_event in request_events:
            emitted_request: BaseEvent = event.emit(request_event)
            completed_request = await emitted_request.now()
            await completed_request.event_results_list(raise_if_none=False)


class AbxDlEnvConfigFileBinaryCacheBackend:
    """Project abxpkg Binary events onto abx-dl derived config."""

    def __init__(self, bus: EventBus, *, plugins: dict[str, Plugin]):
        self.bus = bus
        self.plugins = plugins

    def get(self, request: BinaryRequestEvent) -> AbxBinary | None:
        return None

    async def set(self, request: BinaryRequestEvent | None, binary: AbxBinary) -> None:
        current_config = await get_config(self.bus)
        if binary.loaded_abspath and request is not None:
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
                hydrate_binaries=False,
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
            except KeyError:
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
