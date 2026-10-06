"""Generic plugin inventory and configuration interfaces for embedders.

The catalog knows only about plugin directories, declarative manifests, and
hook files.  It deliberately has no persistence or application-framework
integration; applications such as ArchiveBox can keep those concerns in their
own adapters while sharing the exact inventory used by the downloader CLI.
"""

from __future__ import annotations

import json
import mimetypes
import os
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from abx_plugins import get_plugins_dir
from abx_plugins.plugins.base import utils as plugin_utils

from .models import Hook, Plugin, PluginCommand, PluginConfig, parse_hook_filename


def _plugin_dirs(plugins_dir: Path | None = None) -> list[Path]:
    if plugins_dir is not None:
        return [plugins_dir]

    dirs = [Path(get_plugins_dir())]
    override = os.environ.get("ABX_PLUGINS_DIR")
    if override:
        for raw_path in override.split(os.pathsep):
            path = Path(raw_path).expanduser()
            if path and path not in dirs:
                dirs.append(path)
    return dirs


def _plugin_runtime_enabled(config: PluginConfig, runtime: str | None = None) -> bool:
    allowed_runtimes = {str(item).strip().lower() for item in config.x_runtimes if str(item).strip()}
    if not allowed_runtimes:
        return True
    current_runtime = str(runtime or os.environ.get("ABX_RUNTIME") or "abx-dl").strip().lower()
    return current_runtime in allowed_runtimes


def _load_plugin(plugin_dir: Path, *, runtime: str | None = None) -> Plugin | None:
    if not plugin_dir.is_dir() or plugin_dir.name.startswith((".", "_")):
        return None

    plugin = Plugin(name=plugin_dir.name, path=plugin_dir)
    config_file = plugin_dir / "config.json"
    if config_file.exists():
        plugin.manifest = json.loads(config_file.read_text())
        plugin.config = PluginConfig.model_validate(plugin.manifest)
        if not _plugin_runtime_enabled(plugin.config, runtime=runtime):
            return None

    for hook_file in plugin_dir.glob("on_*"):
        if not hook_file.is_file() or not os.access(hook_file, os.X_OK):
            continue
        parsed = parse_hook_filename(hook_file.name)
        if parsed is None:
            continue
        event, order, is_background = parsed
        plugin.hooks.append(
            Hook(
                name=hook_file.stem,
                event=event,
                plugin_name=plugin.name,
                path=hook_file,
                order=order,
                is_background=is_background,
            ),
        )
    return plugin


@dataclass(frozen=True)
class PluginCatalog(Mapping[str, Plugin]):
    """One immutable view of discovered plugins and their manifests."""

    plugins: dict[str, Plugin]

    @classmethod
    def discover(
        cls,
        plugins_dir: Path | None = None,
        *,
        extra_plugin_dirs: Iterable[Path] = (),
        runtime: str | None = None,
    ) -> PluginCatalog:
        plugins: dict[str, Plugin] = {}
        for requested_dir in (plugins_dir, *extra_plugin_dirs):
            for base_dir in _plugin_dirs(requested_dir):
                if not base_dir.exists():
                    continue
                for plugin_dir in sorted(base_dir.iterdir()):
                    plugin = _load_plugin(plugin_dir, runtime=runtime)
                    if plugin is not None:
                        plugins[plugin.name] = plugin
        return cls(plugins)

    def __getitem__(self, name: str) -> Plugin:
        return self.plugins[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self.plugins)

    def __len__(self) -> int:
        return len(self.plugins)

    @property
    def schemas(self) -> dict[str, dict[str, Any]]:
        """Return complete config.json documents keyed by plugin name."""
        return {name: dict(plugin.manifest) for name, plugin in self.plugins.items() if plugin.manifest}

    @property
    def properties(self) -> dict[str, dict[str, Any]]:
        """Return the flattened config-property schema owned by the catalog."""
        properties: dict[str, dict[str, Any]] = {}
        for schema in self.schemas.values():
            for key, definition in (schema.get("properties") or {}).items():
                if isinstance(definition, dict):
                    properties[str(key)] = definition
        return properties

    def select(
        self,
        names: Iterable[str] | None = None,
        *,
        disabled_names: Iterable[str] | None = None,
    ) -> PluginCatalog:
        """Select plugins and their transitive ``required_plugins`` dependencies."""
        disabled = {name.lower() for name in disabled_names or ()}
        requested = (
            [name for name, plugin in self.plugins.items() if plugin.config.x_auto_run and name.lower() not in disabled]
            if names is None
            else [name for name in names if name.lower() not in disabled]
        )
        if not requested:
            return type(self)({})

        plugins_by_lower = {name.lower(): plugin for name, plugin in self.plugins.items()}
        names_by_lower = {name.lower(): name for name in self.plugins}
        resolved: set[str] = set()
        blocked: set[str] = set()
        queue = [name.lower() for name in requested]
        while queue:
            name = queue.pop()
            if name in disabled:
                blocked.add(name)
                continue
            if name in resolved or name in blocked:
                continue
            plugin = plugins_by_lower.get(name)
            required = [dependency.lower() for dependency in plugin.config.required_plugins] if plugin else []
            if set(required).intersection(disabled | blocked):
                blocked.add(name)
                continue
            resolved.add(name)
            queue.extend(dependency for dependency in required if dependency not in resolved)

        while True:
            newly_blocked = {
                name.lower()
                for name, plugin in self.plugins.items()
                if name.lower() in resolved and any(dependency.lower() in blocked for dependency in plugin.config.required_plugins)
            }
            if not newly_blocked:
                break
            resolved.difference_update(newly_blocked)
            blocked.update(newly_blocked)

        ordered: dict[str, Plugin] = {}
        visited: set[str] = set()
        visiting: list[str] = []

        def add_with_dependencies(name: str) -> None:
            lower_name = name.lower()
            if lower_name in visited or lower_name not in resolved:
                return
            if lower_name in visiting:
                cycle_start = visiting.index(lower_name)
                cycle = [*visiting[cycle_start:], lower_name]
                raise ValueError(f"Plugin dependency cycle: {' -> '.join(cycle)}")
            plugin = plugins_by_lower.get(lower_name)
            if plugin is None:
                return
            visiting.append(lower_name)
            for dependency in plugin.config.required_plugins:
                add_with_dependencies(dependency)
            visiting.pop()
            visited.add(lower_name)
            ordered[names_by_lower[lower_name]] = plugin

        for name in self.plugins:
            if name.lower() in resolved:
                add_with_dependencies(name)
        return type(self)(ordered)

    def matching_output(self, output_prefixes: Iterable[str]) -> list[str]:
        """Return plugin names matching MIME types, categories, or extensions."""
        prefixes: list[str] = []
        mimetypes.init()
        for raw_prefix in output_prefixes:
            prefix = raw_prefix.strip()
            if not prefix:
                continue
            if "/" in prefix:
                prefixes.append(prefix)
                continue
            prefixes.append(f"{prefix}/")
            extension = prefix if prefix.startswith(".") else f".{prefix}"
            for type_map in (mimetypes.types_map, mimetypes.common_types):
                mimetype = type_map.get(extension)
                if mimetype and mimetype not in prefixes:
                    prefixes.append(mimetype)
            guessed, _encoding = mimetypes.guess_type(f"file{extension}")
            if guessed and guessed not in prefixes:
                prefixes.append(guessed)

        return [
            name
            for name, plugin in self.plugins.items()
            if any(
                mimetype.startswith(prefix) or prefix.startswith(mimetype)
                for mimetype in plugin.config.output_mimetypes
                for prefix in prefixes
            )
        ]

    def hooks(
        self,
        event_name: str,
        *,
        names: list[str] | None = None,
        disabled_names: list[str] | None = None,
    ) -> list[tuple[Plugin, Hook]]:
        """Return dependency-expanded hooks in their canonical execution order."""
        event_name = event_name.removesuffix("Event")
        selected = self.select(names, disabled_names=disabled_names)
        hooks = [(plugin, hook) for plugin in selected.values() for hook in plugin.filter_hooks(event_name)]
        return sorted(hooks, key=lambda item: item[1].sort_key)

    def groups(self, *, include_hidden: bool = False) -> dict[str, list[Plugin]]:
        """Group plugins by generic manifest category for presentation clients."""
        grouped: dict[str, list[Plugin]] = defaultdict(list)
        for plugin in self.plugins.values():
            if plugin.config.hidden and not include_hidden:
                continue
            grouped[plugin.config.category or "other"].append(plugin)
        return {
            category: sorted(plugins, key=lambda plugin: (plugin.config.display_order, plugin.name))
            for category, plugins in grouped.items()
        }

    def template_path(self, plugin_name: str, template_name: str) -> Path | None:
        """Return a plugin-owned presentation asset without interpreting it."""
        templates_root = (self.plugins[plugin_name].path / "templates").resolve()
        template = (templates_root / f"{template_name}.html").resolve()
        try:
            template.relative_to(templates_root)
        except ValueError:
            return None
        return template if template.is_file() else None

    def command(self, plugin_name: str, command_name: str) -> PluginCommand | None:
        """Resolve one manifest-declared executable without interpreting it."""
        plugin = self.plugins[plugin_name]
        command = plugin.config.commands.get(command_name)
        if not command:
            return None
        plugin_root = plugin.path.resolve()
        command_path = (plugin_root / command[0]).resolve()
        try:
            command_path.relative_to(plugin_root)
        except ValueError:
            return None
        if not command_path.is_file() or not command_path.stat().st_mode & 0o111:
            return None
        return PluginCommand(
            name=command_name,
            plugin_name=plugin_name,
            path=command_path,
            args=list(command[1:]),
        )


@dataclass(frozen=True)
class PluginConfigResolver:
    """Resolve declarative plugin schemas without application-framework state."""

    catalog: PluginCatalog

    @property
    def schemas(self) -> dict[str, dict[str, Any]]:
        return self.catalog.schemas

    @property
    def properties(self) -> dict[str, dict[str, Any]]:
        return self.catalog.properties

    def canonical_key(self, key: str) -> str:
        return plugin_utils.resolve_alias(key, self.schemas)

    def resolve(
        self,
        *,
        global_config: Mapping[str, Any] | None = None,
        user_config: Mapping[str, Any] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        return plugin_utils.resolve_plugin_configs(
            self.schemas,
            global_config=dict(global_config or {}),
            user_config=dict(user_config or {}),
            environ=dict(environ or {}),
        )

    def enabled_plugin_names(
        self,
        *,
        resolved: Mapping[str, Mapping[str, Any]] | None = None,
        global_config: Mapping[str, Any] | None = None,
        user_config: Mapping[str, Any] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> list[str]:
        """Return enabled plugins with required-plugin dependencies expanded."""
        resolved_config = resolved or self.resolve(
            global_config=global_config,
            user_config=user_config,
            environ=environ,
        )
        enabled: list[str] = []
        disabled: list[str] = []
        for name, plugin in self.catalog.items():
            enabled_key = plugin.enabled_key
            value = resolved_config.get(name, {}).get(enabled_key, True)
            if isinstance(value, str):
                value = value.strip().lower() not in {"", "0", "false", "no", "off"}
            (enabled if value else disabled).append(name)
        return list(self.catalog.select(enabled, disabled_names=disabled))

    def enabled_plugin_names_from_flat(self, config: Mapping[str, Any]) -> list[str]:
        """Select plugins from an already-resolved flat application config."""
        enabled: list[str] = []
        disabled: list[str] = []
        for name, plugin in self.catalog.items():
            value = config.get(plugin.enabled_key, True)
            if isinstance(value, str):
                value = value.strip().lower() not in {"", "0", "false", "no", "off"}
            (enabled if value else disabled).append(name)
        return list(self.catalog.select(enabled, disabled_names=disabled))

    def runtime_settings(self, plugin_name: str, config: Mapping[str, Any], *, default_timeout: int = 300) -> dict[str, Any]:
        """Return conventional enabled/timeout/binary values for one plugin."""
        plugin = self.catalog[plugin_name]
        enabled_value = config.get(plugin.enabled_key, True)
        if isinstance(enabled_value, str):
            enabled_value = enabled_value.strip().lower() not in {"", "0", "false", "no", "off"}
        timeout = config.get(f"{plugin_name.upper()}_TIMEOUT") or config.get("TIMEOUT", default_timeout)
        binary = config.get(f"{plugin_name.upper()}_BINARY", plugin_name)
        return {"enabled": bool(enabled_value), "timeout": int(timeout), "binary": str(binary)}
