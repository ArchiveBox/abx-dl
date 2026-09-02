"""Generic plugin inventory and configuration interfaces for embedders.

The catalog knows only about plugin directories, declarative manifests, and
hook files.  It deliberately has no persistence or application-framework
integration; applications such as ArchiveBox can keep those concerns in their
own adapters while sharing the exact inventory used by the downloader CLI.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from abx_plugins.plugins.base import utils as plugin_utils

from .models import Hook, Plugin, PluginCommand, discover_plugins, filter_plugins


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
        plugins = discover_plugins(plugins_dir=plugins_dir, runtime=runtime)
        for extra_dir in extra_plugin_dirs:
            plugins.update(discover_plugins(plugins_dir=extra_dir, runtime=runtime))
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

    def select(self, names: list[str] | None = None, *, disabled_names: list[str] | None = None) -> PluginCatalog:
        return type(self)(filter_plugins(self.plugins, names, disabled_names=disabled_names))

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
