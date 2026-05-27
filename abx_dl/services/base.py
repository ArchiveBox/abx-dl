"""Base service class for shared bus/service utilities."""

from typing import ClassVar

from abxbus import BaseEvent, EventBus

from ..models import Plugin


def plugin_with_required_plugin_names(plugin: Plugin, plugins: dict[str, Plugin]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    queue = [plugin.name]
    by_lower_name = {name.lower(): candidate for name, candidate in plugins.items()}
    while queue:
        name = queue.pop(0)
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        candidate = by_lower_name.get(key)
        if candidate is None:
            continue
        names.append(candidate.name)
        queue.extend(candidate.config.required_plugins)
    return names


class BaseService:
    """Base class for services that share one EventBus."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = []
    EMITS: ClassVar[list[type[BaseEvent]]] = []

    def __init__(self, bus: EventBus):
        self.bus = bus
