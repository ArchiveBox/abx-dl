"""BinaryService — resolves binary dependencies via provider on_Binary hooks."""

from pathlib import Path
from typing import Any, Callable, ClassVar

from bubus import BaseEvent, EventBus

from ..config import build_env_for_plugin
from ..events import BinaryEvent, MachineEvent
from ..models import VisibleRecord, write_jsonl
from ..orchestrator import _binary_env_key, _parse_jsonl_records, _run_binary_hook
from ..plugins import Plugin
from .base import BaseService


class BinaryService(BaseService):
    """Resolves Binary JSONL records by running provider on_Binary hooks."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [BinaryEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [MachineEvent]

    def __init__(
        self,
        bus: EventBus,
        *,
        shared_config: dict[str, Any],
        plugins: dict[str, Plugin],
        auto_install: bool,
        output_dir: Path,
        index_path: Path,
        emit_jsonl: bool,
        emit_result: Callable[[VisibleRecord], None],
    ):
        self.shared_config = shared_config
        self.plugins = plugins
        self.auto_install = auto_install
        self.output_dir = output_dir
        self.index_path = index_path
        self.emit_jsonl = emit_jsonl
        self.emit_result = emit_result
        super().__init__(bus)

    async def on_BinaryEvent(self, event: BinaryEvent) -> None:
        record = event.record
        name = record.get('name', '').strip()
        if not name:
            return
        abspath = str(record.get('abspath', '')).strip()
        if abspath:
            self.shared_config[_binary_env_key(name)] = abspath
            return
        # Run provider on_Binary hooks to resolve
        providers = record.get('binproviders') or record.get('binprovider') or 'env'
        for provider_name in [p.strip() for p in str(providers).split(',') if p.strip()]:
            if not self.auto_install and provider_name != 'env':
                continue
            provider_plugin = self.plugins.get(provider_name)
            if not provider_plugin:
                continue
            provider_output_dir = self.output_dir / provider_plugin.name
            provider_output_dir.mkdir(parents=True, exist_ok=True)
            provider_env = build_env_for_plugin(
                provider_plugin.name, provider_plugin.config_schema, self.shared_config,
                run_output_dir=self.output_dir,
            )
            for binary_hook in provider_plugin.get_binary_hooks():
                proc = _run_binary_hook(binary_hook, record, provider_output_dir, provider_env)
                write_jsonl(self.index_path, proc, also_print=self.emit_jsonl)
                self.emit_result(proc)
                resolved = False
                for emitted in _parse_jsonl_records(proc.stdout):
                    if emitted.get('type') == 'Machine':
                        await self.bus.emit(MachineEvent(record=emitted))
                    elif emitted.get('type') == 'Binary':
                        emitted_name = emitted.get('name', '').strip()
                        emitted_abspath = str(emitted.get('abspath', '')).strip()
                        if emitted_abspath and emitted_name:
                            self.shared_config[_binary_env_key(emitted_name)] = emitted_abspath
                            if emitted_name == name:
                                resolved = True
                if resolved:
                    return
