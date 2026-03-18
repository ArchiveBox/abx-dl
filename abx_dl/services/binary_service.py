"""BinaryService — resolves binary dependencies via provider on_Binary hooks."""

import json
from pathlib import Path
from typing import Callable, ClassVar

from bubus import BaseEvent, EventBus

from ..events import BinaryEvent, MachineEvent, ProcessEvent
from ..models import VisibleRecord
from ..plugins import Plugin
from .base import BaseService
from .machine_service import MachineService


def _binary_env_key(name: str) -> str:
    normalized = ''.join(ch if ch.isalnum() else '_' for ch in name).upper()
    return f'{normalized}_BINARY'


class BinaryService(BaseService):
    """Resolves Binary JSONL records by emitting ProcessEvent for provider on_Binary hooks."""

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [BinaryEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [MachineEvent, ProcessEvent]

    def __init__(
        self,
        bus: EventBus,
        *,
        machine: MachineService,
        plugins: dict[str, Plugin],
        auto_install: bool,
        output_dir: Path,
        emit_result: Callable[[VisibleRecord], None],
    ):
        self.machine = machine
        self.plugins = plugins
        self.auto_install = auto_install
        self.output_dir = output_dir
        self.emit_result = emit_result
        super().__init__(bus)

    async def on_BinaryEvent(self, event: BinaryEvent) -> None:
        record = event.record
        name = record.get('name', '').strip()
        if not name:
            return
        abspath = str(record.get('abspath', '')).strip()
        if abspath:
            # Binary already resolved — emit MachineEvent to update config
            await self.bus.emit(MachineEvent(record={
                'type': 'Machine', '_method': 'update',
                'key': f'config/{_binary_env_key(name)}', 'value': abspath,
            }))
            return

        # Build hook args from the record
        from ..models import uuid7
        hook_args = [f'--name={name}']
        binary_id = str(record.get('binary_id') or uuid7())
        hook_args.append(f'--binary-id={binary_id}')
        binproviders = str(record.get('binproviders') or record.get('binprovider') or '').strip()
        if binproviders:
            hook_args.append(f'--binproviders={binproviders}')
        overrides = record.get('overrides')
        if overrides is not None:
            hook_args.append(f'--overrides={json.dumps(overrides)}')
        custom_cmd = record.get('custom_cmd', record.get('custom-cmd'))
        if custom_cmd:
            hook_args.append(f'--custom-cmd={custom_cmd}')

        # Broadcast to all plugins' on_Binary hooks — each hook decides
        # internally whether it's the right provider for this binary.
        for plugin in self.plugins.values():
            for binary_hook in plugin.get_binary_hooks():
                plugin_output_dir = self.output_dir / plugin.name
                plugin_output_dir.mkdir(parents=True, exist_ok=True)
                plugin_env = self.machine.get_env_for_plugin(
                    plugin, run_output_dir=self.output_dir,
                )
                machine_id = plugin_env.get('MACHINE_ID', '')
                await self.bus.emit(ProcessEvent(
                    plugin_name=plugin.name, hook_name=binary_hook.name,
                    hook_path=str(binary_hook.path),
                    hook_args=hook_args + [f'--machine-id={machine_id}'],
                    is_background=False, output_dir=str(plugin_output_dir),
                    env=plugin_env, timeout=300,
                ))
                # Stop once resolved
                if self.machine.shared_config.get(_binary_env_key(name)):
                    return
