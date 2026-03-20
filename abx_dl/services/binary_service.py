"""BinaryService — resolves binary dependencies by broadcasting to provider hooks."""

import json
from pathlib import Path
from typing import Callable, ClassVar

from bubus import BaseEvent, EventBus

from ..events import BinaryEvent, BinaryInstalledEvent, BinaryLoadedEvent, MachineEvent, ProcessEvent
from ..models import VisibleRecord
from ..models import Hook, Plugin
from .base import BaseService
from .machine_service import MachineService


def _binary_env_key(name: str) -> str:
    """Convert a binary name to its env var key, e.g. 'yt-dlp' → 'YT_DLP_BINARY'."""
    normalized = ''.join(ch if ch.isalnum() else '_' for ch in name).upper()
    return f'{normalized}_BINARY'


class BinaryService(BaseService):
    """Resolves binary dependencies emitted by hooks during installation.

    When a hook needs a binary, it outputs JSONL like::

        {"type": "Binary", "name": "chromium", "binproviders": "puppeteer",
         "overrides": {"puppeteer": ["chromium@latest", "--install-deps"]}}

    ProcessService parses this and emits a BinaryEvent. This service handles it.

    Handler registration order (all on BinaryEvent)::

        on_BinaryEvent          — if abspath is set, emit BinaryLoadedEvent
        on_Binary__10_npm_...   — pre-registered provider hook handler
        on_Binary__11_pip_...   — pre-registered provider hook handler
        on_Binary__12_brew_...  — pre-registered provider hook handler
        on_Binary__13_apt_...   — pre-registered provider hook handler
        ...

    After provider hooks run, if the binary was resolved, BinaryInstalledEvent
    is emitted. Each provider handler checks early-exit: if a prior provider
    already resolved the binary, the handler is a no-op.

    bubus detail: all handlers run serially in registration order, so the
    early-exit check works correctly.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [BinaryEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        MachineEvent, ProcessEvent, BinaryLoadedEvent, BinaryInstalledEvent,
    ]

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
        self.auto_install = auto_install
        self.output_dir = output_dir
        self.emit_result = emit_result
        # Pre-collect all binary hooks at init time (sorted by order across all plugins)
        self.binary_hooks: list[tuple[Plugin, Hook]] = sorted(
            [
                (plugin, hook)
                for plugin in plugins.values()
                for hook in plugin.get_binary_hooks()
            ],
            key=lambda x: x[1].sort_key,
        )
        super().__init__(bus)
        self._register_binary_hook_handlers()

    def _register_binary_hook_handlers(self) -> None:
        """Register one handler per binary hook on BinaryEvent.

        These are registered *after* ``on_BinaryEvent`` (which handles the
        already-resolved path). A final handler emits BinaryInstalledEvent
        if a provider successfully resolved the binary.
        """
        for plugin, hook in self.binary_hooks:
            handler = self._make_binary_hook_handler(plugin, hook)
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self.bus.on(BinaryEvent, handler)

        # Final handler: emit BinaryInstalledEvent if a provider resolved it
        self.bus.on(BinaryEvent, self._emit_installed_event)

    def _make_binary_hook_handler(self, plugin: Plugin, hook: Hook):
        """Create an async handler that runs one binary provider hook.

        The handler reads args from the BinaryEvent at call time and checks
        early-exit (binary already resolved by a prior provider) before running.
        """
        async def handler(event: BinaryEvent, _plugin=plugin, _hook=hook) -> None:
            if not event.name or event.abspath:
                return
            # Early exit: a prior provider already resolved this binary
            if self.machine.shared_config.get(_binary_env_key(event.name)):
                return

            from ..models import uuid7
            binary_id = event.binary_id or uuid7()
            hook_args = [f'--name={event.name}', f'--binary-id={binary_id}']
            if event.binproviders:
                hook_args.append(f'--binproviders={event.binproviders}')
            if event.overrides is not None:
                hook_args.append(f'--overrides={json.dumps(event.overrides)}')
            if event.custom_cmd:
                hook_args.append(f'--custom-cmd={event.custom_cmd}')

            plugin_output_dir = self.output_dir / _plugin.name
            plugin_output_dir.mkdir(parents=True, exist_ok=True)
            plugin_env = self.machine.get_env_for_plugin(
                _plugin, run_output_dir=self.output_dir,
            )
            machine_id = plugin_env.get('MACHINE_ID', '')
            await self.bus.emit(ProcessEvent(
                plugin_name=_plugin.name, hook_name=_hook.name,
                hook_path=str(_hook.path),
                hook_args=hook_args + [f'--machine-id={machine_id}'],
                is_background=False, output_dir=str(plugin_output_dir),
                env=plugin_env, timeout=300,
            ))

        return handler

    async def on_BinaryEvent(self, event: BinaryEvent) -> None:
        """Handle already-resolved binaries by registering their path in config.

        This runs before the per-hook handlers. If ``event.abspath`` is set,
        the binary is already resolved — emit MachineEvent to update shared_config
        and BinaryLoadedEvent to notify observers.
        Unresolved binaries fall through to the per-hook handlers.
        """
        if not event.name or not event.abspath:
            return
        await self.bus.emit(MachineEvent(
            _method='update',
            key=f'config/{_binary_env_key(event.name)}',
            value=event.abspath,
        ))
        await self.bus.emit(BinaryLoadedEvent(
            name=event.name,
            abspath=event.abspath,
            binprovider=getattr(event, 'binprovider', '') or '',
            binary_id=event.binary_id,
            machine_id=event.machine_id,
        ))

    async def _emit_installed_event(self, event: BinaryEvent) -> None:
        """Emit BinaryInstalledEvent if a provider hook resolved the binary.

        Runs after all per-hook handlers. Checks if the binary's env key
        appeared in shared_config (set by a provider hook's BinaryEvent →
        on_BinaryEvent → MachineEvent chain).
        """
        if not event.name or event.abspath:
            # Already handled by on_BinaryEvent (BinaryLoadedEvent emitted)
            return
        abspath = self.machine.shared_config.get(_binary_env_key(event.name), '')
        if not abspath:
            return
        await self.bus.emit(BinaryInstalledEvent(
            name=event.name,
            abspath=abspath,
            binary_id=event.binary_id,
            machine_id=event.machine_id,
        ))
