"""BinaryService — resolves binary dependencies by broadcasting to provider hooks."""

import json
from pathlib import Path
from typing import ClassVar

from bubus import BaseEvent, EventBus

from ..events import BinaryEvent, BinaryInstalledEvent, BinaryLoadedEvent, MachineEvent, ProcessEvent, ProcessStdoutEvent
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

    ProcessService routes this via ProcessStdoutEvent.
    on_ProcessStdoutEvent picks up type=Binary records and emits
    BinaryEvent, which triggers the provider hook chain.

    Handler registration order (all on BinaryEvent)::

        on_Binary__10_npm_...   — provider hook handler
        on_Binary__11_pip_...   — provider hook handler
        on_Binary__12_brew_...  — provider hook handler
        on_Binary__13_apt_...   — provider hook handler
        ...
        on_BinaryEvent          — runs last: emits BinaryLoadedEvent or
                                  BinaryInstalledEvent depending on outcome

    Provider hooks skip early if the binary is already resolved (abspath set on
    the event, or env key already in shared_config from a prior provider).
    on_BinaryEvent runs last and branches:
    - abspath set → register path via MachineEvent + emit BinaryLoadedEvent
    - no abspath but config resolved → emit BinaryInstalledEvent

    bubus detail: all handlers run serially in registration order, so the
    early-exit check works correctly.
    """

    LISTENS_TO: ClassVar[list[type[BaseEvent]]] = [ProcessStdoutEvent, BinaryEvent]
    EMITS: ClassVar[list[type[BaseEvent]]] = [
        BinaryEvent, MachineEvent, ProcessEvent, BinaryLoadedEvent, BinaryInstalledEvent,
    ]

    def __init__(
        self,
        bus: EventBus,
        *,
        machine: MachineService,
        plugins: dict[str, Plugin],
        auto_install: bool,
        output_dir: Path,
    ):
        self.machine = machine
        self.auto_install = auto_install
        self.output_dir = output_dir
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

    def _attach_handlers(self) -> None:
        """Register handlers in correct order.

        1. ProcessStdoutEvent → BinaryEvent routing
        2. Provider hooks on BinaryEvent (try to install)
        3. on_BinaryEvent last (emits BinaryLoadedEvent or BinaryInstalledEvent)
        """
        self.bus.on(ProcessStdoutEvent, self.on_ProcessStdoutEvent)

        for plugin, hook in self.binary_hooks:
            handler = self._make_provider_hook_handler(plugin, hook)
            handler.__name__ = hook.name
            handler.__qualname__ = hook.name
            self.bus.on(BinaryEvent, handler)

        # on_BinaryEvent runs last — branches on outcome
        self.bus.on(BinaryEvent, self.on_BinaryEvent)

    def _make_provider_hook_handler(self, plugin: Plugin, hook: Hook):
        """Create an async handler that runs one binary provider hook.

        The handler reads args from the BinaryEvent at call time and checks
        early-exit (binary already resolved by a prior provider) before running.
        """
        async def handler(event: BinaryEvent, _plugin=plugin, _hook=hook) -> None:
            if not event.name or event.abspath:
                # Already resolved or no name — skip
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

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        """Route type=Binary records to BinaryEvent."""
        try:
            record = json.loads(event.line)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(record, dict) or record.pop('type', '') != 'Binary':
            return
        await self.bus.emit(BinaryEvent(**record))

    async def on_BinaryEvent(self, event: BinaryEvent) -> None:
        """Handle binary resolution — runs after all provider hooks.

        Branches on the outcome:
        - **abspath set**: the binary was detected at a known path (either
          the requesting hook found it, or a provider hook just installed it
          and reported the path). Register in config + emit BinaryLoadedEvent.
        - **no abspath, but config resolved**: a provider hook installed the
          binary (its nested BinaryEvent → MachineEvent chain updated
          shared_config). Emit BinaryInstalledEvent.
        """
        if not event.name:
            return

        if event.abspath:
            # Binary path provided — register in config and notify
            await self.bus.emit(MachineEvent(
                method='update',
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
        else:
            # No abspath — check if a provider resolved it
            abspath = self.machine.shared_config.get(_binary_env_key(event.name), '')
            if abspath:
                await self.bus.emit(BinaryInstalledEvent(
                    name=event.name,
                    abspath=abspath,
                    binary_id=event.binary_id,
                    machine_id=event.machine_id,
                ))
