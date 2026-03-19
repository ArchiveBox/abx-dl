"""BinaryService — resolves binary dependencies by broadcasting to provider hooks."""

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
    """Convert a binary name to its env var key, e.g. 'yt-dlp' → 'YT_DLP_BINARY'."""
    normalized = ''.join(ch if ch.isalnum() else '_' for ch in name).upper()
    return f'{normalized}_BINARY'


class BinaryService(BaseService):
    """Resolves binary dependencies emitted by hooks during installation.

    When a hook needs a binary, it outputs JSONL like::

        {"type": "Binary", "name": "chromium", "binproviders": "puppeteer",
         "overrides": {"puppeteer": ["chromium@latest", "--install-deps"]}}

    ProcessService parses this and emits a BinaryEvent. This service handles it:

    Resolution flow (two paths)::

        BinaryEvent (with abspath set)
        └── Already resolved: emit MachineEvent to register path in config
            e.g. CHROMIUM_BINARY=/path/to/chrome

        BinaryEvent (without abspath — needs install)
        └── Broadcast: emit ProcessEvent for each plugin's on_Binary hooks
            │   apt  → on_Binary__13_apt_install.py --name=chromium --binproviders=puppeteer
            │   brew → on_Binary__12_brew_install.py --name=chromium --binproviders=puppeteer
            │   npm  → on_Binary__10_npm_install.py --name=chromium --binproviders=puppeteer
            │   pip  → on_Binary__11_pip_install.py --name=chromium --binproviders=puppeteer
            │   ...
            └── Each hook checks if it's the right provider → installs or skips
                On success: hook outputs Binary JSONL with abspath
                    → ProcessService emits BinaryEvent (with abspath)
                    → This service emits MachineEvent to register the path

    Early exit: once the binary is resolved (its env key appears in shared_config),
    remaining provider hooks are skipped. This is checked via
    ``MachineService.shared_config`` after each provider hook completes.

    Edge case: provider hooks that don't match the requested binproviders silently
    skip (exit 0, no stdout). The hook itself decides whether it's the right
    provider — the runner broadcasts to all and lets them self-select.

    bubus detail: all provider ProcessEvents are awaited serially (not fire-and-forget),
    so the early-exit check works correctly. Each provider hook runs to completion
    before the next one starts.
    """

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
        """Handle a binary resolution request.

        If ``event.abspath`` is set, the binary is already resolved — just register
        it in config. Otherwise, broadcast to provider hooks to install it.
        """
        if not event.name:
            return
        if event.abspath:
            # Binary already resolved — register its path in shared_config
            # so subsequent hooks can find it via env vars (e.g. WGET_BINARY=/usr/bin/wget)
            await self.bus.emit(MachineEvent(
                _method='update',
                key=f'config/{_binary_env_key(event.name)}',
                value=event.abspath,
            ))
            return

        # Binary needs install — broadcast to all plugins' on_Binary hooks.
        # Each provider hook decides internally whether it handles this binary
        # (based on --binproviders arg matching the provider's name).
        from ..models import uuid7
        binary_id = event.binary_id or uuid7()
        hook_args = [f'--name={event.name}', f'--binary-id={binary_id}']
        if event.binproviders:
            hook_args.append(f'--binproviders={event.binproviders}')
        if event.overrides is not None:
            hook_args.append(f'--overrides={json.dumps(event.overrides)}')
        if event.custom_cmd:
            hook_args.append(f'--custom-cmd={event.custom_cmd}')

        for plugin in self.plugins.values():
            for binary_hook in plugin.get_binary_hooks():
                plugin_output_dir = self.output_dir / plugin.name
                plugin_output_dir.mkdir(parents=True, exist_ok=True)
                plugin_env = self.machine.get_env_for_plugin(
                    plugin, run_output_dir=self.output_dir,
                )
                machine_id = plugin_env.get('MACHINE_ID', '')
                # Awaited (not fire-and-forget) so we can check for early exit
                await self.bus.emit(ProcessEvent(
                    plugin_name=plugin.name, hook_name=binary_hook.name,
                    hook_path=str(binary_hook.path),
                    hook_args=hook_args + [f'--machine-id={machine_id}'],
                    is_background=False, output_dir=str(plugin_output_dir),
                    env=plugin_env, timeout=300,
                ))
                # Early exit: if a provider hook resolved the binary, its stdout
                # triggered BinaryEvent(abspath=...) → MachineEvent → shared_config
                # update. No need to try remaining providers.
                if self.machine.shared_config.get(_binary_env_key(event.name)):
                    return
