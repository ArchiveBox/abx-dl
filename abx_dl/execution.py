"""Framework-free execution entry points for individual plugin hooks."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

from abxbus import EventBus

from .events import ProcessCompletedEvent, ProcessEvent, slow_warning_timeout
from .models import Hook, PluginCommand
from .orchestrator import create_bus
from .services.process_service import ProcessService


def build_hook_args(values: Mapping[str, Any]) -> list[str]:
    """Encode application-owned values using the standalone hook CLI contract."""
    args: list[str] = []
    for key, value in values.items():
        if key.startswith("_") or value is None or value == "" or value is False:
            continue
        name = f"--{key.replace('_', '-')}"
        if value is True:
            args.append(name)
        elif isinstance(value, (dict, list)):
            args.append(f"{name}={json.dumps(value)}")
        else:
            rendered = str(value).strip()
            if rendered:
                args.append(f"{name}={rendered}")
    return args


def iter_plugin_command(
    command: PluginCommand,
    *,
    arguments: Mapping[str, Any] | None = None,
    stdin: Iterable[str] = (),
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
    timeout: float = 60,
) -> Iterator[str]:
    """Run a plugin-owned command and yield its stdout lines.

    Commands are black-box executables declared by plugin manifests. The
    caller owns the meaning of their arguments and output; abx-dl only applies
    the shared standalone CLI encoding and subprocess lifecycle.
    """
    argv = [str(command.path), *command.args, *build_hook_args(arguments or {})]
    input_text = "".join(f"{str(line).rstrip(chr(10))}\n" for line in stdin)
    completed = subprocess.run(
        argv,
        cwd=str(cwd or command.path.parent),
        env=dict(env) if env is not None else None,
        input=input_text,
        capture_output=True,
        text=True,
        check=True,
        timeout=timeout,
    )
    yield from completed.stdout.splitlines()


async def execute_hook(
    hook: Hook,
    *,
    output_dir: Path,
    env: Mapping[str, str],
    arguments: Mapping[str, Any] | None = None,
    timeout: int = 60,
    bus: EventBus | None = None,
    attach_process_service: bool = True,
    process_type: str = "hook",
    worker_type: str = "",
) -> ProcessCompletedEvent:
    """Execute one finite hook and return its canonical completion event.

    Embedders provide only filesystem, environment, and CLI inputs. They may
    attach their own event projectors to ``bus`` without making this API aware
    of an application framework or database.
    """
    if hook.is_background:
        raise ValueError("execute_hook() requires a finite foreground hook")

    event_bus = bus or create_bus(total_timeout=float(timeout) + 30.0, name="AbxDlHook")
    if attach_process_service:
        # A caller may reuse one bus for many hooks. ProcessService owns the
        # subprocess side effect, so registering it twice would run every later
        # ProcessEvent twice; projectors are safe to add independently.
        has_process_service = any(
            isinstance(getattr(handler.handler, "__self__", None), ProcessService) for handler in event_bus.handlers.values()
        )
        if not has_process_service:
            ProcessService(event_bus, emit_jsonl=False, interactive_tty=False)

    output_dir.mkdir(parents=True, exist_ok=True)
    process_event = ProcessEvent(
        plugin_name=hook.plugin_name,
        hook_name=hook.name,
        hook_path=str(hook.path),
        hook_args=build_hook_args(arguments or {}),
        is_background=False,
        output_dir=str(output_dir),
        env=dict(env),
        timeout=timeout,
        process_type=process_type,
        worker_type=worker_type,
        url=str((arguments or {}).get("url") or ""),
        event_timeout=float(timeout) + 30.0,
        event_handler_timeout=float(timeout) + 30.0,
        event_handler_slow_timeout=slow_warning_timeout(float(timeout) + 30.0),
    )
    emitted = event_bus.emit(process_event)
    await emitted.now(timeout=float(timeout) + 30.0)
    completed = await event_bus.find(ProcessCompletedEvent, child_of=emitted, past=True, future=False)
    if completed is None:
        raise RuntimeError(f"Hook {hook.full_name} finished without a ProcessCompletedEvent")
    return completed
