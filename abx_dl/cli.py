"""
CLI interface for abx-dl using rich-click.
"""

import asyncio
import json
import os
import re
import sys
import time
from collections import defaultdict, deque
from contextlib import nullcontext
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TypeVar
from collections.abc import Callable, Mapping

import rich_click as click
from rich.console import Console, Group
from rich.highlighter import ReprHighlighter
from rich.live import Live
from rich.markup import escape
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TaskID
from rich.table import Table
from rich.text import Text
from rich import box

from .config import get_config, set_config, CONFIG_FILE
from .dependencies import load_binary
from .events import ArchiveResultEvent, BinaryEvent, BinaryInstalledEvent, ProcessCompletedEvent, ProcessEvent, ProcessStdoutEvent
from .orchestrator import compute_phase_timeout, create_bus, download
from .models import ArchiveResult, Process, now_iso
from .models import INSTALL_URL, Hook, Plugin, discover_plugins, filter_plugins

console = Console()
stderr_console = Console(stderr=True)

click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True

REPR_HIGHLIGHTER = ReprHighlighter()


STATUS_STYLES = {
    "succeeded": "green",
    "noresult": "grey58",
    "noresults": "grey58",
    "failed": "red",
    "skipped": "grey50",
    "started": "yellow",
}


@dataclass
class _LiveProcessRecord:
    id: str
    plugin: str
    hook_name: str
    timeout: int
    phase: str = ""
    started_at: str | None = None
    ended_at: str | None = None
    status: str = "started"
    output: str = ""
    cmd: list[str] = dataclass_field(default_factory=list)
    final_status: str | None = None
    final_output: str = ""
    final_output_is_archive_result: bool = False


VisibleRecord = ArchiveResult | Process | _LiveProcessRecord
T = TypeVar("T")


class DefaultGroup(click.Group):
    """A click Group that runs 'dl' command by default if a URL is found in args."""

    @staticmethod
    def _looks_like_url(arg: str) -> bool:
        return "://" in arg

    def _should_default_to_dl(self, args: list[str]) -> bool:
        if not args:
            return False

        first_non_option = next((arg for arg in args if not arg.startswith("-")), None)
        if first_non_option is None:
            return False

        return self._looks_like_url(first_non_option) and first_non_option not in self.commands

    def parse_args(self, ctx, args):
        if args == ["help"]:
            args[:] = ["--help"]
        if self._should_default_to_dl(args):
            args.insert(0, "dl")
        return super().parse_args(ctx, args)

    def resolve_command(self, ctx, args):
        if not args:
            return super().resolve_command(ctx, args)
        if args[0] in self.commands:
            return super().resolve_command(ctx, args)
        if self._should_default_to_dl(args):
            return super().resolve_command(ctx, ["dl"] + args)
        return super().resolve_command(ctx, args)


def _compact_output(text: str, limit: int = 120) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _flatten_output(text: str) -> str:
    return " ".join(text.split())


def _humanize_special_output(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("{") or not stripped.endswith("}"):
        return text
    try:
        record = json.loads(stripped)
    except Exception:
        return text
    if not isinstance(record, dict) or record.get("type") != "Binary":
        return text
    name = str(record.get("name", "")).strip()
    if not name:
        return text
    output = f"Binary requested: {name}"
    abspath = str(record.get("abspath", "")).strip()
    if abspath:
        output += f" ({abspath})"
    binproviders = str(record.get("binproviders", "")).strip()
    if binproviders:
        output += f" binproviders: {binproviders}"
    return output


def _format_binary_requested_output(text: str) -> Text | None:
    match = re.match(
        r"^Binary requested: (?P<name>\S+)(?: \((?P<abspath>[^)]+)\))?(?: binproviders: (?P<providers>.+))?$",
        text,
    )
    if not match:
        return None

    rendered = Text()
    rendered.append("Binary requested: ", style="grey62")
    rendered.append(match.group("name"), style="bold cyan")

    abspath = match.group("abspath")
    if abspath:
        rendered.append(" (", style="grey50")
        rendered.append(abspath, style="green")
        rendered.append(")", style="grey50")

    providers = match.group("providers")
    if providers:
        rendered.append(" binproviders: ", style="grey62")
        rendered.append(providers, style="yellow")

    return rendered


def _format_install_output(text: str):
    text = _flatten_output(_humanize_special_output(text)).replace('"', "")
    special = _format_binary_requested_output(text)
    if special is not None:
        return special
    return REPR_HIGHLIGHTER(text)


def _format_table_output(text: str, *, flatten: bool) -> Text:
    text = _humanize_special_output(text)
    text = (_flatten_output(text) if flatten else text).replace('"', "")
    special = _format_binary_requested_output(text)
    if special is not None:
        return special
    rendered = REPR_HIGHLIGHTER(text)
    return rendered if isinstance(rendered, Text) else Text(str(rendered))


def _record_muted_style(record: VisibleRecord) -> str | None:
    if _record_status(record) in ("noresult", "noresults"):
        return "grey58"
    if _record_status(record) == "skipped":
        return "grey50"
    return None


def _format_archive_result_line(ar: ArchiveResult) -> str:
    hook_name = escape(ar.hook_name or "-")
    status = escape(ar.status or "-")
    output = escape(_compact_output(_normalize_archive_result_output(ar.output_str or ar.error or "")))
    status_style = STATUS_STYLES.get(ar.status, "white")
    muted_style = _record_muted_style(ar)
    line = f"[dim]{'ArchiveResult':<13}[/dim] [cyan]{hook_name:<40}[/cyan] [{status_style}]{status:<10}[/{status_style}]"
    if output:
        line += f" [{muted_style}]{output}[/{muted_style}]" if muted_style else f" {output}"
    return line


def _parse_process_output(proc: Process) -> str:
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            record = __import__("json").loads(line)
        except Exception:
            continue
        if record.get("type") != "Binary":
            continue
        name = str(record.get("name", "")).strip()
        version = str(record.get("version", "")).strip()
        if version:
            return f"{name} {version}".strip()
        abspath = str(record.get("abspath", "")).strip()
        if abspath:
            return abspath

    stderr = (proc.stderr or "").strip()
    if stderr:
        return stderr
    return (proc.stdout or "").strip()


def _parse_hook_status_marker(stdout: str, stderr: str) -> tuple[str | None, str]:
    for stream in (stdout, stderr):
        for raw_line in reversed(stream.splitlines()):
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("SKIPPED:"):
                return "skipped", line.split(":", 1)[1].strip()
    return None, ""


def _iter_process_records(proc: Process) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            record = json.loads(line)
        except Exception:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def _parse_requested_binary_names(proc: Process) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    for record in _iter_process_records(proc):
        if record.get("type") != "Binary":
            continue
        if str(record.get("abspath", "")).strip():
            continue
        name = str(record.get("name", "")).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)

    return names


def _parse_emitted_binary_names(proc: Process) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    for record in _iter_process_records(proc):
        if record.get("type") != "Binary":
            continue
        name = str(record.get("name", "")).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)

    return names


def _format_install_status(label: str, *, ok: bool) -> str:
    style = "green" if ok else "red"
    if ok and label == "BinaryRequested":
        icon = "⬇️"
    else:
        icon = "✅" if ok else "❌"
    return f"[{style}]{label} {icon}[/{style}]"


def _record_type_name(record: VisibleRecord) -> str:
    if isinstance(record, _LiveProcessRecord):
        return "Process"
    return type(record).__name__


def _format_plugin_list(values: list[str]) -> str:
    return ", ".join(values) if values else "-"


def _format_plugin_badges(values: list[str], *, style: str) -> str:
    if not values:
        return "[bright_black]-[/bright_black]"
    return ", ".join(f"[{style}]{value}[/{style}]" for value in values)


def _plugin_info(plugin: Plugin) -> str:
    info_parts: list[str] = []
    title = plugin.title.strip()
    description = plugin.description.strip()

    if title and title.lower() != plugin.name.lower():
        info_parts.append(title)
    if description:
        info_parts.append(description)

    return _compact_output(" | ".join(info_parts), limit=180) if info_parts else "-"


def _advance_progress(progress: Progress, task_id: TaskID, description: str, *, headroom: int = 1) -> None:
    task = progress.tasks[task_id]
    if task.total is not None and task.completed >= task.total:
        progress.update(task_id, total=max(task.total + 1, task.completed + max(headroom, 1)))
    progress.update(task_id, advance=1, description=description)


def _progress_hook_description(hook_name: str | None) -> str:
    if not hook_name:
        return "[cyan]Running plugins...[/cyan]"
    return f"[cyan]{escape(hook_name)}[/cyan]"


def _latest_active_hook_name(active_row_keys: list[str], live_results: Mapping[str, VisibleRecord]) -> str | None:
    for row_key in reversed(active_row_keys):
        current = live_results.get(row_key)
        if isinstance(current, _LiveProcessRecord):
            return current.hook_name
    return None


def _run_with_debug_bus_log(bus, *, debug: bool, func: Callable[[], T]) -> T:
    try:
        return func()
    finally:
        if debug:
            bus.log_tree()


def _resolve_requested_plugins(plugin_names: tuple[str, ...], all_plugins: Mapping[str, Plugin]) -> list[Plugin]:
    requested: list[Plugin] = []
    for requested_name in plugin_names:
        match = next(
            (plugin for name, plugin in all_plugins.items() if name.lower() == requested_name.lower()),
            None,
        )
        if match is not None:
            requested.append(match)
    return requested


def _record_hook_name(record: VisibleRecord) -> str:
    if isinstance(record, ArchiveResult):
        return record.hook_name or "-"
    if isinstance(record, _LiveProcessRecord):
        return record.hook_name or "-"
    if record.hook_name:
        return record.hook_name
    for part in record.cmd:
        name = Path(part).name
        if name.startswith("on_"):
            return Path(name).stem
    return "-"


def _record_plugin_name(record: VisibleRecord) -> str:
    if isinstance(record, ArchiveResult):
        return record.plugin or ""
    if isinstance(record, _LiveProcessRecord):
        return record.plugin or ""
    return record.plugin or ""


def _render_hook_name_cell(record: VisibleRecord) -> Text:
    hook_name = _record_hook_name(record)
    text = Text(hook_name)

    match = re.match(r"^(on_)([A-Za-z]+)(__)(\d{2})(.*)$", hook_name)
    if match:
        on_prefix, event_name, separator, order, rest = match.groups()
        text.stylize("grey50", 0, len(on_prefix))
        text.stylize("bold cyan", len(on_prefix), len(on_prefix) + len(event_name))
        sep_start = len(on_prefix) + len(event_name)
        text.stylize("grey50", sep_start, sep_start + len(separator))
        order_start = sep_start + len(separator)
        text.stylize("bold magenta", order_start, order_start + len(order))

        rest_offset = order_start + len(order)
        for token, style in (
            (".finite", "green"),
            (".daemon", "yellow"),
            (".bg", "blue"),
        ):
            start = rest.find(token)
            if start >= 0:
                text.stylize(style, rest_offset + start, rest_offset + start + len(token))

    plugin_name = _record_plugin_name(record).strip()
    if plugin_name and plugin_name != "-":
        for match in re.finditer(re.escape(plugin_name), hook_name, flags=re.IGNORECASE):
            text.stylize("bold blue", match.start(), match.end())

    for token, style in (
        ("install", "bright_green"),
        ("launch", "bright_yellow"),
        ("wait", "bright_red"),
    ):
        for match in re.finditer(token, hook_name, flags=re.IGNORECASE):
            text.stylize(style, match.start(), match.end())

    return text


def _record_phase(record: VisibleRecord) -> str:
    if isinstance(record, _LiveProcessRecord):
        return record.phase or ""
    return ""


def _record_status(record: VisibleRecord) -> str:
    if isinstance(record, ArchiveResult):
        return record.status or "-"
    if isinstance(record, _LiveProcessRecord):
        return record.status or "-"
    if record.exit_code is None:
        return "started"
    return "succeeded" if record.exit_code == 0 else "failed"


def _process_args_output(cmd: list[str]) -> str:
    if not cmd:
        return ""
    return repr(cmd[1:] or cmd)


def _record_output(record: VisibleRecord) -> str:
    if isinstance(record, ArchiveResult):
        if record.status == "failed":
            return record.error or _normalize_archive_result_output(record.output_str) or ""
        return _normalize_archive_result_output(record.output_str) or record.error or ""
    if isinstance(record, _LiveProcessRecord):
        if record.output:
            if record.final_output_is_archive_result:
                return _normalize_archive_result_output(record.output)
            return record.output
        if record.status == "failed":
            return _process_args_output(record.cmd)
        return ""
    output = _parse_process_output(record)
    if output:
        return output
    if _record_status(record) == "failed":
        return _process_args_output(record.cmd)
    return ""


def _normalize_archive_result_output(text: str) -> str:
    stripped = text.strip()
    if not stripped or "\n" in stripped:
        return text
    path = Path(stripped)
    if not path.is_absolute():
        return text
    try:
        return os.path.relpath(str(path), Path.cwd())
    except Exception:
        return text


def _render_record_output(record: VisibleRecord) -> str:
    output = _humanize_special_output(_record_output(record))
    if _record_status(record) == "failed":
        return output
    if isinstance(record, ArchiveResult):
        return record.output_str or _compact_output(output)
    if isinstance(record, _LiveProcessRecord):
        return output if record.final_output_is_archive_result else _compact_output(output)
    return _compact_output(output)


def _render_record_output_cell(record: VisibleRecord, *, muted_style: str | None = None) -> Text:
    full_output = _record_status(record) == "failed" or (isinstance(record, ArchiveResult) and bool(record.output_str))
    if isinstance(record, _LiveProcessRecord):
        full_output = full_output or record.final_output_is_archive_result
    cell = _format_table_output(_render_record_output(record), flatten=not full_output)
    if muted_style:
        cell.stylize(muted_style)
    return cell


def _format_install_failure_label(record: VisibleRecord) -> str:
    plugin = record.plugin if isinstance(record, (ArchiveResult, _LiveProcessRecord)) else (record.plugin or "-")
    return f"{plugin} / {_record_hook_name(record)}"


def _record_start_ts(record: VisibleRecord) -> str | None:
    if isinstance(record, ArchiveResult):
        return record.start_ts
    return record.started_at


def _record_end_ts(record: VisibleRecord) -> str | None:
    if isinstance(record, ArchiveResult):
        return record.end_ts
    return record.ended_at


def _record_timeout(record: VisibleRecord, default_timeout_seconds: int) -> int:
    if isinstance(record, ArchiveResult):
        return default_timeout_seconds
    return record.timeout


def _record_key(record: VisibleRecord) -> str:
    if isinstance(record, ArchiveResult):
        return record.hook_name or record.id
    return record.id


def _is_binary_provider_hook_name(hook_name: str) -> bool:
    return hook_name.startswith("on_Binary__")


def _process_event_key(*, plugin_name: str, hook_name: str, snapshot_id: str = "", output_dir: str = "") -> tuple[str, str, str, str]:
    return (plugin_name, hook_name, snapshot_id, output_dir)


def _phase_label_for_event(bus, event) -> str:
    phase_names = {
        "CrawlSetupEvent": "CrawlSetup",
        "SnapshotEvent": "Snapshot",
        "SnapshotCleanupEvent": "SnapshotCleanup",
        "CrawlCleanupEvent": "CrawlCleanup",
    }
    if not hasattr(bus, "event_history"):
        return ""

    current = event
    checked_ids: set[str] = set()
    while current is not None:
        label = phase_names.get(type(current).__name__)
        if label:
            return label
        parent_id = getattr(current, "event_parent_id", None)
        if not parent_id or parent_id in checked_ids:
            break
        checked_ids.add(parent_id)
        current = bus.event_history.get(parent_id)

    return ""


def _format_elapsed(start_ts: str | None, end_ts: str | None, timeout_seconds: int, *, now: datetime | None = None) -> str:
    if not start_ts:
        return "-"

    try:
        start = datetime.fromisoformat(start_ts)
    except ValueError:
        return "-"

    if end_ts:
        try:
            end = datetime.fromisoformat(end_ts)
        except ValueError:
            end = now or datetime.now()
    else:
        end = now or datetime.now()

    elapsed = max(0.0, (end - start).total_seconds())
    return f"{elapsed:.1f}s/{timeout_seconds}s"


def _build_archive_results_table(
    results: list[VisibleRecord],
    *,
    timeout_seconds: int,
    now: datetime | None = None,
    show_header: bool = True,
    stream: bool = False,
    max_width: int | None = None,
) -> Table:
    table = Table(
        show_header=show_header,
        header_style="bold",
        box=None if stream else box.HEAVY_HEAD,
        show_edge=not stream,
        pad_edge=not stream,
        expand=True,
        width=max_width,
    )
    table.add_column("Currently Running", min_width=36, ratio=3, no_wrap=True, overflow="ellipsis")
    if not stream:
        table.add_column("Phase", width=15, no_wrap=True)
    table.add_column("Status", width=10, no_wrap=True)
    table.add_column("Elapsed", width=10 if stream else 12, no_wrap=True)
    table.add_column("Output", min_width=30 if stream else 40, ratio=4, overflow="fold")

    for record in results:
        status = escape(_record_status(record))
        status_style = STATUS_STYLES.get(_record_status(record), "white")
        muted_style = _record_muted_style(record)
        elapsed = escape(
            _format_elapsed(
                _record_start_ts(record),
                _record_end_ts(record),
                _record_timeout(record, timeout_seconds),
                now=now,
            ),
        )
        output = _render_record_output_cell(record, muted_style=muted_style if _render_record_output(record) else None)
        row: list[str | Text] = [
            _render_hook_name_cell(record),
        ]
        if not stream:
            row.append(escape(_record_phase(record)))
        row.extend(
            [
                f"[{status_style}]{status}[/{status_style}]",
                f"[{muted_style}]{elapsed}[/{muted_style}]" if muted_style else elapsed,
                output,
            ],
        )
        table.add_row(*row)

    return table


class _LiveStatusView:
    def __init__(self, results: dict[str, VisibleRecord], progress: Progress, timeout_seconds: int) -> None:
        self.results = results
        self.progress = progress
        self.timeout_seconds = timeout_seconds

    def __rich_console__(self, console, options):
        if self.results:
            yield Group(
                _build_archive_results_table(
                    list(self.results.values()),
                    timeout_seconds=self.timeout_seconds,
                    now=datetime.now(),
                    stream=True,
                    max_width=options.max_width,
                ),
                self.progress,
            )
            return
        yield self.progress


class LiveBusUI:
    def __init__(
        self,
        bus,
        *,
        total_hooks: int,
        timeout_seconds: int,
        ui_console: Console | None = None,
        interactive_tty: bool = True,
    ) -> None:
        self.bus = bus
        self.total_hooks = total_hooks
        self.timeout_seconds = timeout_seconds
        self.ui_console = ui_console or console
        self.interactive_tty = interactive_tty
        self.live_results: dict[str, VisibleRecord] = {}
        self.streamed_header = False
        self.pending_process_rows: dict[tuple[str, str, str, str], deque[str]] = defaultdict(deque)
        self.row_key_by_event_id: dict[str, str] = {}
        self.process_event_by_row_key: dict[str, ProcessEvent] = {}
        self.process_id_to_row_key: dict[str, str] = {}
        self.active_row_keys: list[str] = []
        self.process_row_num = 0
        self.last_live_refresh = 0.0
        self._registrations: list[tuple[object, object]] = []

        if self.interactive_tty:
            self.progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=self.ui_console,
                transient=False,
            )
            self.status_view = _LiveStatusView(self.live_results, self.progress, timeout_seconds)
            self.task_id = self.progress.add_task(_progress_hook_description(None), total=total_hooks)
            self.live = Live(
                self.status_view,
                console=self.ui_console,
                auto_refresh=True,
                refresh_per_second=4,
                transient=True,
                vertical_overflow="visible",
            )
            self._register(ProcessEvent, self.on_ProcessEvent)
            self._register(ProcessStdoutEvent, self.on_ProcessStdoutEvent)
            self._register(ArchiveResultEvent, self.on_ArchiveResultEvent)
            self._register(ProcessCompletedEvent, self.on_ProcessCompletedEvent)
        else:
            self.progress = None
            self.status_view = None
            self.task_id = None
            self.live = None

    def _register(self, event_cls, handler) -> None:
        registration = self.bus.on(event_cls, handler)
        self._registrations.append((event_cls, registration))

    def close(self) -> None:
        while self._registrations:
            event_cls, registration = self._registrations.pop()
            self.bus.off(event_cls, registration)

    def __enter__(self):
        if self.live is not None:
            self.live.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self.live is not None:
                self.live.__exit__(exc_type, exc, tb)
        finally:
            self.close()

    def print_intro(self, *, url: str, output_dir: Path, plugins_label: str) -> None:
        if not self.interactive_tty:
            return
        self.ui_console.print(f"[bold blue]Downloading:[/bold blue] {url}")
        self.ui_console.print(f"[dim]Output: {output_dir.absolute()}[/dim]")
        self.ui_console.print(f"[dim]Plugins: {plugins_label}[/dim]")
        self.ui_console.print()

    def print_summary(self, results: list[ArchiveResult], *, output_dir: Path) -> None:
        if not self.interactive_tty:
            return
        archive_results = [record for record in results if isinstance(record, ArchiveResult)]
        self.ui_console.print()
        self.ui_console.print(
            f"[green]{sum(1 for r in archive_results if r.status == 'succeeded')} succeeded[/green], "
            f"[grey35]{sum(1 for r in archive_results if r.status == 'noresult')} noresult[/grey35], "
            f"[red]{sum(1 for r in archive_results if r.status == 'failed')} failed[/red], "
            f"[bright_black]{sum(1 for r in archive_results if r.status == 'skipped')} skipped[/bright_black]",
        )
        self.ui_console.print(f"[dim]Output: {output_dir.absolute()}[/dim]")

    def _refresh_live(self, *, force: bool = False, min_interval: float = 0.1) -> None:
        if self.live is None:
            return
        now = time.monotonic()
        if not force and (now - self.last_live_refresh) < min_interval:
            return
        self.last_live_refresh = now
        self.live.refresh()

    def _print_completed_row(self, record: VisibleRecord) -> None:
        self.ui_console.print(
            _build_archive_results_table(
                [record],
                timeout_seconds=self.timeout_seconds,
                show_header=not self.streamed_header,
                stream=True,
            ),
        )
        self.streamed_header = True

    def _fallback_row_key(self, *, plugin_name: str, hook_name: str, snapshot_id: str = "", output_dir: str = "") -> str | None:
        pending_key = _process_event_key(
            plugin_name=plugin_name,
            hook_name=hook_name,
            snapshot_id=snapshot_id,
            output_dir=output_dir,
        )
        return self.pending_process_rows[pending_key][0] if self.pending_process_rows[pending_key] else None

    def _match_row_key(self, event) -> str | None:
        parent_id = getattr(event, "event_parent_id", None) or ""
        if parent_id and parent_id in self.row_key_by_event_id:
            return self.row_key_by_event_id[parent_id]
        process_id = getattr(event, "process_id", "") or ""
        if process_id and process_id in self.process_id_to_row_key:
            return self.process_id_to_row_key[process_id]
        return self._fallback_row_key(
            plugin_name=getattr(event, "plugin_name", "") or getattr(event, "plugin", ""),
            hook_name=getattr(event, "hook_name", ""),
            snapshot_id=getattr(event, "snapshot_id", ""),
            output_dir=getattr(event, "output_dir", ""),
        )

    def _apply_archive_result(self, row: _LiveProcessRecord, event: ArchiveResultEvent) -> None:
        row.final_status = event.status or row.final_status
        final_output = event.error or event.output_str or row.final_output
        if final_output:
            row.final_output = final_output
            row.output = final_output
            row.final_output_is_archive_result = bool(event.output_str)
        if row.ended_at:
            row.status = row.final_status or row.status

    async def on_ProcessEvent(self, event: ProcessEvent) -> None:
        if _is_binary_provider_hook_name(event.hook_name) or self.progress is None or self.task_id is None:
            return
        self.process_row_num += 1
        row_key = f"process:{self.process_row_num}"
        self.pending_process_rows[
            _process_event_key(
                plugin_name=event.plugin_name,
                hook_name=event.hook_name,
                snapshot_id=event.snapshot_id,
                output_dir=event.output_dir,
            )
        ].append(row_key)
        self.row_key_by_event_id[event.event_id] = row_key
        self.process_event_by_row_key[row_key] = event
        self.active_row_keys.append(row_key)
        self.live_results[row_key] = _LiveProcessRecord(
            id=row_key,
            plugin=event.plugin_name,
            hook_name=event.hook_name,
            timeout=event.timeout,
            phase=_phase_label_for_event(self.bus, event),
            started_at=datetime.now().isoformat(),
            cmd=[event.hook_path, *event.hook_args],
        )
        current_task = self.progress.tasks[self.task_id]
        seen_hooks = current_task.completed + len(self.active_row_keys)
        total = current_task.total
        if total is not None and seen_hooks > total:
            self.progress.update(self.task_id, total=seen_hooks)
        self.progress.update(self.task_id, description=_progress_hook_description(event.hook_name))
        self._refresh_live(force=True)

    async def on_ProcessStdoutEvent(self, event: ProcessStdoutEvent) -> None:
        if _is_binary_provider_hook_name(event.hook_name):
            return
        row_key = self._match_row_key(event)
        if row_key is None:
            return
        self.row_key_by_event_id[event.event_id] = row_key
        if event.process_id:
            self.process_id_to_row_key[event.process_id] = row_key
        existing = self.live_results.get(row_key)
        if not isinstance(existing, _LiveProcessRecord):
            return
        if event.start_ts and not existing.started_at:
            existing.started_at = event.start_ts
        if not existing.final_output:
            existing.output = event.line
            if self.active_row_keys and row_key == self.active_row_keys[-1]:
                self._refresh_live(min_interval=0.5)

    async def on_ArchiveResultEvent(self, event: ArchiveResultEvent) -> None:
        row_key = self._match_row_key(event)
        if row_key is None:
            return
        self.row_key_by_event_id[event.event_id] = row_key
        if event.process_id:
            self.process_id_to_row_key[event.process_id] = row_key
        existing = self.live_results.get(row_key)
        if isinstance(existing, _LiveProcessRecord):
            self._apply_archive_result(existing, event)
            self._refresh_live(force=True)

    async def on_ProcessCompletedEvent(self, event: ProcessCompletedEvent) -> None:
        if _is_binary_provider_hook_name(event.hook_name) or self.progress is None or self.task_id is None:
            return
        row_key = self._match_row_key(event)
        pending_key = _process_event_key(
            plugin_name=event.plugin_name,
            hook_name=event.hook_name,
            snapshot_id=event.snapshot_id,
            output_dir=event.output_dir,
        )
        if row_key is None:
            row_key = event.process_id or f"process:completed:{len(self.live_results) + 1}"
        elif self.pending_process_rows[pending_key] and self.pending_process_rows[pending_key][0] == row_key:
            self.pending_process_rows[pending_key].popleft()
        if not self.pending_process_rows[pending_key]:
            self.pending_process_rows.pop(pending_key, None)
        self.row_key_by_event_id[event.event_id] = row_key
        if event.process_id:
            self.process_id_to_row_key[event.process_id] = row_key
        existing = self.live_results.get(row_key)
        row = (
            existing
            if isinstance(existing, _LiveProcessRecord)
            else _LiveProcessRecord(
                id=row_key,
                plugin=event.plugin_name,
                hook_name=event.hook_name,
                timeout=self.timeout_seconds,
                phase=_phase_label_for_event(self.bus, event),
            )
        )
        status_marker, status_output = _parse_hook_status_marker(event.stdout, event.stderr)
        row.started_at = event.start_ts or row.started_at
        row.ended_at = event.end_ts or now_iso()
        row.status = row.final_status or status_marker or ("succeeded" if event.exit_code == 0 else "failed")
        if event.exit_code != 0:
            row.output = event.stderr or event.stdout or row.final_output or row.output
        elif row.final_output:
            row.output = row.final_output
        elif status_output:
            row.output = status_output
        elif event.stdout.strip():
            row.output = event.stdout.strip().splitlines()[-1]
        elif event.stderr.strip():
            row.output = event.stderr.strip()
        self.live_results[row_key] = row

        process_event = self.process_event_by_row_key.get(row_key)
        if process_event is not None and hasattr(self.bus, "find"):
            existing_result = await self.bus.find(ArchiveResultEvent, child_of=process_event)
            if isinstance(existing_result, ArchiveResultEvent):
                self._apply_archive_result(row, existing_result)
        if row_key in self.active_row_keys:
            self.active_row_keys.remove(row_key)
        self.live_results.pop(row_key, None)
        self._print_completed_row(row)
        _advance_progress(
            self.progress,
            self.task_id,
            _progress_hook_description(_latest_active_hook_name(self.active_row_keys, self.live_results)),
            headroom=len(self.active_row_keys),
        )
        self._refresh_live(force=True)


@click.group(cls=DefaultGroup, context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(package_name="abx-dl", message="%(version)s")
@click.pass_context
def cli(ctx):
    """
    Download everything from a URL.

    **Examples:**

        abx-dl 'https://example.com'

        abx-dl --plugins=wget,ytdlp,git 'https://example.com'

        abx-dl --no-install 'https://example.com'

        abx-dl dl 'https://example.com'

        abx-dl plugins

        abx-dl plugins wget ytdlp --install
    """
    ctx.ensure_object(dict)
    ctx.obj["plugins"] = discover_plugins()


@cli.command()
@click.argument("url")
@click.option("--plugins", "-p", "plugin_list", help="Comma-separated list of plugins to use")
@click.option("--output", "-o", "output_dir", type=click.Path(), help="Output directory")
@click.option("--timeout", "-t", type=int, help="Timeout in seconds")
@click.option("--dry-run", is_flag=True, help="Enable abx-pkg dry-run mode and skip running snapshot hook subprocesses")
@click.option("--no-install", "no_install", is_flag=True, help="Skip plugins with missing dependencies instead of auto-installing")
@click.option("--debug", is_flag=True, help="Print the EventBus tree on exit or abort")
@click.pass_context
def dl(
    ctx,
    url: str,
    plugin_list: str | None,
    output_dir: str | None,
    timeout: int | None,
    dry_run: bool,
    no_install: bool,
    debug: bool,
):
    """Download a URL using all enabled plugins.

    By default, missing plugin dependencies are lazily auto-installed as needed.
    Use --no-install to skip plugins with missing dependencies instead.

    **Examples:**

        abx-dl 'https://example.com'

        abx-dl --plugins=wget,ytdlp,git 'https://example.com'

        abx-dl --no-install 'https://example.com'

        abx-dl dl 'https://example.com'

        abx-dl dl --plugins=wget,ytdlp,git 'https://example.com'

        abx-dl dl --no-install 'https://example.com'
    """
    plugins = ctx.obj["plugins"]
    selected = [p.strip() for p in plugin_list.split(",")] if plugin_list else None
    out_path = Path(output_dir) if output_dir else Path.cwd()
    config_overrides: dict[str, object] = {"TIMEOUT": timeout} if timeout else {}
    if dry_run:
        config_overrides["DRY_RUN"] = True
    timeout_value = config_overrides.get("TIMEOUT")
    if timeout_value is None:
        timeout_value = get_config("TIMEOUT").get("TIMEOUT")
    timeout_seconds = int(timeout_value) if isinstance(timeout_value, (int, str)) else 60
    stdout_is_tty = sys.stdout.isatty()
    stderr_is_tty = sys.stderr.isatty()
    interactive_tty = stdout_is_tty or stderr_is_tty
    ui_console = stderr_console if stderr_is_tty else console

    if interactive_tty:
        ui_console.print(f"[bold blue]Downloading:[/bold blue] {url}")
        ui_console.print(f"[dim]Output: {out_path.absolute()}[/dim]")
        ui_console.print(f"[dim]Plugins: {', '.join(selected) if selected else f'all ({len(plugins)} available)'}[/dim]")
        ui_console.print()

    selected_plugins = filter_plugins(plugins, selected) if selected else plugins
    crawl_hooks: list[tuple[Plugin, Hook]] = []
    snapshot_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in selected_plugins.values():
        for hook in plugin.get_crawl_hooks():
            crawl_hooks.append((plugin, hook))
        for hook in plugin.get_snapshot_hooks():
            snapshot_hooks.append((plugin, hook))
    total_timeout = compute_phase_timeout(crawl_hooks, config_overrides) + compute_phase_timeout(snapshot_hooks, config_overrides)
    total_hooks = len(crawl_hooks) + len(snapshot_hooks)
    bus = create_bus(total_timeout=total_timeout)
    live_ui = LiveBusUI(
        bus,
        total_hooks=total_hooks,
        timeout_seconds=timeout_seconds,
        ui_console=ui_console,
        interactive_tty=interactive_tty,
    )
    live_ui.print_intro(
        url=url,
        output_dir=out_path,
        plugins_label=", ".join(selected) if selected else f"all ({len(plugins)} available)",
    )

    with live_ui:
        results = _run_with_debug_bus_log(
            bus,
            debug=debug,
            func=lambda: asyncio.run(
                download(
                    url,
                    plugins,
                    out_path,
                    selected,
                    config_overrides or None,
                    auto_install=not no_install,
                    emit_jsonl=not stdout_is_tty,
                    bus=bus,
                    dry_run=dry_run,
                ),
            ),
        )

    live_ui.print_summary(results, output_dir=out_path)


class _BinaryRecord:
    """Lightweight record for displaying binary install results in the CLI."""

    def __init__(
        self,
        *,
        name: str,
        abspath: str,
        plugin: str,
        hook_name: str,
        status: str,
        version: str = "",
        error: str = "",
        order: int = 0,
    ) -> None:
        self.name = name
        self.abspath = abspath
        self.plugin = plugin
        self.hook_name = hook_name
        self.status = status
        self.version = version
        self.error = error
        self.order = order

    @property
    def display_output(self) -> str:
        if self.error:
            return self.error
        parts = []
        if self.abspath:
            parts.append(self.abspath)
        if self.version:
            parts.append(self.version)
        if not parts:
            parts.append(self.name)
        return " ".join(parts)


@dataclass
class _InstallRow:
    kind: str
    name: str
    plugin: str = "-"
    hook_name: str = "-"
    output: str = ""
    providers: tuple[str, ...] = ()
    related_names: tuple[str, ...] = ()
    failure_output: str = ""
    provider_failure: bool = False
    ok: bool = True


def _binary_event_output(event: BinaryEvent) -> str:
    if event.abspath:
        return event.abspath
    parts = [event.name]
    if event.binproviders:
        parts.append(f"providers={event.binproviders}")
    return " ".join(part for part in parts if part).strip()


def _filter_install_rows(rows: list[_InstallRow], visible_plugins: set[str], resolved_names: set[str] | None = None) -> list[_InstallRow]:
    visible_binaries = {row.name for row in rows if row.kind == "BinaryRequested" and row.plugin.lower() in visible_plugins}
    resolved_names = resolved_names or set()
    filtered: list[_InstallRow] = []

    for row in rows:
        if row.provider_failure and row.related_names and all(name in resolved_names for name in row.related_names):
            continue
        if not row.ok:
            filtered.append(row)
            continue
        if row.kind == "BinaryRequested" and row.plugin.lower() in visible_plugins:
            filtered.append(row)
            continue
        if row.kind == "BinaryInstalled" and (not visible_binaries or row.name in visible_binaries):
            filtered.append(row)

    return filtered


def _build_install_table(rows: list[_InstallRow]) -> Table:
    table = Table(title="Install Results")
    table.add_column("Plugin", style="cyan")
    table.add_column("Hook")
    table.add_column("Status")
    table.add_column("Output", overflow="fold")
    for row in rows:
        table.add_row(
            row.plugin,
            row.hook_name,
            _format_install_status(row.kind, ok=row.ok),
            _format_install_output(row.output),
        )
    return table


def _run_plugin_install(
    selected,
    *,
    visible_plugins: set[str] | None = None,
    label_plugins: list[str] | tuple[str, ...] | None = None,
    debug: bool = False,
    dry_run: bool = False,
) -> int:
    console.print(f"[bold]Installing plugin dependencies for {', '.join(label_plugins or sorted(selected))}...[/bold]\n")

    visible_plugins = {name.lower() for name in (visible_plugins or selected)}
    selected = {
        name: plugin.model_copy(
            update={
                "hooks": [
                    hook
                    for hook in plugin.hooks
                    if "Binary" in hook.name
                    or (name.lower() in visible_plugins and "Crawl" in hook.name and "install" in hook.name.lower())
                ],
            },
        )
        for name, plugin in selected.items()
    }
    rows: list[_InstallRow] = []
    installed_names: set[str] = set()
    request_rows_by_name: dict[str, list[_InstallRow]] = {}
    failed_install_rows: list[_InstallRow] = []
    crawl_hooks = [(plugin, hook) for plugin in selected.values() for hook in plugin.get_crawl_hooks()]
    bus = create_bus(total_timeout=compute_phase_timeout(crawl_hooks))
    live = None

    def _refresh_install_table() -> None:
        if live is not None:
            live.update(_build_install_table(_filter_install_rows(rows, visible_plugins, installed_names)), refresh=True)

    def _remember(bucket: dict[str, list[_InstallRow]], row: _InstallRow) -> None:
        bucket.setdefault(row.name, []).append(row)

    def _mark_request_failure(name: str, *, message: str | None = None) -> None:
        for row in request_rows_by_name.get(name, []):
            if not row.ok:
                continue
            row.ok = False
            if message:
                row.output = message
            break

    async def on_BinaryEvent(event: BinaryEvent) -> None:
        if event.abspath:
            return
        row = _InstallRow(
            kind="BinaryRequested",
            name=event.name,
            plugin=event.plugin_name or "-",
            hook_name=event.hook_name or "-",
            output=_binary_event_output(event),
            providers=tuple(provider.strip() for provider in event.binproviders.split(",") if provider.strip() and provider.strip() != "*"),
            related_names=(event.name,),
        )
        rows.append(row)
        _remember(request_rows_by_name, row)
        _refresh_install_table()

    async def on_BinaryInstalledEvent(event: BinaryInstalledEvent) -> None:
        record = _BinaryRecord(
            name=event.name,
            abspath=event.abspath,
            version=event.version,
            plugin=event.binprovider or "-",
            hook_name="-",
            status="installed",
        )
        row = _InstallRow(
            kind="BinaryInstalled",
            name=event.name,
            plugin=event.plugin_name or record.plugin,
            hook_name=event.hook_name or "-",
            output=record.display_output,
            related_names=(event.name,),
        )
        rows.append(row)
        installed_names.add(event.name)
        _refresh_install_table()

    async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
        proc = Process(
            cmd=[],
            plugin=event.plugin_name,
            hook_name=event.hook_name,
            exit_code=event.exit_code,
            stdout=event.stdout,
            stderr=event.stderr,
        )
        requested_names = _parse_requested_binary_names(proc)
        if event.exit_code not in (None, 0):
            if requested_names:
                details = (proc.stderr or "").strip() or _parse_process_output(proc)
                for name in requested_names:
                    _mark_request_failure(name, message=details)
            if event.hook_name.startswith("on_Binary__"):
                details = (proc.stderr or "").strip() or _parse_process_output(proc)
                requested_name = next(
                    (arg.split("=", 1)[1] for arg in event.hook_args if isinstance(arg, str) and arg.startswith("--name=")),
                    "",
                )
                related_names = ()
                if requested_name:
                    related_names = (requested_name,)
                    for row in request_rows_by_name.get(requested_name, []):
                        if row.ok and event.plugin_name in row.providers:
                            row.failure_output = details
                else:
                    related_names = tuple(
                        row.name
                        for request_rows in request_rows_by_name.values()
                        for row in request_rows
                        if event.plugin_name in row.providers
                    )
                row = _InstallRow(
                    kind="BinaryInstalled",
                    name=",".join(related_names) if related_names else event.hook_name,
                    plugin=event.plugin_name,
                    hook_name=event.hook_name,
                    output=details,
                    related_names=related_names,
                    provider_failure=True,
                    ok=False,
                )
                failed_install_rows.append(row)

    bus.on(BinaryEvent, on_BinaryEvent)
    bus.on(BinaryInstalledEvent, on_BinaryInstalledEvent)
    bus.on(ProcessCompletedEvent, on_ProcessCompletedEvent)

    with TemporaryDirectory(prefix="abx-dl-install-") as temp_dir:
        live_enabled = getattr(console.file, "isatty", lambda: False)()
        live_cm = Live(_build_install_table([]), console=console, refresh_per_second=8) if live_enabled else nullcontext()
        with live_cm as active_live:
            live = active_live
            _run_with_debug_bus_log(
                bus,
                debug=debug,
                func=lambda: asyncio.run(
                    download(
                        INSTALL_URL,
                        selected,
                        Path(temp_dir),
                        auto_install=True,
                        emit_jsonl=False,
                        bus=bus,
                        config_overrides={"DRY_RUN": True} if dry_run else None,
                        dry_run=dry_run,
                    ),
                ),
            )

    for name, request_rows in request_rows_by_name.items():
        if name in installed_names:
            continue
        for row in request_rows:
            if not row.ok:
                continue
            row.ok = False
            row.output = row.failure_output or f"Requested binary not resolved: {name}"
    failed_request_rows = [row for request_rows in request_rows_by_name.values() for row in request_rows if not row.ok]
    rows.extend(
        row for row in failed_install_rows if not row.related_names or any(name not in installed_names for name in row.related_names)
    )
    rows = _filter_install_rows(rows, visible_plugins, installed_names)

    if rows:
        if not live_enabled:
            console.print(_build_install_table(rows))
    else:
        console.print("[yellow]No install hooks found for the selected plugins.[/yellow]")

    if failed_request_rows:
        console.print("\n[bold red]Failure details:[/bold red]")
        for row in failed_request_rows:
            console.print(f"\n[bold cyan]{row.plugin} / {row.hook_name}[/bold cyan]")
            console.print(escape(row.output))
        console.print(f"\n[bold red]{len(failed_request_rows)} install step(s) failed.[/bold red]")
        return 1

    console.print("\n[bold green]Done![/bold green]")
    return 0


@cli.command()
@click.argument("plugin_names", nargs=-1)
@click.option("--install", "-i", "do_install", is_flag=True, help="Install plugin dependencies")
@click.option("--dry-run", is_flag=True, help="Enable abx-pkg dry-run mode during --install")
@click.option("--debug", is_flag=True, help="Print the EventBus tree on exit or abort when used with --install")
@click.pass_context
def plugins(ctx, plugin_names: tuple[str, ...], do_install: bool, dry_run: bool, debug: bool):
    """Check and show info for plugins. Optionally install dependencies.

    **Examples:**

        abx-dl plugins                           # check + show info for all plugins

        abx-dl plugins wget ytdlp git            # check + show info for these plugins

        abx-dl plugins --install                 # install all plugins

        abx-dl plugins --install wget ytdlp git  # install only these plugins
    """
    plugins_obj = ctx.obj.get("plugins")
    if isinstance(plugins_obj, dict):
        context_plugins = {name: plugin for name, plugin in plugins_obj.items() if isinstance(name, str) and isinstance(plugin, Plugin)}
        all_plugins = context_plugins if len(context_plugins) == len(plugins_obj) else discover_plugins()
    else:
        all_plugins = discover_plugins()

    # Filter to selected plugins if specified (resolves required_plugins dependencies)
    if plugin_names:
        selected = filter_plugins(all_plugins, list(plugin_names), include_providers=do_install)
        visible_plugins = set(filter_plugins(all_plugins, list(plugin_names), include_providers=False))
        not_found = [n for n in plugin_names if n.lower() not in {k.lower() for k in all_plugins}]
        if not_found:
            console.print(f"[yellow]Warning: Unknown plugins: {', '.join(not_found)}[/yellow]")
        if not selected:
            console.print("[red]No valid plugins specified.[/red]")
            console.print(f"[dim]Available: {', '.join(sorted(all_plugins.keys()))}[/dim]")
            return
    else:
        selected = all_plugins
        visible_plugins = set(selected)

    if do_install:
        raise SystemExit(
            _run_plugin_install(
                selected,
                visible_plugins=visible_plugins,
                label_plugins=plugin_names,
                debug=debug,
                dry_run=dry_run,
            ),
        )
    else:
        # Check + info mode (default)
        # Show summary table
        table = Table(title="Plugins")
        table.add_column("Name", style="cyan")
        table.add_column("Status")
        table.add_column("Hooks", justify="right")
        table.add_column("Deps")
        table.add_column("Outputs")
        table.add_column("Info")

        all_ok = True
        for name in sorted(selected.keys()):
            plugin = selected[name]
            hooks = plugin.get_crawl_hooks() + plugin.get_snapshot_hooks()
            hooks_count = len(hooks)

            # Check binary status
            if plugin.binaries:
                binary_statuses = []
                for spec in plugin.binaries:
                    binary = load_binary(spec)
                    if binary.is_valid:
                        binary_statuses.append(f"[green]{binary.name}[/green]")
                    else:
                        binary_statuses.append(f"[red]{binary.name}[/red]")
                        all_ok = False
                status = "[green]✓[/green]" if all(b.startswith("[green]") for b in binary_statuses) else "[yellow]○[/yellow]"
            else:
                status = "[green]✓[/green]"

            table.add_row(
                name,
                status,
                str(hooks_count),
                _format_plugin_badges(plugin.required_plugins, style="yellow3"),
                _format_plugin_badges(plugin.output_mimetypes, style="magenta"),
                _plugin_info(plugin),
            )

        console.print(table)
        console.print(f"\n[dim]{len(selected)} plugins[/dim]")

        if not all_ok:
            console.print("[yellow]Some dependencies missing. Run 'abx-dl plugins --install' to install them.[/yellow]")

        detail_plugins = _resolve_requested_plugins(plugin_names, all_plugins) if plugin_names else list(selected.values())

        if len(detail_plugins) == 1:
            plugin = detail_plugins[0]
            display_name = plugin.title or plugin.name

            console.print(f"\n[bold cyan]─── {display_name} ───[/bold cyan]")
            if plugin.title and plugin.title != plugin.name:
                console.print(f"[bold]Plugin:[/bold] {plugin.name}")
            console.print(f"[bold]Path:[/bold] [dim]{plugin.path}[/dim]")
            console.print(f"[bold]Description:[/bold] {plugin.description or '-'}")
            console.print(f"[bold]Depends on:[/bold] {_format_plugin_list(plugin.required_plugins)}")
            console.print(f"[bold]Outputs:[/bold] {_format_plugin_list(plugin.output_mimetypes)}")

            if plugin.config_schema:
                console.print("\n[bold]Config options:[/bold]")
                for key, prop in plugin.config_schema.items():
                    console.print(f"  {key}={prop.get('default', '-')}")
                    if prop.get("description"):
                        console.print(f"    [dim]{prop['description']}[/dim]")

            hooks = plugin.get_crawl_hooks() + plugin.get_snapshot_hooks()
            if hooks:
                console.print("\n[bold]Hooks:[/bold]")
                for hook in hooks:
                    bg = " [dim](background)[/dim]" if hook.is_background else ""
                    console.print(f"  {hook.order:02d}: {hook.name}{bg}")


@cli.command()
@click.argument("plugin_names", nargs=-1)
@click.option("--dry-run", is_flag=True, help="Enable abx-pkg dry-run mode while resolving install hooks")
@click.option("--debug", is_flag=True, help="Print the EventBus tree on exit or abort")
@click.pass_context
def install(ctx, plugin_names: tuple[str, ...], dry_run: bool, debug: bool):
    """Shortcut for 'abx-dl plugins --install [plugins...]'."""
    ctx.invoke(plugins, plugin_names=plugin_names, do_install=True, dry_run=dry_run, debug=debug)


@cli.command()
@click.option("--get", "get_key", help="Get a specific config value")
@click.option("--set", "set_pair", help="Set a config value (KEY=value)")
@click.pass_context
def config(ctx, get_key: str | None, set_pair: str | None):
    """Show or modify configuration.

    **Examples:**

        abx-dl config                        # show all config

        abx-dl config --get TIMEOUT          # get a specific value

        abx-dl config --set TIMEOUT=120      # set a value persistently
    """
    import json

    # Get plugin schemas for alias resolution and full config
    all_plugins = ctx.obj.get("plugins", discover_plugins())
    plugin_schemas = {name: p.config_schema for name, p in all_plugins.items() if p.config_schema}

    if set_pair:
        if "=" not in set_pair:
            console.print("[red]Invalid format. Use --set KEY=value[/red]")
            return
        key, value = set_pair.split("=", 1)
        # Try to parse value as JSON, handle Python-style booleans, otherwise treat as string
        try:
            parsed_value = json.loads(value)
        except json.JSONDecodeError:
            # Handle Python-style booleans
            if value.lower() in ("true", "yes", "1"):
                parsed_value = True
            elif value.lower() in ("false", "no", "0"):
                parsed_value = False
            else:
                parsed_value = value
        saved = set_config(plugin_schemas, **{key: parsed_value})
        for canonical_key, val in saved.items():
            print(f"{canonical_key}={json.dumps(val)}")
        stderr_console.print(f"[dim]Saved to {CONFIG_FILE}[/dim]")
        return

    if get_key:
        result = get_config(get_key, plugin_schemas=plugin_schemas)
        value = result.get(get_key)
        if value is not None:
            print(f"{get_key}={json.dumps(value)}")
        else:
            stderr_console.print(f"[yellow]{get_key} is not set[/yellow]")
        return

    # Show all config grouped by section
    grouped = get_config(plugin_schemas=plugin_schemas)
    for section, values in grouped.items():
        print(f"# {section}")
        for key, value in values.items():
            print(f"{key}={json.dumps(value)}")
        print()


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
