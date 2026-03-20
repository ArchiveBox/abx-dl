"""
CLI interface for abx-dl using rich-click.
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import rich_click as click
from rich.console import Console, Group
from rich.live import Live
from rich.markup import escape
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from .config import get_config, set_config, CONFIG_FILE
from .dependencies import load_binary
from .events import ArchiveResultEvent, BinaryInstalledEvent, ProcessCompletedEvent
from .orchestrator import compute_phase_timeout, create_bus, download
from .models import ArchiveResult, Process
from .models import INSTALL_URL, Hook, Plugin, discover_plugins, filter_plugins

console = Console()
stderr_console = Console(stderr=True)

click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True


STATUS_STYLES = {
    'succeeded': 'green',
    'noresult': 'grey35',
    'failed': 'red',
    'skipped': 'bright_black',
    'started': 'yellow',
}


VisibleRecord = ArchiveResult | Process


class DefaultGroup(click.Group):
    """A click Group that runs 'dl' command by default if a URL is found in args."""

    def _should_default_to_dl(self, args: list[str]) -> bool:
        if not args:
            return False

        first_non_option = next((arg for arg in args if not arg.startswith('-')), None)
        if first_non_option is None:
            return False

        return first_non_option not in self.commands

    def parse_args(self, ctx, args):
        if self._should_default_to_dl(args):
            args.insert(0, 'dl')
        return super().parse_args(ctx, args)

    def resolve_command(self, ctx, args):
        if not args:
            return super().resolve_command(ctx, args)
        if args[0] in self.commands:
            return super().resolve_command(ctx, args)
        return super().resolve_command(ctx, ['dl'] + args)


def _compact_output(text: str, limit: int = 120) -> str:
    compact = ' '.join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + '...'


def _record_muted_style(record: VisibleRecord) -> str | None:
    if _record_status(record) == 'noresult':
        return 'grey35'
    return None


def _format_archive_result_line(ar: ArchiveResult) -> str:
    hook_name = escape(ar.hook_name or '-')
    status = escape(ar.status or '-')
    output = escape(_compact_output(ar.output_str or ar.error or ''))
    status_style = STATUS_STYLES.get(ar.status, 'white')
    muted_style = _record_muted_style(ar)
    line = (
        f"[dim]{'ArchiveResult':<13}[/dim] "
        f"[cyan]{hook_name:<40}[/cyan] "
        f"[{status_style}]{status:<10}[/{status_style}]"
    )
    if output:
        line += f" [{muted_style}]{output}[/{muted_style}]" if muted_style else f" {output}"
    return line


def _parse_process_output(proc: Process) -> str:
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith('{'):
            continue
        try:
            record = __import__('json').loads(line)
        except Exception:
            continue
        if record.get('type') != 'Binary':
            continue
        name = str(record.get('name', '')).strip()
        version = str(record.get('version', '')).strip()
        if version:
            return f'{name} {version}'.strip()
        abspath = str(record.get('abspath', '')).strip()
        if abspath:
            return abspath

    stderr = (proc.stderr or '').strip()
    if stderr:
        return stderr
    return (proc.stdout or '').strip()


def _parse_requested_binary_names(proc: Process) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith('{'):
            continue
        try:
            record = json.loads(line)
        except Exception:
            continue
        if record.get('type') != 'Binary':
            continue
        if str(record.get('abspath', '')).strip():
            continue
        name = str(record.get('name', '')).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)

    return names


def _record_type_name(record: VisibleRecord) -> str:
    return type(record).__name__


def _format_plugin_list(values: list[str]) -> str:
    return ', '.join(values) if values else '-'


def _plugin_info(plugin) -> str:
    info_parts: list[str] = []
    title = plugin.title.strip()
    description = plugin.description.strip()
    depends = _format_plugin_list(plugin.required_plugins)
    outputs = _format_plugin_list(plugin.output_mimetypes)

    if title and title.lower() != plugin.name.lower():
        info_parts.append(title)
    if description:
        info_parts.append(description)
    if depends != '-':
        info_parts.append(f"deps: {depends}")
    if outputs != '-':
        info_parts.append(f"outputs: {outputs}")

    return _compact_output(' | '.join(info_parts), limit=180) if info_parts else '-'


def _resolve_requested_plugins(plugin_names: tuple[str, ...], all_plugins: dict[str, object]) -> list[object]:
    requested: list[object] = []
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
        return record.hook_name or '-'
    if record.hook_name:
        return record.hook_name
    for part in record.cmd:
        name = Path(part).name
        if name.startswith('on_'):
            return Path(name).stem
    return '-'


def _record_status(record: VisibleRecord) -> str:
    if isinstance(record, ArchiveResult):
        return record.status or '-'
    return 'succeeded' if record.exit_code == 0 else 'failed'


def _record_output(record: VisibleRecord) -> str:
    if isinstance(record, ArchiveResult):
        if record.status == 'failed':
            return record.error or record.output_str or ''
        return record.output_str or record.error or ''
    return _parse_process_output(record)


def _format_install_failure_label(record: VisibleRecord) -> str:
    plugin = record.plugin if isinstance(record, ArchiveResult) else (record.plugin or '-')
    return f'{plugin} / {_record_hook_name(record)}'


def _record_start_ts(record: VisibleRecord) -> str | None:
    return record.start_ts if isinstance(record, ArchiveResult) else record.started_at


def _record_end_ts(record: VisibleRecord) -> str | None:
    return record.end_ts if isinstance(record, ArchiveResult) else record.ended_at


def _record_timeout(record: VisibleRecord, default_timeout_seconds: int) -> int:
    return default_timeout_seconds if isinstance(record, ArchiveResult) else record.timeout


def _record_key(record: VisibleRecord) -> str:
    if isinstance(record, ArchiveResult):
        return record.hook_name or record.id
    return record.id


def _format_elapsed(start_ts: str | None, end_ts: str | None, timeout_seconds: int, *, now: datetime | None = None) -> str:
    if not start_ts:
        return '-'

    try:
        start = datetime.fromisoformat(start_ts)
    except ValueError:
        return '-'

    if end_ts:
        try:
            end = datetime.fromisoformat(end_ts)
        except ValueError:
            end = now or datetime.now()
    else:
        end = now or datetime.now()

    elapsed = max(0.0, (end - start).total_seconds())
    return f'{elapsed:.1f}s/{timeout_seconds}s'


def _build_archive_results_table(results: list[VisibleRecord], *, timeout_seconds: int, now: datetime | None = None) -> Table:
    table = Table(show_header=True, header_style='bold')
    table.add_column('Type', style='dim', width=13, no_wrap=True)
    table.add_column('Hook Name', style='cyan', width=40, no_wrap=True)
    table.add_column('Status', width=10, no_wrap=True)
    table.add_column('Elapsed', width=12, no_wrap=True)
    table.add_column('Output')

    for record in results:
        status = escape(_record_status(record))
        status_style = STATUS_STYLES.get(_record_status(record), 'white')
        muted_style = _record_muted_style(record)
        elapsed = escape(
            _format_elapsed(
                _record_start_ts(record),
                _record_end_ts(record),
                _record_timeout(record, timeout_seconds),
                now=now,
            )
        )
        output = escape(_compact_output(_record_output(record)))
        table.add_row(
            _record_type_name(record),
            escape(_record_hook_name(record)),
            f'[{status_style}]{status}[/{status_style}]',
            f'[{muted_style}]{elapsed}[/{muted_style}]' if muted_style else elapsed,
            f'[{muted_style}]{output}[/{muted_style}]' if muted_style and output else output,
        )

    return table


class _LiveStatusView:
    def __init__(self, results: dict[str, VisibleRecord], progress: Progress, timeout_seconds: int) -> None:
        self.results = results
        self.progress = progress
        self.timeout_seconds = timeout_seconds

    def __rich_console__(self, console, options):
        yield Group(
            _build_archive_results_table(
                list(self.results.values()),
                timeout_seconds=self.timeout_seconds,
                now=datetime.now(),
            ),
            self.progress,
        )


@click.group(cls=DefaultGroup)
@click.version_option(package_name='abx-dl')
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
    ctx.obj['plugins'] = discover_plugins()


@cli.command()
@click.argument('url')
@click.option('--plugins', '-p', 'plugin_list', help='Comma-separated list of plugins to use')
@click.option('--output', '-o', 'output_dir', type=click.Path(), help='Output directory')
@click.option('--timeout', '-t', type=int, help='Timeout in seconds')
@click.option('--no-install', 'no_install', is_flag=True, help='Skip plugins with missing dependencies instead of auto-installing')
@click.pass_context
def dl(ctx, url: str, plugin_list: str | None, output_dir: str | None, timeout: int | None, no_install: bool):
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
    plugins = ctx.obj['plugins']
    selected = [p.strip() for p in plugin_list.split(',')] if plugin_list else None
    out_path = Path(output_dir) if output_dir else Path.cwd()
    config_overrides = {'TIMEOUT': timeout} if timeout else {}
    timeout_seconds = int((config_overrides or {}).get('TIMEOUT') or get_config('TIMEOUT').get('TIMEOUT') or 60)
    stdout_is_tty = sys.stdout.isatty()
    stderr_is_tty = sys.stderr.isatty()
    interactive_tty = stdout_is_tty or stderr_is_tty
    ui_console = stderr_console if stderr_is_tty else console

    if interactive_tty:
        ui_console.print(f"[bold blue]Downloading:[/bold blue] {url}")
        ui_console.print(f"[dim]Output: {out_path.absolute()}[/dim]")
        ui_console.print(f"[dim]Plugins: {', '.join(selected) if selected else f'all ({len(plugins)} available)'}[/dim]")
        ui_console.print()

    selected_plugins = (
        {name: plugin for name, plugin in plugins.items() if name in selected}
        if selected
        else plugins
    )
    crawl_hooks: list[tuple[Plugin, Hook]] = []
    snapshot_hooks: list[tuple[Plugin, Hook]] = []
    for plugin in selected_plugins.values():
        for hook in plugin.get_crawl_hooks():
            crawl_hooks.append((plugin, hook))
        for hook in plugin.get_snapshot_hooks():
            snapshot_hooks.append((plugin, hook))
    total_timeout = (
        compute_phase_timeout(crawl_hooks, config_overrides)
        + compute_phase_timeout(snapshot_hooks, config_overrides)
    )
    total_hooks = len(crawl_hooks) + len(snapshot_hooks)
    live_results: dict[str, VisibleRecord] = {}
    bus = create_bus(total_timeout=total_timeout)

    if interactive_tty:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=ui_console,
            transient=False,
        )
        status_view = _LiveStatusView(live_results, progress, timeout_seconds)
        task_id = progress.add_task("[cyan]Running plugins...", total=total_hooks)

        async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
            """Advance progress for every completed hook (fg and bg)."""
            progress.update(task_id, advance=1, description=f"[cyan]{escape(event.hook_name)}[/cyan]")

        async def on_ArchiveResultEvent(event: ArchiveResultEvent) -> None:
            ar = ArchiveResult(
                snapshot_id=event.snapshot_id, plugin=event.plugin,
                id=event.id, hook_name=event.hook_name, status=event.status,
                process_id=event.process_id or None,
                output_str=event.output_str, output_files=event.output_files,
                start_ts=event.start_ts or None, end_ts=event.end_ts or None,
                error=event.error or None,
            )
            live_results[_record_key(ar)] = ar

        bus.on(ProcessCompletedEvent, on_ProcessCompletedEvent)
        bus.on(ArchiveResultEvent, on_ArchiveResultEvent)

        with Live(
            status_view,
            console=ui_console,
            refresh_per_second=8,
            transient=False,
            vertical_overflow='visible',
        ):
            results = asyncio.run(download(
                url, plugins, out_path, selected, config_overrides or None,
                auto_install=not no_install, emit_jsonl=not stdout_is_tty,
                bus=bus,
            ))
    else:
        results = asyncio.run(download(
            url, plugins, out_path, selected, config_overrides or None,
            auto_install=not no_install, emit_jsonl=not stdout_is_tty,
            bus=bus,
        ))

    if interactive_tty:
        archive_results = [record for record in results if isinstance(record, ArchiveResult)]
        ui_console.print()
        ui_console.print(
            f"[green]{sum(1 for r in archive_results if r.status == 'succeeded')} succeeded[/green], "
            f"[grey35]{sum(1 for r in archive_results if r.status == 'noresult')} noresult[/grey35], "
            f"[red]{sum(1 for r in archive_results if r.status == 'failed')} failed[/red], "
            f"[bright_black]{sum(1 for r in archive_results if r.status == 'skipped')} skipped[/bright_black]"
        )
        ui_console.print(f"[dim]Output: {out_path.absolute()}[/dim]")


class _BinaryRecord:
    """Lightweight record for displaying binary install results in the CLI."""

    def __init__(self, *, name: str, abspath: str, plugin: str, hook_name: str,
                 status: str, version: str = '', error: str = '') -> None:
        self.name = name
        self.abspath = abspath
        self.plugin = plugin
        self.hook_name = hook_name
        self.status = status
        self.version = version
        self.error = error

    @property
    def display_output(self) -> str:
        if self.error:
            return self.error
        parts = [self.name]
        if self.version:
            parts.append(self.version)
        if self.abspath:
            parts.append(self.abspath)
        return ' '.join(parts)


def _run_plugin_install(selected) -> int:
    console.print("[bold]Installing plugin dependencies...[/bold]\n")

    binaries: dict[str, _BinaryRecord] = {}  # keyed by binary name
    hooks_ran: dict[tuple[str, str], Process] = {}  # all completed hooks
    failed_hooks: dict[tuple[str, str], Process] = {}  # hooks that failed
    crawl_hooks = [(plugin, hook) for plugin in selected.values() for hook in plugin.get_crawl_hooks()]
    bus = create_bus(total_timeout=compute_phase_timeout(crawl_hooks))

    async def on_BinaryInstalledEvent(event: BinaryInstalledEvent) -> None:
        binaries[event.name] = _BinaryRecord(
            name=event.name, abspath=event.abspath, version=event.version,
            plugin=event.binprovider or '-', hook_name='-', status='installed',
        )

    async def on_ProcessCompletedEvent(event: ProcessCompletedEvent) -> None:
        key = (event.plugin_name, event.hook_name)
        proc = Process(
            cmd=[], plugin=event.plugin_name, hook_name=event.hook_name,
            exit_code=event.exit_code, stdout=event.stdout, stderr=event.stderr,
        )
        hooks_ran[key] = proc
        if event.exit_code not in (None, 0):
            failed_hooks[key] = proc

    bus.on(BinaryInstalledEvent, on_BinaryInstalledEvent)
    bus.on(ProcessCompletedEvent, on_ProcessCompletedEvent)

    with TemporaryDirectory(prefix='abx-dl-install-') as temp_dir:
        asyncio.run(download(
            INSTALL_URL,
            selected,
            Path(temp_dir),
            auto_install=True,
            emit_jsonl=False,
            bus=bus,
        ))

    no_install_hooks = [name for name, plugin in sorted(selected.items()) if not plugin.get_crawl_hooks()]
    unresolved_hooks: dict[tuple[str, str], str] = {}
    for key, proc in hooks_ran.items():
        if proc.exit_code not in (None, 0):
            continue
        missing_binaries = [
            name
            for name in _parse_requested_binary_names(proc)
            if name not in binaries
        ]
        if missing_binaries:
            unresolved_hooks[key] = f"Requested binary not resolved: {', '.join(sorted(missing_binaries))}"

    if binaries or hooks_ran:
        table = Table(title="Install Results")
        table.add_column("Plugin", style="cyan")
        table.add_column("Hook")
        table.add_column("Status")
        table.add_column("Output")

        # Show binary results if any were resolved
        for record in sorted(binaries.values(), key=lambda r: r.name):
            status_style = STATUS_STYLES.get('succeeded', 'green')
            table.add_row(
                record.plugin,
                record.name,
                f'[{status_style}]{record.status}[/{status_style}]',
                _compact_output(record.display_output),
            )

        # Show hooks that ran (succeeded or failed) but didn't produce binary events
        for (plugin_name, hook_name), proc in sorted(hooks_ran.items()):
            status = 'succeeded' if proc.exit_code in (None, 0) else 'failed'
            if (plugin_name, hook_name) in unresolved_hooks:
                status = 'failed'
            status_style = STATUS_STYLES.get(status, 'white')
            table.add_row(
                plugin_name,
                hook_name,
                f'[{status_style}]{status}[/{status_style}]',
                _compact_output(unresolved_hooks.get((plugin_name, hook_name), _parse_process_output(proc))),
            )

        console.print(table)
    else:
        console.print("[yellow]No install hooks found for the selected plugins.[/yellow]")

    if no_install_hooks:
        console.print(f"[dim]No install hooks: {', '.join(no_install_hooks)}[/dim]")

    if failed_hooks or unresolved_hooks:
        console.print("\n[bold red]Failure details:[/bold red]")
        for (plugin_name, hook_name), proc in sorted(failed_hooks.items()):
            details = (proc.stderr or '').strip()
            if not details:
                continue
            console.print(f"\n[bold cyan]{plugin_name} / {hook_name}[/bold cyan]")
            console.print(escape(details))
        for (plugin_name, hook_name), message in sorted(unresolved_hooks.items()):
            console.print(f"\n[bold cyan]{plugin_name} / {hook_name}[/bold cyan]")
            console.print(escape(message))
        console.print(f"\n[bold red]{len(failed_hooks) + len(unresolved_hooks)} install step(s) failed.[/bold red]")
        return 1

    console.print("\n[bold green]Done![/bold green]")
    return 0


@cli.command()
@click.argument('plugin_names', nargs=-1)
@click.option('--install', '-i', 'do_install', is_flag=True, help='Install plugin dependencies')
@click.pass_context
def plugins(ctx, plugin_names: tuple[str, ...], do_install: bool):
    """Check and show info for plugins. Optionally install dependencies.

    **Examples:**

        abx-dl plugins                           # check + show info for all plugins

        abx-dl plugins wget ytdlp git            # check + show info for these plugins

        abx-dl plugins --install                 # install all plugins

        abx-dl plugins --install wget ytdlp git  # install only these plugins
    """
    all_plugins = ctx.obj.get('plugins', discover_plugins())

    # Filter to selected plugins if specified (resolves required_plugins dependencies)
    if plugin_names:
        selected = filter_plugins(all_plugins, list(plugin_names), include_providers=do_install)
        not_found = [n for n in plugin_names if n.lower() not in {k.lower() for k in all_plugins}]
        if not_found:
            console.print(f"[yellow]Warning: Unknown plugins: {', '.join(not_found)}[/yellow]")
        if not selected:
            console.print("[red]No valid plugins specified.[/red]")
            console.print(f"[dim]Available: {', '.join(sorted(all_plugins.keys()))}[/dim]")
            return
    else:
        selected = all_plugins

    if do_install:
        raise SystemExit(_run_plugin_install(selected))
    else:
        # Check + info mode (default)
        # Show summary table
        table = Table(title="Plugins")
        table.add_column("Name", style="cyan")
        table.add_column("Status")
        table.add_column("Hooks", justify="right")
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
                binaries_str = ', '.join(binary_statuses)
                status = "[green]✓[/green]" if all(b.startswith('[green]') for b in binary_statuses) else "[yellow]○[/yellow]"
            else:
                status = "[green]✓[/green]"

            table.add_row(
                name,
                status,
                str(hooks_count),
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
                    if prop.get('description'):
                        console.print(f"    [dim]{prop['description']}[/dim]")

            hooks = plugin.get_crawl_hooks() + plugin.get_snapshot_hooks()
            if hooks:
                console.print("\n[bold]Hooks:[/bold]")
                for hook in hooks:
                    bg = " [dim](background)[/dim]" if hook.is_background else ""
                    console.print(f"  {hook.order:02d}: {hook.name}{bg}")


@cli.command()
@click.argument('plugin_names', nargs=-1)
@click.pass_context
def install(ctx, plugin_names: tuple[str, ...]):
    """Shortcut for 'abx-dl plugins --install [plugins...]'."""
    ctx.invoke(plugins, plugin_names=plugin_names, do_install=True)


@cli.command()
@click.option('--get', 'get_key', help='Get a specific config value')
@click.option('--set', 'set_pair', help='Set a config value (KEY=value)')
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
    all_plugins = ctx.obj.get('plugins', discover_plugins())
    plugin_schemas = {name: p.config_schema for name, p in all_plugins.items() if p.config_schema}

    if set_pair:
        if '=' not in set_pair:
            console.print("[red]Invalid format. Use --set KEY=value[/red]")
            return
        key, value = set_pair.split('=', 1)
        # Try to parse value as JSON, handle Python-style booleans, otherwise treat as string
        try:
            parsed_value = json.loads(value)
        except json.JSONDecodeError:
            # Handle Python-style booleans
            if value.lower() in ('true', 'yes', '1'):
                parsed_value = True
            elif value.lower() in ('false', 'no', '0'):
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


if __name__ == '__main__':
    main()
