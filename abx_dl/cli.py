"""
CLI interface for abx-dl using rich-click.
"""

import sys
from pathlib import Path

import rich_click as click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from .dependencies import load_binary, install_binary
from .executor import download
from .models import ArchiveResult
from .plugins import discover_plugins

console = Console()

click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True


class DefaultGroup(click.Group):
    """A click Group that runs 'dl' command by default if a URL is found in args."""

    def resolve_command(self, ctx, args):
        if not args:
            return super().resolve_command(ctx, args)
        if args[0] in self.commands:
            return super().resolve_command(ctx, args)
        return super().resolve_command(ctx, ['dl'] + args)


@click.group(cls=DefaultGroup)
@click.version_option(package_name='abx-dl')
@click.pass_context
def cli(ctx):
    """
    Download everything from a URL.

    **Examples:**

        abx-dl 'https://example.com'

        abx-dl --plugins=favicon,title,wget 'https://example.com'

        abx-dl plugins
    """
    ctx.ensure_object(dict)
    ctx.obj['plugins'] = discover_plugins()


@cli.command()
@click.argument('url')
@click.option('--plugins', '-p', 'plugin_list', help='Comma-separated list of plugins to use')
@click.option('--output', '-o', 'output_dir', type=click.Path(), help='Output directory')
@click.option('--timeout', '-t', type=int, help='Timeout in seconds')
@click.pass_context
def dl(ctx, url: str, plugin_list: str | None, output_dir: str | None, timeout: int | None):
    """Download a URL using all enabled plugins."""
    plugins = ctx.obj['plugins']
    selected = [p.strip() for p in plugin_list.split(',')] if plugin_list else None
    out_path = Path(output_dir) if output_dir else Path.cwd()
    config_overrides = {'TIMEOUT': timeout} if timeout else {}
    is_tty = sys.stdout.isatty()

    results: list[ArchiveResult] = []
    gen = download(url, plugins, out_path, selected, config_overrides or None)

    if is_tty:
        # Rich progress display for TTY
        console.print(f"[bold blue]Downloading:[/bold blue] {url}")
        console.print(f"[dim]Output: {out_path.absolute()}[/dim]")
        console.print(f"[dim]Plugins: {', '.join(selected) if selected else f'all ({len(plugins)} available)'}[/dim]\n")

        # Count total hooks for progress bar
        total = sum(len(p.get_crawl_hooks()) + len(p.get_snapshot_hooks()) for p in plugins.values())

        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TaskProgressColumn(), console=console) as progress:
            task = progress.add_task("[cyan]Running plugins...", total=total)
            for ar in gen:
                results.append(ar)
                icon = {"succeeded": "[green]✓[/green]", "failed": "[red]✗[/red]", "skipped": "[yellow]○[/yellow]"}.get(ar.status, "?")
                progress.update(task, advance=1, description=f"{icon} {ar.plugin}")

        # Results table
        console.print()
        table = Table(title="Results")
        table.add_column("Plugin", style="cyan")
        table.add_column("Status", style="bold")
        table.add_column("Output")

        for ar in results:
            status_style = {'succeeded': '[green]succeeded[/green]', 'failed': '[red]failed[/red]', 'skipped': '[yellow]skipped[/yellow]'}.get(ar.status, ar.status)
            output = ar.output_str or ar.error or ''
            table.add_row(ar.plugin, status_style, output[:50] + '...' if len(output) > 50 else output)

        console.print(table)
        console.print()
        console.print(f"[green]{sum(1 for r in results if r.status == 'succeeded')} succeeded[/green], "
                      f"[red]{sum(1 for r in results if r.status == 'failed')} failed[/red], "
                      f"[yellow]{sum(1 for r in results if r.status == 'skipped')} skipped[/yellow]")
        console.print(f"[dim]Output: {out_path.absolute()}[/dim]")
    else:
        # JSONL output for non-TTY (handled by executor, just consume generator)
        for ar in gen:
            results.append(ar)


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

    # Filter to selected plugins if specified
    if plugin_names:
        selected = {n: all_plugins[n] for n in plugin_names if n in all_plugins}
        not_found = [n for n in plugin_names if n not in all_plugins]
        if not_found:
            console.print(f"[yellow]Warning: Unknown plugins: {', '.join(not_found)}[/yellow]")
        if not selected:
            console.print(f"[red]No valid plugins specified.[/red]")
            console.print(f"[dim]Available: {', '.join(sorted(all_plugins.keys()))}[/dim]")
            return
    else:
        selected = all_plugins

    if do_install:
        # Install mode
        console.print("[bold]Installing plugin dependencies...[/bold]\n")
        for name, plugin in sorted(selected.items()):
            if not plugin.binaries:
                continue
            console.print(f"[cyan]{name}[/cyan]")
            for spec in plugin.binaries:
                binary = install_binary(spec)
                if binary.is_valid:
                    console.print(f"  [green]✓[/green] {binary.name} ({binary.loaded_version or 'unknown'}) - {binary.loaded_abspath}")
                else:
                    console.print(f"  [red]✗[/red] {binary.name} - not found")
        console.print("\n[bold green]Done![/bold green]")
    else:
        # Check + info mode (default)
        # Show summary table
        table = Table(title="Plugins")
        table.add_column("Name", style="cyan")
        table.add_column("Status")
        table.add_column("Hooks", justify="right")
        table.add_column("Binaries")

        all_ok = True
        for name in sorted(selected.keys()):
            plugin = selected[name]
            hooks_count = len(plugin.get_snapshot_hooks())

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
                binaries_str = '-'
                status = "[green]✓[/green]"

            table.add_row(name, status, str(hooks_count), binaries_str)

        console.print(table)
        console.print(f"\n[dim]{len(selected)} plugins[/dim]")

        if not all_ok:
            console.print("[yellow]Some dependencies missing. Run 'abx-dl plugins --install' to install them.[/yellow]")

        # Show detailed info if only a few plugins selected
        if len(selected) <= 3:
            for name, plugin in sorted(selected.items()):
                console.print(f"\n[bold cyan]─── {plugin.name} ───[/bold cyan]")
                console.print(f"[dim]Path: {plugin.path}[/dim]")

                if plugin.config_schema:
                    console.print("\n[bold]Config options:[/bold]")
                    for key, prop in plugin.config_schema.items():
                        console.print(f"  {key}={prop.get('default', '-')}")
                        if prop.get('description'):
                            console.print(f"    [dim]{prop['description']}[/dim]")

                if plugin.binaries:
                    console.print("\n[bold]Binaries:[/bold]")
                    for spec in plugin.binaries:
                        binary = load_binary(spec)
                        status = "[green]✓[/green]" if binary.is_valid else "[red]✗[/red]"
                        version = f" ({binary.loaded_version})" if binary.is_valid and binary.loaded_version else ""
                        path = f" - {binary.loaded_abspath}" if binary.is_valid else ""
                        console.print(f"  {status} {spec.get('name', '?')}{version}{path}")
                        console.print(f"    [dim]providers: {spec.get('binproviders', 'env')}[/dim]")

                hooks = plugin.get_snapshot_hooks()
                if hooks:
                    console.print("\n[bold]Hooks:[/bold]")
                    for hook in hooks:
                        bg = " [dim](background)[/dim]" if hook.is_background else ""
                        console.print(f"  Step {hook.step}.{hook.priority}: {hook.name}{bg}")


@cli.command()
@click.argument('plugin_names', nargs=-1)
@click.pass_context
def install(ctx, plugin_names: tuple[str, ...]):
    """Shortcut for 'abx-dl plugins --install [plugins...]'."""
    ctx.invoke(plugins, plugin_names=plugin_names, do_install=True)


def main():
    cli(obj={})


if __name__ == '__main__':
    main()
