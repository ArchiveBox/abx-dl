"""
CLI interface for abx-dl using rich-click.
"""

from pathlib import Path

import rich_click as click
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from .dependencies import DependencyManager
from .executor import download_live, HookResult
from .plugins import discover_plugins, get_plugin_names

console = Console()

click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True


class DefaultGroup(click.Group):
    """A click Group that runs 'dl' command by default if a URL is found in args."""

    def resolve_command(self, ctx, args):
        # If no args or first arg is a known command, proceed normally
        if not args:
            return super().resolve_command(ctx, args)

        cmd_name = args[0]
        # If it's a known command, proceed normally
        if cmd_name in self.commands:
            return super().resolve_command(ctx, args)

        # Otherwise assume it's a URL and use 'dl' command
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

    config_overrides = {}
    if timeout:
        config_overrides['TIMEOUT'] = timeout

    console.print(f"[bold blue]Downloading:[/bold blue] {url}")
    console.print(f"[dim]Output: {out_path.absolute()}[/dim]")

    if selected:
        console.print(f"[dim]Plugins: {', '.join(selected)}[/dim]")
    else:
        console.print(f"[dim]Plugins: all ({len(plugins)} available)[/dim]")

    console.print()

    # Run with live progress
    hook_results: list[HookResult] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        result_gen = download_live(
            url=url,
            plugins=plugins,
            output_dir=out_path,
            selected_plugins=selected,
            config_overrides=config_overrides if config_overrides else None,
        )

        # Get total hooks count and create progress task
        total_hooks, gen = result_gen
        task = progress.add_task("[cyan]Running plugins...", total=total_hooks)

        for hook_result in gen:
            hook_results.append(hook_result)
            status_icon = {"succeeded": "[green]✓[/green]", "failed": "[red]✗[/red]", "skipped": "[yellow]○[/yellow]"}.get(hook_result.status, "?")
            progress.update(task, advance=1, description=f"{status_icon} {hook_result.hook.plugin_name}")

    console.print()

    # Show results table
    table = Table(title="Results")
    table.add_column("Plugin", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Output")

    for hook_result in hook_results:
        status_style = {
            'succeeded': '[green]succeeded[/green]',
            'failed': '[red]failed[/red]',
            'skipped': '[yellow]skipped[/yellow]',
        }.get(hook_result.status, hook_result.status)

        output = hook_result.output_path or hook_result.error or ''
        if len(output) > 50:
            output = output[:47] + '...'

        table.add_row(
            hook_result.hook.plugin_name,
            status_style,
            output,
        )

    console.print(table)

    # Summary
    succeeded = [r for r in hook_results if r.status == 'succeeded']
    failed = [r for r in hook_results if r.status == 'failed']
    skipped = [r for r in hook_results if r.status == 'skipped']

    console.print()
    console.print(f"[green]{len(succeeded)} succeeded[/green], "
                  f"[red]{len(failed)} failed[/red], "
                  f"[yellow]{len(skipped)} skipped[/yellow]")
    console.print(f"[dim]Output: {out_path.absolute()}[/dim]")


@cli.command()
@click.pass_context
def plugins(ctx):
    """List available plugins."""
    all_plugins = ctx.obj.get('plugins', discover_plugins())

    table = Table(title="Available Plugins")
    table.add_column("Name", style="cyan")
    table.add_column("Hooks", justify="right")
    table.add_column("Binaries")

    for name in sorted(all_plugins.keys()):
        plugin = all_plugins[name]
        hooks_count = len(plugin.get_snapshot_hooks())
        binaries = ', '.join(b.get('name', '') for b in plugin.binaries) or '-'

        table.add_row(name, str(hooks_count), binaries)

    console.print(table)
    console.print(f"\n[dim]{len(all_plugins)} plugins available[/dim]")


@cli.command()
@click.argument('plugin_names', required=False)
@click.pass_context
def install(ctx, plugin_names: str | None):
    """Install dependencies for plugins."""
    all_plugins = ctx.obj.get('plugins', discover_plugins())
    dm = DependencyManager()

    if plugin_names:
        names = [n.strip() for n in plugin_names.split(',')]
        plugins_to_install = {n: all_plugins[n] for n in names if n in all_plugins}
    else:
        plugins_to_install = all_plugins

    console.print("[bold]Installing plugin dependencies...[/bold]\n")

    for name, plugin in plugins_to_install.items():
        if not plugin.binaries:
            continue

        console.print(f"[cyan]{name}[/cyan]")

        results = dm.install_plugin_dependencies(plugin.binaries)
        for bin_name, info in results.items():
            if info.is_available:
                console.print(f"  [green]✓[/green] {bin_name} ({info.version or 'unknown'}) - {info.abspath}")
            else:
                console.print(f"  [red]✗[/red] {bin_name} - not found")

    console.print("\n[bold green]Done![/bold green]")


@cli.command()
@click.argument('plugin_names', required=False)
@click.pass_context
def check(ctx, plugin_names: str | None):
    """Check if plugin dependencies are available."""
    all_plugins = ctx.obj.get('plugins', discover_plugins())
    dm = DependencyManager()

    if plugin_names:
        names = [n.strip() for n in plugin_names.split(',')]
        plugins_to_check = {n: all_plugins[n] for n in names if n in all_plugins}
    else:
        plugins_to_check = all_plugins

    table = Table(title="Dependency Status")
    table.add_column("Plugin", style="cyan")
    table.add_column("Binary")
    table.add_column("Status")
    table.add_column("Version")
    table.add_column("Path")

    all_ok = True

    for name, plugin in sorted(plugins_to_check.items()):
        if not plugin.binaries:
            continue

        results = dm.check_plugin_dependencies(plugin.binaries)
        for bin_name, info in results.items():
            if info.is_available:
                status = "[green]✓[/green]"
            else:
                status = "[red]✗[/red]"
                all_ok = False

            table.add_row(
                name,
                bin_name,
                status,
                info.version or '-',
                info.abspath or '-',
            )

    console.print(table)

    if all_ok:
        console.print("\n[bold green]All dependencies available![/bold green]")
    else:
        console.print("\n[bold yellow]Some dependencies missing. Run 'abx-dl install' to install them.[/bold yellow]")


@cli.command()
@click.argument('plugin_name')
@click.pass_context
def info(ctx, plugin_name: str):
    """Show detailed info about a plugin."""
    all_plugins = ctx.obj.get('plugins', discover_plugins())

    if plugin_name not in all_plugins:
        console.print(f"[red]Plugin not found: {plugin_name}[/red]")
        console.print(f"[dim]Available: {', '.join(sorted(all_plugins.keys()))}[/dim]")
        return

    plugin = all_plugins[plugin_name]

    console.print(f"[bold cyan]{plugin.name}[/bold cyan]")
    console.print(f"[dim]Path: {plugin.path}[/dim]\n")

    # Config options
    if plugin.config_schema:
        console.print("[bold]Config options:[/bold]")
        for key, prop in plugin.config_schema.items():
            default = prop.get('default', '-')
            desc = prop.get('description', '')
            console.print(f"  {key}={default}")
            if desc:
                console.print(f"    [dim]{desc}[/dim]")
        console.print()

    # Binaries
    if plugin.binaries:
        console.print("[bold]Binaries:[/bold]")
        for binary in plugin.binaries:
            name = binary.get('name', '?')
            providers = binary.get('binproviders', 'env')
            console.print(f"  {name} (providers: {providers})")
        console.print()

    # Hooks
    hooks = plugin.get_snapshot_hooks()
    if hooks:
        console.print("[bold]Hooks:[/bold]")
        for hook in hooks:
            bg = " [dim](background)[/dim]" if hook.is_background else ""
            console.print(f"  Step {hook.step}.{hook.priority}: {hook.name}{bg}")


def main():
    """Entry point for CLI."""
    cli(obj={})


if __name__ == '__main__':
    main()
