from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from rich import box
from rich.table import Table


def binary_dependency_status(*, enabled: bool, valid: bool) -> str:
    if not enabled:
        return "[grey23]disabled[/grey23]"
    return "✅" if valid else "❌"


def binary_dependency_table(rows: Sequence[Mapping[str, Any]]) -> Table:
    table = Table(title="Binary Dependencies", box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Binary", no_wrap=True, max_width=28)
    table.add_column("Plugin", no_wrap=True, max_width=24)
    table.add_column("Status", justify="center", no_wrap=True, width=8)
    table.add_column("Version", no_wrap=True, width=16)
    table.add_column("Provider", no_wrap=True, width=8)
    table.add_column("Path", no_wrap=True, overflow="ellipsis", ratio=1)
    for row in rows:
        table.add_row(
            str(row["binary"]),
            str(row["plugin"]),
            row["status"],
            str(row["version"]),
            str(row["provider"]),
            row["path"],
            style=str(row.get("style") or ""),
        )
    return table
