"""Helpers for collecting output file metadata without reading file contents."""

from __future__ import annotations

import mimetypes
from pathlib import Path
from collections.abc import Iterable

from pydantic import BaseModel


for strict in (True, False):
    mimetypes.add_type("application/warc", ".warc", strict=strict)


class OutputFile(BaseModel):
    """Metadata for a file emitted by a hook."""

    path: str
    extension: str = ""
    mimetype: str = ""
    size: int = 0


def guess_mimetype(path: str | Path) -> str:
    """Guess a file mimetype from its path without reading the file contents."""
    path_str = path.as_posix() if isinstance(path, Path) else str(path)
    mimetype, encoding = mimetypes.guess_type(path_str, strict=False)
    if mimetype:
        return mimetype

    if encoding == "gzip":
        inner_path = Path(path_str).with_suffix("")
        inner_mimetype, _inner_encoding = mimetypes.guess_type(inner_path.as_posix(), strict=False)
        if inner_mimetype:
            return inner_mimetype
        return "application/gzip"

    return ""


def output_file_from_path(file_path: Path, *, relative_to: Path) -> OutputFile:
    """Build OutputFile metadata from a file path using stat + extension lookup."""
    rel_path = file_path.relative_to(relative_to)
    try:
        size = file_path.stat().st_size
    except OSError:
        size = 0
    return OutputFile(
        path=str(rel_path),
        extension=file_path.suffix.lower().lstrip("."),
        mimetype=guess_mimetype(rel_path),
        size=size,
    )


def collect_output_files(output_dir: Path) -> list[OutputFile]:
    """Collect metadata for all non-symlink files under an output directory."""
    if not output_dir.is_dir():
        return []

    output_files = [
        output_file_from_path(file_path, relative_to=output_dir)
        for file_path in output_dir.rglob("*")
        if file_path.is_file() and not file_path.is_symlink()
    ]
    output_files.sort(key=lambda output_file: output_file.path)
    return output_files


def collect_output_files_from_paths(output_dir: Path, file_paths: Iterable[Path]) -> list[OutputFile]:
    """Collect metadata for a specific set of non-symlink files under an output directory."""
    output_files = [
        output_file_from_path(file_path, relative_to=output_dir)
        for file_path in file_paths
        if file_path.is_file() and not file_path.is_symlink()
    ]
    output_files.sort(key=lambda output_file: output_file.path)
    return output_files
