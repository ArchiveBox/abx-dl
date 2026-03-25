"""Helpers for collecting output file metadata without reading file contents."""

from __future__ import annotations

import mimetypes
import stat
from pathlib import Path
from collections.abc import Iterable

from pydantic import BaseModel


for strict in (True, False):
    mimetypes.add_type("application/warc", ".warc", strict=strict)


OUTPUT_FILE_METADATA_SUFFIXES = (".stdout.log", ".stderr.log", ".log", ".pid", ".sh")


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
        size = file_path.lstat().st_size
    except OSError:
        size = 0
    return OutputFile(
        path=str(rel_path),
        extension=file_path.suffix.lower().lstrip("."),
        mimetype=guess_mimetype(rel_path),
        size=size,
    )


def scan_output_files(output_dir: Path, file_paths: Iterable[Path] | None = None) -> list[OutputFile]:
    """Collect metadata for real hook output files, excluding process artifacts."""
    if not output_dir.is_dir():
        return []

    paths = output_dir.rglob("*") if file_paths is None else file_paths
    output_files = []
    for file_path in paths:
        try:
            stat_result = file_path.lstat()
        except OSError:
            continue
        if stat.S_ISLNK(stat_result.st_mode) or not stat.S_ISREG(stat_result.st_mode):
            continue
        relative_path = file_path.relative_to(output_dir).as_posix()
        if any(relative_path.endswith(suffix) for suffix in OUTPUT_FILE_METADATA_SUFFIXES):
            continue
        output_files.append(output_file_from_path(file_path, relative_to=output_dir))

    output_files.sort(key=lambda output_file: output_file.path)
    return output_files
