"""Helpers for collecting output file metadata without reading file contents."""

from __future__ import annotations

import mimetypes
import os
import stat
from pathlib import Path
from collections.abc import Iterable

from pydantic import BaseModel


for strict in (True, False):
    mimetypes.add_type("application/warc", ".warc", strict=strict)


OUTPUT_FILE_METADATA_SUFFIXES = (".stdout.log", ".stderr.log", ".log", ".pid", ".sh")

EXECUTABLE_BITS = 0o111
BROKEN_SYMLINK_SUFFIX = ".broken-symlink.txt"


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
    """Collect metadata for real hook output files, excluding process artifacts.

    Also sanitizes the tree as it walks: strips +x from regular files, and
    replaces symlinks whose target escapes ``output_dir`` with a plain-text
    ``{name}.broken-symlink.txt`` sibling holding the original target string.
    Both operations are naturally idempotent.
    """
    if not output_dir.is_dir():
        return []

    try:
        containment_root = output_dir.resolve()
    except OSError:
        containment_root = output_dir

    paths = output_dir.rglob("*") if file_paths is None else file_paths
    output_files = []
    for file_path in paths:
        try:
            stat_result = file_path.lstat()
        except OSError:
            continue
        if stat.S_ISLNK(stat_result.st_mode):
            _neutralize_escaping_symlink(file_path, containment_root)
            continue
        if not stat.S_ISREG(stat_result.st_mode):
            continue
        if stat_result.st_mode & EXECUTABLE_BITS:
            try:
                file_path.chmod(stat_result.st_mode & ~EXECUTABLE_BITS)
            except OSError:
                pass
        relative_path = file_path.relative_to(output_dir).as_posix()
        if any(relative_path.endswith(suffix) for suffix in OUTPUT_FILE_METADATA_SUFFIXES):
            continue
        output_files.append(output_file_from_path(file_path, relative_to=output_dir))

    output_files.sort(key=lambda output_file: output_file.path)
    return output_files


def _neutralize_escaping_symlink(link_path: Path, containment_root: Path) -> None:
    """Replace a symlink whose target escapes containment_root with a text record.

    Symlinks that resolve inside containment_root are left alone. Escaping links
    are deleted and a sibling ``{name}.broken-symlink.txt`` holding the original
    target string is written in their place — breaking the traversal while
    preserving the forensic reference.
    """
    try:
        target = os.readlink(link_path)
    except OSError:
        return
    try:
        resolved = link_path.resolve(strict=False)
    except OSError:
        return
    if resolved == containment_root or resolved.is_relative_to(containment_root):
        return

    record_path = link_path.with_name(link_path.name + BROKEN_SYMLINK_SUFFIX)
    try:
        link_path.unlink()
    except OSError:
        return
    try:
        record_path.write_text(str(target) + "\n", encoding="utf-8")
    except OSError:
        pass
