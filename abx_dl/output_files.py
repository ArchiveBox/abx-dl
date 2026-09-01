"""Helpers for collecting output file metadata without reading file contents."""

from __future__ import annotations

import mimetypes
import os
import stat
from pathlib import Path
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


for strict in (True, False):
    mimetypes.add_type("application/warc", ".warc", strict=strict)


OUTPUT_FILE_METADATA_SUFFIXES = (".stdout.log", ".stderr.log", ".log", ".pid", ".sh")

EXECUTABLE_BITS = 0o111
BROKEN_SYMLINK_SUFFIX = ".broken-symlink.txt"


class OutputFile(BaseModel):
    """Metadata for a file emitted by a hook."""

    model_config = ConfigDict(extra="allow")

    path: str
    extension: str = ""
    mimetype: str = ""
    size: int = 0


class OutputManifest(BaseModel):
    """Canonical normalized metadata for one hook output directory."""

    files: list[OutputFile] = Field(default_factory=list)
    total_size: int = 0
    mimetypes: list[str] = Field(default_factory=list)

    @classmethod
    def from_files(cls, files: Iterable[OutputFile | Mapping[str, Any]]) -> OutputManifest:
        normalized = [item if isinstance(item, OutputFile) else OutputFile.model_validate(item) for item in files]
        normalized.sort(key=lambda item: item.path)
        mime_sizes: dict[str, int] = defaultdict(int)
        total_size = 0
        for output_file in normalized:
            size = max(int(output_file.size or 0), 0)
            total_size += size
            if output_file.mimetype:
                mime_sizes[output_file.mimetype] += size
        mimetypes = [name for name, _size in sorted(mime_sizes.items(), key=lambda item: (-item[1], item[0]))]
        return cls(files=normalized, total_size=total_size, mimetypes=mimetypes)

    @classmethod
    def from_value(cls, value: Any) -> OutputManifest:
        if value is None:
            return cls()
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            import json

            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                # Callers also pass a single unencoded path. Preserve it as one
                # file instead of silently turning malformed JSON into no output.
                pass
            if isinstance(value, str):
                value = [value]
        if isinstance(value, Mapping):
            files = []
            for path, metadata in value.items():
                payload = dict(metadata) if isinstance(metadata, Mapping) else {}
                payload["path"] = str(path)
                payload.setdefault("extension", Path(str(path)).suffix.lower().lstrip("."))
                payload.setdefault("mimetype", guess_mimetype(str(path)))
                files.append(payload)
            return cls.from_files(files)
        if isinstance(value, Iterable):
            files = []
            for item in value:
                if isinstance(item, str):
                    files.append({"path": item, "extension": Path(item).suffix.lower().lstrip("."), "mimetype": guess_mimetype(item)})
                elif isinstance(item, OutputFile):
                    files.append(item)
                elif isinstance(item, Mapping) and item.get("path"):
                    path = str(item["path"])
                    payload = dict(item)
                    payload["path"] = path
                    payload.setdefault("extension", Path(path).suffix.lower().lstrip("."))
                    payload.setdefault("mimetype", guess_mimetype(path))
                    files.append(payload)
            return cls.from_files(files)
        return cls()

    @classmethod
    def scan(
        cls,
        output_dir: Path,
        file_paths: Iterable[Path] | None = None,
        *,
        containment_root: Path | None = None,
    ) -> OutputManifest:
        return cls.from_files(scan_output_files(output_dir, file_paths, containment_root=containment_root))

    def as_mapping(self) -> dict[str, dict[str, Any]]:
        return {output_file.path: output_file.model_dump(exclude={"path"}) for output_file in self.files}


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


def scan_output_files(
    output_dir: Path,
    file_paths: Iterable[Path] | None = None,
    *,
    containment_root: Path | None = None,
) -> list[OutputFile]:
    """Collect metadata for real hook output files, excluding process artifacts.

    Also sanitizes the tree as it walks: strips +x from regular files, and
    replaces symlinks whose target escapes ``containment_root`` with a plain-text
    ``{name}.broken-symlink.txt`` sibling holding the original target string.
    Both operations are naturally idempotent.
    """
    if not output_dir.is_dir():
        return []

    try:
        containment_root = (containment_root or output_dir).resolve()
    except OSError:
        containment_root = containment_root or output_dir

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
        if ".hooks" in file_path.relative_to(output_dir).parts:
            continue
        if stat_result.st_mode & EXECUTABLE_BITS:
            try:
                file_path.chmod(stat_result.st_mode & ~EXECUTABLE_BITS)
            except OSError:
                # Best-effort: if chmod fails (read-only FS, permission denied),
                # leave the bit set rather than failing the whole scan.
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
        # Forensic record is best-effort; the symlink itself has already been
        # removed above, so sanitization has succeeded even if we can't write.
        pass
