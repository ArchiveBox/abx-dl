import os
import stat
from pathlib import Path

from abx_dl.output_files import scan_output_files


def test_scan_output_files_excludes_symlinked_files_and_dirs(tmp_path: Path) -> None:
    (tmp_path / "real.txt").write_text("hello")
    (tmp_path / "real.txt.link").symlink_to(tmp_path / "real.txt")

    target_dir = tmp_path / "target-dir"
    target_dir.mkdir()
    (target_dir / "nested.txt").write_text("world")
    (tmp_path / "linked-dir").symlink_to(target_dir, target_is_directory=True)

    output_files = scan_output_files(tmp_path)

    assert [output_file.path for output_file in output_files] == ["real.txt", "target-dir/nested.txt"]
    assert sum(output_file.size for output_file in output_files) == len("hello") + len("world")


def test_scan_output_files_strips_executable_bits(tmp_path: Path) -> None:
    script = tmp_path / "script.sh.x"
    script.write_text("#!/bin/sh\necho hi\n")
    script.chmod(0o755)

    scan_output_files(tmp_path)

    assert not stat.S_IMODE(script.lstat().st_mode) & 0o111


def test_scan_output_files_neutralizes_symlink_that_escapes(tmp_path: Path) -> None:
    snap_dir = tmp_path / "snap"
    snap_dir.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret")

    escaping_link = snap_dir / "leak"
    escaping_link.symlink_to(outside)

    scan_output_files(snap_dir)

    assert not escaping_link.exists() and not escaping_link.is_symlink()
    record = snap_dir / "leak.broken-symlink.txt"
    assert record.is_file() and not record.is_symlink()
    assert record.read_text().strip() == str(outside)


def test_scan_output_files_leaves_internal_symlinks_alone(tmp_path: Path) -> None:
    inside = tmp_path / "real.txt"
    inside.write_text("hi")
    internal_link = tmp_path / "alias"
    internal_link.symlink_to(inside)

    scan_output_files(tmp_path)

    assert internal_link.is_symlink()
    assert os.readlink(internal_link) == str(inside)
    assert not (tmp_path / "alias.broken-symlink").exists()


def test_scan_output_files_symlink_neutralization_is_idempotent(tmp_path: Path) -> None:
    outside = tmp_path / "outside.txt"
    outside.write_text("x")
    snap_dir = tmp_path / "snap"
    snap_dir.mkdir()
    (snap_dir / "leak").symlink_to(outside)

    scan_output_files(snap_dir)
    scan_output_files(snap_dir)

    record = snap_dir / "leak.broken-symlink.txt"
    assert record.is_file() and not record.is_symlink()
    assert record.read_text().strip() == str(outside)
    assert not (snap_dir / "leak").exists()
