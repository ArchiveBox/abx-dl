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
