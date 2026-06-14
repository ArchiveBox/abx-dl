from __future__ import annotations

from pathlib import Path

from pdm.backend.hooks import Context


def _git_dir(root: Path) -> Path:
    git_dir = root / ".git"
    if git_dir.is_file():
        gitdir_path = git_dir.read_text().strip().removeprefix("gitdir:").strip()
        return Path(gitdir_path) if Path(gitdir_path).is_absolute() else root / gitdir_path
    return git_dir


def _add_git_file(context: Context, files: dict[str, Path], git_dir: Path, relpath: str) -> None:
    src = git_dir / relpath
    if not src.exists():
        return

    # Keep the packaged runtime metadata as a tiny .git subset so the same
    # commit resolver works in dev checkouts, sdists, wheels, and Docker images.
    dst = context.ensure_build_dir() / ".git" / relpath
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(src.read_text(), encoding="utf-8")
    files[f".git/{relpath}"] = dst


def pdm_build_update_files(context: Context, files: dict[str, Path]) -> None:
    git_dir = _git_dir(context.root)
    _add_git_file(context, files, git_dir, "HEAD")

    try:
        head = (git_dir / "HEAD").read_text().strip()
    except Exception:
        return

    if not head.startswith("ref: "):
        return

    ref = head.removeprefix("ref: ").strip()
    _add_git_file(context, files, git_dir, ref)
