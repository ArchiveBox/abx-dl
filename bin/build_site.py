# /// script
# requires-python = ">=3.12"
# dependencies = ["markdown==3.8.2"]
# ///
"""Build the GitHub Pages site directly from README.md: uv run bin/build_site.py."""

from pathlib import Path
import re
import shutil

import markdown

ROOT = Path(__file__).resolve().parent.parent
readme = ROOT.joinpath("README.md").read_text()
# Documentation-only test setup stays hidden, just as it does on GitHub.
readme = re.sub(r"<!--.*?-->", "", readme, flags=re.DOTALL)
content = markdown.markdown(readme, extensions=["fenced_code", "tables", "toc", "sane_lists"])
# README-relative file links should lead to source, not nonexistent Pages paths.
content = re.sub(r'(href|src)="\./([^\"]+)"', r'\1="https://github.com/ArchiveBox/abx-dl/blob/main/\2"', content)
output = ROOT / "dist" / "site"
output.mkdir(parents=True, exist_ok=True)
output.joinpath("index.html").write_text(ROOT.joinpath("website/index.html").read_text().replace("<!-- README -->", content))
output.joinpath("style.css").write_text(ROOT.joinpath("website/style.css").read_text())
shutil.copytree(ROOT / "website/assets", output / "assets", dirs_exist_ok=True)
output.joinpath(".nojekyll").touch()
print(f"Built {output / 'index.html'}")
