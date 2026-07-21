#!/usr/bin/env bash

set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ "$#" -ne 1 ]]; then
    echo "Usage: $0 <version>" >&2
    exit 2
fi

uv run python - "$1" <<'PY'
from pathlib import Path
import re
import sys

version = sys.argv[1]
if not re.fullmatch(r'\d+\.\d+\.\d+(?:-?rc\d+)?', version):
    raise SystemExit(f'Unsupported version format: {version}')
path = Path('pyproject.toml')
text = path.read_text()
match = re.search(r'^version = "([^"]+)"$', text, re.MULTILINE)
if not match:
    raise SystemExit('Failed to find version in pyproject.toml')
def parse(value):
    base, _, rc = value.replace('-rc', 'rc').partition('rc')
    major, minor, patch = map(int, base.split('.'))
    return major, minor, patch, 0 if rc else 1, int(rc or 0)
if parse(version) <= parse(match.group(1)):
    raise SystemExit(f'New version {version} must be greater than {match.group(1)}')
updated, count = re.subn(r'^version = "[^"]+"$', f'version = "{version}"', text, count=1, flags=re.MULTILINE)
if count != 1:
    raise SystemExit('Failed to update version in pyproject.toml')
path.write_text(updated)
print(version)
PY
