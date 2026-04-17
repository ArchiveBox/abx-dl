#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE_DIR="$(cd "${REPO_DIR}/.." && pwd)"
cd "${REPO_DIR}"

TAG_PREFIX="v"
PYPI_PACKAGE="abx-dl"

source_optional_env() {
    if [[ -f "${REPO_DIR}/.env" ]]; then
        set -a
        # shellcheck disable=SC1091
        source "${REPO_DIR}/.env"
        set +a
    fi
}

repo_slug() {
    python3 - <<'PY'
import re
import subprocess

remote = subprocess.check_output(
    ['git', 'remote', 'get-url', 'origin'],
    text=True,
).strip()

patterns = [
    r'github\.com[:/](?P<slug>[^/]+/[^/.]+)(?:\.git)?$',
    r'github\.com/(?P<slug>[^/]+/[^/.]+)(?:\.git)?$',
]

for pattern in patterns:
    match = re.search(pattern, remote)
    if match:
        print(match.group('slug'))
        raise SystemExit(0)

raise SystemExit(f'Unable to parse GitHub repo slug from remote: {remote}')
PY
}

default_branch() {
    if [[ -n "${DEFAULT_BRANCH:-}" ]]; then
        echo "${DEFAULT_BRANCH}"
        return 0
    fi
    if git symbolic-ref refs/remotes/origin/HEAD >/dev/null 2>&1; then
        git symbolic-ref refs/remotes/origin/HEAD | sed 's#^refs/remotes/origin/##'
        return 0
    fi
    git remote show origin | sed -n '/HEAD branch/s/.*: //p' | head -n 1
}

current_version() {
    python3 - <<'PY'
from pathlib import Path
import re

text = Path('pyproject.toml').read_text()
match = re.search(r'^version = "([^"]+)"$', text, re.MULTILINE)
if not match:
    raise SystemExit('Failed to find version in pyproject.toml')
print(match.group(1))
PY
}

bump_version() {
    python3 - <<'PY'
from pathlib import Path
import re

text = Path('pyproject.toml').read_text()
match = re.search(r'^version = "([^"]+)"$', text, re.MULTILINE)
if not match:
    raise SystemExit('Failed to find version in pyproject.toml')

major, minor, patch = [int(part) for part in match.group(1).split('.')]
next_version = f'{major}.{minor}.{patch + 1}'

Path('pyproject.toml').write_text(
    re.sub(r'^version = "[^"]+"$', f'version = "{next_version}"', text, count=1, flags=re.MULTILINE)
)
print(next_version)
PY
}

read_repo_version() {
    local repo_dir="$1"
    if [[ ! -f "${repo_dir}/pyproject.toml" ]]; then
        return 1
    fi

    python3 - "${repo_dir}/pyproject.toml" <<'PY'
from pathlib import Path
import re
import sys

text = Path(sys.argv[1]).read_text()
match = re.search(r'^version = "([^"]+)"$', text, re.MULTILINE)
if not match:
    raise SystemExit('Failed to find version')
print(match.group(1))
PY
}

update_internal_dependencies() {
    local abxbus_version abxpkg_version abx_plugins_version

    abxbus_version="$(read_repo_version "${WORKSPACE_DIR}/abxbus" || true)"
    abxpkg_version="$(read_repo_version "${WORKSPACE_DIR}/abxpkg" || true)"
    abx_plugins_version="$(read_repo_version "${WORKSPACE_DIR}/abx-plugins" || true)"

    python3 - "${abxbus_version}" "${abxpkg_version}" "${abx_plugins_version}" <<'PY'
from pathlib import Path
import re
import sys

path = Path('pyproject.toml')
text = path.read_text()
for name, version in (
    ('abxbus', sys.argv[1]),
    ('abxpkg', sys.argv[2]),
    ('abx-plugins', sys.argv[3]),
):
    if version:
        text = re.sub(rf'("{re.escape(name)}>=)[^"]+(")', rf'\g<1>{version}\2', text)
path.write_text(text)
PY
}

compare_versions() {
    python3 - "$1" "$2" <<'PY'
import re
import sys

def parse(version: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:rc(\d+))?', version)
    if not match:
        raise SystemExit(f'Unsupported version format: {version}')
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), int(rc) if rc is not None else 10_000)

left, right = sys.argv[1], sys.argv[2]
if parse(left) > parse(right):
    print('gt')
elif parse(left) == parse(right):
    print('eq')
else:
    print('lt')
PY
}

latest_release_version() {
    local slug="$1"
    local raw_tags
    raw_tags="$(gh api "repos/${slug}/releases?per_page=100" --jq '.[].tag_name' || true)"
    RELEASE_TAGS="${raw_tags}" TAG_PREFIX_VALUE="${TAG_PREFIX}" python3 - <<'PY'
import os
import re

def parse(version: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:rc(\d+))?', version)
    if not match:
        return (-1, -1, -1, -1)
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), int(rc) if rc is not None else 10_000)

prefix = os.environ.get('TAG_PREFIX_VALUE', '')
versions = [line.strip() for line in os.environ.get('RELEASE_TAGS', '').splitlines() if line.strip()]
if prefix:
    versions = [version[len(prefix):] if version.startswith(prefix) else version for version in versions]
if not versions:
    print('')
else:
    print(max(versions, key=parse))
PY
}

wait_for_runs() {
    local slug="$1"
    local event="$2"
    local sha="$3"
    local label="$4"
    local runs_json
    local attempts=0

    while :; do
        runs_json="$(GH_FORCE_TTY=0 GH_PAGER=cat gh run list --repo "${slug}" --event "${event}" --commit "${sha}" --limit 20 --json databaseId,status,conclusion,workflowName)"
        if [[ "$(jq 'length' <<<"${runs_json}")" -gt 0 ]]; then
            break
        fi
        attempts=$((attempts + 1))
        if [[ "${attempts}" -ge 30 ]]; then
            echo "Timed out waiting for ${label} workflows to start" >&2
            return 1
        fi
        sleep 10
    done

    while read -r run_id; do
        gh run watch "${run_id}" --repo "${slug}" --exit-status
    done < <(jq -r '.[].databaseId' <<<"${runs_json}")
}

wait_for_pypi() {
    local package_name="$1"
    local expected_version="$2"
    local attempts=0
    local published_version

    while :; do
        published_version="$(curl -fsSL "https://pypi.org/pypi/${package_name}/json" | jq -r '.info.version')"
        if [[ "${published_version}" == "${expected_version}" ]]; then
            return 0
        fi
        attempts=$((attempts + 1))
        if [[ "${attempts}" -ge 30 ]]; then
            echo "Timed out waiting for ${package_name}==${expected_version} on PyPI" >&2
            return 1
        fi
        sleep 10
    done
}

run_checks() {
    uv sync --all-extras --all-groups --no-cache --upgrade
    uv run prek run --all-files
    uv build
}

validate_release_state() {
    local slug="$1"
    local branch="$2"
    local current latest relation

    if [[ "$(git branch --show-current)" != "${branch}" ]]; then
        echo "Skipping release-state validation on non-default branch $(git branch --show-current)"
        return 0
    fi

    current="$(current_version)"
    latest="$(latest_release_version "${slug}")"
    if [[ -z "${latest}" ]]; then
        echo "No published releases found for ${slug}; release state is valid"
        return 0
    fi

    relation="$(compare_versions "${current}" "${latest}")"
    if [[ "${relation}" == "lt" ]]; then
        echo "Current version ${current} is behind latest published version ${latest}" >&2
        return 1
    fi

    echo "Release state is valid: local=${current} latest=${latest}"
}

create_release() {
    local slug="$1"
    local version="$2"
    if gh release view "${TAG_PREFIX}${version}" --repo "${slug}" >/dev/null 2>&1; then
        echo "GitHub release ${TAG_PREFIX}${version} already exists"
        return 0
    fi
    gh release create "${TAG_PREFIX}${version}" \
        --repo "${slug}" \
        --target "$(git rev-parse HEAD)" \
        --title "${TAG_PREFIX}${version}" \
        --generate-notes
}

publish_artifacts() {
    local version="$1"
    local pypi_token="${UV_PUBLISH_TOKEN:-${PYPI_TOKEN:-${PYPI_PAT_SECRET:-}}}"

    if curl -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" | jq -e --arg version "${version}" '.releases[$version] | length > 0' >/dev/null 2>&1; then
        echo "${PYPI_PACKAGE} ${version} already published on PyPI"
    else
        if [[ -n "${pypi_token}" ]]; then
            UV_PUBLISH_TOKEN="${pypi_token}" uv publish --username=__token__ dist/*
        else
            echo "Missing PyPI credentials: set UV_PUBLISH_TOKEN or PYPI_TOKEN" >&2
            return 1
        fi
    fi

    wait_for_pypi "${PYPI_PACKAGE}" "${version}"
}

main() {
    local slug branch version latest relation

    source_optional_env
    slug="$(repo_slug)"
    branch="$(default_branch)"

    if [[ "${GITHUB_EVENT_NAME:-}" == "push" ]]; then
        validate_release_state "${slug}" "${branch}"
        return 0
    fi

    if [[ "$(git branch --show-current)" != "${branch}" ]]; then
        echo "Release must run from ${branch}, found $(git branch --show-current)" >&2
        return 1
    fi

    version="$(current_version)"
    latest="$(latest_release_version "${slug}")"
    if [[ -z "${latest}" ]]; then
        relation="gt"
    else
        relation="$(compare_versions "${version}" "${latest}")"
    fi

    if [[ "${relation}" == "eq" ]]; then
        update_internal_dependencies
        version="$(bump_version)"
        run_checks

        git add -A
        git commit -m "release: ${TAG_PREFIX}${version}"
        git push origin "${branch}"

        wait_for_runs "${slug}" push "$(git rev-parse HEAD)" "push"
    elif [[ "${relation}" == "gt" ]]; then
        if [[ -n "$(git status --short)" ]]; then
            echo "Refusing to publish existing unreleased version ${version} with a dirty worktree" >&2
            return 1
        fi
        run_checks
        wait_for_runs "${slug}" push "$(git rev-parse HEAD)" "push"
    else
        echo "Current version ${version} is behind latest GitHub release ${latest}" >&2
        return 1
    fi

    publish_artifacts "${version}"
    create_release "${slug}" "${version}"

    latest="$(latest_release_version "${slug}")"
    relation="$(compare_versions "${latest}" "${version}")"
    if [[ "${relation}" != "eq" ]]; then
        echo "GitHub release version mismatch: expected ${version}, got ${latest}" >&2
        return 1
    fi

    echo "Released ${PYPI_PACKAGE} ${version}"
}

main "$@"
