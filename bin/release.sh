#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_DIR}"

TAG_PREFIX="v"
PYPI_PACKAGE="abx-dl"
ARTIFACT_DIR_TO_CLEAN=""

cleanup_artifact_dir() {
    if [[ -n "${ARTIFACT_DIR_TO_CLEAN}" ]]; then
        ARTIFACT_DIR_TO_CLEAN="${ARTIFACT_DIR_TO_CLEAN}" "${UV_BINARY}" run --no-project python - <<'PY'
import os
import shutil

shutil.rmtree(os.environ["ARTIFACT_DIR_TO_CLEAN"])
PY
    fi
}

trap cleanup_artifact_dir EXIT

source_optional_env() {
    if [[ -f "${REPO_DIR}/.env" ]]; then
        set -a
        # shellcheck disable=SC1091
        source "${REPO_DIR}/.env"
        set +a
    fi
}

repo_slug() { "${GH_BINARY}" repo view --json nameWithOwner --jq .nameWithOwner; }

current_version() {
    "${UV_BINARY}" run --no-project python - <<'PY'
from pathlib import Path
import re
match = re.search(r'^version = "([^"]+)"$', Path('pyproject.toml').read_text(), re.MULTILINE)
if not match:
    raise SystemExit('Failed to find version in pyproject.toml')
print(match.group(1))
PY
}

compare_versions() {
    "${UV_BINARY}" run --no-project python - "$1" "$2" <<'PY'
import re, sys
def parse(version):
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:-?rc(\d+))?', version)
    if not match:
        raise SystemExit(f'Unsupported version format: {version}')
    major, minor, patch, rc = match.groups()
    return int(major), int(minor), int(patch), 0 if rc is not None else 1, int(rc or 0)
left, right = map(parse, sys.argv[1:3])
print('gt' if left > right else 'eq' if left == right else 'lt')
PY
}

latest_published_version() {
    local slug="$1" pypi_json github_json
    pypi_json="$("${CURL_BINARY}" -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json")"
    github_json="$("${GH_BINARY}" api "repos/${slug}/releases?per_page=100")"
    PYPI_JSON="${pypi_json}" GITHUB_JSON="${github_json}" TAG_PREFIX="${TAG_PREFIX}" "${UV_BINARY}" run --no-project python - <<'PY'
import json, os, re
def parse(version):
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:-?rc(\d+))?', version)
    if not match:
        return -1, -1, -1, -1, -1
    major, minor, patch, rc = match.groups()
    return int(major), int(minor), int(patch), 0 if rc is not None else 1, int(rc or 0)
versions = set(json.loads(os.environ['PYPI_JSON'])["releases"])
versions.update(
    release["tag_name"].removeprefix(os.environ["TAG_PREFIX"])
    for release in json.loads(os.environ["GITHUB_JSON"])
)
versions = [version for version in versions if parse(version)[0] >= 0]
print(max(versions, key=parse) if versions else '')
PY
}

pypi_has_version() {
    # shellcheck disable=SC2016
    "${CURL_BINARY}" -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" \
        | "${JQ_BINARY}" -e --arg version "$1" '.releases[$version] | length > 0' >/dev/null
}

tag_target() {
    local tag="$1" output target
    output="$("${GIT_BINARY}" ls-remote origin "refs/tags/${tag}^{}")"
    target="${output%%[[:space:]]*}"
    if [[ -z "${target}" ]]; then
        output="$("${GIT_BINARY}" ls-remote origin "refs/tags/${tag}")"
        target="${output%%[[:space:]]*}"
    fi
    printf '%s\n' "${target}"
}

github_release_has_version() { "${GH_BINARY}" release view "${TAG_PREFIX}$1" --repo "$2" >/dev/null 2>&1; }

verify_existing_tag() {
    local tag="${TAG_PREFIX}$1" sha="$2" target
    target="$(tag_target "${tag}")"
    if [[ -n "${target}" && "${target}" != "${sha}" ]]; then
        echo "Tag ${tag} points to ${target}, not release SHA ${sha}" >&2
        return 1
    fi
}

require_clean_exact_checkout() {
    local sha="$1" branch="${RELEASE_BRANCH:-main}"
    [[ "${sha}" =~ ^[0-9a-f]{40}$ ]] || { echo "RELEASE_SHA must be a full commit SHA" >&2; return 1; }
    [[ "$("${GIT_BINARY}" rev-parse HEAD)" == "${sha}" ]] || { echo "HEAD does not match RELEASE_SHA ${sha}" >&2; return 1; }
    [[ -z "$("${GIT_BINARY}" status --short)" ]] || { echo "Refusing to release from a dirty worktree" >&2; return 1; }
    "${GIT_BINARY}" fetch --quiet --no-tags origin "+refs/heads/${branch}:refs/remotes/origin/${branch}"
    "${GIT_BINARY}" merge-base --is-ancestor "${sha}" "refs/remotes/origin/${branch}" || { echo "${sha} is not on ${branch}" >&2; return 1; }
}

download_tested_python_artifacts() {
    local slug="$1" run_id="$2" sha="$3" version="$4" destination="$5"
    local artifact_name="python-dist-${sha}"
    ARTIFACT_DIR="${destination}" "${UV_BINARY}" run --no-project python -c 'import os; from pathlib import Path; Path(os.environ["ARTIFACT_DIR"]).mkdir(parents=True, exist_ok=True)'
    "${GH_BINARY}" run download "${run_id}" --repo "${slug}" --name "${artifact_name}" --dir "${destination}"

    ARTIFACT_DIR="${destination}" EXPECTED_VERSION="${version}" "${UV_BINARY}" run --no-project python - <<'PY'
import hashlib
import os
import re
from pathlib import Path

artifact_dir = Path(os.environ["ARTIFACT_DIR"])
version = os.environ["EXPECTED_VERSION"]
wheels = sorted(artifact_dir.glob("*.whl"))
sdists = sorted(artifact_dir.glob("*.tar.gz"))
if len(wheels) != 1 or len(sdists) != 1:
    raise SystemExit(f"Expected one tested wheel and one tested sdist, got: {sorted(artifact_dir.iterdir())}")
normalized = re.escape(version.replace("-", "_"))
for artifact in (*wheels, *sdists):
    if not re.match(rf"abx[_-]dl-{normalized}(?:-|\.)", artifact.name):
        raise SystemExit(f"Artifact version does not match {version}: {artifact.name}")

expected = {}
for line in (artifact_dir / "SHA256SUMS").read_text().splitlines():
    digest, filename = line.split(maxsplit=1)
    expected[filename] = digest
for artifact in (*wheels, *sdists):
    actual = hashlib.sha256(artifact.read_bytes()).hexdigest()
    if expected.get(artifact.name) != actual:
        raise SystemExit(f"Checksum mismatch for tested artifact: {artifact.name}")
PY
}

publish_to_pypi() {
    local artifact_dir="$1"
    "${UV_BINARY}" publish --trusted-publishing always "${artifact_dir}"/*.whl "${artifact_dir}"/*.tar.gz
}

create_release() {
    local slug="$1" version="$2" sha="$3"
    local release_args=()
    if github_release_has_version "${version}" "${slug}"; then
        verify_existing_tag "${version}" "${sha}"
        return 0
    fi
    verify_existing_tag "${version}" "${sha}"
    if [[ "${version}" =~ rc[0-9]+$ ]]; then
        release_args+=(--prerelease)
    fi
    "${GH_BINARY}" release create "${TAG_PREFIX}${version}" --repo "${slug}" --target "${sha}" --title "${TAG_PREFIX}${version}" --generate-notes "${release_args[@]}"
}

main() {
    local slug version latest release_sha target artifact_dir ci_run_id pypi_exists=false github_exists=false
    source_optional_env
    slug="$(repo_slug)"
    version="$(current_version)"
    release_sha="${RELEASE_SHA:-$("${GIT_BINARY}" rev-parse HEAD)}"
    require_clean_exact_checkout "${release_sha}"
    latest="$(latest_published_version "${slug}")"
    if [[ -n "${latest}" && "$(compare_versions "${version}" "${latest}")" == "lt" ]]; then
        echo "Source version ${version} is behind published version ${latest}" >&2
        return 1
    fi
    target="$(tag_target "${TAG_PREFIX}${version}")"
    pypi_has_version "${version}" && pypi_exists=true
    github_release_has_version "${version}" "${slug}" && github_exists=true
    if [[ "${pypi_exists}" == true && "${github_exists}" == true && -n "${target}" ]]; then
        "${GIT_BINARY}" merge-base --is-ancestor "${target}" "refs/remotes/origin/${RELEASE_BRANCH:-main}" || {
            echo "Fully published tag ${TAG_PREFIX}${version} is not on ${RELEASE_BRANCH:-main}" >&2
            return 1
        }
    fi
    if [[ "${github_exists}" == true && "${target}" != "${release_sha}" ]]; then
        echo "Cannot recover partial release ${version}: no tag anchors it to ${release_sha}" >&2
        return 1
    fi
    if [[ "${pypi_exists}" == true && -n "${target}" && "${target}" != "${release_sha}" ]]; then
        echo "Cannot recover partial release ${version}: tag does not point to ${release_sha}" >&2
        return 1
    fi
    ci_run_id="${CI_RUN_ID:-}"
    [[ "${ci_run_id}" =~ ^[0-9]+$ ]] || { echo "CI_RUN_ID must identify the successful CI workflow run" >&2; return 1; }
    artifact_dir="$("${UV_BINARY}" run --no-project python -c 'import tempfile; print(tempfile.mkdtemp())')"
    ARTIFACT_DIR_TO_CLEAN="${artifact_dir}"
    download_tested_python_artifacts "${slug}" "${ci_run_id}" "${release_sha}" "${version}" "${artifact_dir}"
    [[ "${pypi_exists}" == true ]] || publish_to_pypi "${artifact_dir}"
    create_release "${slug}" "${version}" "${release_sha}"
    "${GH_BINARY}" release upload "${TAG_PREFIX}${version}" --repo "${slug}" \
        "${artifact_dir}"/*.whl "${artifact_dir}"/*.tar.gz "${artifact_dir}"/SHA256SUMS --clobber
    echo "Released ${PYPI_PACKAGE} ${version} from ${release_sha}"
}

main "$@"
