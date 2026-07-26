#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

SCRIPT_DIR="${BASH_SOURCE[0]%/*}"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_DIR}"

TAG_PREFIX="v"
PYPI_PACKAGE="abx-dl"
ARTIFACT_DIR_TO_CLEAN=""
VERIFY_DIR_TO_CLEAN=""

require_release_binaries() {
    local key value expected_bin
    expected_bin="${ABXPKG_LIB_DIR:?ABXPKG_LIB_DIR is required}/env/bin"
    for key in GH_BINARY GIT_BINARY CURL_BINARY JQ_BINARY UV_BINARY; do
        value="${!key:-}"
        [[ -n "${value}" && -x "${value}" && "${value%/*}" == "${expected_bin}" ]] || {
            echo "${key} must be an executable projected through ${expected_bin}" >&2
            return 1
        }
    done
}

cleanup_artifact_dir() {
    if [[ -n "${ARTIFACT_DIR_TO_CLEAN}" ]]; then
        ARTIFACT_DIR_TO_CLEAN="${ARTIFACT_DIR_TO_CLEAN}" "${UV_BINARY}" run --no-project python - <<'PY'
import os
import shutil

shutil.rmtree(os.environ["ARTIFACT_DIR_TO_CLEAN"])
PY
    fi
}

cleanup_verify_dir() {
    if [[ -n "${VERIFY_DIR_TO_CLEAN}" ]]; then
        VERIFY_DIR_TO_CLEAN="${VERIFY_DIR_TO_CLEAN}" "${UV_BINARY}" run --no-project python - <<'PY'
import os
import shutil

shutil.rmtree(os.environ["VERIFY_DIR_TO_CLEAN"])
PY
    fi
}

cleanup_release_dirs() {
    local cleanup_status=0
    cleanup_verify_dir || cleanup_status=1
    cleanup_artifact_dir || cleanup_status=1
    return "${cleanup_status}"
}

trap cleanup_release_dirs EXIT

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
    local slug="$1" pypi_versions github_tags
    pypi_versions="$("${CURL_BINARY}" -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" | "${JQ_BINARY}" -r '.releases | keys[]')"
    github_tags="$("${GH_BINARY}" api "repos/${slug}/releases?per_page=100" --jq '.[].tag_name')"
    PYPI_VERSIONS="${pypi_versions}" GITHUB_TAGS="${github_tags}" TAG_PREFIX="${TAG_PREFIX}" "${UV_BINARY}" run --no-project python - <<'PY'
import os, re
def parse(version):
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:-?rc(\d+))?', version)
    if not match:
        return -1, -1, -1, -1, -1
    major, minor, patch, rc = match.groups()
    return int(major), int(minor), int(patch), 0 if rc is not None else 1, int(rc or 0)
versions = set(os.environ['PYPI_VERSIONS'].splitlines())
versions.update(
    tag.removeprefix(os.environ["TAG_PREFIX"])
    for tag in os.environ["GITHUB_TAGS"].splitlines()
)
versions = [version for version in versions if parse(version)[0] >= 0]
print(max(versions, key=parse) if versions else '')
PY
}

pypi_artifact_status() {
    local version="$1" artifact_dir="$2" pypi_urls
    pypi_urls="$("${CURL_BINARY}" -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" | "${JQ_BINARY}" -c --arg version "${version}" ".releases[\$version] // []")" || return 1
    PYPI_URLS="${pypi_urls}" ARTIFACT_DIR="${artifact_dir}" EXPECTED_VERSION="${version}" "${UV_BINARY}" run --no-project python - <<'PY'
import hashlib
import json
import os
import re
from pathlib import Path

version = os.environ["EXPECTED_VERSION"]
artifact_dir = Path(os.environ["ARTIFACT_DIR"])
expected_names = {
    f"abx_dl-{version}-py3-none-any.whl",
    f"abx_dl-{version}.tar.gz",
}
manifest = {}
for line in (artifact_dir / "SHA256SUMS").read_text().splitlines():
    digest, filename = line.split(maxsplit=1)
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or Path(filename).name != filename:
        raise SystemExit(f"Invalid checksum entry: {line}")
    if filename in manifest:
        raise SystemExit(f"Duplicate checksum entry: {filename}")
    manifest[filename] = digest
if set(manifest) != expected_names:
    raise SystemExit("Checksum manifest must name the exact wheel and sdist")
for filename, digest in manifest.items():
    if hashlib.sha256((artifact_dir / filename).read_bytes()).hexdigest() != digest:
        raise SystemExit(f"Tested artifact digest mismatch for {filename}")

published_files = json.loads(os.environ["PYPI_URLS"])
if not published_files:
    print("absent")
    for filename in sorted(expected_names):
        print(filename)
    raise SystemExit(0)
published = {item["filename"]: item["digests"]["sha256"] for item in published_files}
if len(published) != len(published_files) or not set(published).issubset(expected_names):
    raise SystemExit("PyPI release contains duplicate or unexpected distributions")
for filename, digest in published.items():
    if manifest[filename] != digest:
        raise SystemExit(f"PyPI digest mismatch for {filename}")

missing = sorted(expected_names - set(published))
print("partial" if missing else "complete")
for filename in missing:
    print(filename)
PY
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

github_release_metadata_is_valid() {
    local version="$1" slug="$2" release_json
    release_json="$("${GH_BINARY}" release view "${TAG_PREFIX}${version}" --repo "${slug}" --json isDraft,isPrerelease,tagName)" || return 1
    RELEASE_JSON="${release_json}" EXPECTED_VERSION="${version}" TAG_PREFIX="${TAG_PREFIX}" "${UV_BINARY}" run --no-project python - <<'PY'
import json
import os
import re

version = os.environ["EXPECTED_VERSION"]
release = json.loads(os.environ["RELEASE_JSON"])
expected_prerelease = re.search(r"rc[0-9]+$", version) is not None
if release["tagName"] != f'{os.environ["TAG_PREFIX"]}{version}':
    raise SystemExit("GitHub release tag does not match the source version")
if release["isDraft"] or release["isPrerelease"] != expected_prerelease:
    raise SystemExit("GitHub release draft/prerelease metadata is incorrect")
PY
}

github_release_has_assets() {
    local version="$1" slug="$2" assets_json verify_dir validation_status=0
    assets_json="$("${GH_BINARY}" release view "${TAG_PREFIX}${version}" --repo "${slug}" --json assets)" || return 1
    verify_dir="$("${UV_BINARY}" run --no-project python -c 'import tempfile; print(tempfile.mkdtemp())')" || return 1
    VERIFY_DIR_TO_CLEAN="${verify_dir}"
    "${GH_BINARY}" release download "${TAG_PREFIX}${version}" --repo "${slug}" --pattern SHA256SUMS --dir "${verify_dir}" || validation_status=$?
    if [[ "${validation_status}" -eq 0 ]]; then
        ASSETS_JSON="${assets_json}" VERIFY_DIR="${verify_dir}" EXPECTED_VERSION="${version}" "${UV_BINARY}" run --no-project python - <<'PY' || validation_status=$?
import json
import os
import re
from pathlib import Path

version = os.environ["EXPECTED_VERSION"]
expected_names = {
    f"abx_dl-{version}-py3-none-any.whl",
    f"abx_dl-{version}.tar.gz",
    "SHA256SUMS",
}
assets = json.loads(os.environ["ASSETS_JSON"])["assets"]
if {asset["name"] for asset in assets} != expected_names:
    raise SystemExit("Published release asset set is incomplete or contains extras")
published = {asset["name"]: asset.get("digest", "") for asset in assets}
lines = (Path(os.environ["VERIFY_DIR"]) / "SHA256SUMS").read_text().splitlines()
manifest = {}
for line in lines:
    digest, filename = line.split(maxsplit=1)
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or Path(filename).name != filename:
        raise SystemExit(f"Invalid checksum entry: {line}")
    if filename in manifest:
        raise SystemExit(f"Duplicate checksum entry: {filename}")
    manifest[filename] = digest
artifact_names = expected_names - {"SHA256SUMS"}
if set(manifest) != artifact_names:
    raise SystemExit("Published checksum manifest does not name exactly the wheel and sdist")
for filename, digest in manifest.items():
    if published[filename] != f"sha256:{digest}":
        raise SystemExit(f"Published digest mismatch for {filename}")
PY
    fi
    if ! cleanup_verify_dir; then
        [[ "${validation_status}" -ne 0 ]] || validation_status=1
    fi
    VERIFY_DIR_TO_CLEAN=""
    return "${validation_status}"
}

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
expected_artifact_names = {
    f"abx_dl-{version}-py3-none-any.whl",
    f"abx_dl-{version}.tar.gz",
}
wheels = sorted(artifact_dir.glob("*.whl"))
sdists = sorted(artifact_dir.glob("*.tar.gz"))
if len(wheels) != 1 or len(sdists) != 1:
    raise SystemExit(f"Expected one tested wheel and one tested sdist, got: {sorted(artifact_dir.iterdir())}")
artifact_names = {wheels[0].name, sdists[0].name}
if artifact_names != expected_artifact_names:
    raise SystemExit(f"Artifact names do not exactly match version {version}: {sorted(artifact_names)}")
published_names = {path.name for path in artifact_dir.iterdir() if path.is_file()}
if published_names != artifact_names | {"SHA256SUMS"}:
    raise SystemExit(f"Unexpected tested artifact set: {sorted(published_names)}")
expected = {}
for line in (artifact_dir / "SHA256SUMS").read_text().splitlines():
    fields = line.split(maxsplit=1)
    if len(fields) != 2:
        raise SystemExit(f"Invalid checksum entry: {line}")
    digest, filename = fields
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or Path(filename).name != filename:
        raise SystemExit(f"Invalid checksum entry: {line}")
    if filename in expected:
        raise SystemExit(f"Duplicate checksum entry: {filename}")
    expected[filename] = digest
if set(expected) != artifact_names:
    raise SystemExit(f"Checksum manifest names the wrong artifacts: {sorted(expected)}")
for artifact in (*wheels, *sdists):
    actual = hashlib.sha256(artifact.read_bytes()).hexdigest()
    if expected.get(artifact.name) != actual:
        raise SystemExit(f"Checksum mismatch for tested artifact: {artifact.name}")
PY
}

publish_to_pypi() {
    local artifact_dir="$1"
    shift
    local filenames=("$@") artifacts=() filename
    [[ "${#filenames[@]}" -gt 0 ]] || { echo "No missing PyPI artifacts were selected for publication" >&2; return 1; }
    for filename in "${filenames[@]}"; do
        [[ "${filename}" == "${filename##*/}" && -f "${artifact_dir}/${filename}" ]] || {
            echo "Missing tested PyPI artifact: ${filename}" >&2
            return 1
        }
        artifacts+=("${artifact_dir}/${filename}")
    done
    "${UV_BINARY}" publish --trusted-publishing always "${artifacts[@]}"
}

create_release() {
    local slug="$1" version="$2" sha="$3"
    local release_args=()
    if github_release_has_version "${version}" "${slug}"; then
        verify_existing_tag "${version}" "${sha}"
        return 0
    fi
    if [[ "${version}" =~ rc[0-9]+$ ]]; then
        release_args+=(--prerelease)
    fi
    "${GH_BINARY}" release create "${TAG_PREFIX}${version}" --repo "${slug}" --verify-tag --title "${TAG_PREFIX}${version}" --generate-notes "${release_args[@]}"
}

create_release_tag() {
    local version="$1" sha="$2" tag="${TAG_PREFIX}$1"
    verify_existing_tag "${version}" "${sha}"
    if [[ -n "$(tag_target "${tag}")" ]]; then
        return 0
    fi
    "${GIT_BINARY}" tag "${tag}" "${sha}"
    "${GIT_BINARY}" push origin "refs/tags/${tag}:refs/tags/${tag}"
    [[ "$(tag_target "${tag}")" == "${sha}" ]] || {
        echo "Tag ${tag} was not published at release SHA ${sha}" >&2
        return 1
    }
}

main() {
    local slug version latest release_sha target artifact_dir ci_run_id pypi_output pypi_state github_exists=false github_complete=false
    local pypi_lines=() pypi_missing=()
    source_optional_env
    require_release_binaries
    slug="$(repo_slug)"
    version="$(current_version)"
    release_sha="${RELEASE_SHA:-$("${GIT_BINARY}" rev-parse HEAD)}"
    require_clean_exact_checkout "${release_sha}"
    target="$(tag_target "${TAG_PREFIX}${version}")"
    ci_run_id="${CI_RUN_ID:-}"
    [[ "${ci_run_id}" =~ ^[0-9]+$ ]] || { echo "CI_RUN_ID must identify the successful CI workflow run" >&2; return 1; }
    artifact_dir="$("${UV_BINARY}" run --no-project python -c 'import tempfile; print(tempfile.mkdtemp())')"
    ARTIFACT_DIR_TO_CLEAN="${artifact_dir}"
    download_tested_python_artifacts "${slug}" "${ci_run_id}" "${release_sha}" "${version}" "${artifact_dir}"

    pypi_output="$(pypi_artifact_status "${version}" "${artifact_dir}")"
    mapfile -t pypi_lines <<< "${pypi_output}"
    pypi_state="${pypi_lines[0]}"
    pypi_missing=("${pypi_lines[@]:1}")
    [[ "${pypi_state}" == "absent" || "${pypi_state}" == "partial" || "${pypi_state}" == "complete" ]] || {
        echo "Invalid PyPI artifact state: ${pypi_state}" >&2
        return 1
    }
    if github_release_has_version "${version}" "${slug}"; then
        github_release_metadata_is_valid "${version}" "${slug}"
        github_exists=true
    fi
    if [[ "${github_exists}" == true ]] && github_release_has_assets "${version}" "${slug}"; then
        github_complete=true
    fi
    latest="$(latest_published_version "${slug}")"
    if [[ -n "${latest}" && "$(compare_versions "${version}" "${latest}")" == "lt" ]]; then
        echo "Source version ${version} is behind published version ${latest}" >&2
        return 1
    fi
    if [[ "${pypi_state}" == "complete" && "${github_complete}" == true ]]; then
        [[ -n "${target}" ]] || { echo "Fully published ${version} is missing tag ${TAG_PREFIX}${version}" >&2; return 1; }
        "${GIT_BINARY}" merge-base --is-ancestor "${target}" "refs/remotes/origin/${RELEASE_BRANCH:-main}" || {
            echo "Fully published tag ${TAG_PREFIX}${version} is not on ${RELEASE_BRANCH:-main}" >&2
            return 1
        }
        echo "${PYPI_PACKAGE} ${version} is already fully released from ${target}"
        return 0
    fi
    if [[ ( "${pypi_state}" != "absent" || "${github_exists}" == true ) && "${target}" != "${release_sha}" ]]; then
        echo "Cannot recover partial release ${version}: no tag anchors it to ${release_sha}" >&2
        return 1
    fi
    create_release_tag "${version}" "${release_sha}"
    [[ "${pypi_state}" == "complete" ]] || publish_to_pypi "${artifact_dir}" "${pypi_missing[@]}"
    create_release "${slug}" "${version}" "${release_sha}"
    "${GH_BINARY}" release upload "${TAG_PREFIX}${version}" --repo "${slug}" \
        "${artifact_dir}"/*.whl "${artifact_dir}"/*.tar.gz "${artifact_dir}"/SHA256SUMS --clobber
    echo "Released ${PYPI_PACKAGE} ${version} from ${release_sha}"
}

main "$@"
