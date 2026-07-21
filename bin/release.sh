#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_DIR}"

TAG_PREFIX="v"
PYPI_PACKAGE="abx-dl"
PRE_RELEASE_WORKFLOWS=("test.yml|Parallel Plugin Tests")
POST_TAG_WORKFLOWS=("docker.yml|Build Docker image")

source_optional_env() {
    if [[ -f "${REPO_DIR}/.env" ]]; then
        set -a
        # shellcheck disable=SC1091
        source "${REPO_DIR}/.env"
        set +a
    fi
}

repo_slug() { gh repo view --json nameWithOwner --jq .nameWithOwner; }

current_version() {
    uv run python - <<'PY'
from pathlib import Path
import re
match = re.search(r'^version = "([^"]+)"$', Path('pyproject.toml').read_text(), re.MULTILINE)
if not match:
    raise SystemExit('Failed to find version in pyproject.toml')
print(match.group(1))
PY
}

compare_versions() {
    uv run python - "$1" "$2" <<'PY'
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
    local slug="$1" pypi_versions github_versions versions
    pypi_versions="$(curl -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" | jq -r '.releases | keys[]')"
    github_versions="$(gh api "repos/${slug}/releases?per_page=100" --jq '.[].tag_name' | sed "s/^${TAG_PREFIX}//")"
    versions="$(printf '%s\n%s\n' "${pypi_versions}" "${github_versions}" | sort -u)"
    RELEASE_VERSIONS="${versions}" uv run python - <<'PY'
import os, re
def parse(version):
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:-?rc(\d+))?', version)
    if not match:
        return -1, -1, -1, -1, -1
    major, minor, patch, rc = match.groups()
    return int(major), int(minor), int(patch), 0 if rc is not None else 1, int(rc or 0)
versions = [line for line in os.environ['RELEASE_VERSIONS'].splitlines() if parse(line)[0] >= 0]
print(max(versions, key=parse) if versions else '')
PY
}

pypi_has_version() {
    curl -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" | jq -e --arg version "$1" '.releases[$version] | length > 0' >/dev/null
}

wait_for_pypi() {
    local version="$1" attempts=0
    until pypi_has_version "${version}"; do
        attempts=$((attempts + 1))
        [[ "${attempts}" -lt 30 ]] || { echo "Timed out waiting for ${PYPI_PACKAGE}==${version} on PyPI" >&2; return 1; }
        sleep 10
    done
}

tag_target() {
    local tag="$1" target
    target="$(git ls-remote origin "refs/tags/${tag}^{}" | awk 'NR == 1 {print $1}')"
    [[ -n "${target}" ]] || target="$(git ls-remote origin "refs/tags/${tag}" | awk 'NR == 1 {print $1}')"
    printf '%s\n' "${target}"
}

github_release_has_version() { gh release view "${TAG_PREFIX}$1" --repo "$2" >/dev/null 2>&1; }

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
    [[ "$(git rev-parse HEAD)" == "${sha}" ]] || { echo "HEAD does not match RELEASE_SHA ${sha}" >&2; return 1; }
    [[ -z "$(git status --short)" ]] || { echo "Refusing to release from a dirty worktree" >&2; return 1; }
    git fetch --quiet --no-tags origin "+refs/heads/${branch}:refs/remotes/origin/${branch}"
    git merge-base --is-ancestor "${sha}" "refs/remotes/origin/${branch}" || { echo "${sha} is not on ${branch}" >&2; return 1; }
}

wait_for_required_workflows() {
    local slug="$1" sha="$2"
    shift 2
    local spec workflow workflow_name runs attempts state run_id
    for spec in "$@"; do
        workflow="${spec%%|*}"
        workflow_name="${spec#*|}"
        attempts=0
        while :; do
            runs="$(env -u GH_FORCE_TTY GH_PROMPT_DISABLED=1 GH_PAGER=cat NO_COLOR=1 gh run list --repo "${slug}" --workflow "${workflow}" --event push --commit "${sha}" --limit 10 --json databaseId,workflowName,headSha,status,conclusion,event)"
            state="$(jq -r --arg name "${workflow_name}" --arg sha "${sha}" '[.[] | select(.workflowName == $name and .headSha == $sha and .event == "push")] | if length == 1 then (.[0] | [.databaseId,.status,(.conclusion // "")] | @tsv) elif length == 0 then "missing" else "ambiguous" end' <<<"${runs}")"
            [[ "${state}" != ambiguous ]] || { echo "Multiple ${workflow_name} runs found for ${sha}" >&2; return 1; }
            if [[ "${state}" != missing ]]; then
                IFS=$'\t' read -r run_id _ _ <<<"${state}"
                break
            fi
            attempts=$((attempts + 1))
            if [[ "${attempts}" -ge 12 ]]; then
                echo "Required workflow ${workflow} did not start for ${sha}" >&2
                return 1
            fi
            sleep 5
        done
        env -u GH_FORCE_TTY GH_PROMPT_DISABLED=1 GH_PAGER=cat NO_COLOR=1 \
            gh run watch "${run_id}" --repo "${slug}" --exit-status
        env -u GH_FORCE_TTY GH_PROMPT_DISABLED=1 GH_PAGER=cat NO_COLOR=1 \
            gh run view "${run_id}" --repo "${slug}" \
                --json workflowName,headSha,event,status,conclusion \
            | jq -e --arg name "${workflow_name}" --arg sha "${sha}" \
                '.workflowName == $name and .headSha == $sha and .event == "push" and .status == "completed" and .conclusion == "success"' \
                >/dev/null
    done
}

publish_to_pypi() {
    (
        local version="$1" build_dir
        build_dir="$(mktemp -d)"
        trap 'rm -rf "${build_dir}"' EXIT
        uv build --out-dir "${build_dir}"
        uv publish --trusted-publishing always "${build_dir}"/*
    )
}

create_release() {
    local slug="$1" version="$2" sha="$3"
    if github_release_has_version "${version}" "${slug}"; then
        verify_existing_tag "${version}" "${sha}"
        return 0
    fi
    verify_existing_tag "${version}" "${sha}"
    gh release create "${TAG_PREFIX}${version}" --repo "${slug}" --target "${sha}" --title "${TAG_PREFIX}${version}" --generate-notes
}

main() {
    local slug version latest release_sha target pypi_exists=false github_exists=false
    source_optional_env
    slug="$(repo_slug)"
    version="$(current_version)"
    release_sha="${RELEASE_SHA:-$(git rev-parse HEAD)}"
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
        echo "${PYPI_PACKAGE} ${version} is already fully released from ${target}"
        return 0
    fi
    if [[ ( "${pypi_exists}" == true || "${github_exists}" == true ) && "${target}" != "${release_sha}" ]]; then
        echo "Cannot recover partial release ${version}: no tag anchors it to ${release_sha}" >&2
        return 1
    fi
    wait_for_required_workflows "${slug}" "${release_sha}" "${PRE_RELEASE_WORKFLOWS[@]}"
    create_release "${slug}" "${version}" "${release_sha}"
    wait_for_required_workflows "${slug}" "${release_sha}" "${POST_TAG_WORKFLOWS[@]}"
    [[ "${pypi_exists}" == true ]] || publish_to_pypi "${version}"
    wait_for_pypi "${version}"
    github_release_has_version "${version}" "${slug}"
    verify_existing_tag "${version}" "${release_sha}"
    echo "Released ${PYPI_PACKAGE} ${version} from ${release_sha}"
}

main "$@"
