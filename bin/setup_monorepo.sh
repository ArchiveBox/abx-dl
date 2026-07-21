#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
GITHUB_BASE="${GITHUB_BASE:-https://github.com/ArchiveBox}"
MONOREPO_REMOTE="${MONOREPO_REMOTE:-$GITHUB_BASE/monorepo.git}"
REPO_NAMES=(abxbus abxpkg abx-plugins abx-dl archivebox)

repo_branch() {
    if [[ "$1" == "archivebox" ]]; then
        printf '%s\n' dev
    else
        printf '%s\n' main
    fi
}

is_member_repo() {
    local repo_root="$1"
    local repo_name

    for repo_name in "${REPO_NAMES[@]}"; do
        if [[ "$(basename "$repo_root")" == "$repo_name" ]]; then
            return 0
        fi
    done

    return 1
}

monorepo_remote_matches() {
    case "$1" in
        git@github.com:ArchiveBox/monorepo.git | \
        git+ssh://git@github.com/ArchiveBox/monorepo.git | \
        https://github.com/ArchiveBox/monorepo.git)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

have_ldap_build_deps() {
    if command -v dpkg-query >/dev/null 2>&1; then
        dpkg-query -W -f='${Status}' libldap2-dev 2>/dev/null | grep -q 'install ok installed' && return 0
    fi

    if command -v brew >/dev/null 2>&1; then
        brew --prefix openldap >/dev/null 2>&1 && return 0
    fi

    return 1
}

ensure_ldap_build_deps() {
    if have_ldap_build_deps; then
        return
    fi

    printf 'Ensuring LDAP build dependencies\n'

    if command -v apt >/dev/null 2>&1 && sudo -n apt install -y libldap2-dev >/dev/null 2>&1; then
        return
    fi

    if command -v brew >/dev/null 2>&1 && brew install openldap >/dev/null 2>&1; then
        return
    fi

    printf 'Could not install LDAP build dependencies. Install libldap2-dev with apt or openldap with Homebrew.\n' >&2
    return 1
}

sync_workspace() {
    uv sync --locked --all-packages --all-extras --no-cache --active
}

ensure_setup_link() {
    local repo_name="$1"
    local repo_dir="$ROOT_DIR/$repo_name"
    local link_path="$repo_dir/bin/setup_monorepo.sh"
    local source_path="$ROOT_DIR/bin/setup.sh"

    mkdir -p "$repo_dir/bin"

    if [[ -e "$link_path" ]] && [[ "$source_path" -ef "$link_path" ]]; then
        return
    fi

    if [[ -d "$link_path" && ! -L "$link_path" ]]; then
        printf 'Refusing to replace directory: %s\n' "$link_path" >&2
        exit 1
    fi

    rm -f "$link_path"
    ln "$source_path" "$link_path"
}

bootstrap_monorepo_root() {
    local monorepo_root="$1"
    local origin_url=""

    if [[ -d "$monorepo_root/.git" ]]; then
        if git -C "$monorepo_root" remote get-url origin >/dev/null 2>&1; then
            origin_url="$(git -C "$monorepo_root" remote get-url origin)"
        fi

        if [[ -n "$origin_url" ]] && ! monorepo_remote_matches "$origin_url"; then
            printf 'Refusing to reuse existing git repo at %s (origin: %s)\n' "$monorepo_root" "$origin_url" >&2
            exit 1
        fi

        if [[ -z "$origin_url" ]]; then
            git -C "$monorepo_root" remote add origin "$MONOREPO_REMOTE"
        fi

        printf 'Updating monorepo root: %s\n' "$monorepo_root"
        git -C "$monorepo_root" fetch --quiet origin main
        git -C "$monorepo_root" merge --ff-only --quiet origin/main
        printf 'Updated monorepo root\n'
        return
    fi

    printf 'Bootstrapping monorepo root in %s\n' "$monorepo_root"
    git -C "$monorepo_root" init -b main >/dev/null
    git -C "$monorepo_root" remote add origin "$MONOREPO_REMOTE"
    git -C "$monorepo_root" fetch --depth=1 origin main --quiet

    if git -C "$monorepo_root" checkout -B main --track origin/main >/dev/null 2>&1; then
        printf 'Initialized monorepo root\n'
    else
        printf 'Failed to materialize monorepo root in %s; existing files likely conflict with tracked monorepo files\n' "$monorepo_root" >&2
        exit 1
    fi
}

if is_member_repo "$SCRIPT_REPO_ROOT"; then
    ROOT_DIR="$(cd -- "$SCRIPT_REPO_ROOT/.." && pwd)"
    bootstrap_monorepo_root "$ROOT_DIR"
elif [[ -f "$SCRIPT_REPO_ROOT/pyproject.toml" ]]; then
    ROOT_DIR="$SCRIPT_REPO_ROOT"
else
    printf 'Unable to infer monorepo root from script location: %s\n' "$SCRIPT_DIR" >&2
    exit 1
fi

ensure_member_repo() {
    local repo_name="$1"
    local repo_dir="$ROOT_DIR/$repo_name"
    local branch
    branch="$(repo_branch "$repo_name")"

    if [[ -d "$repo_dir/.git" ]]; then
        printf 'Updating existing checkout: %s\n' "$repo_name"
        [[ "$(git -C "$repo_dir" branch --show-current)" == "$branch" ]] || {
            printf '%s must be checked out on %s\n' "$repo_name" "$branch" >&2
            return 1
        }
        git -C "$repo_dir" fetch --quiet origin "$branch"
        git -C "$repo_dir" merge --ff-only --quiet "origin/$branch"
        printf 'Updated: %s\n' "$repo_name"
        return
    fi

    if [[ -e "$repo_dir" ]]; then
        printf 'Refusing to overwrite existing path: %s\n' "$repo_dir" >&2
        exit 1
    fi

    printf 'Cloning %s/%s.git -> %s\n' "$GITHUB_BASE" "$repo_name" "$repo_name"
    git clone --branch "$branch" --single-branch "$GITHUB_BASE/$repo_name.git" "$repo_dir"
}

for repo_name in "${REPO_NAMES[@]}"; do
    ensure_member_repo "$repo_name"
done

for repo_name in "${REPO_NAMES[@]}"; do
    ensure_setup_link "$repo_name"
done

cd "$ROOT_DIR"
if declare -F deactivate >/dev/null; then
    deactivate
fi
rm -Rf ./*/.venv   # delete all sub-repo venvs, the monorepo venv needs to take precedence

uv venv --allow-existing "$ROOT_DIR/.venv"
# shellcheck disable=SC1091
source "$ROOT_DIR/.venv/bin/activate"
ensure_ldap_build_deps
sync_workspace
echo
echo
echo "[√] Monorepo setup complete, cloned and pulled: ${REPO_NAMES[*]}"
echo "    MONOREPO_ROOT=$ROOT_DIR"
echo "    VIRTUAL_ENV=$VIRTUAL_ENV"
echo "    PYTHON_BIN=$VIRTUAL_ENV/bin/python"
echo "    NODE_BIN=$(which node)"
echo
echo "TIPS:"
echo " - Always use 'uv run ...' within each subrepo, never in the root & never run 'python ...' directly"
echo " - Always read $ROOT_DIR/README.md into context before starting any work"
