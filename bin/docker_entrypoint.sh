#!/bin/bash

set -Eeuo pipefail
umask 0002

ARCHIVEBOX_USER="${ARCHIVEBOX_USER:-archivebox}"
DEFAULT_ARCHIVEBOX_GID="${DEFAULT_ARCHIVEBOX_GID:-911}"
DATA_DIR="${DATA_DIR:-/out}"
PERSONAS_DIR="${PERSONAS_DIR:-/data/personas}"
CONFIG_DIR="${CONFIG_DIR:-/opt/archivebox}"
CRASH_REPORTS_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/chromium/Crash Reports"

if [[ "$(id -u)" == "0" ]]; then
    target_gid="$(stat -c '%g' "$DATA_DIR" 2>/dev/null || echo "$DEFAULT_ARCHIVEBOX_GID")"
    [[ "$target_gid" != "0" ]] || target_gid="$DEFAULT_ARCHIVEBOX_GID"

    groupmod -o -g "$target_gid" "$ARCHIVEBOX_USER"

    mkdir -p "$DATA_DIR" "$PERSONAS_DIR"
    chmod a+rwx "$DATA_DIR" "$PERSONAS_DIR"

    for path in "$HOME" "$CONFIG_DIR" "$CONFIG_DIR/google-chrome-for-testing" "$CRASH_REPORTS_DIR"; do
        mkdir -p "$path"
        chown -h "$ARCHIVEBOX_USER:$ARCHIVEBOX_USER" "$path"
        chmod u+rwx,g+rwx "$path"
    done

    exec setpriv --reuid="$ARCHIVEBOX_USER" --regid="$ARCHIVEBOX_USER" --init-groups abx-dl "$@"
fi

exec abx-dl "$@"
