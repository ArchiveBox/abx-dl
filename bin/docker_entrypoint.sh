#!/bin/bash

set -Eeuo pipefail
umask 0002

ARCHIVEBOX_USER="${ARCHIVEBOX_USER:-archivebox}"
DEFAULT_ARCHIVEBOX_GID="${DEFAULT_ARCHIVEBOX_GID:-911}"
DATA_DIR="${DATA_DIR:-/out}"
PERSONAS_DIR="${PERSONAS_DIR:-/data/personas}"
CONFIG_DIR="${CONFIG_DIR:-/opt/archivebox}"

if [[ "$(id -u)" == "0" ]]; then
    target_gid="$(stat -c '%g' "$DATA_DIR" 2>/dev/null || echo "$DEFAULT_ARCHIVEBOX_GID")"
    [[ "$target_gid" != "0" ]] || target_gid="$DEFAULT_ARCHIVEBOX_GID"

    groupmod -o -g "$target_gid" "$ARCHIVEBOX_USER"
    for path in "$DATA_DIR" "$PERSONAS_DIR" "$CONFIG_DIR/google-chrome-for-testing"; do
        mkdir -p "$path"
        chown -h "$ARCHIVEBOX_USER:$ARCHIVEBOX_USER" "$path"
        chmod u+rwx,g+rwx "$path"
    done

    exec setpriv --reuid="$ARCHIVEBOX_USER" --regid="$ARCHIVEBOX_USER" --init-groups abx-dl "$@"
fi

exec abx-dl "$@"
