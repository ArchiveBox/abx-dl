# syntax=docker/dockerfile:1.7

# Dockerfile for abx-dl. This image owns the shared downloader runtime layer:
# Python, Node, abx-dl/abxpkg/abx-plugins, Chromium, and downloader plugin-managed tools.
# ArchiveBox-specific server pieces such as sonic and supervisor intentionally
# remain owned by the ArchiveBox image.
#
# Build from the abx-dl package directory:
#   docker buildx build ./abx-dl -f ./abx-dl/Dockerfile \
#       --build-context abxbus=./abxbus \
#       --build-context abxpkg=./abxpkg \
#       --build-context abx-plugins=./abx-plugins \
#       -t archivebox/abx-dl:dev

ARG NODE_VERSION=24.18.0
ARG UV_VERSION=0.10.6

FROM --platform=$TARGETPLATFORM node:${NODE_VERSION}-trixie-slim AS node-runtime
FROM --platform=$TARGETPLATFORM debian:trixie-slim AS abx-dl-runtime-base

ARG UV_VERSION

LABEL name="abx-dl" \
    maintainer="Nick Sweeting <dockerfile@archivebox.io>" \
    description="All-in-one CLI tool to download and extract content from URLs" \
    homepage="https://github.com/ArchiveBox/abx-dl" \
    documentation="https://github.com/ArchiveBox/abx-dl" \
    org.opencontainers.image.title="abx-dl" \
    org.opencontainers.image.vendor="ArchiveBox" \
    org.opencontainers.image.description="All-in-one CLI tool to download and extract content from URLs" \
    org.opencontainers.image.source="https://github.com/ArchiveBox/abx-dl"

ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

ENV TZ=UTC \
    LANGUAGE=en_US:en \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    DEBIAN_FRONTEND=noninteractive \
    APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1 \
    PYTHONIOENCODING=UTF-8 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_COMPILE=1 \
    PIP_ONLY_BINARY=aiohttp \
    npm_config_loglevel=error

ENV PYTHON_VERSION=3.13.12 \
    NODE_VERSION=24.18.0

ENV ARCHIVEBOX_USER=archivebox \
    DEFAULT_ARCHIVEBOX_UID=911 \
    DEFAULT_ARCHIVEBOX_GID=911 \
    IN_DOCKER=True

ENV CODE_DIR=/app \
    DATA_DIR=/out \
    CONFIG_DIR=/opt/archivebox \
    ABXPKG_LIB_DIR=/opt/archivebox/lib \
    PERSONAS_DIR=/data/personas \
    CHROME_HEADLESS=true \
    CHROME_SANDBOX=false \
    CHROME_ISOLATION=crawl

ENV UV_COMPILE_BYTECODE=false \
    UV_PYTHON_PREFERENCE=managed \
    UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/venv \
    VIRTUAL_ENV=/venv \
    PIP_VENV_PYTHON=/venv/bin/python3 \
    PATH="/venv/bin:/opt/node/bin:$PATH"

ENV HOME=/home/archivebox \
    XDG_CONFIG_HOME=/opt/archivebox \
    XDG_CACHE_HOME=/opt/archivebox/lib/cache

SHELL ["/bin/bash", "-o", "pipefail", "-o", "errexit", "-o", "errtrace", "-o", "nounset", "-c"]
WORKDIR "$CODE_DIR"

RUN echo 'Binary::apt::APT::Keep-Downloaded-Packages "0";' > /etc/apt/apt.conf.d/99keep-cache \
    && echo 'APT::Install-Recommends "0";' > /etc/apt/apt.conf.d/99no-install-recommends \
    && echo 'APT::Install-Suggests "0";' > /etc/apt/apt.conf.d/99no-install-suggests

RUN (echo "[i] Docker build for abx-dl starting..." \
    && echo "PLATFORM=${TARGETPLATFORM} ARCH=$(uname -m) (${TARGETARCH} ${TARGETVARIANT})" \
    && echo "BUILD_START_TIME=$(date +"%Y-%m-%d %H:%M:%S %s") TZ=${TZ} LANG=${LANG}" \
    && uname -a \
    && sed -n '1,7p' /etc/os-release \
    ) | tee -a /VERSION.txt

# Bootstrap packages only. Downloader/browser/media runtimes are installed by
# their owning plugin install hooks in separate layers below.
RUN echo "[+] APT Installing abx-dl bootstrap dependencies for $TARGETPLATFORM..." \
    && apt-get update -qq \
    && apt-get install -qq -y \
        ca-certificates curl dumb-init findutils util-linux procps openssl unzip xz-utils zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=node-runtime /usr/local /opt/node

RUN export PATH="/opt/node/bin:$PATH" \
    && (which node && which npm) | tee -a /VERSION.txt

RUN curl -LsSf "https://astral.sh/uv/${UV_VERSION}/install.sh" | env UV_INSTALL_DIR=/bin sh

# Normalize the managed interpreter in its creation layer so cache fingerprints
# survive OCI layer materialization without copying the Python tree up later.
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    echo "[+] UV Creating /venv using python ${PYTHON_VERSION} for ${TARGETPLATFORM}..." \
    && uv venv /venv --python "${PYTHON_VERSION}" \
    && uv pip install setuptools pip wheel \
    && touch -h -d "@$(date +%s)" "$(readlink -f /venv/bin/python)" \
    && (which python3 && which uv && uv python find) | tee -a /VERSION.txt

########################################################################################################
FROM abx-dl-runtime-base AS abx-dl-builder

WORKDIR "$CODE_DIR"
COPY --from=abxbus --chown=root:root --chmod=755 pyproject.toml README.md LICENSE /src/abxbus/
COPY --from=abxpkg --chown=root:root --chmod=755 pyproject.toml README.md LICENSE /src/abxpkg/
COPY --from=abx-plugins --chown=root:root --chmod=755 pyproject.toml README.md LICENSE /src/abx-plugins/
COPY --chown=root:root --chmod=755 pyproject.toml README.md LICENSE "$CODE_DIR/"
# Release automation changes only these version fields on its follow-up commit.
# Install a canonical version while building the expensive browser/tool layer so
# that metadata-only bumps do not invalidate it; the real versions are overlaid
# from the original contexts after every binary has been installed and checked.
RUN sed -i -E 's/^version = "[^"]+"/version = "0.0.0"/' \
        /src/abxbus/pyproject.toml \
        /src/abxpkg/pyproject.toml \
        /src/abx-plugins/pyproject.toml \
        "$CODE_DIR/pyproject.toml"
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    echo "[+] UV Installing external Python dependencies from local package metadata..." \
    && /venv/bin/python3 -c 'import re, tomllib; paths = ["/src/abxbus/pyproject.toml", "/src/abxpkg/pyproject.toml", "/src/abx-plugins/pyproject.toml", "/app/pyproject.toml"]; skip = {"abxbus", "abxpkg", "abx-plugins", "abx-dl"}; deps = []; [deps.extend(tomllib.load(open(path, "rb"))["project"].get("dependencies", [])) for path in paths]; seen = set(); print("\n".join(dep for dep in deps if (name := re.split(r"[<>=!~;\\[]", dep, 1)[0].strip().lower()) not in skip and not (dep in seen or seen.add(dep))))' > /tmp/abx-dl-requirements.txt \
    && uv pip install --refresh -r /tmp/abx-dl-requirements.txt

COPY --from=abxbus --chown=root:root --chmod=755 abxbus /src/abxbus/abxbus
COPY --from=abxpkg --chown=root:root --chmod=755 abxpkg /src/abxpkg/abxpkg
COPY --from=abx-plugins --chown=root:root --chmod=755 abx_plugins /src/abx-plugins/abx_plugins
COPY --chown=root:root --chmod=755 abx_dl "$CODE_DIR/abx_dl"
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    echo "[*] Installing local abxbus/abxpkg/abx-plugins/abx-dl Python source code..." \
    && uv pip install --no-deps /src/abxbus /src/abxpkg /src/abx-plugins "$CODE_DIR" \
    && /usr/bin/uv pip show abx-dl | tee -a /VERSION.txt \
    && rm -f /venv/bin/uv /venv/bin/uvx \
    && rm -rf /venv/lib/python3.*/site-packages/pip* /venv/lib/python3.*/site-packages/setuptools* /venv/lib/python3.*/site-packages/wheel* /venv/bin/pip /venv/bin/pip3 /venv/bin/pip3.* /venv/bin/wheel \
    && (which abx-dl && abx-dl --version) | tee -a /VERSION.txt

########################################################################################################
FROM scratch AS abx-dl-release-packages

# Select only installable package inputs here. Mounting each repository root in
# the final stage would make BuildKit upload unrelated local caches/workspaces.
COPY --from=abxbus pyproject.toml README.md LICENSE /abxbus/
COPY --from=abxbus abxbus /abxbus/abxbus
COPY --from=abxpkg pyproject.toml README.md LICENSE /abxpkg/
COPY --from=abxpkg abxpkg /abxpkg/abxpkg
COPY --from=abx-plugins pyproject.toml README.md LICENSE /abx-plugins/
COPY --from=abx-plugins abx_plugins /abx-plugins/abx_plugins
COPY pyproject.toml README.md LICENSE /abx-dl/
COPY abx_dl /abx-dl/abx_dl

########################################################################################################
FROM abx-dl-runtime-base

COPY --from=abx-dl-builder /venv /venv
COPY --from=abx-dl-builder /VERSION.txt /VERSION.txt
COPY --chown=root:root --chmod=755 bin/docker_entrypoint.sh /usr/local/bin/abx-dl-docker-entrypoint

RUN echo "[*] Setting up $ARCHIVEBOX_USER user uid=${DEFAULT_ARCHIVEBOX_UID}..." \
    && groupadd --system "$ARCHIVEBOX_USER" \
    && useradd --system --create-home --gid "$ARCHIVEBOX_USER" --groups audio,video "$ARCHIVEBOX_USER" \
    && usermod -u "$DEFAULT_ARCHIVEBOX_UID" "$ARCHIVEBOX_USER" \
    && groupmod -g "$DEFAULT_ARCHIVEBOX_GID" "$ARCHIVEBOX_USER" \
    && install -d -o "$DEFAULT_ARCHIVEBOX_UID" -g "$DEFAULT_ARCHIVEBOX_GID" "$DATA_DIR" "$CONFIG_DIR" "$ABXPKG_LIB_DIR" \
    && echo "ARCHIVEBOX_USER=$ARCHIVEBOX_USER ARCHIVEBOX_UID=$(id -u "$ARCHIVEBOX_USER") ARCHIVEBOX_GID=$(id -g "$ARCHIVEBOX_USER")" | tee -a /VERSION.txt

# abxpkg fingerprints installed files by size, mode, owner, and nanosecond
# mtime. Install and compile everything first, then canonicalize mtimes to a
# fixed epoch because OCI layer materialization cannot preserve arbitrary
# installer mtimes consistently. checked-hash pycs remain valid after the
# normalization. This one scratch mount intentionally backs HOME,
# XDG_CACHE_HOME, and ABXPKG_TMP_CACHE_DIR: package managers disagree about
# cache locations, and leaving XDG pointed at /opt would silently bake their
# downloads into the runtime image despite the BuildKit mount. Installed tools
# and abxpkg's derived state remain under ABXPKG_LIB_DIR; only disposable
# download state goes into this mount. After cleanup, the last in-layer install
# uses the real runtime cache path once so uv may seed its tiny interpreter
# index; a strict size cap prevents package payloads from slipping back in.
RUN --mount=type=cache,target=/var/tmp/abxpkg-cache,sharing=locked,mode=1777,id=abxpkg-tmp-$TARGETARCH$TARGETVARIANT \
    echo "[+] Installing Chrome and plugin dependencies..." \
    && export HOME=/var/tmp/abxpkg-cache XDG_CACHE_HOME=/var/tmp/abxpkg-cache ABXPKG_TMP_CACHE_DIR=/var/tmp/abxpkg-cache \
    && abx-dl install chrome \
    && abx-dl install \
    && rm -rf /usr/lib/*-linux-gnu/dri /usr/lib/*-linux-gnu/libLLVM*.so* /usr/lib/*-linux-gnu/libz3.so.* \
    && rm -rf /usr/share/icons /usr/share/doc /usr/share/man /usr/share/bash-completion /usr/share/zsh /usr/share/info /usr/share/lintian /usr/share/bug \
    && install -d -m 755 /usr/share/man/man1 \
    && rm -f /usr/lib/jvm/java-*-openjdk-*/lib/server/classes*.jsa \
    && rm -f /venv/bin/uv /venv/bin/uvx \
    && find "$ABXPKG_LIB_DIR" \( ! -user "$DEFAULT_ARCHIVEBOX_UID" -o ! -group "$DEFAULT_ARCHIVEBOX_GID" \) -exec chown -h "$DEFAULT_ARCHIVEBOX_UID:$DEFAULT_ARCHIVEBOX_GID" {} + \
    && STDLIB_DIR="$(/venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("stdlib"))')" \
    && PURELIB_DIR="$(/venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')" \
    && /venv/bin/python -m compileall --invalidation-mode checked-hash -q "$STDLIB_DIR" "$PURELIB_DIR" \
    && env HOME=/home/archivebox XDG_CACHE_HOME=/var/tmp/abxpkg-cache setpriv --reuid="$ARCHIVEBOX_USER" --regid="$ARCHIVEBOX_USER" --init-groups abx-dl install \
    && find /venv "$ABXPKG_LIB_DIR" -exec touch -h -d '@946684800' {} + \
    && find "$ABXPKG_LIB_DIR/cache" -mindepth 1 -maxdepth 1 -exec rm -rf {} + \
    && env -u ABXPKG_TMP_CACHE_DIR HOME=/home/archivebox XDG_CACHE_HOME="$ABXPKG_LIB_DIR/cache" setpriv --reuid="$ARCHIVEBOX_USER" --regid="$ARCHIVEBOX_USER" --init-groups abx-dl install \
    && CACHE_BYTES="$(du -sb "$ABXPKG_LIB_DIR/cache" | cut -f1)" \
    && (( CACHE_BYTES < 1048576 )) \
    && rm -rf /var/lib/apt/lists/* /tmp/*

# These values are deliberately declared after the expensive tool layer. CI
# canonicalizes version-only metadata before BuildKit hashes its contexts, then
# supplies the exact released values here so autobumps invalidate only the
# package overlay and provenance, never the installed browser/toolchain.
ARG ABXBUS_VERSION
ARG ABXPKG_VERSION
ARG ABX_PLUGINS_VERSION
ARG ABX_DL_VERSION
ARG ABX_DL_COMMIT_HASH

# Overlay the exact released package metadata only after the expensive toolchain
# layer. Source changes still invalidate that layer because its canonical build
# above contains the same source; a version-only autobump now changes just this
# small layer. The selected sources are copied to writable scratch space because
# build backends create temporary files beside pyproject.toml, then removed in
# this same layer so no second source tree is baked into the image.
RUN --mount=type=bind,from=abx-dl-release-packages,source=/,target=/src/actual,ro \
    --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    cp -a /src/actual /tmp/actual \
    && for package_version in \
        "/tmp/actual/abxbus/pyproject.toml|$ABXBUS_VERSION" \
        "/tmp/actual/abxpkg/pyproject.toml|$ABXPKG_VERSION" \
        "/tmp/actual/abx-plugins/pyproject.toml|$ABX_PLUGINS_VERSION" \
        "/tmp/actual/abx-dl/pyproject.toml|$ABX_DL_VERSION"; do \
        IFS='|' read -r package_file package_version <<< "$package_version"; \
        if [[ -n "$package_version" ]]; then sed -i -E "s/^version = \"[^\"]+\"/version = \"$package_version\"/" "$package_file"; fi; \
    done \
    && /usr/bin/uv pip install --no-deps \
        /tmp/actual/abxbus /tmp/actual/abxpkg /tmp/actual/abx-plugins /tmp/actual/abx-dl \
    && rm -rf /tmp/actual \
    && PURELIB_DIR="$(/venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')" \
    && /venv/bin/python -m compileall --invalidation-mode checked-hash -q \
        "$PURELIB_DIR/abxbus" "$PURELIB_DIR/abxpkg" "$PURELIB_DIR/abx_plugins" "$PURELIB_DIR/abx_dl" \
    && find "$PURELIB_DIR" -maxdepth 1 \( -name 'abxbus*' -o -name 'abxpkg*' -o -name 'abx_plugins*' -o -name 'abx_dl*' \) -exec touch -h -d '@946684800' {} + \
    && find /venv/bin -maxdepth 1 -type f -name 'abx*' -exec touch -h -d '@946684800' {} + \
    && /usr/bin/uv pip show abx-dl | tee -a /VERSION.txt \
    && if [[ "$ABX_DL_COMMIT_HASH" =~ ^[0-9a-fA-F]{40}$ ]]; then echo "COMMIT_HASH=$ABX_DL_COMMIT_HASH" | tee -a /VERSION.txt; fi

# The diagnostics below do not install binaries, but they exercise both
# check-mode and install-mode projections, whose derived cache shapes differ.
# Stabilize once after all checks and only then take the baseline hash. The
# final install is intentional and must not be removed as redundant: with
# networking disabled, both abxpkg's derived records and uv's small runtime
# index must remain byte-for-byte unchanged. If it repairs metadata or attempts
# an install, the image build fails.
RUN --network=none env -u ABXPKG_TMP_CACHE_DIR HOME=/home/archivebox \
    setpriv --reuid="$ARCHIVEBOX_USER" --regid="$ARCHIVEBOX_USER" --init-groups \
    bash -c '(echo -e "\n\n[+] abx-dl runtime versions" \
        && abx-dl version \
        && test -f "$(/venv/bin/python -c "import json; print(json.__cached__)")" \
        && test -f "$(/venv/bin/python -c "import abxpkg.cli; print(abxpkg.cli.__cached__)")" \
        && test -f "$(/venv/bin/python -c "import pydantic; print(pydantic.__cached__)")" \
        && abxpkg load /opt/node/bin/node \
        && abxpkg load /venv/bin/python3 \
        && abx-dl plugins \
        && abxpkg load rg \
        && ! command -v gcc \
        && ! command -v g++ \
        && ! command -v make \
        && ! command -v cargo \
        && ! command -v sonic \
        && ! command -v supervisord \
        && abx-dl install \
        && (find "$ABXPKG_LIB_DIR" -name derived.env -type f -exec sha256sum {} +; find "$XDG_CACHE_HOME" -type f -exec sha256sum {} +) | sort > /tmp/cache-before \
        && abx-dl install \
        && (find "$ABXPKG_LIB_DIR" -name derived.env -type f -exec sha256sum {} +; find "$XDG_CACHE_HOME" -type f -exec sha256sum {} +) | sort > /tmp/cache-after \
        && diff -u /tmp/cache-before /tmp/cache-after \
        && rm -f /tmp/cache-before /tmp/cache-after \
        && echo -e "\n\n[√] Finished abx-dl Docker build successfully." \
        && echo -e "BUILD_END_TIME=$(date +"%Y-%m-%d %H:%M:%S %s")\n\n" \
        )' | tee -a /VERSION.txt

WORKDIR /out
VOLUME ["/out", "/data/personas"]
ENTRYPOINT ["dumb-init", "--", "abx-dl-docker-entrypoint"]
CMD ["--help"]
