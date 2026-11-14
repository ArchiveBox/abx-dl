# syntax=docker/dockerfile:1.7

# Dockerfile for abx-dl. This image owns the shared downloader runtime layer:
# Python, Node, abx-dl/abxpkg/abx-plugins, Chromium, and downloader plugin-managed tools.
# WHY: abx-dl is the reusable downloader runtime. Only extraction dependencies
# belong here. ArchiveBox-only services (Sonic, OpenCode, supervisor, Django)
# must be installed in ArchiveBox's added layers, never this shared base.
# REQUIRED INSTALLATION CONTRACT — do not work around failures:
# 1. IMAGE CONSTRUCTION: preinstall ALL plugins supported by runtime="abx-dl"
#    and ALL their required_binaries, including disabled-by-default plugins
#    (Java/OpenDataLoader, TLSNotary, OpenTimestamps, Claude capture tools).
#    Use the normal plugin dependency pipeline: abx-dl install -> config.json
#    required_binaries -> abxpkg. Enabled defaults control captures, NOT image
#    contents. Never add manual apt/pip/npm installs to hide pipeline failures.
#    OS/interpreter bootstrap needed to run that pipeline is a separate concern;
#    it must not become a second hand-maintained list of plugin dependencies.
# 2. FINISHED IMAGE: every dependency is already present in the image layers.
#    Verify the ENTIRE supported catalog offline as UID 911. Installation must
#    discover the existing binaries and be a no-op: no downloads, package
#    installs, sudo, or root repair. A missing dependency means the image build
#    or dependency discovery is broken; fix that cause and rebuild the image.
#    Never change the runtime verification to root or preinstall before it.
# 3. NON-CONTAINER CI: test real runtime auto-installation through the same
#    CLI/required_binaries/abxpkg path users invoke. GitHub's runner has
#    passwordless sudo; abxpkg must escalate automatically when needed. Never
#    preinstall test dependencies manually or wrap the tested command in sudo
#    to conceal a broken installer. This is distinct from the image no-op test.
# 4. SIZE: the complete compressed image budget is 900 MiB. Only mechanical or
#    internal optimizations preserving ALL user-facing functionality are allowed.
#    Never remove dependencies, disable plugins/features, weaken assertions, or
#    skip checks to pass. If the complete image exceeds the limit, LET CI FAIL
#    and explicitly ask the maintainer what to do. Do not raise it unilaterally.
#
# Build from the abx-dl package directory:
#   docker buildx build ./abx-dl -f ./abx-dl/Dockerfile \
#       --build-context abxbus=./abxbus \
#       --build-context abxpkg=./abxpkg \
#       --build-context abx-plugins=./abx-plugins \
#       -t archivebox/abx-dl:dev

ARG NODE_VERSION=24.22.2
ARG UV_VERSION=0.13.5

FROM --platform=$TARGETPLATFORM node:${NODE_VERSION}-trixie-slim AS node-runtime
# Drop non-runtime ELF symbols before copying Node into the runtime base.
# Dynamic symbols required by native addons remain; binutils stays in this stage.
RUN apt-get update \
    && apt-get install -y --no-install-recommends binutils \
    && strip --strip-unneeded /usr/local/bin/node

FROM --platform=$TARGETPLATFORM debian:trixie-slim AS abx-dl-runtime-base

ARG UV_VERSION
ARG NODE_VERSION

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

ENV PYTHON_VERSION=3.13.17 \
    NODE_VERSION=${NODE_VERSION}

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
    echo "[+] Installing Chrome, then the complete downloader plugin catalog..." \
    && export HOME=/var/tmp/abxpkg-cache XDG_CACHE_HOME=/var/tmp/abxpkg-cache ABXPKG_TMP_CACHE_DIR=/var/tmp/abxpkg-cache \
    # npm postinstall subprocesses ignore XDG_CACHE_HOME and use the resolved
    # user home. Keep their downloaded tarballs on the build cache mount too.
    && export npm_config_cache=/var/tmp/abxpkg-cache/npm \
    # Let uv compile each tool with its own interpreter during installation.
    # Wheels can include source for other Python versions that is never imported;
    # compileall's all-files success requirement rejects otherwise valid tools.
    # SOURCE_DATE_EPOCH selects checked-hash caches, preserving their validity
    # when installed file mtimes are normalized below. Runtime writes stay off.
    && export UV_COMPILE_BYTECODE=true SOURCE_DATE_EPOCH=946684800 \
    && export ABX_DOCKER_PLUGINS="$(/venv/bin/python3 -c 'from abx_dl.catalog import PluginCatalog; print(" ".join(PluginCatalog.discover(runtime="abx-dl")))')" \
    && abx-dl install chrome \
    # Explicit names install every downloader plugin, including disabled opt-ins.
    # Native hardlinks share identical Python/Node dependencies across tools.
    # Keep their package caches on this layer's filesystem during installation.
    # The existing cache cleanup removes downloads after all installs finish.
    && UV_LINK_MODE=hardlink npm_config_package_import_method=hardlink abx-dl install $ABX_DOCKER_PLUGINS \
    # Wheels and native addons may ship compiler debug data (curl-cffi adds 32 MiB).
    # Remove debug data and addon-local symbols before fingerprints and OCI export.
    && strip_lib=/tmp/abx-dl-strip \
    && ABXPKG_NO_CACHE=True abxpkg env --install --binproviders=env,apt --lib="$strip_lib" --overrides='{"apt":{"install_args":["binutils"]}}' strip >/dev/null \
    # Some npm archives bundle native addons for a different CPU alongside
    # their platform-specific optional dependencies. Those ELF files cannot
    # load in this image's Node runtime; retain every matching native addon.
    && node_machine="$(readelf -h /opt/node/bin/node | sed -n 's/.*Machine: *//p')" \
    && test -n "$node_machine" \
    && find "$ABXPKG_LIB_DIR/pnpm" "$ABXPKG_LIB_DIR/npm" -type f -name '*.node' -print0 > /tmp/abx-dl-node-addons \
    && while IFS= read -r -d '' native_addon; do \
        magic=''; \
        if IFS= read -r -N 4 magic < "$native_addon" && [[ "$magic" == $'\x7fELF' ]]; then \
            addon_machine="$(readelf -h "$native_addon" | sed -n 's/.*Machine: *//p')"; \
            test -n "$addon_machine" || exit 1; \
            if [[ "$addon_machine" != "$node_machine" ]]; then \
                rm "$native_addon"; \
            else \
                "$strip_lib/env/bin/strip" --strip-unneeded "$native_addon" || exit $?; \
            fi; \
        fi; \
    done < /tmp/abx-dl-node-addons \
    && find "$ABXPKG_LIB_DIR/uv" -type f -name '*.so*' -print0 > /tmp/abx-dl-native-libraries \
    && while IFS= read -r -d '' native_library; do \
        magic=''; \
        if IFS= read -r -N 4 magic < "$native_library" && [[ "$magic" == $'\x7fELF' ]]; then \
            "$strip_lib/env/bin/strip" --strip-debug "$native_library" || exit $?; \
        fi; \
    done < /tmp/abx-dl-native-libraries \
    && abxpkg run --binproviders=env --lib="$strip_lib" apt-get purge -y binutils \
    && abxpkg run --binproviders=env --lib="$strip_lib" apt-get autoremove -y \
    && rm -rf "$strip_lib" /tmp/abx-dl-native-libraries /tmp/abx-dl-node-addons \
    # The locked provider still predates abxpkg's libc-selection fix. Retain
    # this cleanup until the normal dependency cascade reaches 1.13.89.
    && find "$ABXPKG_LIB_DIR/pnpm/packages/claudecode/node_modules" -type l -name 'claude-code-linux-*-musl' -delete \
    && find "$ABXPKG_LIB_DIR/pnpm/packages/claudecode/node_modules/.pnpm" -maxdepth 1 -type d -name '@anthropic-ai+claude-code-linux-*-musl@*' -exec rm -rf {} + \
    # Some upstream distributions include identical compiled assets at multiple
    # paths. Retain every path and byte, sharing their immutable file contents.
    && hardlink --ignore-time --respect-xattrs --include '\.(so(\.[0-9]+)*|wasm)$' "$ABXPKG_LIB_DIR" \
    && rm -rf /usr/lib/*-linux-gnu/dri /usr/lib/*-linux-gnu/libLLVM*.so* /usr/lib/*-linux-gnu/libz3.so.* \
    && rm -rf /usr/share/icons /usr/share/doc /usr/share/man /usr/share/bash-completion /usr/share/zsh /usr/share/info /usr/share/lintian /usr/share/bug \
    && install -d -m 755 /usr/share/man/man1 \
    && rm -f /usr/lib/jvm/java-*-openjdk-*/lib/server/classes*.jsa \
    && rm -f /venv/bin/uv /venv/bin/uvx \
    && STDLIB_DIR="$(/venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("stdlib"))')" \
    && PURELIB_DIR="$(/venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')" \
    && /venv/bin/python -m compileall --invalidation-mode checked-hash -q "$STDLIB_DIR" "$PURELIB_DIR" \
    # Include newly compiled bytecode before runtime installs fingerprint it;
    # otherwise child images copy it into another layer just to fix ownership.
    && find "$ABXPKG_LIB_DIR" \( ! -user "$DEFAULT_ARCHIVEBOX_UID" -o ! -group "$DEFAULT_ARCHIVEBOX_GID" \) -exec chown -h "$DEFAULT_ARCHIVEBOX_UID:$DEFAULT_ARCHIVEBOX_GID" {} + \
    && env HOME=/home/archivebox XDG_CACHE_HOME=/var/tmp/abxpkg-cache setpriv --reuid="$ARCHIVEBOX_USER" --regid="$ARCHIVEBOX_USER" --init-groups abx-dl install $ABX_DOCKER_PLUGINS \
    && find /venv "$ABXPKG_LIB_DIR" -exec touch -h -d '@946684800' {} + \
    # Execution plans from root provisioning cannot serve the runtime user.
    # Discard only those plans; keep valid runtime plans and binary provenance.
    && setpriv --reuid="$ARCHIVEBOX_USER" --regid="$ARCHIVEBOX_USER" --init-groups \
        /usr/bin/uv run --no-project python -c 'import os; from pathlib import Path; from abxpkg.config import load_derived_cache, save_derived_cache; path = Path(os.environ["ABXPKG_LIB_DIR"]) / "env/derived.env"; records = load_derived_cache(path); [record.update(request_exec_projections={key: projection for key, projection in record["request_exec_projections"].items() if projection["validation"]["euid"] == os.geteuid()}) for record in records.values() if "request_exec_projections" in record]; save_derived_cache(path, records)' \
    && find "$ABXPKG_LIB_DIR/cache" -mindepth 1 -maxdepth 1 -exec rm -rf {} + \
    && env -u ABXPKG_TMP_CACHE_DIR HOME=/home/archivebox XDG_CACHE_HOME="$ABXPKG_LIB_DIR/cache" setpriv --reuid="$ARCHIVEBOX_USER" --regid="$ARCHIVEBOX_USER" --init-groups abx-dl install $ABX_DOCKER_PLUGINS \
    && CACHE_BYTES="$(du -sb "$ABXPKG_LIB_DIR/cache" | cut -f1)" \
    && (( CACHE_BYTES < 1048576 )) \
    && rm -rf /var/lib/apt/lists/* /tmp/*

# These values are deliberately declared after the expensive tool layer. CI
# canonicalizes version-only metadata before BuildKit hashes its contexts, then
# supplies the exact released values here so autobumps invalidate only the
# package overlay and provenance, never the installed browser/toolchain.
ARG ABXBUS_VERSION
ARG DIST_ABXPKG_VERSION
ARG ABX_PLUGINS_VERSION
ARG ABX_DL_VERSION
ARG ABX_DL_COMMIT_HASH

# The canonical build already installed and compiled these exact source trees;
# reinstalling them here just to change four version strings would duplicate
# every package in a new OCI layer and make the ArchiveBox child image repeat
# those bytes again when repairing ownership. Metadata readers disagree here:
# importlib reads METADATA, while uv also trusts the dist-info directory name.
# Therefore copy/rename only that tiny directory and adjust RECORD; package
# code remains untouched. PEP 376 explicitly allows an empty hash/size for an
# installed RECORD entry, so blank only the METADATA checksum we intentionally
# mutate instead of adding checksum-recalculation machinery. overlayfs cannot
# rename a directory out of a lower OCI layer, hence the small copy + whiteout.
RUN PURELIB_DIR="$(/venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')" \
    && for package_version in \
        "abxbus|$ABXBUS_VERSION" \
        "abxpkg|$DIST_ABXPKG_VERSION" \
        "abx_plugins|$ABX_PLUGINS_VERSION" \
        "abx_dl|$ABX_DL_VERSION"; do \
        IFS='|' read -r package version <<< "$package_version"; \
        [[ -n "$version" ]] || continue; \
        old_name="${package}-0.0.0.dist-info"; \
        new_name="${package}-${version}.dist-info"; \
        cp -a "$PURELIB_DIR/$old_name" "$PURELIB_DIR/$new_name"; \
        sed -i -E "s/^Version: .*/Version: $version/; s|^$old_name/|$new_name/|" "$PURELIB_DIR/$new_name/METADATA" "$PURELIB_DIR/$new_name/RECORD"; \
        sed -i -E "s|^($new_name/METADATA),.*$|\1,,|" "$PURELIB_DIR/$new_name/RECORD"; \
        rm -rf "$PURELIB_DIR/$old_name"; \
    done
# Metadata inspection runs as root after runtime ownership is finalized. Do not
# let uv create a root-owned cache that the runtime user cannot open for writes.
RUN /usr/bin/uv --no-cache pip show abx-dl | tee -a /VERSION.txt \
    && if [[ "$ABX_DL_COMMIT_HASH" =~ ^[0-9a-fA-F]{40}$ ]]; then echo "COMMIT_HASH=$ABX_DL_COMMIT_HASH" | tee -a /VERSION.txt; fi

# The diagnostics below do not install binaries, but they exercise both
# check-mode and install-mode projections, whose derived cache shapes differ.
# Stabilize once after all checks and only then take the baseline hash. The
# final install is intentional and must not be removed as redundant: with
# networking disabled, both abxpkg's derived records and uv's small runtime
# index must remain byte-for-byte unchanged. If it repairs metadata or attempts
# an install, the image build fails.
RUN --network=none export ABX_DOCKER_PLUGINS="$(/venv/bin/python3 -c 'from abx_dl.catalog import PluginCatalog; print(" ".join(PluginCatalog.discover(runtime="abx-dl")))')" \
    && env -u ABXPKG_TMP_CACHE_DIR HOME=/home/archivebox \
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
        && abxpkg load --binproviders=env --min-version=11.0.0 java \
        && abxpkg run --binproviders=uv opendataloader-pdf --help \
        && abxpkg run --binproviders=uv papers-dl --help \
        && "$ABXPKG_LIB_DIR/pnpm/packages/claudecode/node_modules/.bin/claude" --version \
        && "$ABXPKG_LIB_DIR/pnpm/packages/opencode/node_modules/.bin/opencode" --version \
        && ! command -v gcc \
        && ! command -v g++ \
        && ! command -v make \
        && ! command -v cargo \
        && ! command -v sonic \
        && ! command -v supervisord \
        && abx-dl install $ABX_DOCKER_PLUGINS \
        && /usr/bin/uv run --no-project python -c "import abx_dl, abxpkg" \
        && (find "$ABXPKG_LIB_DIR" -name derived.env -type f -exec sha256sum {} +; find "$XDG_CACHE_HOME" -type f -exec sha256sum {} +) | sort > /tmp/cache-before \
        && abx-dl install $ABX_DOCKER_PLUGINS \
        && (find "$ABXPKG_LIB_DIR" -name derived.env -type f -exec sha256sum {} +; find "$XDG_CACHE_HOME" -type f -exec sha256sum {} +) | sort > /tmp/cache-after \
        && diff -u /tmp/cache-before /tmp/cache-after \
        && rm -f /tmp/cache-before /tmp/cache-after \
        && echo -e "\n\n[√] Finished abx-dl Docker build successfully." \
        && echo -e "BUILD_END_TIME=$(date +"%Y-%m-%d %H:%M:%S %s")\n\n" \
        )' | tee -a /VERSION.txt

WORKDIR /out
# Do not declare /data/personas: an anonymous nested volume hides personas in
# ArchiveBox's documented bind mount of the parent /data collection directory.
VOLUME ["/out"]
ENTRYPOINT ["dumb-init", "--", "abx-dl-docker-entrypoint"]
CMD ["--help"]
