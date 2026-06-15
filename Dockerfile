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

ARG NODE_VERSION=24

FROM --platform=$TARGETPLATFORM node:${NODE_VERSION}-trixie-slim AS node-runtime
FROM --platform=$TARGETPLATFORM debian:trixie-slim AS abx-dl-runtime-base

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

ENV PYTHON_VERSION=3.13 \
    NODE_VERSION=24

ENV ARCHIVEBOX_USER=archivebox \
    DEFAULT_ARCHIVEBOX_UID=911 \
    DEFAULT_ARCHIVEBOX_GID=911 \
    IN_DOCKER=True

ENV CODE_DIR=/app \
    DATA_DIR=/out \
    CONFIG_DIR=/opt/archivebox \
    ABXPKG_LIB_DIR=/opt/archivebox/lib \
    PLAYWRIGHT_BROWSERS_PATH=/opt/archivebox/lib/playwright/cache \
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
        ca-certificates curl dumb-init util-linux procps openssl unzip xz-utils zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=node-runtime /usr/local /opt/node

RUN export PATH="/opt/node/bin:$PATH" \
    && (which node && which npm) | tee -a /VERSION.txt

RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/bin sh

RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    echo "[+] UV Creating /venv using python ${PYTHON_VERSION} for ${TARGETPLATFORM}..." \
    && uv venv /venv --python "${PYTHON_VERSION}" \
    && uv pip install setuptools pip wheel \
    && (which python3 && which uv && uv python find) | tee -a /VERSION.txt

########################################################################################################
FROM abx-dl-runtime-base AS abx-dl-builder

WORKDIR "$CODE_DIR"
COPY --from=abxbus --chown=root:root --chmod=755 pyproject.toml README.md LICENSE /src/abxbus/
COPY --from=abxpkg --chown=root:root --chmod=755 pyproject.toml README.md LICENSE /src/abxpkg/
COPY --from=abx-plugins --chown=root:root --chmod=755 pyproject.toml README.md LICENSE /src/abx-plugins/
COPY --chown=root:root --chmod=755 pyproject.toml README.md LICENSE "$CODE_DIR/"
RUN --mount=type=bind,source=pyproject.toml,target=/app/pyproject.toml \
    --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    echo "[+] UV Installing external Python dependencies from local package metadata..." \
    && /venv/bin/python3 -c 'import re, tomllib; paths = ["/src/abxbus/pyproject.toml", "/src/abxpkg/pyproject.toml", "/src/abx-plugins/pyproject.toml", "/app/pyproject.toml"]; skip = {"abxbus", "abxpkg", "abx-plugins", "abx-dl"}; deps = []; [deps.extend(tomllib.load(open(path, "rb"))["project"].get("dependencies", [])) for path in paths]; seen = set(); print("\n".join(dep for dep in deps if (name := re.split(r"[<>=!~;\\[]", dep, 1)[0].strip().lower()) not in skip and not (dep in seen or seen.add(dep))))' > /tmp/abx-dl-requirements.txt \
    && uv pip install --refresh -r /tmp/abx-dl-requirements.txt

COPY --from=abxbus --chown=root:root --chmod=755 abxbus /src/abxbus/abxbus
COPY --from=abxpkg --chown=root:root --chmod=755 abxpkg /src/abxpkg/abxpkg
COPY --from=abx-plugins --chown=root:root --chmod=755 abx_plugins /src/abx-plugins/abx_plugins
COPY --chown=root:root --chmod=755 abx_dl "$CODE_DIR/abx_dl"
COPY --chown=root:root --chmod=755 .git "$CODE_DIR/.git"
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    echo "[*] Installing local abxbus/abxpkg/abx-plugins/abx-dl Python source code..." \
    && COMMIT_HASH="$( \
        if [[ -f "$CODE_DIR/.git/HEAD" ]]; then \
            HEAD_REF="$(cat "$CODE_DIR/.git/HEAD")"; \
            if [[ "$HEAD_REF" =~ ^[0-9a-fA-F]{40}$ ]]; then \
                echo "$HEAD_REF"; \
            elif [[ "$HEAD_REF" == ref:\ * ]]; then \
                REF_PATH="${HEAD_REF#ref: }"; \
                cat "$CODE_DIR/.git/$REF_PATH" 2>/dev/null || awk -v ref="$REF_PATH" '$2 == ref {print $1}' "$CODE_DIR/.git/packed-refs" 2>/dev/null || true; \
            fi; \
        fi)" \
    && if [[ "$COMMIT_HASH" =~ ^[0-9a-fA-F]{40}$ ]]; then echo "COMMIT_HASH=$COMMIT_HASH" | tee -a /VERSION.txt; fi \
    && uv pip install --no-deps /src/abxbus /src/abxpkg /src/abx-plugins "$CODE_DIR" \
    && /usr/bin/uv pip show abx-dl | tee -a /VERSION.txt \
    && rm -f /venv/bin/uv /venv/bin/uvx \
    && rm -rf /venv/lib/python3.*/site-packages/pip* /venv/lib/python3.*/site-packages/setuptools* /venv/lib/python3.*/site-packages/wheel* /venv/bin/pip /venv/bin/pip3 /venv/bin/pip3.* /venv/bin/wheel \
    && (which abx-dl && abx-dl version) | tee -a /VERSION.txt

########################################################################################################
FROM abx-dl-runtime-base

COPY --from=abx-dl-builder /venv /venv
COPY --from=abx-dl-builder /VERSION.txt /VERSION.txt

RUN echo "[*] Setting up $ARCHIVEBOX_USER user uid=${DEFAULT_ARCHIVEBOX_UID}..." \
    && groupadd --system "$ARCHIVEBOX_USER" \
    && useradd --system --create-home --gid "$ARCHIVEBOX_USER" --groups audio,video "$ARCHIVEBOX_USER" \
    && usermod -u "$DEFAULT_ARCHIVEBOX_UID" "$ARCHIVEBOX_USER" \
    && groupmod -g "$DEFAULT_ARCHIVEBOX_GID" "$ARCHIVEBOX_USER" \
    && install -d -o "$DEFAULT_ARCHIVEBOX_UID" -g "$DEFAULT_ARCHIVEBOX_GID" "$DATA_DIR" "$CONFIG_DIR" "$ABXPKG_LIB_DIR" "$PLAYWRIGHT_BROWSERS_PATH" \
    && echo "ARCHIVEBOX_USER=$ARCHIVEBOX_USER ARCHIVEBOX_UID=$(id -u "$ARCHIVEBOX_USER") ARCHIVEBOX_GID=$(id -g "$ARCHIVEBOX_USER")" | tee -a /VERSION.txt

RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.npm,sharing=locked,id=npm-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/npm,sharing=locked,id=abxpkg-npm-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/pnpm,sharing=locked,id=abxpkg-pnpm-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/pip,sharing=locked,id=pip-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/ms-playwright,sharing=locked,id=browsers-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/var/tmp/abxpkg-cache,sharing=locked,mode=1777,id=abxpkg-tmp-$TARGETARCH$TARGETVARIANT \
    echo "[+] Installing Chrome and plugin dependencies..." \
    && apt-get update -qq \
    && apt-get install -qq -y --no-install-recommends binutils \
    && export HOME=/var/tmp/abxpkg-cache ABXPKG_TMP_CACHE_DIR=/var/tmp/abxpkg-cache \
    && python3 -c 'from abx_dl.models import discover_plugins; [print(f"export {plugin.enabled_key}=True") for plugin in discover_plugins(runtime="abx-dl").values() if plugin.enabled_key in plugin.config.properties]' > /tmp/abx-dl-enable-plugins.env \
    && sort /tmp/abx-dl-enable-plugins.env | tee -a /VERSION.txt \
    && source /tmp/abx-dl-enable-plugins.env \
    && ABXPKG_NO_CACHE=True ABXPKG_INSTALL_TIMEOUT=900 ABXPKG_POSTINSTALL_SCRIPTS=True ABXPKG_MIN_RELEASE_AGE=0 TIMEOUT=900 abx-dl install chrome \
    && CHROME_BINARY="$ABXPKG_LIB_DIR/playwright/bin/chromium" \
    && export CHROME_BINARY \
    && test -x "$CHROME_BINARY" \
    && abxpkg load --binproviders=env --abspath="$CHROME_BINARY" --min-version=149.0.0 chromium | tee -a /VERSION.txt \
    && ABXPKG_NO_CACHE=True ABXPKG_INSTALL_TIMEOUT=900 ABXPKG_POSTINSTALL_SCRIPTS=True ABXPKG_MIN_RELEASE_AGE=0 TIMEOUT=900 abx-dl install \
    && mkdir -p "$ABXPKG_LIB_DIR/env/bin" \
    && ln -sf /usr/bin/git "$ABXPKG_LIB_DIR/env/bin/git" \
    && rm -rf "$ABXPKG_LIB_DIR"/playwright/cache/ffmpeg-* \
    && find "$ABXPKG_LIB_DIR"/chromewebstore -type f -name '*.crx' -delete \
    && find "$ABXPKG_LIB_DIR"/playwright/cache -path '*/chrome-linux*/locales/*' ! -name 'en-US.pak' -delete \
    && find "$ABXPKG_LIB_DIR"/playwright/cache -path '*/chrome-linux*/*.pak.info' -delete \
    && rm -f "$ABXPKG_LIB_DIR"/playwright/cache/chromium-*/chrome-linux*/libvk_swiftshader.so "$ABXPKG_LIB_DIR"/playwright/cache/chromium-*/chrome-linux*/libGLESv2.so \
    && rm -f "$ABXPKG_LIB_DIR"/playwright/cache/chromium-*/chrome-linux*/chrome_200_percent.pak \
    && rm -rf "$ABXPKG_LIB_DIR"/playwright/cache/chromium-*/chrome-linux*/MEIPreload "$ABXPKG_LIB_DIR"/playwright/cache/chromium-*/chrome-linux*/PrivacySandboxAttestationsPreloaded "$ABXPKG_LIB_DIR"/playwright/cache/chromium-*/chrome-linux*/WidevineCdm \
    && rm -rf "$ABXPKG_LIB_DIR"/pnpm/packages/singlefile/node_modules/.pnpm/selenium-webdriver@*/node_modules/selenium-webdriver/bin/macos "$ABXPKG_LIB_DIR"/pnpm/packages/singlefile/node_modules/.pnpm/selenium-webdriver@*/node_modules/selenium-webdriver/bin/windows \
    && if [[ "$TARGETARCH" == "arm64" ]]; then rm -f "$ABXPKG_LIB_DIR"/pnpm/packages/liteparse/node_modules/.pnpm/@llamaindex+liteparse@*/node_modules/@llamaindex/liteparse/liteparse.linux-x64-gnu.node "$ABXPKG_LIB_DIR"/pnpm/packages/liteparse/node_modules/.pnpm/@llamaindex+liteparse@*/node_modules/@llamaindex/liteparse/libpdfium.so; fi \
    && find "$ABXPKG_LIB_DIR"/pnpm /opt/node -type f -name '*.map' -delete \
    && rm -rf /usr/lib/*-linux-gnu/dri /usr/lib/*-linux-gnu/libLLVM*.so* /usr/lib/*-linux-gnu/libz3.so.* \
    && rm -rf /usr/share/icons /usr/share/doc /usr/share/man /usr/share/bash-completion /usr/share/zsh /usr/share/info /usr/share/lintian /usr/share/bug \
    && rm -rf /opt/node/include /opt/node/share/doc /opt/node/share/man \
    && rm -f /opt/node/CHANGELOG.md /opt/node/README.md /opt/node/LICENSE \
    && rm -f /usr/lib/jvm/java-*-openjdk-*/lib/server/classes*.jsa \
    && strip --strip-unneeded "$CHROME_BINARY" \
    && (find "$ABXPKG_LIB_DIR" -type f \( -name '*.so' -o -name '*.node' \) -exec strip --strip-unneeded {} + 2>/dev/null || true) \
    && apt-get purge -y --auto-remove binutils \
    && rm -f /venv/bin/uv /venv/bin/uvx \
    && find "$ABXPKG_LIB_DIR" \( ! -user "$DEFAULT_ARCHIVEBOX_UID" -o ! -group "$DEFAULT_ARCHIVEBOX_GID" \) -exec chown "$DEFAULT_ARCHIVEBOX_UID:$DEFAULT_ARCHIVEBOX_GID" {} + \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN (echo -e "\n\n[+] abx-dl runtime versions" \
    && abx-dl version \
    && abxpkg load --binproviders=env /opt/node/bin/node \
    && abxpkg load --binproviders=env /venv/bin/python3 \
    && python3 -c 'from abx_dl.models import discover_plugins; [print(f"export {plugin.enabled_key}=True") for plugin in discover_plugins(runtime="abx-dl").values() if plugin.enabled_key in plugin.config.properties]' > /tmp/abx-dl-enable-plugins.env \
    && source /tmp/abx-dl-enable-plugins.env \
    && CHROME_BINARY="$ABXPKG_LIB_DIR/playwright/bin/chromium" \
    && export CHROME_BINARY \
    && abxpkg load --binproviders=env --abspath="$CHROME_BINARY" --min-version=149.0.0 chromium \
    && abx-dl plugins \
    && abxpkg load --binproviders=env rg \
    && ! command -v gcc \
    && ! command -v g++ \
    && ! command -v make \
    && ! command -v cargo \
    && ! command -v sonic \
    && ! command -v supervisord \
    && echo -e "\n\n[√] Finished abx-dl Docker build successfully." \
    && echo -e "BUILD_END_TIME=$(date +"%Y-%m-%d %H:%M:%S %s")\n\n" \
    ) | tee -a /VERSION.txt

WORKDIR /out
VOLUME ["/out", "/data/personas"]
ENTRYPOINT ["dumb-init", "--", "abx-dl"]
CMD ["--help"]
