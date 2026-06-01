# syntax=docker/dockerfile:1.7

# Dockerfile for abx-dl. This image owns the shared downloader runtime layer:
# Python, Node, abx-dl/abxpkg/abx-plugins, Chromium, and downloader plugin-managed tools.
# ArchiveBox-specific search/server pieces such as ripgrep, sonic, and
# supervisor intentionally remain owned by the ArchiveBox image.
#
# Build from the abx-dl package directory:
#   docker buildx build ./abx-dl -f ./abx-dl/Dockerfile \
#       --build-context abxbus=./abxbus \
#       --build-context abxpkg=./abxpkg \
#       --build-context abx-plugins=./abx-plugins \
#       -t archivebox/abx-dl:dev

ARG NODE_VERSION=24

FROM --platform=$TARGETPLATFORM node:${NODE_VERSION}-bookworm-slim AS node-runtime
FROM --platform=$TARGETPLATFORM debian:bookworm-slim AS abx-dl-runtime-base

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
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ONLY_BINARY=aiohttp \
    npm_config_loglevel=error

ENV PYTHON_VERSION=3.12 \
    NODE_VERSION=24

ENV ARCHIVEBOX_USER=archivebox \
    DEFAULT_PUID=911 \
    DEFAULT_PGID=911 \
    IN_DOCKER=True

ENV CODE_DIR=/app \
    DATA_DIR=/out \
    LIB_DIR=/opt/archivebox/lib \
    ABXPKG_LIB_DIR=/opt/archivebox/lib \
    PERSONAS_DIR=/data/personas \
    CHROME_HEADLESS=true \
    CHROME_SANDBOX=false \
    CHROME_ISOLATION=crawl

ENV UV_COMPILE_BYTECODE=0 \
    UV_PYTHON_PREFERENCE=managed \
    UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/venv \
    VIRTUAL_ENV=/venv \
    PIP_VENV_PYTHON=/venv/bin/python3 \
    PATH="/venv/bin:/opt/node/bin:$PATH"

SHELL ["/bin/bash", "-o", "pipefail", "-o", "errexit", "-o", "errtrace", "-o", "nounset", "-c"]
WORKDIR "$CODE_DIR"

RUN echo 'Binary::apt::APT::Keep-Downloaded-Packages "1";' > /etc/apt/apt.conf.d/99keep-cache \
    && echo 'APT::Install-Recommends "0";' > /etc/apt/apt.conf.d/99no-install-recommends \
    && echo 'APT::Install-Suggests "0";' > /etc/apt/apt.conf.d/99no-install-suggests \
    && rm -f /etc/apt/apt.conf.d/docker-clean

RUN (echo "[i] Docker build for abx-dl starting..." \
    && echo "PLATFORM=${TARGETPLATFORM} ARCH=$(uname -m) (${TARGETARCH} ${TARGETVARIANT})" \
    && echo "BUILD_START_TIME=$(date +"%Y-%m-%d %H:%M:%S %s") TZ=${TZ} LANG=${LANG}" \
    && uname -a \
    && sed -n '1,7p' /etc/os-release \
    && env \
    ) | tee -a /VERSION.txt

# Bootstrap packages only. Downloader/browser/media runtimes are installed by
# their owning plugin install hooks in separate layers below.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-$TARGETARCH$TARGETVARIANT \
    echo "[+] APT Installing abx-dl bootstrap dependencies for $TARGETPLATFORM..." \
    && apt-get update -qq \
    && apt-get install -qq -y \
        ca-certificates curl dumb-init gosu procps openssl unzip xz-utils zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=node-runtime /usr/local /opt/node

RUN export PATH="/opt/node/bin:$PATH" \
    && (/opt/node/bin/node --version && /opt/node/bin/npm --version) | tee -a /VERSION.txt

RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/bin sh

RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    echo "[+] UV Creating /venv using python ${PYTHON_VERSION} for ${TARGETPLATFORM}..." \
    && uv venv /venv --python "${PYTHON_VERSION}" \
    && uv pip install setuptools pip wheel \
    && (which python3 && python3 --version && which uv && uv self version && uv python find) | tee -a /VERSION.txt

ENV PYTHONDONTWRITEBYTECODE=1


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
    && uv pip install --refresh -r /tmp/abx-dl-requirements.txt \
    && find /venv -type d -name __pycache__ -prune -exec rm -rf {} + \
    && find /venv -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete

COPY --from=abxbus --chown=root:root --chmod=755 abxbus /src/abxbus/abxbus
COPY --from=abxpkg --chown=root:root --chmod=755 abxpkg /src/abxpkg/abxpkg
COPY --from=abx-plugins --chown=root:root --chmod=755 abx_plugins /src/abx-plugins/abx_plugins
COPY --chown=root:root --chmod=755 abx_dl "$CODE_DIR/abx_dl"
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    echo "[*] Installing local abxbus/abxpkg/abx-plugins/abx-dl Python source code..." \
    && uv pip install --no-deps /src/abxbus /src/abxpkg /src/abx-plugins "$CODE_DIR" \
    && (uv pip show abx-dl && which abx-dl && abx-dl --version) | tee -a /VERSION.txt \
    && find /venv /src "$CODE_DIR" -type d -name __pycache__ -prune -exec rm -rf {} + \
    && find /venv /src "$CODE_DIR" -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete

########################################################################################################
FROM abx-dl-runtime-base

COPY --from=abx-dl-builder /venv /venv
COPY --from=abx-dl-builder /VERSION.txt /VERSION.txt

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.npm,sharing=locked,id=npm-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/pip,sharing=locked,id=pip-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/puppeteer,sharing=locked,id=puppeteer-$TARGETARCH$TARGETVARIANT \
    echo "[+] Installing provider plugin dependencies..." \
    && apt-get update -qq \
    && mkdir -p "$LIB_DIR" \
    && ABXPKG_INSTALL_TIMEOUT=900 TIMEOUT=900 PUID=0 PGID=0 abx-dl plugins --install apt bash npm pip puppeteer

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.npm,sharing=locked,id=npm-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/ms-playwright,sharing=locked,id=browsers-$TARGETARCH$TARGETVARIANT \
    echo "[+] Installing Playwright-managed Chrome into $LIB_DIR..." \
    && apt-get update -qq \
    && abxpkg install --no-cache --install-timeout=900 --binproviders=playwright chrome \
    && CHROME_BINARY="$(abxpkg load --binproviders=playwright chromium | awk 'NF {print $2; exit}')" \
    && export CHROME_BINARY \
    && test -x "$CHROME_BINARY" \
    && "$CHROME_BINARY" --version | tee -a /VERSION.txt

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.npm,sharing=locked,id=npm-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/pip,sharing=locked,id=pip-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/puppeteer,sharing=locked,id=puppeteer-$TARGETARCH$TARGETVARIANT \
    echo "[+] Installing browser plugin dependencies..." \
    && CHROME_BINARY="$(abxpkg load --binproviders=playwright chromium | awk 'NF {print $2; exit}')" \
    && export CHROME_BINARY \
    && ABXPKG_INSTALL_TIMEOUT=900 TIMEOUT=900 PUID=0 PGID=0 abx-dl plugins --install \
        chrome chromewebstore accessibility consolelog dns dom headers redirects responses \
        screenshot pdf chrome_mhtml chrome_screencast sslcerts \
        parse_dom_outlinks seo archivewebpage singlefile ublock \
        istilldontcareaboutcookies modalcloser infiniscroll claudechrome

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.npm,sharing=locked,id=npm-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/pip,sharing=locked,id=pip-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/puppeteer,sharing=locked,id=puppeteer-$TARGETARCH$TARGETVARIANT \
    echo "[+] Installing downloader and document plugin dependencies..." \
    && ABXPKG_INSTALL_TIMEOUT=900 TIMEOUT=900 PUID=0 PGID=0 abx-dl plugins --install \
        wget git ytdlp gallerydl forumdl papersdl opendataloader archivedotorg \
        htmltotext readability mercury defuddle trafilatura liteparse \
        claudecode claudecodecleanup claudecodeextract

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.npm,sharing=locked,id=npm-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/pip,sharing=locked,id=pip-$TARGETARCH$TARGETVARIANT \
    --mount=type=cache,target=/root/.cache/puppeteer,sharing=locked,id=puppeteer-$TARGETARCH$TARGETVARIANT \
    echo "[+] Installing lightweight plugin dependencies..." \
    && ABXPKG_INSTALL_TIMEOUT=900 TIMEOUT=900 PUID=0 PGID=0 abx-dl plugins --install \
        archivedotorg base brew cargo favicon hashes media parse_html_urls \
        parse_jsonl_urls parse_netscape_urls parse_rss_urls parse_txt_urls \
        search_backend_sqlite ssl staticfile title

RUN echo "[+] Cleaning plugin-managed runtime caches..." \
    && find "$LIB_DIR" -type d \( \
            -name __pycache__ -o -name test -o -name tests -o -name doc -o -name docs -o -name example -o -name examples \
        \) -prune -exec rm -rf {} + \
    && find "$LIB_DIR" -type f \( \
            -name '*.pyc' -o -name '*.pyo' -o -name '*.map' -o -name '*.ts' -o -name '*.md' -o -name '*.markdown' \
        \) -delete \
    && find "$LIB_DIR" -type f -name '*.crx' -delete \
    && if [ -d "$LIB_DIR/puppeteer/cache" ]; then \
        find "$LIB_DIR/puppeteer/cache" -type d -name WidevineCdm -prune -exec rm -rf {} +; \
        find "$LIB_DIR/puppeteer/cache" -type f -path '*/locales/*' ! -name 'en-US.pak' -delete; \
    fi \
    && CHROME_BINARY="$(abxpkg load --binproviders=playwright chromium | awk 'NF {print $2; exit}')" \
    && export CHROME_BINARY \
    && abx-dl plugins chrome \
    && ! command -v gcc \
    && ! command -v g++ \
    && ! command -v make \
    && ! command -v cargo \
    && ! command -v rg \
    && ! command -v sonic \
    && ! command -v supervisord \
    && rm -rf /root/.cache /var/cache/apt/* /var/lib/apt/lists/*

RUN echo "[*] Setting up $ARCHIVEBOX_USER user uid=${DEFAULT_PUID}..." \
    && groupadd --system "$ARCHIVEBOX_USER" \
    && useradd --system --create-home --gid "$ARCHIVEBOX_USER" --groups audio,video "$ARCHIVEBOX_USER" \
    && usermod -u "$DEFAULT_PUID" "$ARCHIVEBOX_USER" \
    && groupmod -g "$DEFAULT_PGID" "$ARCHIVEBOX_USER" \
    && mkdir -p "$DATA_DIR" "$LIB_DIR" \
    && chown -R "$DEFAULT_PUID:$DEFAULT_PGID" "$DATA_DIR" "$LIB_DIR" \
    && echo "ARCHIVEBOX_USER=$ARCHIVEBOX_USER PUID=$(id -u "$ARCHIVEBOX_USER") PGID=$(id -g "$ARCHIVEBOX_USER")" | tee -a /VERSION.txt

WORKDIR /out

RUN (echo -e "\n\n[+] abx-dl runtime versions" \
    && abx-dl --version \
    && /opt/node/bin/node --version \
    && /venv/bin/python3 --version \
    && CHROME_BINARY="$(abxpkg load --binproviders=playwright chromium | awk 'NF {print $2; exit}')" \
    && export CHROME_BINARY \
    && abx-dl plugins chrome \
    && ! command -v gcc \
    && ! command -v g++ \
    && ! command -v make \
    && ! command -v cargo \
    && ! command -v rg \
    && ! command -v sonic \
    && ! command -v supervisord \
    && echo -e "\n\n[√] Finished abx-dl Docker build successfully." \
    && echo -e "BUILD_END_TIME=$(date +"%Y-%m-%d %H:%M:%S %s")\n\n" \
    ) | tee -a /VERSION.txt \
    && rm -rf /root/.cache /var/cache/apt/* /var/lib/apt/lists/*

WORKDIR /out
VOLUME ["/out", "/data/personas"]
ENTRYPOINT ["dumb-init", "--", "abx-dl"]
CMD ["--help"]
