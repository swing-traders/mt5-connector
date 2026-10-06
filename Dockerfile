# The MT5 server: the terminal under Wine with the connector's EA compiled in, the connector's HTTP
# server under the Windows Python and its push hub under the environment's Linux Python, headless.
# Build context: the repository root.
ARG VERSION_WINE=10.0.0.0~trixie-1
ARG VERSION_WINE_MONO=9.4.0
ARG VERSION_PYTHON_WINDOWS=3.13.16

FROM docker.io/library/debian:trixie-slim AS fetch

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /artifacts
COPY image/artifacts.txt ./
RUN set -e; \
    while read -r name url sha256; do \
        curl -fsSL --retry 3 -o "$name" "$url"; \
        printf '%s  %s\n' "$sha256" "$name" >> SHA256SUMS; \
    done < artifacts.txt; \
    sha256sum -c SHA256SUMS
COPY image/requirements-wine.txt image/first-start.sh ./


FROM docker.io/mambaorg/micromamba:2.8-debian13-slim
ARG VERSION_WINE
ARG VERSION_WINE_MONO
ARG VERSION_PYTHON_WINDOWS

# The registry links a package to its repository by this label; the base image's own value names
# micromamba's repository.
LABEL org.opencontainers.image.source=https://github.com/swing-traders/mt5-connector

USER root
COPY --from=fetch /artifacts/winehq.key /etc/apt/keyrings/winehq-archive.key

# WineHQ's wine-stable hard-depends on its i386 half. rsync restores the terminal's baked state at
# every start.
RUN export DEBIAN_FRONTEND=noninteractive \
    && dpkg --add-architecture i386 \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && printf '%s\n' \
        'Types: deb' \
        'URIs: https://dl.winehq.org/wine-builds/debian' \
        'Suites: trixie' \
        'Components: main' \
        'Architectures: amd64 i386' \
        'Signed-By: /etc/apt/keyrings/winehq-archive.key' \
        > /etc/apt/sources.list.d/winehq.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        winehq-stable="$VERSION_WINE" \
        wine-stable="$VERSION_WINE" \
        wine-stable-amd64="$VERSION_WINE" \
        wine-stable-i386="$VERSION_WINE" \
        xvfb \
        xauth \
        fonts-liberation \
        tini \
        rsync \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 65532 mt5 \
    && useradd --uid 65532 --gid 65532 --create-home --home-dir /home/mt5 mt5

USER mt5
WORKDIR /home/mt5
# The Windows Python's directory is C:\Python<major><minor>.
ARG PYTHON_WINDOWS_MAJOR_MINOR=${VERSION_PYTHON_WINDOWS%.*}
# The EA templates are written against MT5_HUB_PORT, so the ports are the image's, not a run's.
ENV HOME=/home/mt5 \
    WINEPREFIX=/home/mt5/.wine \
    WINEARCH=win64 \
    WINEDEBUG=-all \
    MT5_TERMINAL_PATH="C:\\Program Files\\MetaTrader 5\\terminal64.exe" \
    MT5_PYTHON_DIR="C:\\Python${PYTHON_WINDOWS_MAJOR_MINOR/./}" \
    MT5_API_PORT=5000 \
    MT5_HUB_PORT=9000

# The base's shell runs a RUN's lone command as PID 1, which Xvfb never tells it is ready, so a RUN
# that is one xvfb-run runs it under tini. Disabling mscoree and mshtml keeps wineboot from opening
# its blocking Mono/Gecko download prompt.
RUN --mount=type=bind,from=fetch,source=/artifacts,target=/artifacts \
    tini -- xvfb-run -a sh -ec ' \
        WINEDLLOVERRIDES="mscoree,mshtml=" wineboot -u; \
        wineserver -w; \
        WINEDLLOVERRIDES=mscoree=d wine msiexec \
            /i "Z:\\artifacts\\wine-mono-${VERSION_WINE_MONO}-x86.msi" /qn; \
        wineserver -w'

RUN --mount=type=bind,from=fetch,source=/artifacts,target=/artifacts \
    tini -- xvfb-run -a sh -ec ' \
        wine "/artifacts/python-${VERSION_PYTHON_WINDOWS}-amd64.exe" /quiet InstallAllUsers=1 \
            PrependPath=1 Include_test=0 Include_launcher=0 "TargetDir=$MT5_PYTHON_DIR"; \
        wineserver -w; \
        wine python -m pip install --no-cache-dir --require-hashes \
            -r Z:/artifacts/requirements-wine.txt; \
        wineserver -w'

# The web installer fetches whatever terminal build is current. It exits 1 after a successful
# install, so the first start is the success check; offline it hangs instead of failing. The history
# store ships empty, so the volume mounted on it starts with nothing from the image.
RUN --mount=type=bind,from=fetch,source=/artifacts,target=/artifacts \
    xvfb-run -a sh -c ' \
        timeout 600 wine /artifacts/mt5setup.exe /auto; \
        if [ $? -eq 124 ]; then \
            wineserver -k; \
            echo "mt5setup.exe: no exit within 600s" >&2; \
            exit 1; \
        fi; \
        wineserver -w' \
    && xvfb-run -a sh /artifacts/first-start.sh \
    && rm -rf "$WINEPREFIX/drive_c/users/mt5/AppData/Roaming/MetaQuotes/WebInstall" \
    && find "$WINEPREFIX/drive_c/Program Files/MetaTrader 5/Bases" -mindepth 1 -delete

# The checkout's runtime environment, its client editable from the copy, is the base environment,
# root-owned and read-only. Root's steps run in root's home, leaving nothing in the runtime user's.
USER root
ARG MAMBA_DOCKERFILE_ACTIVATE=1
COPY environment.yml VERSION /opt/mt5-connector/
COPY packages/client /opt/mt5-connector/packages/client
RUN export HOME=/root PIP_ROOT_USER_ACTION=ignore PIP_NO_CACHE_DIR=1 \
    && micromamba install --yes --name base --file /opt/mt5-connector/environment.yml \
    && micromamba clean --all --yes \
    && rm -rf "$MAMBA_ROOT_PREFIX/pkgs" \
    && chown -R root:root "$MAMBA_ROOT_PREFIX" \
    && chmod -R go-w "$MAMBA_ROOT_PREFIX"

# One wheel of the checkout's server installs into both Pythons; the hub runs from this one.
COPY packages/server /opt/mt5-connector/packages/server
RUN export HOME=/root PIP_ROOT_USER_ACTION=ignore PIP_NO_CACHE_DIR=1 \
    && python -m pip wheel --no-deps --wheel-dir /opt/mt5-connector/dist \
        /opt/mt5-connector/packages/server \
    && python -m pip install /opt/mt5-connector/dist/mt5_connector_server-*.whl \
    && python -m pip check

ENV PATH="${MAMBA_ROOT_PREFIX}/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1

# The hashed dependencies stand as installed; pip check fails the build where they no longer satisfy
# the server's own pins.
USER mt5
RUN tini -- xvfb-run -a sh -ec ' \
        wine python -m pip install --no-cache-dir --no-deps --no-index \
            --find-links Z:/opt/mt5-connector/dist mt5-connector-server; \
        wine python -m pip check; \
        wineserver -w'

COPY image/ /opt/mt5/

RUN tini -- xvfb-run -a sh /opt/mt5/install-ea.sh

# The terminal as every start finds it, which the entrypoint restores: its directory, where the
# chart profile the startup ini names holds no chart, the logs are empty and the history store,
# being the volume's, is left out; its AppData; and Wine's registry, as the last Wine of the build
# saved it.
USER root
RUN terminal_dir="$WINEPREFIX/drive_c/Program Files/MetaTrader 5" \
    && for charts in "$terminal_dir/Profiles/Charts" "$terminal_dir/MQL5/Profiles/Charts"; do \
        if [ -d "$charts" ]; then \
            find "$charts" -mindepth 1 -maxdepth 1 -type d -iname default -printf 'emptying %p\n' \
                -exec find {} -mindepth 1 -delete \; || exit 1; \
        fi; \
    done \
    && mkdir -p /opt/mt5-baked/registry \
    && rsync -rlpt --exclude=/Bases --exclude='/logs/*' --exclude='/MQL5/logs/*' \
        "$terminal_dir/" /opt/mt5-baked/terminal/ \
    && rsync -rlpt "$WINEPREFIX/drive_c/users/mt5/AppData/Roaming/MetaQuotes/" \
        /opt/mt5-baked/metaquotes/ \
    && rsync -lpt "$WINEPREFIX/system.reg" "$WINEPREFIX/user.reg" "$WINEPREFIX/userdef.reg" \
        /opt/mt5-baked/registry/

# A debug build carries x11vnc, which the entrypoint starts on the display, loopback only.
ARG DEBUG=0
RUN case "$DEBUG" in \
        0) ;; \
        1) apt-get update \
            && apt-get install -y --no-install-recommends x11vnc \
            && rm -rf /var/lib/apt/lists/* ;; \
        *) echo "DEBUG is 0 or 1, not $DEBUG" >&2; exit 1 ;; \
    esac
USER mt5

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/mt5/entrypoint.sh"]
