# Build context: the repository root. Only the installed terminal leaves the install stage.
ARG VERSION_WINE=11.19~trixie-1

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
COPY image/first-start.sh ./

FROM docker.io/mambaorg/micromamba:2.8-debian13-slim AS install
ARG VERSION_WINE
USER root
COPY --from=fetch /artifacts/winehq.key /etc/apt/keyrings/winehq-archive.key
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
        winehq-devel="$VERSION_WINE" \
        wine-devel="$VERSION_WINE" \
        wine-devel-amd64="$VERSION_WINE" \
        wine-devel-i386="$VERSION_WINE" \
        xvfb xauth fonts-liberation tini \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --gid 65532 mt5 \
    && useradd --uid 65532 --gid 65532 --create-home --home-dir /home/mt5 mt5
USER mt5
WORKDIR /home/mt5
ENV HOME=/home/mt5 \
    WINEPREFIX=/home/mt5/.wine \
    WINEARCH=win64 \
    WINEDEBUG=-all
# The base's shell runs a RUN's lone command as PID 1, which Xvfb never tells it is ready, so a RUN
# that is one xvfb-run runs it under tini. Disabling mscoree and mshtml keeps wineboot from opening
# its blocking Mono/Gecko download prompt.
RUN --mount=type=bind,from=fetch,source=/artifacts,target=/artifacts \
    tini -- xvfb-run -a sh -ec ' \
        WINEDLLOVERRIDES="mscoree,mshtml=" wineboot -u; \
        wineserver -w; \
        WINEDLLOVERRIDES=mscoree=d wine msiexec \
            /i "Z:\\artifacts\\wine-mono.msi" /qn; \
        wineserver -w'
# The web installer fetches whatever terminal is current. It exits 1 after a successful install, so
# the first start is the success check. When it cannot finish, offline or with a terminal that
# cannot start, it hangs and ignores SIGTERM, so the bound kills it. The shell here is not a lone
# xvfb-run, so this RUN needs no tini.
RUN --mount=type=bind,from=fetch,source=/artifacts,target=/artifacts \
    xvfb-run -a sh -c ' \
        timeout -s KILL 600 wine /artifacts/mt5setup.exe /auto; \
        if [ $? -eq 137 ]; then \
            wineserver -k; \
            echo "mt5setup.exe: no exit within 600s" >&2; \
            exit 1; \
        fi; \
        wineserver -w' \
    && xvfb-run -a sh /artifacts/first-start.sh \
    && rm -rf "$WINEPREFIX/drive_c/users/mt5/AppData/Roaming/MetaQuotes/WebInstall" \
    # The history store ships empty, so the volume mounted on it starts with nothing from the image.
    && find "$WINEPREFIX/drive_c/Program Files/MetaTrader 5/Bases" -mindepth 1 -delete \
    && find "$WINEPREFIX/drive_c/Program Files/MetaTrader 5" \
        -type f -ipath '*/logs/*' -delete

FROM scratch
LABEL org.opencontainers.image.source=https://github.com/swing-traders/mt5-connector \
    org.opencontainers.image.description="Installed MetaTrader 5 terminal"
COPY --from=install ["/home/mt5/.wine/drive_c/Program Files/MetaTrader 5", "/terminal"]
COPY --from=install /home/mt5/.wine/drive_c/users/mt5/AppData/Roaming/MetaQuotes /metaquotes
COPY --from=install /home/mt5/.terminal-build /terminal-build
