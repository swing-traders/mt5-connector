# Stage 1: Base image with apt packages
FROM ghcr.io/linuxserver/baseimage-selkies:arch-2026-08-07-ls182 AS base

ENV TITLE=MetaTrader
ENV WINEARCH=win64
ENV WINEPREFIX="/config/.wine"
ENV DISPLAY=:0
ENV SELKIES_IS_MANUAL_RESOLUTION_MODE=true
ENV SELKIES_MANUAL_WIDTH=1920
ENV SELKIES_MANUAL_HEIGHT=1080
# Ensure the directory exists with correct permissions
RUN mkdir -p /config/.wine && \
    chown -R abc:abc /config/.wine && \
    chmod -R 755 /config/.wine

# Update package lists and upgrade packages
RUN pacman -Suy --noconfirm
# Install required packages
RUN pacman -Sy --noconfirm \
    dos2unix \
    python-pip \
    python-websockets \
    wget \
    python-pyxdg \
    wine \
    imagemagick

# Stage 2: Final image
FROM base

# Built from the repository root: the server imports the client package's inventory and broker clock.
COPY mt5connect/__init__.py mt5connect/mirror.py mt5connect/broker_clock.py /app/mt5connect/
COPY mt5server/__init__.py /app/mt5server/
COPY mt5server/app /app/mt5server/app
COPY mt5server/scripts /scripts
COPY mt5server/root /
COPY mt5server/mt5ticks /mt5ticks
RUN dos2unix /scripts/*.sh && \
    chmod +x /scripts/*.sh

RUN touch /var/log/mt5_setup.log && \
    chown abc:abc /var/log/mt5_setup.log && \
    chmod 644 /var/log/mt5_setup.log

EXPOSE 3000 5000 5001 8001 9000 18812
ENV MT5_API_PORT=5000
ENV WS_PORT=9000
ENV MT5_TERMINAL_PATH="C:\\Program Files\\MetaTrader 5\\terminal64.exe"
VOLUME /config
