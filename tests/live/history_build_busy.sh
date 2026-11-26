#!/bin/bash
set -euo pipefail
terminal="$WINEPREFIX/drive_c/Program Files/MetaTrader 5"
stage="$WINEPREFIX/drive_c/mt5build"
mkdir -p "$stage"
cp /probe/history_build_busy.mq5 /probe/ticks_under_test.mqh "$stage/"
sources=$(python -c 'import pathlib, mt5connector.server as s; print(pathlib.Path(s.__file__).parent / "mql5")')
cp -R "$sources/Include" "$stage/"
python -m mt5connector.server.mql5gen "$stage/Include/hub_wire.mqh" >/dev/null
trap 'wineserver -k >/dev/null 2>&1 || :' EXIT
wine "$terminal/MetaEditor64.exe" '/compile:C:\mt5build\history_build_busy.mq5' \
    '/log:C:\mt5build\compile.log' >/dev/null 2>&1 || :
wineserver -w
iconv -f UTF-16 -t UTF-8 "$stage/compile.log" | tr -d '\r'
cp "$stage/history_build_busy.ex5" /tmp/tickgen_raw.ex5
