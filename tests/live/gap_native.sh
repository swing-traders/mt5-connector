#!/bin/bash
set -euo pipefail
readonly td="$WINEPREFIX/drive_c/Program Files/MetaTrader 5"
readonly rt=/tmp/native-custom-probe
export DISPLAY=:0
export MT5_SERVER_SEARCH_STARTED_MARKER="probe: native server search started"
export MT5_SERVER_SEARCH_NUMBER=1
gui() {
    timeout -k 5 60 wine "$MT5_PYTHON_DIR\\python.exe" 'Z:\opt\mt5\terminal_gui.py' "$@" \
        >"$rt/gui-private.log" 2>&1
}
cleanup() {
    wineserver -k >/dev/null 2>&1 || :
    kill "$xvfb" 2>/dev/null || :
}
mkdir -p -m 0700 "$rt"
rm -f /tmp/.X0-lock /tmp/.X11-unix/X0
Xvfb :0 -screen 0 1920x1080x24 -nolisten tcp >/dev/null 2>&1 &
xvfb=$!
trap cleanup EXIT
for ((i=0;i<30;i++)); do
    [[ ! -S /tmp/.X11-unix/X0 ]] || break
    sleep 1
done
cp /src/GapNative.mq5 "$WINEPREFIX/drive_c/GapNative.mq5"
wine "$td/MetaEditor64.exe" '/compile:C:\GapNative.mq5' \
    '/log:C:\GapNative.log' >"$rt/compiler-private.log" 2>&1 || :
result=$(iconv -f UTF-16 -t UTF-8 "$WINEPREFIX/drive_c/GapNative.log" \
    | tr -d '\r' | grep '^Result:')
[[ "$result" == "Result: 0 errors, 0 warnings"* ]] \
    || { printf 'CUSTOM_PROBE_COMPILE_FAILED: %s\n' "$result"; exit 2; }
printf '%s\n' "$result"
cp "$WINEPREFIX/drive_c/GapNative.ex5" "$td/MQL5/Scripts/"
mkdir -p "$td/MQL5/Files"
cp /job/input/native-*.csv "$td/MQL5/Files/"
case "$PROBE_MODE" in
    install) ;;
    read) : >"$td/MQL5/Files/read-only.marker" ;;
    reload) : >"$td/MQL5/Files/reload.marker" ;;
    *) echo CUSTOM_PROBE_MODE_INVALID; exit 2 ;;
esac
(
    umask 077
    {
        printf '[Common]\r\nLogin=%s\r\nPassword="%s"\r\nServer=%s\r\n' \
            "$MT5_LOGIN" "$MT5_PASSWORD" "$MT5_SERVER"
        printf 'AutoConfiguration=true\r\nProxyEnable=false\r\n'
        printf '[Charts]\r\nProfile=default\r\nMaxBars=100000000\r\n'
        printf '[Experts]\r\nAllowDllImport=0\r\nEnabled=1\r\n'
    } >"$rt/search.ini"
    {
        cat "$rt/search.ini"
        printf '[StartUp]\r\nSymbol=BTCUSD.a\r\nPeriod=M1\r\nScript=GapNative\r\n'
    } >"$rt/import.ini"
)
wine "$td/terminal64.exe" '/config:Z:\tmp\native-custom-probe\search.ini' \
    /portable >/dev/null 2>&1 &
terminal=$!
for ((i=1;i<=120;i++)); do
    if gui main-window; then break; fi
    ((i != 120)) || { echo CUSTOM_PROBE_WINDOW_FAILED; exit 3; }
    sleep 1
done
gui dismiss-dialogs "Open an Account" || :
gui search-server || { echo CUSTOM_PROBE_SERVER_CHECK_FAILED; exit 3; }
wine taskkill /IM terminal64.exe >/dev/null 2>&1 || :
wait "$terminal" || :
wine "$td/terminal64.exe" '/config:Z:\tmp\native-custom-probe\import.ini' \
    /portable >/dev/null 2>&1 &
terminal=$!
outcome="$td/MQL5/Files/reference-floor/outcome.txt"
for ((i=0;i<120;i++)); do
    [[ ! -f "$outcome" ]] || break
    kill -0 "$terminal" 2>/dev/null || { echo CUSTOM_PROBE_OUTPUT_MISSING; exit 4; }
    sleep 5
done
[[ -f "$outcome" && "$(cat "$outcome")" == done ]] \
    || { echo CUSTOM_PROBE_IMPORT_FAILED; exit 4; }
cp "$td/MQL5/Files/reference-floor/"*.csv /job/out/
chmod a+r /job/out/*.csv
wine taskkill /IM terminal64.exe >/dev/null 2>&1 || :
wait "$terminal" || :
printf 'CUSTOM_PROBE_NATIVE_INSTALL_FINISHED\n'
