#!/bin/sh
# Proves the copied terminal starts in this prefix at its pinned build.
set -eu

IFS= read -r expected_build < "$HOME/.terminal-build"
case "$expected_build" in
    ''|*[!0-9]*) echo "terminal: copied build file is not a build number" >&2; exit 1 ;;
esac

terminal_dir="$WINEPREFIX/drive_c/Program Files/MetaTrader 5"
started='MetaTrader 5 x64 build [0-9]+ started'

terminal_log() {
    for log in "$terminal_dir"/logs/*.log; do
        if [ -f "$log" ]; then
            iconv -f UTF-16LE -t UTF-8 "$log"
        fi
    done
}

earlier_starts=$(terminal_log | grep -cE "$started" || :)
trap 'wineserver -k' EXIT
wine "$terminal_dir/terminal64.exe" /portable &

build=""
waited=0
until [ -n "$build" ] || [ "$waited" -ge 120 ]; do
    sleep 1
    waited=$((waited + 1))
    build=$(terminal_log \
        | awk -v skip="$earlier_starts" "/$started/ { n++ } n > skip" \
        | sed -nE 's/.*MetaTrader 5 x64 build ([0-9]+) started.*/\1/p' \
        | head -n 1)
done

if [ -z "$build" ]; then
    echo "terminal: copied terminal logged no start within 120s" >&2
    exit 1
fi
if [ "$build" != "$expected_build" ]; then
    echo "terminal: copied terminal build $build differs from pinned $expected_build" >&2
    exit 1
fi
printf 'terminal: copied terminal build %s started\n' "$build"
