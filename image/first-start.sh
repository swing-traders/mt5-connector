#!/bin/sh
# Starts the installed terminal once in portable mode, waits for its log to show a start and the
# first-start MQL5 recompile, then stops Wine and records the terminal build in ~/.terminal-build.
set -eu

terminal_dir="$WINEPREFIX/drive_c/Program Files/MetaTrader 5"
started='MetaTrader 5 x64 build [0-9]+ started'

terminal_log() {
    for log in "$terminal_dir"/logs/*.log; do
        iconv -f UTF-16LE -t UTF-8 "$log"
    done
}

# Only lines from this start count, whatever an earlier start already logged.
earlier_starts=$(terminal_log | grep -cE "$started" || :)

wine "$terminal_dir/terminal64.exe" /portable &

this_start=""
waited=0
until printf '%s\n' "$this_start" | grep -q 'full recompilation has been finished' \
    || [ "$waited" -ge 180 ]; do
    sleep 1
    waited=$((waited + 1))
    this_start=$(terminal_log | awk -v skip="$earlier_starts" "/$started/ { n++ } n > skip")
done

wineserver -k

if ! printf '%s\n' "$this_start" | grep -q 'full recompilation has been finished'; then
    echo "terminal: no start with a finished recompile logged within 180s" >&2
    exit 1
fi
printf '%s\n' "$this_start" | sed -nE "1s/.*MetaTrader 5 x64 build ([0-9]+) started.*/\1/p" \
    > "$HOME/.terminal-build"
grep -qE '^[0-9]+$' "$HOME/.terminal-build"
