#!/bin/sh
# Places the connector's EA and startup script in the terminal, compiled, and writes the two chart
# templates that attach the EA. Runs once, at image build, under Xvfb.
set -eu

terminal_dir="$WINEPREFIX/drive_c/Program Files/MetaTrader 5"
mql5="$terminal_dir/MQL5"
wheel_mql5="$(winepath -u "$MT5_PYTHON_DIR")/Lib/site-packages/mt5connector/server/mql5"
# MetaEditor truncates a /compile: path at its first space, so each source compiles from here.
stage="$WINEPREFIX/drive_c/mt5build"

cp "$wheel_mql5/Expert/ticks.mq5" "$mql5/Experts/"
cp "$wheel_mql5/Scripts/ticks_setup.mq5" "$mql5/Scripts/"
cp "$wheel_mql5/Include/JAson.mqh" "$mql5/Include/"
cp -R "$wheel_mql5/Include/MQL5Book" "$mql5/Include/"

rm -rf "$stage"
mkdir "$stage"
cp "$wheel_mql5/Expert/ticks.mq5" "$wheel_mql5/Scripts/ticks_setup.mq5" "$stage/"
cp -R "$wheel_mql5/Include" "$stage/"

# MetaEditor honours only the last /compile: of an invocation, and its exit status is not the
# compile's result: the .ex5 and the log's `Result:` line are.
for source in ticks ticks_setup; do
    wine "$terminal_dir/MetaEditor64.exe" "/compile:C:\\mt5build\\$source.mq5" \
        "/log:C:\\mt5build\\$source.log" || :
    wineserver -w
    result=$(iconv -f UTF-16 -t UTF-8 "$stage/$source.log" | tr -d '\r' | grep '^Result:' || :)
    echo "MetaEditor $source.mq5: ${result:-no Result line}"
    if [ ! -f "$stage/$source.ex5" ] \
        || ! printf '%s\n' "$result" | grep -Eq '^Result: 0 errors, 0 warnings(,|$)'; then
        iconv -f UTF-16 -t UTF-8 "$stage/$source.log" >&2 || :
        echo "MetaEditor: $source.mq5 did not compile clean" >&2
        exit 1
    fi
done
cp "$stage/ticks.ex5" "$mql5/Experts/"
cp "$stage/ticks_setup.ex5" "$mql5/Scripts/"
rm -rf "$stage"

# The terminal reads a template only as it saves one: UTF-16LE behind a byte-order mark, CRLF.
write_template() {
    name=$1
    spawner=$2
    for directory in "$mql5/Profiles/Templates" "$terminal_dir/Profiles/Templates"; do
        mkdir -p "$directory"
        {
            printf '\377\376'
            sed -e "s/@HUB_PORT@/$MT5_HUB_PORT/" -e "s/@SPAWNER@/$spawner/" -e 's/$/\r/' \
                /opt/mt5/ticks.tpl.in | iconv -f UTF-8 -t UTF-16LE
        } > "$directory/$name"
    done
}
write_template ticks_spawner.tpl true
write_template ticks.tpl false
