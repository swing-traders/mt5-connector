#!/bin/sh
# Places the connector's EA, its history indicator and the startup script in the terminal, compiled
# with the EA's half of the hub protocol generated from the installed wheel, and writes the three
# chart templates that attach the EA in its roles. Runs once, at image build, under Xvfb.
set -eu

terminal_dir="$WINEPREFIX/drive_c/Program Files/MetaTrader 5"
mql5="$terminal_dir/MQL5"
wheel_mql5="$(winepath -u "$MT5_PYTHON_DIR")/Lib/site-packages/mt5connector/server/mql5"
# MetaEditor truncates a /compile: path at its first space, so each source compiles from here.
stage="$WINEPREFIX/drive_c/mt5build"

rm -rf "$stage"
mkdir "$stage"
cp "$wheel_mql5/Expert/ticks.mq5" "$wheel_mql5/Scripts/ticks_setup.mq5" \
    "$wheel_mql5/Indicators/hub_history.mq5" "$stage/"
cp -R "$wheel_mql5/Include" "$stage/"
# The Linux Python's wheel is the one the Windows Python installed.
python -m mt5connector.server.mql5gen "$stage/Include/hub_wire.mqh"
echo "generated $stage/Include/hub_wire.mqh"

cp "$wheel_mql5/Expert/ticks.mq5" "$mql5/Experts/"
cp "$wheel_mql5/Scripts/ticks_setup.mq5" "$mql5/Scripts/"
cp "$wheel_mql5/Indicators/hub_history.mq5" "$mql5/Indicators/"
cp "$stage/Include/hub_codec.mqh" "$stage/Include/hub_wire.mqh" "$mql5/Include/"
cp -R "$stage/Include/MQL5Book" "$mql5/Include/"

# MetaEditor honours only the last /compile: of an invocation, and its exit status is not the
# compile's result: the .ex5 and the log's `Result:` line are.
for source in ticks ticks_setup hub_history; do
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
cp "$stage/hub_history.ex5" "$mql5/Indicators/"
rm -rf "$stage"

# An enum input travels as its value, which the generated ENUM_CHART_ROLE assigns.
role_value() {
    value=$(sed -n "s/^ *$1 = \([0-9][0-9]*\),\{0,1\}\$/\1/p" "$mql5/Include/hub_wire.mqh")
    if [ -z "$value" ]; then
        echo "hub_wire.mqh assigns $1 no value" >&2
        exit 1
    fi
    echo "$value"
}

# The terminal reads a template only as it saves one: UTF-16LE behind a byte-order mark, CRLF.
write_template() {
    name=$1
    role=$2
    for directory in "$mql5/Profiles/Templates" "$terminal_dir/Profiles/Templates"; do
        mkdir -p "$directory"
        {
            printf '\377\376'
            sed -e "s/@HUB_PORT@/$MT5_HUB_PORT/" -e "s/@ROLE@/$role/" -e 's/$/\r/' \
                /opt/mt5/ticks.tpl.in | iconv -f UTF-8 -t UTF-16LE
        } > "$directory/$name"
    done
}
spawner=$(role_value ROLE_SPAWNER)
publisher=$(role_value ROLE_PUBLISHER)
history=$(role_value ROLE_HISTORY)
write_template ticks_spawner.tpl "$spawner"
write_template ticks.tpl "$publisher"
write_template ticks_history.tpl "$history"
