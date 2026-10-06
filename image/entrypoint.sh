#!/bin/bash
# Boots the MT5 terminal, the connector's push hub and its HTTP server, and ends the container with
# whichever of them exits first, or on a stop request. It prints its own steps and the terminal's
# logs.
set -euo pipefail

readonly terminal_dir="$WINEPREFIX/drive_c/Program Files/MetaTrader 5"
readonly metaquotes_dir="$WINEPREFIX/drive_c/users/mt5/AppData/Roaming/MetaQuotes"
readonly baked_dir=/opt/mt5-baked
readonly runtime_dir=/tmp/mt5
readonly startup_ini="$runtime_dir/setup.ini"
readonly search_ini="$runtime_dir/search.ini"
readonly window_wait_seconds=120
# A stop's bounds together — a call in flight, the terminal's close, the log tail's, the server's
# and the hub's, the display's and the kill — stay under the stop timeout the server README asks the
# container to be given.
readonly close_wait_seconds=60
readonly stop_wait_seconds=10
readonly call_seconds=10
readonly gui_seconds=40
readonly restore_seconds=40
readonly kill_after_seconds=5
readonly -a driver=(wine "$MT5_PYTHON_DIR\\python.exe" 'Z:\opt\mt5\terminal_gui.py')
export DISPLAY=:0

declare -A process_names=()
terminal_pid=
tail_pid=

step() {
    echo "mt5: $*"
}

fail() {
    echo "mt5: $*" >&2
    exit 1
}

# A call bounded on the clock: killed outright when it outlasts its signal to stop.
bounded() {
    timeout --kill-after="$kill_after_seconds" "$call_seconds" "$@"
}

# Runs a command bounded by `seconds`, failing the start naming `what` when the command fails or
# outlasts its bound.
run_bounded() {
    local what=$1 seconds=$2 status=0
    shift 2
    timeout --kill-after="$kill_after_seconds" "$seconds" "$@" || status=$?
    case $status in
        0) ;;
        124 | 137) fail "$what did not finish within ${seconds}s" ;;
        *) fail "$what failed" ;;
    esac
}

# The driver of the terminal's dialogs, by control identity from the image's Windows Python. A
# window that stops answering would hold it, so it is bounded like any call.
gui() {
    timeout --kill-after="$kill_after_seconds" "$gui_seconds" "${driver[@]}" "$@"
}

# Runs a driver action; when it fails, its own line names what it could not find.
act() {
    local what=$1
    shift
    run_bounded "$what" "$gui_seconds" "${driver[@]}" "$@"
}

# Polls a command once a second until it succeeds, failing the start once `seconds` have passed on
# the clock, however long each poll took.
await() {
    local what=$1 seconds=$2
    shift 2
    local deadline=$((SECONDS + seconds))
    until "$@" >/dev/null 2>&1; do
        end_with_stopped
        if ((SECONDS >= deadline)); then
            fail "no $what within ${seconds}s"
        fi
        sleep 1
    done
}

require_settings() {
    local name
    for name in MT5_LOGIN MT5_PASSWORD MT5_SERVER MT5_SPAWNER_SYMBOL; do
        if [[ -z "${!name:-}" ]]; then
            fail "$name is not set"
        fi
    done
}

# The display is up once its socket is: a start removes whatever socket and lock a killed X server
# left, since nothing of an earlier run is still running.
start_display() {
    local socket="/tmp/.X11-unix/X${DISPLAY#:}"
    rm -f "$socket" "/tmp/.X${DISPLAY#:}-lock"
    Xvfb "$DISPLAY" -screen 0 1920x1080x24 -nolisten tcp >/dev/null 2>&1 &
    process_names[$!]=Xvfb
    await "display" 30 test -S "$socket"
    if command -v x11vnc >/dev/null; then
        x11vnc -display "$DISPLAY" -localhost -forever -shared -nopw -quiet >/dev/null 2>&1 &
        step "x11vnc on 127.0.0.1:5900"
    fi
}

# Every start finds the terminal as the image baked it: what an earlier run wrote to its directory
# but the history store, the volume, to its AppData or to Wine's registry is undone, compared by
# content whatever its size and time. Wine reads the registry as it starts, so no Wine runs before.
restore_baked_terminal() {
    run_bounded "the restore of the terminal's baked state" "$restore_seconds" bash -ec '
        rsync -rlpt --checksum --delete --exclude=/Bases "$1/terminal/" "$2/"
        rsync -rlpt --checksum --delete "$1/metaquotes/" "$3/"
        rsync -lpt --checksum "$1/registry/system.reg" "$1/registry/user.reg" \
            "$1/registry/userdef.reg" "$4/"
    ' restore "$baked_dir" "$terminal_dir" "$metaquotes_dir" "$WINEPREFIX"
    step "restored the terminal's baked state, its history store aside"
}

start_terminal() {
    wine "$terminal_dir/terminal64.exe" "$@" /portable >/dev/null 2>&1 &
    terminal_pid=$!
}

# The terminal's windows are found by their class: their titles carry the account.
await_terminal_window() {
    await "terminal window" "$window_wait_seconds" gui main-window
}

# Asks the terminal to close as a user would — taskkill without /F posts WM_CLOSE to its windows —
# and waits for it to exit; false when it outlasts `close_wait_seconds`.
request_terminal_close() {
    local deadline=$((SECONDS + close_wait_seconds))
    bounded wine taskkill /IM terminal64.exe >/dev/null 2>&1 || :
    while kill -0 "$terminal_pid" 2>/dev/null; do
        if ((SECONDS >= deadline)); then
            return 1
        fi
        sleep 1
    done
}

close_terminal() {
    request_terminal_close || fail "the terminal did not close within ${close_wait_seconds}s"
    wait "$terminal_pid" || :
    terminal_pid=
}

# The terminal logs in only to a trade server its server list holds: searching the server's name in
# the account dialog adds its broker's servers, which the terminal keeps when it closes. A terminal
# started with no account opens that dialog on its own, so this session starts with one.
acquire_server_list() {
    step "fetching the trade server's broker into the server list"
    start_terminal "$(config_argument "$search_ini")"
    await_terminal_window
    act "the server search" search-server
    close_terminal
}

# The baked ini plus the account's credentials and the spawner's symbol, taken from the environment
# and written where only this user reads them; the search session's copy runs no startup script.
write_inis() {
    mkdir -p -m 0700 "$runtime_dir"
    (
        umask 077
        write_ini startup >"$startup_ini"
        write_ini search >"$search_ini"
    )
}

write_ini() {
    awk -v session="$1" '
        /^\[/ { skipping = (session == "search" && $0 == "[StartUp]") }
        skipping { next }
        { print $0 "\r" }
        $0 == "[Common]" {
            print "Login=" ENVIRON["MT5_LOGIN"] "\r"
            print "Password=\"" ENVIRON["MT5_PASSWORD"] "\"\r"
            print "Server=" ENVIRON["MT5_SERVER"] "\r"
        }
        $0 == "[StartUp]" { print "Symbol=" ENVIRON["MT5_SPAWNER_SYMBOL"] "\r" }
    ' /opt/mt5/setup.ini
}

# The /config: argument naming an ini by its path on Wine's Z: drive.
config_argument() {
    echo "/config:Z:${1//\//\\}"
}

# Puts 127.0.0.1 on the Options dialog's WebRequest list, which no ini key sets.
allow_webrequest() {
    step "adding 127.0.0.1 to the WebRequest allowlist"
    act "the WebRequest allowlist" allow-webrequest 127.0.0.1
}

# Streams the terminal's journal and experts log into the container's log. Its exit ends nothing: a
# stopped tail is restarted, skipping what the logs hold at its first read, so the lines written
# since the stopped tail's last read are lost.
start_log_tail() {
    python /opt/mt5/tail_logs.py "$@" "$terminal_dir" &
    tail_pid=$!
}

restart_stopped_log_tail() {
    local status=0
    if ! kill -0 "$tail_pid" 2>/dev/null; then
        wait "$tail_pid" || status=$?
        step "the log tail exited with status $status, restarting it"
        start_log_tail --from-end
    fi
}

# A stopped tail first streams what the logs still hold; one that outlasts its bound is killed.
stop_log_tail() {
    terminate "$tail_pid"
    if kill -0 "$tail_pid" 2>/dev/null; then
        step "killing the log tail, still running"
        kill -KILL "$tail_pid" 2>/dev/null || :
    else
        step "the log tail stopped"
    fi
}

start_hub() {
    step "starting the hub"
    mt5-connector-hub &
    process_names[$!]=hub
}

# The server exits when no EA sample verifies the broker clock within its bootstrap window: the loud
# end of a start whose login failed.
start_server() {
    step "starting the server"
    wine "$MT5_PYTHON_DIR\\Scripts\\mt5-connector-server.exe" &
    process_names[$!]=server
}

# Sends SIGTERM to `pids` and waits up to `stop_wait_seconds` for them all to exit.
terminate() {
    local deadline=$((SECONDS + stop_wait_seconds)) pid
    kill -TERM "$@" 2>/dev/null || :
    for pid in "$@"; do
        while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do
            sleep 1
        done
    done
}

# Closes the terminal as a user would, so it writes out what it holds and its last lines reach the
# log, then stops the log tail, the server and the hub, and the display last; whatever outlasts its
# bound is killed.
stop_everything() {
    local pid services=() display=() remaining=()
    if [[ -n "$terminal_pid" ]] && kill -0 "$terminal_pid" 2>/dev/null; then
        step "closing the terminal"
        if request_terminal_close; then
            step "the terminal closed"
        else
            step "the terminal did not close within ${close_wait_seconds}s"
        fi
    fi
    if [[ -n "$tail_pid" ]]; then
        stop_log_tail
    fi
    for pid in "${!process_names[@]}"; do
        case "${process_names[$pid]}" in
            server | hub) services+=("$pid") ;;
            Xvfb) display+=("$pid") ;;
        esac
    done
    terminate "${services[@]}"
    terminate "${display[@]}"
    for pid in "${!process_names[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            remaining+=("${process_names[$pid]}")
        fi
    done
    if ((${#remaining[@]} > 0)); then
        step "killing ${remaining[*]}, still running"
    fi
    bounded wineserver -k 2>/dev/null || :
    kill -KILL "${!process_names[@]}" 2>/dev/null || :
}

# A stop request ends the container with the terminal's own exit status, or 0 when none was running.
stop_on_request() {
    local status=0
    trap '' TERM INT
    trap - EXIT
    step "stop requested"
    stop_everything
    if [[ -n "$terminal_pid" ]]; then
        wait "$terminal_pid" || status=$?
    fi
    exit "$status"
}

# Polled: `wait -n` on some bash releases passes over a child that exited before it was called.
supervise() {
    while true; do
        end_with_stopped
        restart_stopped_log_tail
        sleep 1
    done
}

# Ends the container with the exit of the first supervised process found stopped, if one has.
end_with_stopped() {
    local pid
    for pid in "${!process_names[@]}"; do
        if ! kill -0 "$pid" 2>/dev/null; then
            end_with "$pid"
        fi
    done
}

# Ends the container with the status the supervised process `pid` exited with.
end_with() {
    local status
    trap '' TERM INT
    trap - EXIT
    set +e
    wait "$1"
    status=$?
    set -e
    step "${process_names[$1]} exited with status $status"
    stop_everything
    exit "$status"
}

# A boot step failing because a supervised process stopped reports that process's exit instead.
trap end_with_stopped EXIT
trap stop_on_request TERM INT

require_settings
start_display
restore_baked_terminal
write_inis
acquire_server_list
step "launching the terminal"
start_terminal "$(config_argument "$startup_ini")"
process_names[$terminal_pid]=terminal
start_log_tail
await_terminal_window
allow_webrequest
start_hub
start_server
supervise
