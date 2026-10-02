#!/bin/bash

source /scripts/02-common.sh

log_message "RUNNING" "06-install-libraries.sh"

# Every start reconciles the Wine prefix to the exact pins; pip leaves satisfied ones untouched.
log_message "INFO" "Installing the server's pinned requirements in Windows"
if ! $wine_executable python -m pip install --no-cache-dir -r /app/mt5server/app/requirements.txt; then
    log_message "ERROR" "Installing the server's pinned requirements failed"
    exit 1
fi
