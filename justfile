# Task runner. Run `just` (no args) to see the list of available recipes.

set positional-arguments
set quiet

default:
    @just --list

# ---- Lint ----

lint: black-check ruff-check

lint-fix: black-fix ruff-fix

ruff-check:
    ruff check

ruff-fix:
    ruff check --fix

black-check:
    black --check .

black-fix:
    black .

# ---- Tests ----

test:
    pytest

# Build the latest terminal through its logged manual Action.
terminal-latest:
    #!/usr/bin/env bash
    set -euo pipefail
    dispatched=$(gh workflow run terminal-image.yml)
    printf '%s\n' "$dispatched"
    if [[ "$dispatched" =~ /actions/runs/([0-9]+) ]]; then
        run_id=${BASH_REMATCH[1]}
    else
        echo "terminal dispatch returned no run URL" >&2
        exit 1
    fi
    gh run watch "$run_id" --exit-status
    gh run view "$run_id" --log \
        | awk '/build [0-9]+ (published|is already tagged)|terminal digest:/'
