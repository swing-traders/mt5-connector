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
