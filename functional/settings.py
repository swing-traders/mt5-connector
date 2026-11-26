"""Private environment-file settings and output redaction for certification."""

import json
import os
import re
from pathlib import Path

PRIVATE = ("MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER")
SYMBOL = "BTCUSD.a"


def load(path: Path) -> dict[str, str]:
    values = {}
    for line in path.read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            key, separator, value = line.partition("=")
            if not separator or not re.fullmatch(r"[A-Z_][A-Z_0-9]*", key):
                raise ValueError("the environment file needs unquoted NAME=value lines")
            values[key] = value
    for key in PRIVATE:
        if not values.get(key):
            raise ValueError(f"{key} is missing")
    if not values["MT5_LOGIN"].isdecimal() or int(values["MT5_LOGIN"]) <= 0:
        raise ValueError("MT5_LOGIN must be a positive integer")
    for key in ("MT5_SPAWNER_SYMBOL", "MT5_LIVE_HUB_SYMBOL"):
        values.setdefault(key, SYMBOL)
        if values[key] != SYMBOL:
            raise ValueError(f"{key} must name {SYMBOL}")
    return values


def write(path: Path, values: dict[str, str]) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as file:
        for key, value in values.items():
            file.write(f"{key}={value}\n")


class Redactor:
    def __init__(self, values):
        self._hidden = set()
        self.hide(*(values[key] for key in PRIVATE))

    def hide(self, *values):
        for value in values:
            if value:
                self._hidden.update(
                    (str(value), repr(str(value))[1:-1], json.dumps(str(value))[1:-1])
                )

    def __call__(self, text: str) -> str:
        for value in sorted(self._hidden, key=len, reverse=True):
            text = text.replace(value, "[redacted]")
        return text
