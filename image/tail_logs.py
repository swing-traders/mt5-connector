"""Streams the MT5 terminal's journal and its experts log to stdout as UTF-8, each line prefixed by
its source."""

from __future__ import annotations

import argparse
import codecs
import os
import re
import signal
import sys
import time
from enum import StrEnum
from pathlib import Path

POLL_SECONDS = 0.5
# A log per UTC day, named by its date; the directories hold logs of other names too.
DAY_LOG = re.compile(r"\d{8}\.log")


class Source(StrEnum):
    TERMINAL = "terminal"
    EXPERTS = "experts"


class DayLog:
    """The newest day's log in one directory, followed onto the next day's when it appears."""

    def __init__(self, directory: Path, *, from_end: bool) -> None:
        self.directory = directory
        self._from_end = from_end
        self._path: Path | None = None
        self._file = None
        self._decoder = codecs.getincrementaldecoder("utf-16-le")()
        self._pending = ""

    def read_lines(self) -> list[str]:
        """The complete lines written since the last read, the previous day's remainder first when a
        new day's log has appeared."""
        newest = max(
            (entry.name for entry in os.scandir(self.directory) if DAY_LOG.fullmatch(entry.name)),
            default=None,
        )
        lines = []
        if newest is not None and self.directory / newest != self._path:
            if self._file is not None:
                lines += self._read() + self._close()
            self._open(self.directory / newest)
        # Only a log already there at the first read is skipped: a later one is read from its start.
        self._from_end = False
        if self._file is not None:
            lines += self._read()
        return lines

    def read_to_end(self) -> list[str]:
        """Every line left in the current log, the one it ends on without a line break included."""
        lines = self.read_lines()
        if self._file is not None:
            lines += self._close()
        return lines

    def _close(self) -> list[str]:
        # A character the terminal had half written is dropped, never decoded.
        if self._decoder.getstate()[0]:
            print(
                f"tail_logs: dropped a partial character at the end of {self._path}",
                file=sys.stderr,
            )
        remainder = [self._pending] if self._pending else []
        self._pending = ""
        self._decoder.reset()
        self._file.close()
        self._file = None
        return remainder

    def _open(self, path: Path) -> None:
        self._path = path
        self._file = path.open("rb", buffering=0)
        # A UTF-16 character starts at an even offset, and the terminal may be halfway through one.
        if self._from_end:
            end = self._file.seek(0, os.SEEK_END)
            self._file.seek(end - end % 2)

    def _read(self) -> list[str]:
        self._pending += self._decoder.decode(self._file.read())
        *lines, self._pending = self._pending.split("\n")
        return lines


def emit(source: Source, lines: list[str]) -> None:
    for line in lines:
        text = line.removeprefix("﻿").removesuffix("\r")
        sys.stdout.buffer.write(f"{source}: {text}\n".encode())
    sys.stdout.buffer.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("terminal_dir", type=Path)
    parser.add_argument(
        "--from-end",
        action="store_true",
        help="Skip what the current logs already hold. Default: stream them from their start.",
    )
    args = parser.parse_args()
    logs = {
        Source.TERMINAL: DayLog(args.terminal_dir / "logs", from_end=args.from_end),
        Source.EXPERTS: DayLog(args.terminal_dir / "MQL5" / "logs", from_end=args.from_end),
    }
    # A stop ends the tail once it has streamed what the logs hold.
    stopping = []
    signal.signal(signal.SIGTERM, lambda signum, frame: stopping.append(signum))
    while not stopping:
        for source, log in logs.items():
            emit(source, log.read_lines())
        time.sleep(POLL_SECONDS)
    for source, log in logs.items():
        emit(source, log.read_to_end())


if __name__ == "__main__":
    main()
