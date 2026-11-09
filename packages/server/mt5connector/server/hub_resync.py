"""Scheduled downloads of idle symbols' tick stores and the UTC days they certify.

State: the volume's verified month downloads and latest outcomes; the current pass's symbols and
attempts; one symbol's deleted months and outstanding verification read. Only the download record
is persisted, atomically under the history store.
"""

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import numpy as np

from mt5connector.server.settings import HubSettings
from mt5connector.server.wire import binary, calls, mirror
from mt5connector.server.wire.broker_clock import BrokerClock

_DAY_S = 86_400
_RESYNC_ATTEMPTS = 3


class Outcome(StrEnum):
    DOWNLOADED = "downloaded"
    FAILED = "failed"


class _Read(StrEnum):
    MEASURE = "measure"
    BARS = "bars"
    TICKS = "ticks"


@dataclass
class _Record:
    months: dict[str, int] = field(default_factory=dict)
    outcome: Outcome = Outcome.DOWNLOADED
    month: str = ""
    reason: str = ""


@dataclass
class _Download:
    symbol: str
    directory: Path
    months: list[str]
    present: set[str]
    read: _Read = _Read.MEASURE
    asked: bool = False
    arguments: dict = field(default_factory=dict)
    rows: list = field(default_factory=list)


class Resync:
    """Re-downloads due months serially, after history charts and their tick stores go idle."""

    def __init__(
        self,
        clock: BrokerClock,
        settings: HubSettings,
        *,
        ready: Callable[[], bool],
        idle: Callable[[str], bool],
        request: Callable[[calls.Call, dict], bool],
    ) -> None:
        self._clock = clock
        self._settings = settings
        self._ready = ready
        self._idle = idle
        self._request = request
        self._path = settings.history_store / ".mt5-connector" / "resync.json"
        self._records: dict[str, _Record] = {}
        if self._path.exists():
            for symbol, record in json.loads(self._path.read_text()).items():
                self._records[symbol] = _Record(
                    record["months"], Outcome(record["outcome"]), record["month"], record["reason"]
                )
        self._scheduled_day: int | None = None
        self._pending: dict[str, Path] = {}
        self._attempts: dict[str, int] = {}
        self._retry_months: dict[str, set[str]] = {}
        self._active: _Download | None = None
        self._now = 0

    def sweep(self, now: int) -> None:
        """Advances the start pass or the daily pass at the configured broker hour."""
        if not self._ready():
            return
        self._now = now
        broker = self._clock.to_broker(now)
        day, seconds = divmod(broker, _DAY_S)
        scheduled = day if seconds >= self._settings.resync_hour * 3600 else day - 1
        if (
            self._active is None
            and not self._pending
            and (self._scheduled_day is None or scheduled > self._scheduled_day)
        ):
            self._scheduled_day = scheduled
            self._attempts = {}
            self._retry_months = {}
            self._pending = {
                path.name: path
                for path in sorted(self._settings.history_store.glob("*/ticks/*"))
                if path.is_dir()
            }
        if self._active is None:
            for symbol, directory in list(self._pending.items()):
                due = self._due(symbol, directory, broker)
                if not due:
                    del self._pending[symbol]
                elif self._idle(symbol):
                    self._retry_months[symbol] = set(due)
                    for month in due:
                        (directory / f"{month}.tkc").unlink(missing_ok=True)
                    present = {path.stem for path in directory.glob("*.tkc")}
                    self._active = _Download(symbol, directory, due, present)
                    break
        if self._active is not None and not self._active.asked:
            self._ask()

    def _due(self, symbol: str, directory: Path, broker: int) -> list[str]:
        record = self._records.get(symbol, _Record())
        present = {path.stem for path in directory.glob("*.tkc")}
        months = present | record.months.keys() | self._retry_months.get(symbol, set())
        if record.outcome is Outcome.FAILED:
            months.add(record.month)
        due = []
        today = broker - broker % _DAY_S
        for month in sorted(months):
            _, end = _month_bounds(month)
            through = self._clock.to_utc(min(today, end))
            if month not in present or record.months.get(month, 0) < through:
                due.append(month)
        return due

    def _ask(self) -> None:
        download = self._active
        if download.read is _Read.MEASURE:
            call = calls.Call.RANGES
        elif download.read is _Read.BARS:
            call = calls.Call.COPY_RATES_RANGE
        else:
            call = calls.Call.COPY_TICKS_RANGE
        download.asked = True
        sent = self._request(call, {"symbol": download.symbol} | download.arguments)
        if not sent:
            download.asked = False

    def answer(self, frame: binary.Answer, now: int) -> None:
        """Takes the current internal read's answer; an error consumes one attempt."""
        self._now = now
        download = self._active
        if frame.error is not None:
            self._failed(frame.error.message)
        elif download.read is _Read.MEASURE:
            missing = [
                month
                for month in download.months
                if not (download.directory / f"{month}.tkc").is_file()
            ]
            if missing:
                self._failed(f"the measurement did not restore {', '.join(missing)}")
            else:
                lo, end = _month_bounds(download.months[-1])
                broker = self._clock.to_broker(now)
                hi = min(end, broker - broker % _DAY_S) - 60
                if hi < lo:
                    self._downloaded()
                else:
                    download.read = _Read.BARS
                    download.arguments = {
                        "timeframe": mirror.TIMEFRAME_M1,
                        "date_from": self._clock.to_utc(lo),
                        "date_to": self._clock.to_utc(hi),
                    }
                    self._ask()
        else:
            call = (
                calls.Call.COPY_RATES_RANGE
                if download.read is _Read.BARS
                else calls.Call.COPY_TICKS_RANGE
            )
            download.rows.append(calls.read_hub_result(calls.CALLS[call], frame))
            if frame.last:
                rows = np.concatenate(download.rows)
                download.rows = []
                if download.read is _Read.BARS:
                    bearing = rows[rows["tick_volume"] > 0]
                    if len(bearing) == 0:
                        self._downloaded()
                    else:
                        opened = int(bearing["time"][-1])
                        download.read = _Read.TICKS
                        download.arguments = {
                            "date_from": opened,
                            "date_to": opened + 59,
                            "flags": mirror.COPY_TICKS_INFO,
                        }
                        self._ask()
                elif len(rows) == 0:
                    self._failed("the ended bar's minute answered no tick")
                else:
                    self._downloaded()

    def _failed(self, reason: str) -> None:
        download = self._active
        attempts = self._attempts.get(download.symbol, 0) + 1
        self._attempts[download.symbol] = attempts
        if attempts == _RESYNC_ATTEMPTS:
            record = self._records.setdefault(download.symbol, _Record())
            record.outcome = Outcome.FAILED
            record.month = download.months[-1]
            record.reason = reason
            self._persist()
            del self._pending[download.symbol]
        self._active = None

    def _downloaded(self) -> None:
        download = self._active
        record = self._records.setdefault(download.symbol, _Record())
        for path in download.directory.glob("*.tkc"):
            if path.stem not in download.present:
                record.months[path.stem] = self._now
        record.outcome = Outcome.DOWNLOADED
        record.month = ""
        record.reason = ""
        self._persist()
        del self._pending[download.symbol]
        self._active = None

    def _persist(self) -> None:
        self._path.parent.mkdir(exist_ok=True)
        temporary = self._path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps({symbol: asdict(record) for symbol, record in self._records.items()})
        )
        temporary.replace(self._path)

    def failure(self, symbol: str) -> str:
        record = self._records.get(symbol)
        if record is not None and record.outcome is Outcome.FAILED:
            return f"{record.month}: {record.reason}"
        return ""

    def claimable_through(self, symbol: str, first_tick: int, now: int) -> int:
        """The exclusive UTC-day edge certified by downloads of every broker block it contains."""
        if not any(
            path.is_dir() for path in self._settings.history_store.glob(f"*/ticks/{symbol}")
        ):
            return 0
        today = now - now % _DAY_S
        if first_tick == 0:
            return today
        edge = min(today, first_tick - first_tick % _DAY_S)
        record = self._records.get(symbol, _Record())
        while edge < today:
            first = self._clock.to_broker(edge)
            last = self._clock.to_broker(edge + _DAY_S - 1)
            months = {_month(first), _month(last)}
            if not all(
                month in record.months
                and self._clock.to_broker(record.months[month]) // _DAY_S > last // _DAY_S
                for month in months
            ):
                break
            edge += _DAY_S
        return edge


def _month(broker: int) -> str:
    return datetime.fromtimestamp(broker, UTC).strftime("%Y%m")


def _month_bounds(month: str) -> tuple[int, int]:
    start = datetime.strptime(month, "%Y%m").replace(tzinfo=UTC)
    year, number = divmod(start.year * 12 + start.month, 12)
    end = start.replace(year=year, month=number + 1)
    return int(start.timestamp()), int(end.timestamp())
