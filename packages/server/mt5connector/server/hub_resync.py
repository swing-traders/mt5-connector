"""Per-symbol tick-store re-syncs and the UTC days their downloads certify.

State: verified month downloads and outcomes, the store listing, each symbol's first download and
examination day, and fixed-month machines with their deletion facts and deadlines. Abandoned
deletions drain by symbol. Only the download record is persisted. File facts enter in the sweep;
answers wait on their own machine.
"""

import asyncio
import json
import logging
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
logger = logging.getLogger(__name__)


class Outcome(StrEnum):
    DOWNLOADED = "downloaded"
    FAILED = "failed"


class _Read(StrEnum):
    MEASURE = "measure"
    BARS = "bars"
    TICKS = "ticks"


class Cause(StrEnum):
    PASS = "the scheduled pass"
    STORE = "a store that appeared"


class Stage(StrEnum):
    DUE = "due"
    RELEASED = "released"
    WAITING = "waiting"
    MEASURING = "measuring"
    VERIFYING = "verifying"


@dataclass
class _Record:
    months: dict[str, int] = field(default_factory=dict)
    outcome: Outcome = Outcome.DOWNLOADED
    month: str = ""
    reason: str = ""
    synced_through: int = 0


@dataclass
class _FirstDownload:
    pending: int = 0
    retry_at: float = 0


@dataclass
class _Machine:
    symbol: str
    months: tuple[str, ...]
    stage: Stage = Stage.DUE
    attempt: int = 1
    present: set[str] = field(default_factory=set)
    read: _Read = _Read.MEASURE
    asked: bool = False
    arguments: dict = field(default_factory=dict)
    rows: list = field(default_factory=list)
    answers: list[binary.Answer] = field(default_factory=list)
    retry_at: float = 0
    fetched_at: int = 0
    opening: bool = False
    needs_confirmation: bool = False
    deleting: asyncio.Task[None] | None = None
    deletion_started_at: float | None = None
    written_back: tuple[str, ...] = ()


class TickStore:
    """Lists and changes the terminal's month files away from the serving loop."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def snapshot(self) -> dict[str, set[str]]:
        return {
            path.name: {month.stem for month in path.glob("*.tkc")}
            for path in sorted(self._root.glob("*/ticks/*"))
            if path.is_dir()
        }

    async def listing(self) -> dict[str, set[str]]:
        return await asyncio.to_thread(self.snapshot)

    async def delete(self, symbol: str, months: tuple[str, ...]) -> None:
        def remove():
            for month in months:
                for path in self._root.glob(f"*/ticks/{symbol}/{month}.tkc"):
                    path.unlink()

        await asyncio.to_thread(remove)

    async def persist(self, records: dict[str, dict]) -> None:
        def write():
            path = self._root / ".mt5-connector" / "resync.json"
            path.parent.mkdir(exist_ok=True)
            temporary = path.with_suffix(".tmp")
            temporary.write_text(json.dumps(records))
            temporary.replace(path)

        await asyncio.to_thread(write)


class Resync:
    """Re-downloads each due symbol independently, closing its history chart before deletion."""

    def __init__(
        self,
        clock: BrokerClock,
        settings: HubSettings,
        *,
        ready: Callable[[], bool],
        release: Callable[[str], bool],
        closed_at: Callable[[str], float],
        monotonic: Callable[[], float],
        request: Callable[[calls.Call, dict], bool],
    ) -> None:
        self._clock = clock
        self._settings = settings
        self._ready = ready
        self._release = release
        self._closed_at = closed_at
        self._monotonic = monotonic
        self._request = request
        self._path = settings.history_store / ".mt5-connector" / "resync.json"
        self._records: dict[str, _Record] = {}
        if self._path.exists():
            for symbol, record in json.loads(self._path.read_text()).items():
                self._records[symbol] = _Record(
                    record["months"],
                    Outcome(record["outcome"]),
                    record["month"],
                    record["reason"],
                    record.get("synced_through", 0),
                )
        self._store = TickStore(settings.history_store)
        self._listing = self._store.snapshot()
        self._examined_day: dict[str, int] = {}
        self._examined_months: dict[str, set[str]] = {}
        self._first_downloads: dict[str, _FirstDownload] = {}
        self._machines: dict[str, _Machine] = {}
        self._retiring: dict[str, asyncio.Task[None]] = {}
        self._dirty = False

    def _scheduled(self, now: int) -> int:
        day, seconds = divmod(self._clock.to_broker(now), _DAY_S)
        return day if seconds >= self._settings.resync_hour * 3600 else day - 1

    def running(self, symbol: str) -> bool:
        return symbol in self._machines

    def first_download_running(self, symbol: str) -> bool:
        """Whether the initial download or its re-ask hold still uses the history chart."""
        download = self._first_downloads.get(symbol)
        return download is not None and (
            download.pending > 0 or self._monotonic() < download.retry_at
        )

    def join_first_download(self, symbol: str) -> bool:
        """Joins an initial download until its first result, including a deferral's re-ask."""
        self._expire_first_download(symbol)
        if symbol not in self._first_downloads and (
            symbol in self._listing or symbol in self._records
        ):
            return False
        download = self._first_downloads.setdefault(symbol, _FirstDownload())
        download.pending += 1
        download.retry_at = 0
        return True

    def first_download(self, symbol: str, now: int, *, has_ticks: bool = True) -> None:
        """Notes the first completed history download of a previously storeless symbol."""
        if self._first_downloads.pop(symbol, None) is not None and has_ticks:
            self._records[symbol] = _Record(synced_through=now)
            self._dirty = True

    def end_first_download(self, symbol: str, *, syncing: bool = False) -> None:
        """Releases a request's initial download, holding an EA deferral for its re-ask."""
        download = self._first_downloads.get(symbol)
        if download is not None:
            download.pending -= 1
            if syncing:
                download.retry_at = self._monotonic() + 2 * self._settings.retry_after_s
            self._expire_first_download(symbol)

    def _expire_first_download(self, symbol: str) -> None:
        download = self._first_downloads.get(symbol)
        if (
            download is not None
            and download.pending == 0
            and self._monotonic() >= download.retry_at
        ):
            del self._first_downloads[symbol]

    def dispatch(self, symbol: str, months, cause: Cause) -> None:
        """Starts a fixed-month re-sync; an existing machine is a caller error."""
        if symbol in self._machines:
            raise ValueError(f"{symbol} already has a re-sync")
        self._machines[symbol] = _Machine(
            symbol, tuple(sorted(months)), needs_confirmation=symbol in self._retiring
        )
        self._transition(self._machines[symbol], Stage.DUE, cause)

    def _transition(self, machine: _Machine, stage: Stage, detail) -> None:
        machine.stage = stage
        logger.info(
            "re-sync of %s: %s %s (attempt %d of %d); %s",
            machine.symbol,
            stage,
            ", ".join(machine.months),
            machine.attempt,
            _RESYNC_ATTEMPTS,
            detail,
        )

    def unsettled(
        self,
        symbol: str,
        now: int,
        source: calls.Source = calls.Source.SERVER_SYNCED,
        *,
        first_tick: int = 0,
    ) -> str:
        """The symbol's outstanding re-sync, examining due months before it can be served."""
        if source is calls.Source.TERMINAL:
            machine = self._machines.get(symbol)
            if machine is not None and machine.needs_confirmation:
                return f"the months of {symbol} are deleted for its re-sync; back once measured"
            return ""
        if symbol not in self._machines:
            self._examine(symbol, now)
        machine = self._machines.get(symbol)
        if machine is not None:
            return (
                f"{symbol}: {', '.join(machine.months)} {machine.stage}, "
                f"attempt {machine.attempt} of {_RESYNC_ATTEMPTS}"
            )
        record = self._records.get(symbol)
        if (
            first_tick
            and symbol not in self._listing
            and record is not None
            and record.synced_through
            and not record.months
        ):
            return f"the tick store of {symbol} is being created; it is examined once listed"
        return ""

    def _examine(self, symbol: str, now: int, *, again=False) -> None:
        self._expire_first_download(symbol)
        if (
            symbol in self._first_downloads
            or symbol not in self._listing
            or symbol in self._machines
        ):
            return
        record = self._records.get(symbol, _Record())
        if record.synced_through:
            for month in self._listing[symbol]:
                if month not in record.months and _month_bounds(month)[1] <= self._clock.to_broker(
                    record.synced_through
                ):
                    record.months[month] = record.synced_through
                    self._dirty = True
        scheduled = self._scheduled(now)
        appeared = record.synced_through > 0 and max(record.months.values(), default=0) <= (
            record.synced_through
        )
        changed = self._examined_months.get(symbol) != self._listing[symbol]
        if again or self._examined_day.get(symbol, -1) < scheduled or (appeared and changed):
            self._examined_day[symbol] = scheduled
            self._examined_months[symbol] = set(self._listing[symbol])
            due = self._due(symbol, scheduled * _DAY_S)
            if due:
                self.dispatch(symbol, due, Cause.STORE if appeared else Cause.PASS)

    async def sweep(self, now: int) -> None:
        """Refreshes file facts, advances every symbol and persists the changed record once."""
        if not self._ready():
            return
        for symbol, task in list(self._retiring.items()):
            if task.done():
                task.exception()
                del self._retiring[symbol]
                machine = self._machines.get(symbol)
                if machine is not None:
                    machine.deletion_started_at = None
        self._listing = await self._store.listing()
        for symbol in list(self._first_downloads):
            self._expire_first_download(symbol)
        for symbol in self._listing:
            self._examine(symbol, now)
        for machine in list(self._machines.values()):
            deletion = self._retiring.get(machine.symbol, machine.deleting)
            if deletion is not None:
                if machine.deletion_started_at is None:
                    machine.deletion_started_at = self._monotonic()
                if not deletion.done() and (
                    self._monotonic() - machine.deletion_started_at >= calls.HISTORY_BUSY_S
                ):
                    self._failed(
                        machine,
                        f"the deletion of {machine.symbol} did not finish within "
                        f"{calls.HISTORY_BUSY_S:g} s",
                        now,
                    )
                    continue
            if machine.stage is Stage.DUE and self._release(machine.symbol):
                self._transition(machine, Stage.RELEASED, "its history chart is closed")
            if machine.stage is Stage.RELEASED:
                machine.needs_confirmation = True
                if machine.symbol in self._retiring:
                    continue
                if machine.deleting is None:
                    machine.deletion_started_at = self._monotonic()
                    machine.deleting = asyncio.create_task(
                        self._delete(
                            machine.symbol,
                            machine.months,
                            machine.deletion_started_at + calls.HISTORY_BUSY_S,
                        )
                    )
                if machine.deleting.done():
                    try:
                        machine.deleting.result()
                    except Exception as failure:
                        self._failed(machine, str(failure), now)
                    else:
                        self._listing[machine.symbol].difference_update(machine.months)
                        for month in machine.written_back:
                            logger.info(
                                "re-sync of %s: %s was written back during the wait; deleted again",
                                machine.symbol,
                                month,
                            )
                        machine.written_back = ()
                        machine.present = set(self._listing[machine.symbol])
                        elapsed = self._monotonic() - self._closed_at(machine.symbol)
                        self._transition(
                            machine,
                            Stage.WAITING,
                            f"its history chart closed {elapsed:g} s ago; reopening in "
                            f"{max(0, self._settings.resync_reopen_seconds - elapsed):g} s",
                        )
                    machine.deleting = None
                    machine.deletion_started_at = None
            if machine.stage is Stage.WAITING:
                if self._monotonic() - self._closed_at(machine.symbol) >= (
                    self._settings.resync_reopen_seconds
                ):
                    if not machine.opening:
                        machine.written_back = tuple(
                            sorted(self._listing[machine.symbol].intersection(machine.months))
                        )
                    if machine.written_back:
                        self._transition(
                            machine,
                            Stage.RELEASED,
                            "its due months were written back during the wait",
                        )
                    else:
                        self._ask(machine)
            if machine.stage in (Stage.WAITING, Stage.MEASURING, Stage.VERIFYING):
                answers, machine.answers = machine.answers, []
                for frame in answers:
                    self._answer(machine, frame, now)
                    if self._machines.get(machine.symbol) is not machine:
                        break
                if (
                    self._machines.get(machine.symbol) is machine
                    and machine.stage in (Stage.MEASURING, Stage.VERIFYING)
                    and not machine.asked
                    and self._monotonic() >= machine.retry_at
                ):
                    self._ask(machine)
        if self._dirty:
            records = {}
            for symbol, record in self._records.items():
                records[symbol] = asdict(record)
                if not record.synced_through:
                    del records[symbol]["synced_through"]
            self._dirty = False
            await self._store.persist(records)

    async def _delete(self, symbol: str, months: tuple[str, ...], deadline: float) -> None:
        await self._store.delete(symbol, months)
        if self._monotonic() > deadline:
            raise TimeoutError(
                f"the deletion of {symbol} did not finish within {calls.HISTORY_BUSY_S:g} s"
            )

    async def close(self) -> None:
        """Cancels deletion waiters without waiting for their worker threads."""
        tasks = [
            machine.deleting for machine in self._machines.values() if machine.deleting is not None
        ]
        tasks.extend(self._retiring.values())
        for task in tasks:
            task.cancel()

    def closing_hello(self, symbol: str) -> None:
        """Returns a closing chart's machine to DUE so its close keeps being renewed."""
        machine = self._machines.get(symbol)
        if machine is not None and machine.stage is not Stage.DUE:
            self._transition(machine, Stage.DUE, "its closing history EA returned")

    def hello(self, symbol: str, now: int) -> None:
        """Dates a reopened machine's download at the fresh history EA's greeting."""
        machine = self._machines.get(symbol)
        if machine is not None and machine.stage is Stage.WAITING:
            if self._monotonic() - self._closed_at(symbol) < self._settings.resync_reopen_seconds:
                self._transition(machine, Stage.DUE, "its history EA returned before the reopen")
            else:
                machine.fetched_at = now
                self._transition(machine, Stage.MEASURING, "its fresh history EA said hello")

    def _due(self, symbol: str, today: int) -> list[str]:
        record = self._records.get(symbol, _Record())
        present = self._listing[symbol]
        months = present | record.months.keys()
        if record.outcome is Outcome.FAILED:
            months.add(record.month)
        due = []
        for month in sorted(months):
            _, end = _month_bounds(month)
            through = self._clock.to_utc(min(today, end))
            if month not in present or record.months.get(month, 0) < through:
                due.append(month)
        return due

    def _ask(self, download: _Machine) -> None:
        if download.stage is Stage.WAITING and not download.opening:
            download.present = set(self._listing[download.symbol])
            download.opening = True
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

    def answer(self, symbol: str, frame: binary.Answer, now: int) -> None:
        """Queues an internal answer on the symbol that owns its request."""
        self._machines[symbol].answers.append(frame)

    def chart_failed(self, symbol: str, error: int, now: int) -> None:
        """Fails a running machine's attempt on a chart-open failure."""
        machine = self._machines.get(symbol)
        if machine is not None:
            self._failed(
                machine, f"the history chart of {symbol} failed to open: MQL5 error {error}", now
            )

    def ea_gone(self, symbol: str, now: int) -> None:
        """Fails a running machine's attempt on an unexpected history EA departure."""
        machine = self._machines.get(symbol)
        if machine is not None:
            self._failed(machine, f"the history EA for {symbol} left", now)

    def _answer(self, download: _Machine, frame: binary.Answer, now: int) -> None:
        if frame.error is not None:
            if download.read is _Read.MEASURE and frame.error.kind is binary.ErrorKind.SYNCING:
                download.asked = False
                download.retry_at = self._monotonic() + frame.error.retry_after_ms / 1000
            else:
                self._failed(download, frame.error.message, now)
        elif download.read is _Read.MEASURE:
            missing = [
                month for month in download.months if month not in self._listing[download.symbol]
            ]
            if missing:
                self._failed(download, f"the measurement did not restore {', '.join(missing)}", now)
            else:
                download.needs_confirmation = False
                lo, end = _month_bounds(download.months[-1])
                broker = self._clock.to_broker(now)
                hi = min(end, broker - broker % _DAY_S) - 60
                if hi < lo:
                    self._downloaded(download, now)
                else:
                    download.read = _Read.BARS
                    download.arguments = {
                        "timeframe": mirror.TIMEFRAME_M1,
                        "date_from": self._clock.to_utc(lo),
                        "date_to": self._clock.to_utc(hi),
                    }
                    download.asked = False
                    self._transition(download, Stage.VERIFYING, "measured; checking ended bars")
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
                        self._downloaded(download, now)
                    else:
                        opened = int(bearing["time"][-1])
                        download.read = _Read.TICKS
                        download.arguments = {
                            "date_from": opened,
                            "date_to": opened + 59,
                            "flags": mirror.COPY_TICKS_INFO,
                        }
                        download.asked = False
                elif len(rows) == 0:
                    self._failed(download, "the ended bar's minute answered no tick", now)
                else:
                    self._downloaded(download, now)

    def _failed(self, download: _Machine, reason: str, now: int) -> None:
        download.answers = []
        if download.deleting is not None:
            self._retiring[download.symbol] = download.deleting
            download.deleting = None
        if download.symbol in self._retiring:
            download.deletion_started_at = self._monotonic()
        else:
            download.deletion_started_at = None
        logger.info(
            "re-sync of %s: attempt %d of %d failed: %s",
            download.symbol,
            download.attempt,
            _RESYNC_ATTEMPTS,
            reason,
        )
        if download.attempt == _RESYNC_ATTEMPTS:
            record = self._records.setdefault(download.symbol, _Record())
            record.outcome = Outcome.FAILED
            record.month = download.months[-1]
            record.reason = reason
            self._dirty = True
            del self._machines[download.symbol]
            self._examined_day[download.symbol] = self._scheduled(now)
            self._examined_months[download.symbol] = set(self._listing[download.symbol])
        else:
            download.attempt += 1
            download.read = _Read.MEASURE
            download.asked = False
            download.arguments = {}
            download.rows = []
            download.retry_at = 0
            download.fetched_at = 0
            download.opening = False
            self._transition(download, Stage.DUE, reason)

    def _downloaded(self, download: _Machine, now: int) -> None:
        record = self._records.setdefault(download.symbol, _Record())
        # A month present before the reopen can still be the terminal's local recording.
        restored = {
            month
            for month in self._listing[download.symbol] - download.present
            if month in download.months
            or _month_bounds(month)[1] <= self._clock.to_broker(download.fetched_at)
        }
        for month in restored:
            record.months[month] = download.fetched_at
        record.outcome = Outcome.DOWNLOADED
        record.month = ""
        record.reason = ""
        self._dirty = True
        logger.info("re-sync of %s: recorded %s", download.symbol, ", ".join(sorted(restored)))
        del self._machines[download.symbol]
        self._examine(download.symbol, now, again=True)

    def failure(self, symbol: str) -> str:
        record = self._records.get(symbol)
        if record is not None and record.outcome is Outcome.FAILED:
            return f"{record.month}: {record.reason}"
        return ""

    def claimable_through(self, symbol: str, first_tick: int, now: int) -> int:
        """The exclusive UTC-day edge certified by downloads of every broker block it contains."""
        if symbol not in self._listing:
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
