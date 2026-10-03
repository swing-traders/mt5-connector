from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy

# ─────────────────────────────────────────────────────────────────────────────
# TRADE RECORD
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class TradeRecord:
    direction: str
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    pnl: float
    pnl_points: float
    status: str  # WIN or LOSS
    time: str


# ─────────────────────────────────────────────────────────────────────────────
# FVG ZONE
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class FVGZone:
    direction: OrderSide
    gap_low: float
    gap_high: float
    stop_loss: float
    formed_at: int
    active: bool = True
    max_bars: int = 20


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY CONFIG
# ─────────────────────────────────────────────────────────────────────────────


class FVGStrategyConfig(StrategyConfig, frozen=True):
    instrument_id: str
    bar_type: str
    fvg_min_size: float = 0.50
    risk_reward: float = 2.0
    trade_size: Decimal = Decimal("0.10")
    trend_filter: bool = True
    sma_period: int = 50
    warmup_bars: int = 100


# ─────────────────────────────────────────────────────────────────────────────
# FVG STRATEGY
# ─────────────────────────────────────────────────────────────────────────────


class FVGStrategy(Strategy):
    """Fair Value Gap (FVG) strategy for XAUUSD."""

    def __init__(self, config: FVGStrategyConfig) -> None:
        super().__init__(config)

        self.instrument_id = InstrumentId.from_str(config.instrument_id)
        self.bar_type = BarType.from_str(config.bar_type)
        self.fvg_min_size = config.fvg_min_size
        self.risk_reward = config.risk_reward
        self.trade_size = config.trade_size
        self.trend_filter = config.trend_filter
        self.sma_period = config.sma_period
        self.warmup_bars = config.warmup_bars

        # Rolling bar history
        self._bars: list[Bar] = []
        self._closes: list[float] = []

        # Current pending FVG zone
        self._pending_fvg: FVGZone | None = None

        # Active trade tracking
        self._position_side: OrderSide | None = None
        self._entry_price: float | None = None
        self._stop_loss: float | None = None
        self._take_profit: float | None = None

        # Statistics
        self._bar_count = 0
        self._fvg_found = 0
        self._trades = 0
        self._wins = 0
        self._losses = 0
        self._trade_log: list[TradeRecord] = []

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.instrument_id)
        if self.instrument is None:
            self.log.error(f"Instrument {self.instrument_id} not found in cache")
            return
        self.subscribe_bars(self.bar_type)
        self.log.info(f"FVG Strategy started — subscribed to {self.bar_type}")

    def on_bar(self, bar: Bar) -> None:
        self._bar_count += 1
        close = float(bar.close)

        # Warmup
        if self._bar_count < self.warmup_bars:
            self._bars.append(bar)
            self._closes.append(close)
            return

        # Update histories
        self._bars.append(bar)
        if len(self._bars) > max(self.sma_period + 5, 10):
            self._bars.pop(0)

        self._closes.append(close)
        if len(self._closes) > self.sma_period + 5:
            self._closes.pop(0)

        # Exit logic first
        if self._position_side is not None:
            self._check_exit(bar)
            return

        # Need at least 3 bars for FVG detection
        if len(self._bars) < 3:
            return

        # Age out stale pending FVG
        if self._pending_fvg is not None:
            bars_since = sum(1 for b in self._bars if b.ts_event > self._pending_fvg.formed_at)
            if bars_since > self._pending_fvg.max_bars:
                self._pending_fvg = None

        # Check for entry into pending FVG
        if self._pending_fvg is not None:
            self._check_fvg_entry(bar)
            return

        # Detect new FVG
        self._detect_fvg()

    def _detect_fvg(self) -> None:
        bar_a = self._bars[-3]
        bar_b = self._bars[-2]
        bar_c = self._bars[-1]

        high_a = float(bar_a.high)
        low_a = float(bar_a.low)
        low_b = float(bar_b.low)
        high_b = float(bar_b.high)
        high_c = float(bar_c.high)
        low_c = float(bar_c.low)

        sma = self._sma()

        # Bullish FVG
        if high_c < low_a:
            gap_size = low_a - high_c
            if gap_size >= self.fvg_min_size:
                if self.trend_filter and sma is not None:
                    close_c = float(bar_c.close)
                    if close_c < sma:
                        return

                stop_loss = low_b - (gap_size * 0.5)

                self._pending_fvg = FVGZone(
                    direction=OrderSide.BUY,
                    gap_low=high_c,
                    gap_high=low_a,
                    stop_loss=stop_loss,
                    formed_at=bar_c.ts_event,
                )
                self._fvg_found += 1

        # Bearish FVG
        elif low_c > high_a:
            gap_size = low_c - high_a
            if gap_size >= self.fvg_min_size:
                if self.trend_filter and sma is not None:
                    close_c = float(bar_c.close)
                    if close_c > sma:
                        return

                stop_loss = high_b + (gap_size * 0.5)

                self._pending_fvg = FVGZone(
                    direction=OrderSide.SELL,
                    gap_low=high_a,
                    gap_high=low_c,
                    stop_loss=stop_loss,
                    formed_at=bar_c.ts_event,
                )
                self._fvg_found += 1

    def _check_fvg_entry(self, bar: Bar) -> None:
        if self._pending_fvg is None:
            return

        fvg = self._pending_fvg
        close = float(bar.close)
        high = float(bar.high)
        low = float(bar.low)

        bar_touches_zone = low <= fvg.gap_high and high >= fvg.gap_low

        if not bar_touches_zone:
            return

        entry_price = close
        stop_distance = abs(entry_price - fvg.stop_loss)

        if stop_distance < 0.01:
            self._pending_fvg = None
            return

        if fvg.direction == OrderSide.BUY:
            take_profit = entry_price + (stop_distance * self.risk_reward)
        else:
            take_profit = entry_price - (stop_distance * self.risk_reward)

        # Submit order
        quantity = Quantity(float(self.trade_size), self.instrument.size_precision)
        order = self.order_factory.market(
            instrument_id=self.instrument_id,
            order_side=fvg.direction,
            quantity=quantity,
        )
        self.submit_order(order)

        # Track position
        self._position_side = fvg.direction
        self._entry_price = entry_price
        self._stop_loss = fvg.stop_loss
        self._take_profit = take_profit
        self._trades += 1

        self._pending_fvg = None

    def _check_exit(self, bar: Bar) -> None:
        if self._position_side is None:
            return

        high = float(bar.high)
        low = float(bar.low)

        hit_sl = False
        hit_tp = False

        if self._position_side == OrderSide.BUY:
            if low <= self._stop_loss:
                hit_sl = True
            if high >= self._take_profit:
                hit_tp = True
        else:
            if high >= self._stop_loss:
                hit_sl = True
            if low <= self._take_profit:
                hit_tp = True

        if hit_tp or hit_sl:
            exit_price = self._take_profit if hit_tp else self._stop_loss

            if self._position_side == OrderSide.BUY:
                pnl_points = exit_price - self._entry_price
            else:
                pnl_points = self._entry_price - exit_price

            pnl_dollar = round(pnl_points * float(self.trade_size) * 100, 2)

            if hit_tp:
                self._wins += 1
                status = "WIN"
            else:
                self._losses += 1
                status = "LOSS"

            self._trade_log.append(
                TradeRecord(
                    direction="LONG" if self._position_side == OrderSide.BUY else "SHORT",
                    entry_price=self._entry_price,
                    exit_price=exit_price,
                    stop_loss=self._stop_loss,
                    take_profit=self._take_profit,
                    pnl=pnl_dollar,
                    pnl_points=pnl_points,
                    status=status,
                    time=str(bar.ts_event),
                )
            )

            self._close_position()

    def _close_position(self) -> None:
        if self._position_side is None:
            return
        close_side = OrderSide.SELL if self._position_side == OrderSide.BUY else OrderSide.BUY
        for pos in self.cache.positions_open(instrument_id=self.instrument_id):
            order = self.order_factory.market(
                instrument_id=self.instrument_id,
                order_side=close_side,
                quantity=pos.quantity,
            )
            self.submit_order(order)

        self._position_side = None
        self._entry_price = None
        self._stop_loss = None
        self._take_profit = None

    def _sma(self) -> float | None:
        if len(self._closes) < self.sma_period:
            return None
        return sum(self._closes[-self.sma_period :]) / self.sma_period

    # ── Public methods for backtest reporting ─────────────────────────────────

    def get_trade_log(self) -> list[TradeRecord]:
        return self._trade_log

    def get_stats(self, initial_cash: float) -> dict:
        if not self._trade_log:
            return {
                "total_trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "net_pnl": 0.0,
                "gross_profit": 0.0,
                "gross_loss": 0.0,
                "profit_factor": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "best_trade": 0.0,
                "worst_trade": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "final_equity": initial_cash,
                "return_pct": 0.0,
                "total_fvgs": self._fvg_found,
            }

        wins = [t for t in self._trade_log if t.status == "WIN"]
        losses = [t for t in self._trade_log if t.status == "LOSS"]

        gross_profit = sum(t.pnl for t in wins)
        gross_loss = abs(sum(t.pnl for t in losses))
        net_pnl = gross_profit - gross_loss
        total = len(self._trade_log)
        win_rate = (len(wins) / total * 100) if total > 0 else 0.0
        profit_factor = (
            gross_profit / gross_loss
            if gross_loss > 0
            else (float("inf") if gross_profit > 0 else 0.0)
        )

        avg_win = gross_profit / len(wins) if wins else 0.0
        avg_loss = gross_loss / len(losses) if losses else 0.0

        best_trade = max(t.pnl for t in self._trade_log) if self._trade_log else 0.0
        worst_trade = min(t.pnl for t in self._trade_log) if self._trade_log else 0.0

        # Calculate drawdown
        equity = initial_cash
        peak = equity
        max_dd = 0.0
        max_dd_pct = 0.0

        for trade in self._trade_log:
            equity += trade.pnl
            if equity > peak:
                peak = equity
            dd = peak - equity
            dd_pct = (dd / peak) * 100 if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd
                max_dd_pct = dd_pct

        final_equity = initial_cash + net_pnl
        return_pct = (net_pnl / initial_cash) * 100

        return {
            "total_trades": total,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(win_rate, 2),
            "net_pnl": round(net_pnl, 2),
            "gross_profit": round(gross_profit, 2),
            "gross_loss": round(gross_loss, 2),
            "profit_factor": round(profit_factor, 3),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "best_trade": round(best_trade, 2),
            "worst_trade": round(worst_trade, 2),
            "max_drawdown": round(max_dd, 2),
            "max_drawdown_pct": round(max_dd_pct, 2),
            "final_equity": round(final_equity, 2),
            "return_pct": round(return_pct, 2),
            "total_fvgs": self._fvg_found,
        }

    def on_stop(self) -> None:
        self.log.info(
            f"FVG Strategy stopped | trades={self._trades} wins={self._wins} losses={self._losses}"
        )
        self._close_position()
