# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

JSONDict = Dict[str, Any]
TradeSide = Literal["BUY", "SELL"]
ImportStatus = Literal["imported", "skipped_missing", "error"]
CashEventKind = Literal["deposit", "withdrawal", "to_reserve", "to_deployable"]


@dataclass(slots=True)
class ImportResult:
    """Structured result for a single CSV import attempt."""
    ticker: str
    status: ImportStatus
    csv_path: str
    rows_kept: int = 0
    last_date: str = ""
    last_close: Optional[float] = None
    message: str = ""


@dataclass(slots=True)
class ReportContext:
    """Resolved report context for a specific operating mode."""
    mode_label: str
    mode_key: str
    session_class: str
    now_et_iso: str
    t_et: str
    t_plus_1_et: str
    report_date: str
    broker_asof_et: str
    broker_asof_et_datetime: str
    snapshot_kind: str
    reasonable: bool
    rationale: str
    warning: str


@dataclass(slots=True)
class OHLCVRow:
    """Normalized OHLCV row used by the strategy helpers."""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int

    def as_dict(self) -> JSONDict:
        return {
            "Date": self.date,
            "Open": self.open,
            "High": self.high,
            "Low": self.low,
            "Close": self.close,
            "Volume": self.volume,
        }


@dataclass(slots=True)
class SignalInputs:
    """Signal inputs derived from recent close history."""
    close_t: Optional[float] = None
    ma_t: Optional[float] = None
    close_t_minus_5: Optional[float] = None

    def as_dict(self) -> JSONDict:
        return {
            "close_t": self.close_t,
            "ma_t": self.ma_t,
            "close_t_minus_5": self.close_t_minus_5,
        }


@dataclass(slots=True)
class ThresholdInputs:
    """Inputs used to compute next-close threshold values."""
    close_t: Optional[float] = None
    ma_sum_previous: Optional[float] = None
    close_t_minus_5_next: Optional[float] = None

    def as_dict(self) -> JSONDict:
        return {
            "close_t": self.close_t,
            "ma_sum_prev": self.ma_sum_previous,
            "close_t_minus_5_next": self.close_t_minus_5_next,
        }


@dataclass(slots=True)
class TacticalPlan:
    """Computed tactical plan decoupled from state persistence and reporting."""
    signals_inputs: Dict[str, JSONDict] = field(default_factory=dict)
    threshold_inputs: Dict[str, JSONDict] = field(default_factory=dict)
    tactical_rows: list[JSONDict] = field(default_factory=list)
    threshold_rows: list[JSONDict] = field(default_factory=list)


@dataclass(slots=True)
class BacktestCostModel:
    """Explicit execution cost model for historical simulation."""
    fee_rate: float = 0.0
    commission_per_trade: float = 0.0
    slippage_bps: float = 0.0


@dataclass(slots=True)
class TradeRecord:
    """Typed representation of a normalized trade record."""
    trade_id: str
    trade_date_et: str
    ticker: str
    side: str
    shares: int
    gross: float
    fee: float = 0.0
    net: float = 0.0
    price: Optional[float] = None
    source: str = ""
    source_file: str = ""
    source_type: str = ""
    time_tw: str = ""
    notes: str = ""
    extras: JSONDict = field(default_factory=dict)

    def as_dict(self) -> JSONDict:
        payload = {
            "trade_id": self.trade_id,
            "trade_date_et": self.trade_date_et,
            "ticker": self.ticker,
            "side": self.side,
            "shares": self.shares,
            "gross": self.gross,
            "fee": self.fee,
            "net": self.net,
            "price": self.price,
            "source": self.source,
            "source_file": self.source_file,
            "source_type": self.source_type,
            "time_tw": self.time_tw,
            "notes": self.notes,
        }
        payload.update(self.extras)
        return payload


@dataclass(slots=True)
class CashEventRecord:
    """Append-only cash ledger entry kept outside states.json."""
    event_id: str
    event_date_et: str
    kind: CashEventKind
    amount_usd: float
    cash_effect_usd: float
    bucket_from: str = ""
    bucket_to: str = ""
    note: str = ""
    source: str = ""
    ts_utc: str = ""

    def as_dict(self) -> JSONDict:
        return {
            "event_id": self.event_id,
            "event_date_et": self.event_date_et,
            "kind": self.kind,
            "amount_usd": self.amount_usd,
            "cash_effect_usd": self.cash_effect_usd,
            "bucket_from": self.bucket_from,
            "bucket_to": self.bucket_to,
            "note": self.note,
            "source": self.source,
            "ts_utc": self.ts_utc,
        }
