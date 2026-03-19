from __future__ import annotations

import argparse
import traceback

from core.models import ImportResult, ReportContext
from core.reconciliation import (
    _archive_trades,
    _first_token_ticker,
    _group_key_trade,
    _import_trades_from_os_history_xml,
    _is_broker_trade,
    _normalize_trades_inplace,
    _num_from_cell,
    _reconcile_manual_aggregate_trades_against_broker_import,
    _reconcile_manual_aggregates,
    _trade_cash_total_for_match,
    _trade_key,
    _upsert_trades,
    _verify_holdings_with_broker_investment_total,
)
from core.strategy import (
    _allocate_buy_shares_across_triggered_signals,
    _calc_threshold_row,
    _dedupe_by_date_keep_last,
    _derive_signals_inputs_from_history,
    _derive_threshold_inputs_from_history,
    _estimate_tactical_buy_budget_usd,
    _fmt_usd,
    _lookup_action_price_usd,
    _normalize_ma_rule,
    _parse_indicator_window,
    _read_ohlcv_csv,
)
from core import state_engine as runtime
from utils.logger import configure_logging, emit, log_run_header
from utils.parsers import (
    _normalize_time_tw,
    _normalize_trade_date_et,
    _parse_ymd_loose,
    _safe_float,
    _safe_int,
    _to_yyyy_mm_dd,
    _trade_time_tw_to_et_dt,
)


def _patch_runtime() -> None:
    """Route shared helpers into the state engine module."""
    runtime.ImportResult = ImportResult
    runtime.ReportContext = ReportContext

    runtime._to_yyyy_mm_dd = _to_yyyy_mm_dd
    runtime._safe_float = _safe_float
    runtime._safe_int = _safe_int
    runtime._normalize_trade_date_et = _normalize_trade_date_et
    runtime._normalize_time_tw = _normalize_time_tw
    runtime._parse_ymd_loose = _parse_ymd_loose
    runtime._trade_time_tw_to_et_dt = _trade_time_tw_to_et_dt

    runtime._parse_indicator_window = _parse_indicator_window
    runtime._normalize_ma_rule = _normalize_ma_rule
    runtime._fmt_usd = _fmt_usd
    runtime._dedupe_by_date_keep_last = _dedupe_by_date_keep_last
    runtime._read_ohlcv_csv = _read_ohlcv_csv
    runtime._derive_signals_inputs_from_history = _derive_signals_inputs_from_history
    runtime._derive_threshold_inputs_from_history = _derive_threshold_inputs_from_history
    runtime._calc_threshold_row = _calc_threshold_row
    runtime._estimate_tactical_buy_budget_usd = _estimate_tactical_buy_budget_usd
    runtime._lookup_action_price_usd = _lookup_action_price_usd
    runtime._allocate_buy_shares_across_triggered_signals = _allocate_buy_shares_across_triggered_signals

    runtime._first_token_ticker = _first_token_ticker
    runtime._num_from_cell = _num_from_cell
    runtime._import_trades_from_os_history_xml = _import_trades_from_os_history_xml
    runtime._trade_key = _trade_key
    runtime._normalize_trades_inplace = _normalize_trades_inplace
    runtime._upsert_trades = _upsert_trades
    runtime._is_broker_trade = _is_broker_trade
    runtime._archive_trades = _archive_trades
    runtime._group_key_trade = _group_key_trade
    runtime._trade_cash_total_for_match = _trade_cash_total_for_match
    runtime._reconcile_manual_aggregates = _reconcile_manual_aggregates
    runtime._reconcile_manual_aggregate_trades_against_broker_import = _reconcile_manual_aggregate_trades_against_broker_import
    runtime._verify_holdings_with_broker_investment_total = _verify_holdings_with_broker_investment_total


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--states", default="states.json", help="Input states.json path")
    parser.add_argument("--trades-file", default="trades.json", help="External trades JSON path")
    parser.add_argument("--csv-dir", default="./data", help="Directory containing CSV files")
    parser.add_argument("--tickers", default="", help="Comma-separated tickers override")
    parser.add_argument("--out", default="", help="Output path (default: <states_stem>.updated.json)")
    parser.add_argument("--keep-history-rows", type=int, default=0, help="Override history rows to keep (0 = automatic)")
    parser.add_argument("--derive-signals-inputs", choices=["missing", "force", "never"], default="missing")
    parser.add_argument("--derive-threshold-inputs", choices=["missing", "force", "never"], default="missing")
    parser.add_argument("--prices-now-from", choices=["close", "never"], default="close")
    parser.add_argument("--sync-meta", choices=["auto", "never"], default="auto")
    parser.add_argument("--mode", default="", help="Mode for report-scoped updates, such as Premarket, Intraday, or AfterClose")
    parser.add_argument("--now-et", default="", help="Override current ET datetime for report-context resolution")
    parser.add_argument("--render-report", action="store_true", help="Render the markdown report after state updates complete")
    parser.add_argument("--report-schema", default="report_spec.json", help="Schema path used when --render-report is enabled")
    parser.add_argument("--report-dir", default="report", help="Output directory for generated reports")
    parser.add_argument("--report-out", default="", help="Explicit report output path")
    parser.add_argument("--log-file", default="", help="Optional run log path")
    parser.add_argument("--broker-investment-total-usd", type=float, default=None, help="Broker investment total excluding cash, in USD")
    parser.add_argument("--broker-investment-total-kind", choices=["market_value", "cost_basis"], default="cost_basis", help="Interpretation of broker investment total")
    parser.add_argument("--tactical-cash-usd", type=float, default=None, help="Tactical cash balance excluding holdings, in USD")
    parser.add_argument("--initial-investment-usd", type=float, default=None, help="Persistent initial investment amount in USD")
    parser.add_argument("--cash-adjust-usd", type=float, default=None, help="External cash adjustment in USD")
    parser.add_argument("--cash-adjust-note", default="", help="Optional note for external cash adjustments")
    parser.add_argument("--cash-transfer-to-reserve-usd", type=float, default=None, help="Internal transfer between deployable and reserve cash buckets")
    parser.add_argument("--broker-asof-et", default="", help="Semantic broker as-of trade day in ET")
    parser.add_argument("--broker-asof-et-time", default="", help="Optional broker time component in ET")
    parser.add_argument("--broker-asof-et-datetime", default="", help="Optional broker datetime component in ET")
    parser.add_argument("--trades-xml", action="append", default=[], help="Repeatable OSHistoryDealAll.xml path to import")
    parser.add_argument("--trades-import-mode", choices=["append", "reconcile", "replace"], default="reconcile", help="How to merge imported broker XML trades into states.trades")
    parser.add_argument("--trade-reconcile-abs-tol-usd", type=float, default=1.0, help="Absolute USD tolerance when reconciling manual aggregate trades")
    parser.add_argument("--trade-reconcile-rel-tol", type=float, default=0.003, help="Relative tolerance used during trade reconciliation")
    parser.add_argument("--verify-tolerance-usd", type=float, default=1.0, help="Tolerance in USD for broker verification")
    parser.add_argument("--mismatch-policy", choices=["abort", "warn", "force"], default="warn", help="Behavior when broker verification exceeds tolerance")
    parser.add_argument("--diagnose-mismatch", choices=["none", "summary", "full"], default="full", help="Generate reconciliation diagnostics when mismatches occur")
    parser.add_argument("--diagnose-out", default="", help="Explicit path for the reconciliation report")
    args = parser.parse_args()

    logger, log_path = configure_logging("update_states", args.log_file)
    runtime.print = lambda *parts, **kwargs: emit(logger, *parts, **kwargs)
    _patch_runtime()
    logger.info(f"[LOG] file={log_path}")
    log_run_header(logger, "update_states.py", args)

    try:
        exit_code = int(runtime._run_main(args) or 0)
        logger.info(f"[EXIT] code={exit_code}")
    except SystemExit as exc:
        try:
            exit_code = int(exc.code) if exc.code is not None else 0
        except Exception:
            exit_code = 1
        logger.info(f"[EXIT] code={exit_code}")
        raise
    except Exception:
        logger.error("[EXCEPTION] uncaught exception follows")
        traceback.print_exc()
        logger.error("[EXIT] code=1")
        raise
    finally:
        logger.info(f"[LOG] complete file={log_path}")


if __name__ == "__main__":
    main()
