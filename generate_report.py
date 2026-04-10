from __future__ import annotations

import argparse
import json
import traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from core import reporting as runtime
from core.report_meta import _migrate_state_schema, _normalize_mode_key
from core.report_context import _ensure_trading_calendar, _resolve_runtime_report_meta
from core.report_output import _build_report_output
from core.runtime_io import _load_cash_events_payload, _load_runtime_config, _load_trades_payload
from core.state_engine import (
    _compute_keep_history_rows,
    _discover_tickers_from_config,
    _ensure_cash_buckets,
    _hydrate_positions_from_trade_ledger_if_needed,
    _import_csvs_into_states,
    _migrate_legacy_cash_history_to_events,
    _rebuild_market_snapshot_from_history,
    _reprice_and_totals,
    _update_portfolio_performance,
)
from core.tactical_engine import compute_tactical_plan
from utils.config_access import config_cash_events_file, config_trades_file
from utils.dates import ET_TZ
from utils.logger import configure_logging, emit, log_run_header
from utils.precision import state_engine_numeric_precision


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--states", default="states.json")
    parser.add_argument("--config", default="config.json", help="External config JSON path")
    parser.add_argument("--trades-file", default="", help="Optional external trades JSON path. Default: config state_engine.meta.trades_file or trades.json")
    parser.add_argument("--cash-events-file", default="", help="Optional external cash-events JSON path. Default: config state_engine.meta.cash_events_file or cash_events.json")
    parser.add_argument("--schema", default="report_spec.json", help="report_schema.md or report_spec.json")
    parser.add_argument("--mode", required=True, help="Mode snapshot to render, such as Premarket, Intraday, or AfterClose")
    parser.add_argument("--date", default="", help="Optional YYYY-MM-DD used only for the output filename")
    parser.add_argument("--out", default="", help="Explicit output path for the rendered markdown report")
    parser.add_argument("--out-dir", default=".", help="Output directory used when --out is not set")
    parser.add_argument("--csv-dir", default="data", help="CSV directory used to derive transient tactical rows when states.json omits them")
    parser.add_argument("--allow-incomplete-csv-rows", action="store_true", help="Bypass incomplete OHLC rows by skipping them instead of failing")
    parser.add_argument("--derive-signals-inputs", default="force", choices=["never", "missing", "force"], help="How to derive transient signals inputs for report rendering")
    parser.add_argument("--derive-threshold-inputs", default="force", choices=["never", "missing", "force"], help="How to derive transient threshold inputs for report rendering")
    parser.add_argument("--now-et", default="", help="Override current ET datetime for report generation metadata")
    parser.add_argument("--log-file", default="", help="Optional render log path")
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def run_args(args: argparse.Namespace, *, argv: list[str] | None = None) -> int:
    logger, log_path = configure_logging("generate_report", getattr(args, "log_file", ""))
    runtime.print = lambda *parts, **kwargs: emit(logger, *parts, **kwargs)
    log_run_header(logger, "generate_report.py", args, argv=argv)
    logger.info(f"[LOG] file={log_path}")

    exit_code = 0
    try:
        states = json.loads(Path(args.states).read_text(encoding="utf-8"))
        config_path = args.config.strip() or str(Path(args.states).resolve().parent / "config.json")
        config = _load_runtime_config(config_path)
        numeric_precision = state_engine_numeric_precision(config)
        engine_runtime = {"config": config, "history": {}}
        trades_file = args.trades_file.strip() or config_trades_file(config) or "trades.json"
        cash_events_file = args.cash_events_file.strip() or config_cash_events_file(config) or "cash_events.json"
        trades_path = Path(trades_file)
        if not trades_path.is_absolute():
            trades_path = Path(args.states).resolve().parent / trades_path
        cash_events_path = Path(cash_events_file)
        if not cash_events_path.is_absolute():
            cash_events_path = Path(args.states).resolve().parent / cash_events_path
        loaded_trades = _load_trades_payload(str(trades_path))
        loaded_cash_events = _load_cash_events_payload(str(cash_events_path))
        trades = loaded_trades if isinstance(loaded_trades, list) else []
        cash_events = loaded_cash_events if isinstance(loaded_cash_events, list) else []
        _migrate_state_schema(states, ensure_broker_snapshot=True)
        _ensure_trading_calendar(engine_runtime)
        _ensure_cash_buckets(states, usd_amount_ndigits=int(numeric_precision["usd_amount"]))
        _migrate_legacy_cash_history_to_events(
            states,
            cash_events,
            usd_amount_ndigits=int(numeric_precision["usd_amount"]),
        )
        _hydrate_positions_from_trade_ledger_if_needed(states, engine_runtime, trades)
        report_now_et = None
        now_et_raw = str(args.now_et or "").strip()
        if now_et_raw:
            report_now_et = datetime.fromisoformat(now_et_raw)
            report_now_et = report_now_et.replace(tzinfo=ZoneInfo(ET_TZ)) if report_now_et.tzinfo is None else report_now_et.astimezone(ZoneInfo(ET_TZ))
        report_meta = _resolve_runtime_report_meta(
            engine_runtime,
            args.mode,
            report_date=args.date,
            now_et=report_now_et,
        )
        engine_runtime["report_meta"] = dict(report_meta)
        tickers = _discover_tickers_from_config(states, engine_runtime)
        _import_csvs_into_states(
            states,
            engine_runtime,
            csv_dir=args.csv_dir,
            tickers=tickers,
            prices_now_from="close",
            keep_history_rows=_compute_keep_history_rows(states, engine_runtime),
            persist_market_snapshot=False,
            allow_incomplete_rows=bool(args.allow_incomplete_csv_rows),
        )
        _rebuild_market_snapshot_from_history(states, engine_runtime)
        _reprice_and_totals(states, engine_runtime)
        _update_portfolio_performance(states, cash_events, usd_amount_ndigits=int(numeric_precision["usd_amount"]))
        tactical_plan = compute_tactical_plan(
            states,
            engine_runtime,
            derive_signals_inputs=args.derive_signals_inputs,
            derive_threshold_inputs=args.derive_threshold_inputs,
            mode=args.mode,
            trades=trades,
        )
        markdown, output_path = _build_report_output(
            states,
            schema_path=args.schema,
            report_dir=args.out_dir,
            report_out=args.out.strip(),
            mode=args.mode,
            config=config,
            trades=trades,
            cash_events=cash_events,
            tactical_plan=tactical_plan,
            report_meta=report_meta,
            market_history=engine_runtime.get("history"),
        )
        if not args.out.strip() and args.date.strip():
            output_path = str(Path(args.out_dir) / f"{args.date.strip()}_{_normalize_mode_key(args.mode)}.md")
        Path(output_path).write_text(markdown, encoding="utf-8")
        runtime.print(f"[OK] wrote {output_path}")
        logger.info("[EXIT] code=0")
    except Exception:
        logger.error("[EXCEPTION] uncaught exception follows")
        traceback.print_exc()
        exit_code = 1
        logger.error("[EXIT] code=1")
    finally:
        logger.info(f"[LOG] complete file={log_path}")
    return exit_code


def main(argv: list[str] | None = None) -> int:
    return run_args(parse_args(argv), argv=argv)


if __name__ == "__main__":
    raise SystemExit(main())
