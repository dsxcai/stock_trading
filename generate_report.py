from __future__ import annotations

import argparse
import json
import traceback
from pathlib import Path

from core import reporting as runtime
from core.tactical_engine import compute_tactical_plan
from core.report_bundle import build_report_root
from core.state_engine import (
    _compute_keep_history_rows,
    _discover_tickers_from_config,
    _ensure_cash_buckets,
    _ensure_trading_calendar,
    _import_csvs_into_states,
    _load_runtime_config,
    _report_date_from_meta,
    _resolve_runtime_report_meta,
)
from utils.logger import configure_logging, emit, log_run_header
from utils.precision import state_engine_numeric_precision

load_schema = runtime.load_schema
render_report = runtime.render_report
report_date_default = runtime.report_date_default


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--states", default="states.json")
    parser.add_argument("--config", default="config.json", help="External config JSON path")
    parser.add_argument("--trades-file", default="", help="Optional external trades JSON path. Default: config state_engine.trades_file or trades.json")
    parser.add_argument("--schema", default="report_spec.json", help="report_schema.md or report_spec.json")
    parser.add_argument("--mode", required=True, help="Mode snapshot to render, such as Premarket, Intraday, or AfterClose")
    parser.add_argument("--date", default="", help="Optional YYYY-MM-DD used only for the output filename")
    parser.add_argument("--out", default="", help="Explicit output path for the rendered markdown report")
    parser.add_argument("--out-dir", default=".", help="Output directory used when --out is not set")
    parser.add_argument("--csv-dir", default="data", help="CSV directory used to derive transient tactical rows when states.json omits them")
    parser.add_argument("--derive-signals-inputs", default="force", choices=["never", "missing", "force"], help="How to derive transient signals inputs for report rendering")
    parser.add_argument("--derive-threshold-inputs", default="force", choices=["never", "missing", "force"], help="How to derive transient threshold inputs for report rendering")
    parser.add_argument("--log-file", default="", help="Optional render log path")
    args = parser.parse_args()

    logger, log_path = configure_logging("generate_report", args.log_file)
    runtime.print = lambda *parts, **kwargs: emit(logger, *parts, **kwargs)
    log_run_header(logger, "generate_report.py", args)
    logger.info(f"[LOG] file={log_path}")

    try:
        states = json.loads(Path(args.states).read_text(encoding="utf-8"))
        config_path = args.config.strip() or str(Path(args.states).resolve().parent / "config.json")
        config = _load_runtime_config(config_path)
        numeric_precision = state_engine_numeric_precision(config)
        engine_runtime = {"config": config, "history": {}}
        trades_file = args.trades_file.strip() or str((config or {}).get("trades_file") or "trades.json")
        trades_path = Path(trades_file)
        if not trades_path.is_absolute():
            trades_path = Path(args.states).resolve().parent / trades_path
        trades = []
        if trades_path.exists():
            loaded = json.loads(trades_path.read_text(encoding="utf-8"))
            if isinstance(loaded, list):
                trades = loaded
            elif isinstance(loaded, dict) and isinstance(loaded.get("trades"), list):
                trades = loaded.get("trades") or []
        runtime._migrate_state_schema(states)
        _ensure_trading_calendar(engine_runtime)
        _ensure_cash_buckets(states, usd_amount_ndigits=int(numeric_precision["usd_amount"]))
        tickers = _discover_tickers_from_config(states, engine_runtime)
        keep_history_rows = _compute_keep_history_rows(states, engine_runtime)
        _import_csvs_into_states(
            states,
            engine_runtime,
            csv_dir=args.csv_dir,
            tickers=tickers,
            prices_now_from="close",
            keep_history_rows=keep_history_rows,
            persist_market_snapshot=False,
        )
        tactical_plan = compute_tactical_plan(
            states,
            engine_runtime,
            derive_signals_inputs=args.derive_signals_inputs,
            derive_threshold_inputs=args.derive_threshold_inputs,
            mode=args.mode,
            trades=trades,
        )
        report_meta = _resolve_runtime_report_meta(engine_runtime, args.mode, report_date=args.date)
        schema = load_schema(args.schema)
        report_date = args.date.strip() or _report_date_from_meta(report_meta) or report_date_default(states, args.mode)
        output_path = args.out.strip() or str(Path(args.out_dir) / f"{report_date}_{runtime._normalize_mode_key(args.mode)}.md")
        report_root = build_report_root(
            states,
            config=config,
            trades=trades,
            tactical_plan=tactical_plan,
            report_meta=report_meta,
            market_history=engine_runtime.get("history"),
        )
        markdown = render_report(report_root, schema, args.mode)
        Path(output_path).write_text(markdown, encoding="utf-8")
        logger.info(f"[OK] wrote {output_path}")
        logger.info("[EXIT] code=0")
    except SystemExit:
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
