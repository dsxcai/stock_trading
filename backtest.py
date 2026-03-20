from __future__ import annotations

import argparse
import traceback

from core.backtest import run_backtest, write_backtest_outputs
from utils.logger import configure_logging, emit, log_run_header


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json", help="External config JSON path")
    parser.add_argument("--csv-dir", default="./data", help="Directory containing OHLCV CSV files")
    parser.add_argument(
        "--lookback-trading-days",
        type=int,
        default=0,
        help="Optional trading-day lookback when --start-date is not provided",
    )
    parser.add_argument("--start-date", default="", help="Optional inclusive YYYY-MM-DD backtest start date")
    parser.add_argument("--end-date", default="", help="Optional inclusive YYYY-MM-DD backtest end date")
    parser.add_argument("--starting-cash", type=float, default=0.0, help="Optional override for starting cash")
    parser.add_argument("--out-dir", default="backtest", help="Directory for backtest outputs")
    parser.add_argument("--log-file", default="", help="Optional run log path")
    args = parser.parse_args()

    logger, log_path = configure_logging("backtest", args.log_file)
    print_fn = lambda *parts, **kwargs: emit(logger, *parts, **kwargs)
    log_run_header(logger, "backtest.py", args)
    logger.info(f"[LOG] file={log_path}")

    try:
        result = run_backtest(
            config_path=args.config,
            csv_dir=args.csv_dir,
            lookback_trading_days=(args.lookback_trading_days or None),
            start_date_et=(args.start_date or None),
            end_date_et=(args.end_date or None),
            starting_cash=(args.starting_cash if args.starting_cash > 0 else None),
        )
        written = write_backtest_outputs(result, args.out_dir)
        gross = (result.get("gross") or {}).get("summary") or {}
        net = (result.get("net") or {}).get("summary") or {}
        benchmark = (((result.get("net") or {}).get("benchmark") or {}).get("summary") or {})
        print_fn(
            "[OK] gross ending_nav_usd="
            f"{float(gross.get('ending_nav_usd') or 0.0):.4f}, "
            f"net ending_nav_usd={float(net.get('ending_nav_usd') or 0.0):.4f}, "
            f"net profit_rate={float(net.get('profit_rate') or 0.0):.6f}, "
            f"net buy_and_hold_profit_rate={float(benchmark.get('profit_rate') or 0.0):.6f}"
        )
        print_fn(
            "[DONE] wrote "
            f"summary={written['summary']} | equity_curve={written['equity_curve']} | "
            f"gross_trades={written['gross_trades']} | net_trades={written['net_trades']} | report={written['report']}"
        )
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
