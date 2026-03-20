#!/usr/bin/env bash
set -euo pipefail

config_path="config.json"
csv_dir="data"
out_dir="backtest_$(date +%Y%m%d_%H%M%S)"
lookback_trading_days=""
start_date=""
end_date=""
starting_cash=""
log_file=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      config_path="${2:?missing value for --config}"
      shift 2
      ;;
    --csv-dir)
      csv_dir="${2:?missing value for --csv-dir}"
      shift 2
      ;;
    --out-dir)
      out_dir="${2:?missing value for --out-dir}"
      shift 2
      ;;
    --lookback-trading-days)
      lookback_trading_days="${2:?missing value for --lookback-trading-days}"
      shift 2
      ;;
    --start-date)
      start_date="${2:?missing value for --start-date}"
      shift 2
      ;;
    --end-date)
      end_date="${2:?missing value for --end-date}"
      shift 2
      ;;
    --starting-cash)
      starting_cash="${2:?missing value for --starting-cash}"
      shift 2
      ;;
    --log-file)
      log_file="${2:?missing value for --log-file}"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage:
  ./backtest_all_in_one.sh [options]

This script is all-in-one:
1. accept parameters
2. run the historical simulation
3. write summary/equity/trades outputs
4. generate the final markdown report

Options:
  --config PATH                   Config JSON path. Default: config.json
  --csv-dir PATH                  OHLCV CSV directory. Default: data
  --out-dir PATH                  Output directory. Default: backtest_<timestamp>
  --lookback-trading-days N       Trading-day lookback when --start-date is omitted
  --start-date YYYY-MM-DD         Inclusive backtest start date
  --end-date YYYY-MM-DD           Inclusive backtest end date
  --starting-cash AMOUNT          Override starting cash
  --log-file PATH                 Optional log file path
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

cmd=(python3 backtest.py --config "$config_path" --csv-dir "$csv_dir" --out-dir "$out_dir")
if [[ -n "$lookback_trading_days" ]]; then
  cmd+=(--lookback-trading-days "$lookback_trading_days")
fi
if [[ -n "$start_date" ]]; then
  cmd+=(--start-date "$start_date")
fi
if [[ -n "$end_date" ]]; then
  cmd+=(--end-date "$end_date")
fi
if [[ -n "$starting_cash" ]]; then
  cmd+=(--starting-cash "$starting_cash")
fi
if [[ -n "$log_file" ]]; then
  cmd+=(--log-file "$log_file")
fi

echo "[ALL-IN-ONE] simulate historical path and generate markdown report"
printf 'Running:'
printf ' %q' "${cmd[@]}"
printf '\n'
"${cmd[@]}"

echo "Summary JSON: ${out_dir}/summary.json"
echo "Equity Curve: ${out_dir}/equity_curve.csv"
echo "Gross Trades: ${out_dir}/gross_trades.json"
echo "Net Trades: ${out_dir}/net_trades.json"
echo "Markdown Report: ${out_dir}/report.md"
