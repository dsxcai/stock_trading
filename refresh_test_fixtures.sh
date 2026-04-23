#!/usr/bin/env bash
# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT
#
# Regenerate the regression golden fixtures from the frozen fixture inputs.
#
# Run this after changing:
#   - state_engine signal/cash/rounding logic
#   - report_spec.json schema or rendering behavior
#   - trade or cash-event data structures
#
# The script uses the existing fixture inputs (golden_premarket_*.json and
# tests/fixtures/market_data/) with STOCK_TRADING_SKIP_AUTOCSV=1 so the
# output is fully deterministic and matches what the test suite produces.
# It does NOT use or modify the live states.json / trades.json.

set -Eeuo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

NOW_ET="${1:-2026-03-18T08:00:00-04:00}"
TMP_STATES="$(mktemp)"
TMP_REPORT="$(mktemp)"
FIXTURES_DIR="$ROOT_DIR/tests/fixtures"

export STOCK_TRADING_SKIP_AUTOCSV=1

python3 update_states.py \
  --states "$FIXTURES_DIR/golden_premarket_states.json" \
  --trades-file "$FIXTURES_DIR/golden_premarket_trades.json" \
  --cash-events-file "$FIXTURES_DIR/golden_premarket_cash_events.json" \
  --config "$FIXTURES_DIR/test_config.json" \
  --out "$TMP_STATES" \
  --csv-dir "$FIXTURES_DIR/market_data" \
  --derive-signals-inputs force \
  --derive-threshold-inputs force \
  --mode Premarket \
  --render-report \
  --report-schema report_spec.json \
  --report-out "$TMP_REPORT" \
  --now-et "$NOW_ET"

cp "$TMP_STATES" "$FIXTURES_DIR/golden_premarket_states.json"
cp "$TMP_REPORT" "$FIXTURES_DIR/golden_premarket_report.md"

rm -f "$TMP_STATES" "$TMP_REPORT"
echo "Refreshed fixtures with now-et=$NOW_ET"
