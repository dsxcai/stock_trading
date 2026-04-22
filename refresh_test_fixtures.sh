#!/usr/bin/env bash
# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

NOW_ET="${1:-2026-03-18T08:00:00-04:00}"
TMP_STATES="$(mktemp)"
TMP_REPORT="$(mktemp)"

python3 update_states.py \
  --states states.json \
  --trades-file trades.json \
  --out "$TMP_STATES" \
  --csv-dir data \
  --derive-signals-inputs force \
  --derive-threshold-inputs force \
  --mode Premarket \
  --render-report \
  --report-schema report_spec.json \
  --report-out "$TMP_REPORT" \
  --now-et "$NOW_ET"

cp "$TMP_STATES" tests/fixtures/golden_premarket_states.json
cp "$TMP_REPORT" tests/fixtures/golden_premarket_report.md
cp trades.json tests/fixtures/golden_premarket_trades.json

rm -f "$TMP_STATES" "$TMP_REPORT"
echo "Refreshed fixtures with now-et=$NOW_ET"
