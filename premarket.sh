#!/usr/bin/env bash
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR" || exit 1

python3 update_states.py \
  --states states.json \
  --csv-dir data \
  --derive-signals-inputs force \
  --derive-threshold-inputs force \
  --mode Premarket \
  --render-report \
  --report-schema report_spec.json \
  --report-dir report \
  "$@"
