#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR" || exit 1

date_offset() {
  local days="$1"

  if date -v -"${days}"d +"%Y-%m-%d" >/dev/null 2>&1; then
    date -v -"${days}"d +"%Y-%m-%d"
    return 0
  fi

  if date -d "${days} days ago" +"%Y-%m-%d" >/dev/null 2>&1; then
    date -d "${days} days ago" +"%Y-%m-%d"
    return 0
  fi

  echo "Unsupported 'date' implementation on this system." >&2
  return 1
}

START_DATE="$(date_offset 1200)"
END_DATE="$(date_offset 0)"

python3 download_1y.py \
  --config config.json \
  --output-dir data \
  --start "$START_DATE" \
  --end "$END_DATE" \
  "$@"

STAMP_DATE="$END_DATE"
if [[ -f data/GOOG.csv ]]; then
  LAST_DATE="$(tail -n 1 data/GOOG.csv | cut -d, -f1 | tr -d '\r')"
  if [[ "$LAST_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    STAMP_DATE="$LAST_DATE"
  fi
fi

shopt -s nullglob
DATA_FILES=(data/*)
shopt -u nullglob

if (( ${#DATA_FILES[@]} == 0 )); then
  echo "No files found under data/. Nothing to zip." >&2
  exit 1
fi

zip -j -u "${STAMP_DATE}.zip" "${DATA_FILES[@]}"
