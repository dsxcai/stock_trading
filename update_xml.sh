#!/usr/bin/env bash
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR" || exit 1

if [ $# -lt 1 ]; then
  echo "Usage: ./update_xml.sh <capital-xls-path> [extra update_states args...]"
  exit 1
fi

python3 -m extensions.capital_xls_import "$1" \
  --states states.json \
  --out states.json \
  "${@:2}"
