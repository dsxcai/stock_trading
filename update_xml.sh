#!/usr/bin/env bash
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR" || exit 1

if [ $# -lt 1 ]; then
  echo "Usage: ./update_xml.sh <xml-path> [extra update_states args...]"
  exit 1
fi

python3 update_states.py \
  --states states.json \
  --out states.json \
  --trades-xml "$1" \
  --trades-import-mode replace \
  "${@:2}"
