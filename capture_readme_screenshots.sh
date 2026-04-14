#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DESKTOP_DIR="$ROOT_DIR/desktop"

cd "$DESKTOP_DIR" || exit 1

if [[ ! -d node_modules ]]; then
  npm install
fi

export PYTHON="${PYTHON:-$(command -v python3)}"
unset ELECTRON_RUN_AS_NODE || true

npm run build
npm run capture:docs
