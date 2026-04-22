#!/usr/bin/env bash
# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

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
./node_modules/.bin/electron ../utils/capture_gui_screenshots.js
