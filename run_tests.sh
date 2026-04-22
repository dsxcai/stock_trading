#!/usr/bin/env bash
# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

set -Eeuo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"
python3 -m unittest discover -s tests -v
