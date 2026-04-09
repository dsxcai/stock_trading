from __future__ import annotations

import json
import re
from typing import Any, Optional

from utils.dates import (
    ET_TZ,
    TW_TZ,
    _normalize_time_tw,
    _normalize_trade_date_et,
    _parse_ymd_loose,
    _to_yyyy_mm_dd,
    _trade_time_tw_to_et_dt,
    parse_dateish,
)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    return float(text)


def _safe_int(value: Any) -> Optional[int]:
    parsed = _safe_float(value)
    return int(parsed) if parsed is not None else None


def extract_json_from_text(text: str) -> dict[str, Any]:
    candidate = text.strip()
    if candidate.startswith("```"):
        candidate = re.sub(r"^```(?:json)?\s*", "", candidate)
        candidate = re.sub(r"\s*```$", "", candidate)
    return json.loads(candidate)
