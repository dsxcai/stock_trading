from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

ET_TZ = "America/New_York"
TW_TZ = "Asia/Taipei"


def _parse_date_parts(value: str, separator_pattern: str = r"[-/]") -> tuple[int, int, int]:
    parts = re.split(separator_pattern, value.strip())
    if len(parts) != 3:
        raise ValueError(f"Invalid date value: {value!r}")
    year, month, day = (int(part) for part in parts)
    return year, month, day


def _to_yyyy_mm_dd(value: str) -> str:
    """Normalize a loose date string into YYYY-MM-DD."""
    raw = str(value).strip()
    if not raw:
        raise ValueError("Empty date value")
    normalized = raw.replace("/", "-")
    try:
        if "T" in normalized or " " in normalized:
            return datetime.fromisoformat(normalized.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        pass
    year, month, day = _parse_date_parts(normalized, r"-")
    return f"{year:04d}-{month:02d}-{day:02d}"


def _safe_float(value: Any) -> Optional[float]:
    """Parse a numeric value and return None for blank-like inputs."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    return float(text)


def _safe_int(value: Any) -> Optional[int]:
    """Parse an integer value and return None for blank-like inputs."""
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _normalize_trade_date_et(value: str) -> str:
    """Best-effort normalization for ET trade dates."""
    try:
        return _to_yyyy_mm_dd(value)
    except Exception:
        return str(value).strip()


def _normalize_time_tw(value: str) -> str:
    """Normalize a Taiwan timestamp into YYYY/MM/DD HH:MM:SS when possible."""
    raw = str(value or "").strip()
    if not raw:
        return raw
    raw = raw.replace("-", "/")
    if " " not in raw and "T" in raw:
        raw = raw.replace("T", " ")
    parts = raw.split()
    if len(parts) < 2:
        return raw
    date_part, time_part = parts[0], parts[1]

    try:
        year, month, day = _parse_date_parts(date_part.replace("/", "-"), r"-")
        date_part = f"{year:04d}/{month:02d}/{day:02d}"
    except Exception:
        pass

    time_part = time_part.split(".")[0]
    segments = time_part.split(":")
    if len(segments) == 2:
        segments.append("00")
    if len(segments) >= 3:
        try:
            hour, minute, second = (int(segments[0]), int(segments[1]), int(segments[2]))
            time_part = f"{hour:02d}:{minute:02d}:{second:02d}"
        except Exception:
            pass
    return f"{date_part} {time_part}"


def _parse_ymd_loose(value: str) -> Optional[date]:
    """Parse a loose YYYY-MM-DD or YYYY/MM/DD string into a date."""
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(_to_yyyy_mm_dd(raw))
    except Exception:
        return None


def _trade_time_tw_to_et_dt(time_tw: str) -> Optional[datetime]:
    """Convert a Taiwan-local trade timestamp into an ET-aware datetime."""
    normalized = _normalize_time_tw(time_tw)
    if not normalized:
        return None
    parts = normalized.replace("/", "-").split()
    if len(parts) < 2:
        return None
    try:
        year, month, day = _parse_date_parts(parts[0], r"-")
        hour, minute, second = (int(segment) for segment in parts[1].split(":"))
    except Exception:
        return None
    tw_datetime = datetime(year, month, day, hour, minute, second, tzinfo=ZoneInfo(TW_TZ))
    return tw_datetime.astimezone(ZoneInfo(ET_TZ))


def parse_dateish(value: str) -> Optional[datetime]:
    """Parse a loose date or datetime string into a naive datetime for sorting."""
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("/", "-").replace("T", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def extract_json_from_text(text: str) -> dict[str, Any]:
    """Extract JSON payloads from raw JSON text or fenced code blocks."""
    candidate = text.strip()
    if candidate.startswith("```"):
        candidate = re.sub(r"^```(?:json)?\s*", "", candidate)
        candidate = re.sub(r"\s*```$", "", candidate)
    return json.loads(candidate)
