from __future__ import annotations

import re
from datetime import date, datetime
from typing import Optional
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


def _normalize_trade_date_et(value: str) -> str:
    try:
        return _to_yyyy_mm_dd(value)
    except Exception:
        return str(value).strip()


def _normalize_time_tw(value: str) -> str:
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
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(_to_yyyy_mm_dd(raw))
    except Exception:
        return None


def _trade_time_tw_to_et_dt(time_tw: str) -> Optional[datetime]:
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
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("/", "-").replace("T", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None
