# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import datetime as dt
import logging
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    import yfinance as yf
except ModuleNotFoundError:
    yf = None

from utils.config_access import discover_state_engine_tickers, load_state_engine_config
from utils.logger import configure_logging, log_run_header

LOGGER = logging.getLogger("investment.download_1y")

PRICE_COLUMNS = ["Open", "High", "Low", "Close"]
EXPORT_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]


def parse_date(value: str) -> dt.date:
    """Parse a YYYY-MM-DD date string."""
    return dt.date.fromisoformat(value)


def yesterday() -> dt.date:
    """Return yesterday in local system time."""
    return dt.date.today() - dt.timedelta(days=1)


def load_tickers_from_config(path: str) -> list[str]:
    """Load unique tickers from config.json."""
    return discover_state_engine_tickers(load_state_engine_config(path))


def _flatten_download_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Flatten yfinance MultiIndex columns into a single header row."""
    flattened = data.copy()
    if not isinstance(flattened.columns, pd.MultiIndex):
        return flattened

    level0 = [str(value) for value in flattened.columns.get_level_values(0)]
    level1 = [str(value) for value in flattened.columns.get_level_values(1)]
    required = set(EXPORT_COLUMNS)

    if required.issubset(set(level0)):
        flattened.columns = flattened.columns.get_level_values(0)
        return flattened
    if required.issubset(set(level1)):
        flattened.columns = flattened.columns.get_level_values(1)
        return flattened

    flattened.columns = ["_".join(str(part) for part in column if str(part)) for column in flattened.columns]
    return flattened


def _normalize_history_frame(
    data: pd.DataFrame,
    ticker: str,
    *,
    allow_incomplete_rows: bool = False,
    bypass_option_hint: str = "--allow-incomplete-csv-rows",
) -> pd.DataFrame:
    """Normalize downloaded history into Date,Open,High,Low,Close,Volume CSV layout."""
    normalized = _flatten_download_columns(data)
    normalized = normalized.rename(columns=lambda value: str(value).title())

    missing = [column for column in PRICE_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"Missing required columns for {ticker}: {missing}")
    if "Volume" not in normalized.columns:
        normalized["Volume"] = 0

    normalized = normalized[EXPORT_COLUMNS].copy()
    normalized[PRICE_COLUMNS] = normalized[PRICE_COLUMNS].apply(pd.to_numeric, errors="coerce").round(4)
    invalid_mask = normalized[PRICE_COLUMNS].isna().any(axis=1)
    if bool(invalid_mask.any()):
        invalid_dates = pd.to_datetime(normalized.index[invalid_mask], errors="coerce")
        first_bad = invalid_dates[0] if len(invalid_dates) > 0 else None
        first_bad_text = first_bad.strftime("%Y-%m-%d") if not pd.isna(first_bad) else "unknown"
        if not allow_incomplete_rows:
            raise ValueError(
                f"{ticker}: downloaded history contains incomplete OHLC data for Date={first_bad_text}. "
                f"Re-run with {bypass_option_hint} to bypass and skip incomplete rows."
            )
        normalized = normalized.loc[~invalid_mask].copy()
    if normalized.empty:
        raise ValueError(f"No complete OHLC rows returned for {ticker}")
    normalized["Volume"] = pd.to_numeric(normalized["Volume"], errors="raise").astype("int64")

    normalized.index = pd.to_datetime(normalized.index, errors="raise")
    if getattr(normalized.index, "tz", None) is not None:
        normalized.index = normalized.index.tz_localize(None)
    normalized.index.name = "Date"
    return normalized


def _zip_output_dir(output_dir: Path, end_date: dt.date, logger: logging.Logger) -> Path:
    """Zip all files in output_dir into {stamp_date}.zip (no directory paths, like zip -j).

    The stamp date is taken from the last data row of GOOG.csv when present;
    otherwise end_date is used as fallback — matching the get_rec.sh behaviour.
    """
    stamp_date = end_date
    goog_csv = output_dir / "GOOG.csv"
    if goog_csv.is_file():
        try:
            last_line = ""
            with goog_csv.open() as fh:
                for line in fh:
                    stripped = line.strip()
                    if stripped:
                        last_line = stripped
            if last_line:
                candidate = last_line.split(",")[0].strip()
                stamp_date = dt.date.fromisoformat(candidate)
        except Exception as exc:
            logger.warning(f"Could not read stamp date from GOOG.csv: {exc}; using end_date={end_date}")

    data_files = sorted(f for f in output_dir.iterdir() if f.is_file())
    if not data_files:
        raise SystemExit(f"No files found under {output_dir}/. Nothing to zip.")

    zip_path = Path(f"{stamp_date}.zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for f in data_files:
            zf.write(f, arcname=f.name)

    logger.info(f"[ZIP] wrote {zip_path} ({len(data_files)} files)")
    return zip_path


def download_history(
    ticker: str,
    start: dt.date,
    end: dt.date,
    output_dir: Path,
    *,
    output_path: Optional[Path] = None,
    allow_incomplete_rows: bool = False,
    bypass_option_hint: str = "--allow-incomplete-csv-rows",
) -> Path:
    """Download one ticker's daily history and write it as CSV."""
    data = yf.download(
        ticker,
        start=start.isoformat(),
        end=end.isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
    )
    if data.empty:
        raise ValueError(f"No history returned for {ticker}")

    normalized = _normalize_history_frame(
        data,
        ticker,
        allow_incomplete_rows=allow_incomplete_rows,
        bypass_option_hint=bypass_option_hint,
    )
    csv_path = output_path if output_path is not None else (output_dir / f"{ticker}.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(csv_path, float_format="%.4f")
    return csv_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--output-dir", "--outdir", dest="output_dir", default="data")
    parser.add_argument("--start")
    parser.add_argument("--end", default="")
    parser.add_argument("--days-back", dest="days_back", type=int, default=None,
                        help="Set start date to N days ago (overrides --start). get_rec.sh used 1200.")
    parser.add_argument("--tickers", default="")
    parser.add_argument("--allow-incomplete-csv-rows", action="store_true", help="Bypass incomplete OHLC rows from the download source by skipping them")
    parser.add_argument("--zip", dest="zip_output", action="store_true",
                        help="Zip all output files into {stamp_date}.zip after a successful download")
    parser.add_argument("--log-file", default="")
    args = parser.parse_args()

    logger, log_path = configure_logging("download_1y", args.log_file)
    global LOGGER
    LOGGER = logger
    log_run_header(logger, "download_1y.py", args)

    end_date = parse_date(args.end) if args.end else yesterday()
    if args.days_back is not None:
        start_date = end_date - dt.timedelta(days=args.days_back)
    elif args.start:
        start_date = parse_date(args.start)
    else:
        start_date = end_date - dt.timedelta(days=370)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.tickers.strip():
        tickers = [item.strip().upper() for item in args.tickers.split(",") if item.strip()]
    else:
        tickers = load_tickers_from_config(args.config)

    if yf is None:
        raise SystemExit("Missing dependency: yfinance. Install it with: pip install yfinance")

    failures: list[str] = []
    for ticker in tickers:
        try:
            path = download_history(
                ticker,
                start_date,
                end_date + dt.timedelta(days=1),
                output_dir,
                allow_incomplete_rows=bool(args.allow_incomplete_csv_rows),
            )
            logger.info(f"[OK] wrote {path}")
        except Exception as exc:
            failures.append(ticker)
            logger.error(f"[ERR] {ticker}: {exc}")

    if failures:
        raise SystemExit(1)

    if args.zip_output:
        _zip_output_dir(output_dir, end_date, logger)

    logger.info(f"[LOG] complete file={log_path}")


if __name__ == "__main__":
    main()
