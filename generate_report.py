from __future__ import annotations

import argparse
import json
import traceback
from pathlib import Path

from core import reporting as runtime
from utils.logger import configure_logging, emit, log_run_header
from utils.parsers import extract_json_from_text, parse_dateish

runtime._extract_json_from_text = extract_json_from_text
runtime._parse_dateish = parse_dateish

load_schema = runtime.load_schema
render_report = runtime.render_report
report_date_default = runtime.report_date_default


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--states", default="states.json")
    parser.add_argument("--trades-file", default="", help="Optional external trades JSON path. Default: meta.trades_file or trades.json")
    parser.add_argument("--schema", default="report_spec.json", help="report_schema.md or report_spec.json")
    parser.add_argument("--mode", required=True, help="Mode snapshot to render, such as Premarket, Intraday, or AfterClose")
    parser.add_argument("--date", default="", help="Optional YYYY-MM-DD used only for the output filename")
    parser.add_argument("--out", default="", help="Explicit output path for the rendered markdown report")
    parser.add_argument("--out-dir", default=".", help="Output directory used when --out is not set")
    parser.add_argument("--log-file", default="", help="Optional render log path")
    args = parser.parse_args()

    logger, log_path = configure_logging("generate_report", args.log_file)
    runtime.print = lambda *parts, **kwargs: emit(logger, *parts, **kwargs)
    log_run_header(logger, "generate_report.py", args)
    logger.info(f"[LOG] file={log_path}")

    try:
        states = json.loads(Path(args.states).read_text(encoding="utf-8"))
        trades_file = args.trades_file.strip() or str(((states.get("meta") or {}).get("trades_file") or "trades.json"))
        trades_path = Path(trades_file)
        if not trades_path.is_absolute():
            trades_path = Path(args.states).resolve().parent / trades_path
        if trades_path.exists():
            loaded = json.loads(trades_path.read_text(encoding="utf-8"))
            if isinstance(loaded, list):
                states["trades"] = loaded
            elif isinstance(loaded, dict) and isinstance(loaded.get("trades"), list):
                states["trades"] = loaded.get("trades") or []
        runtime._migrate_state_schema(states)
        schema = load_schema(args.schema)
        report_date = args.date.strip() or report_date_default(states, args.mode)
        output_path = args.out.strip() or str(Path(args.out_dir) / f"{report_date}_{runtime._normalize_mode_key(args.mode)}.md")
        markdown = render_report(states, schema, args.mode)
        Path(output_path).write_text(markdown, encoding="utf-8")
        logger.info(f"[OK] wrote {output_path}")
        logger.info("[EXIT] code=0")
    except SystemExit:
        raise
    except Exception:
        logger.error("[EXCEPTION] uncaught exception follows")
        traceback.print_exc()
        logger.error("[EXIT] code=1")
        raise
    finally:
        logger.info(f"[LOG] complete file={log_path}")


if __name__ == "__main__":
    main()
