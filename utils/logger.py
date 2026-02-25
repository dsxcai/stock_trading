from __future__ import annotations

import json
import logging
import os
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple


def default_log_path(script_name: str) -> str:
    """Return a timestamped log file path under ./logs."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pid = os.getpid()
    return str(Path("logs") / f"{script_name}_{timestamp}_{pid}.log")


def configure_logging(script_name: str, log_file: str = "") -> Tuple[logging.Logger, Path]:
    """Configure console and file logging for a script entrypoint."""
    logger_name = f"investment.{script_name}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    log_path = Path(log_file.strip() or default_log_path(script_name))
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger, log_path


def emit(logger: logging.Logger, *parts: Any, sep: str = " ", end: str = "\n") -> None:
    """Log a print-style message using a level inferred from the prefix."""
    message = sep.join(str(part) for part in parts)
    if end and end != "\n":
        message = f"{message}{end}"
    prefix = message.lstrip()
    level = logging.INFO
    if prefix.startswith("[ERR]") or prefix.startswith("[ERROR]") or prefix.startswith("[ABORT]") or prefix.startswith("[EXCEPTION]"):
        level = logging.ERROR
    elif prefix.startswith("[WARN]") or prefix.startswith("[MISMATCH]"):
        level = logging.WARNING
    logger.log(level, message.rstrip("\n"))


def log_run_header(logger: logging.Logger, script_name: str, args: Any) -> None:
    """Write a deterministic execution header for reproducibility."""
    argv = [script_name, *sys.argv[1:]]
    logger.info(f"[RUN] started_at={datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')}")
    logger.info(f"[RUN] cwd={os.getcwd()}")
    logger.info(f"[RUN] argv={shlex.join(argv)}")
    logger.info(f"[RUN] args={json.dumps(vars(args), ensure_ascii=False, sort_keys=True)}")
