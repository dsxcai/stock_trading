from __future__ import annotations

import argparse
from pathlib import Path

from gui.server import run_server


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1", help="Host interface for the local GUI server")
    parser.add_argument("--port", type=int, default=8765, help="Port for the local GUI server")
    parser.add_argument("--open-browser", action="store_true", help="Open the GUI URL in the default browser")
    args = parser.parse_args()
    open_browser = bool(args.open_browser)
    while True:
        action = run_server(Path(__file__).resolve().parent, args.host, int(args.port), open_browser=open_browser)
        if str(action or "").strip().lower() != "restart":
            break
        open_browser = False


if __name__ == "__main__":
    main()
