from __future__ import annotations

import argparse
import importlib
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from gui.server import run_server

WINDOW_TITLE = "Stock Trading GUI"
WINDOW_WIDTH = 1440
WINDOW_HEIGHT = 960
SERVER_START_TIMEOUT_SECONDS = 10.0
SERVER_POLL_INTERVAL_SECONDS = 0.1


class ServerLoopThread(threading.Thread):
    def __init__(self, repo_root: Path, host: str, port: int) -> None:
        super().__init__(name="stock-trading-gui-server", daemon=True)
        self.repo_root = repo_root
        self.host = host
        self.port = int(port)
        self.final_action = "shutdown"
        self.error: BaseException | None = None
        self.finished = threading.Event()

    def run(self) -> None:
        try:
            while True:
                action = str(run_server(self.repo_root, self.host, self.port, open_browser=False) or "shutdown").strip().lower()
                if action != "restart":
                    self.final_action = action or "shutdown"
                    return
        except BaseException as exc:  # pragma: no cover - captured for desktop mode cleanup
            self.error = exc
            self.final_action = "error"
        finally:
            self.finished.set()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1", help="Host interface for the local GUI server")
    parser.add_argument("--port", type=int, default=8765, help="Port for the local GUI server")
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Use the legacy browser mode instead of the desktop window",
    )
    return parser.parse_args()


def _build_client_url(host: str, port: int) -> str:
    value = str(host or "").strip() or "127.0.0.1"
    if value == "0.0.0.0":
        value = "127.0.0.1"
    elif value == "::":
        value = "[::1]"
    elif ":" in value and not value.startswith("["):
        value = f"[{value}]"
    return f"http://{value}:{int(port)}/"


def _wait_for_server(client_url: str, server_thread: ServerLoopThread) -> None:
    health_url = urllib.parse.urljoin(client_url, "healthz")
    deadline = time.monotonic() + SERVER_START_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if server_thread.error is not None:
            raise server_thread.error
        try:
            with urllib.request.urlopen(health_url, timeout=0.5) as response:
                if int(getattr(response, "status", 0) or 0) == 200:
                    return
        except urllib.error.URLError as exc:
            last_error = exc
        except OSError as exc:
            last_error = exc
        if server_thread.finished.is_set():
            break
        time.sleep(SERVER_POLL_INTERVAL_SECONDS)
    if server_thread.error is not None:
        raise server_thread.error
    raise RuntimeError(f"Timed out waiting for the local GUI server at {client_url}") from last_error


def _request_server_shutdown(client_url: str) -> None:
    control_url = urllib.parse.urljoin(client_url, "server-control")
    payload = urllib.parse.urlencode({"server_action": "shutdown"}).encode("utf-8")
    request = urllib.request.Request(
        control_url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(request, timeout=1.0):
            return
    except Exception:
        return


def _load_webview_module():
    try:
        return importlib.import_module("webview")
    except ModuleNotFoundError as exc:
        if exc.name != "webview":
            raise
        raise RuntimeError(
            "pywebview is not installed. Install it with `python3 -m pip install pywebview`, "
            "or run `python3 gui_app.py --open-browser` to use the legacy browser mode."
        ) from exc


def _watch_server(window, server_thread: ServerLoopThread) -> None:
    server_thread.finished.wait()
    try:
        window.destroy()
    except Exception:
        pass


def run_browser_app(repo_root: Path, host: str, port: int, *, open_browser: bool) -> None:
    should_open_browser = bool(open_browser)
    while True:
        action = run_server(repo_root, host, int(port), open_browser=should_open_browser)
        if str(action or "").strip().lower() != "restart":
            break
        should_open_browser = False


def run_desktop_app(repo_root: Path, host: str, port: int) -> None:
    webview = _load_webview_module()
    client_url = _build_client_url(host, port)
    server_thread = ServerLoopThread(repo_root, host, port)
    server_thread.start()
    try:
        _wait_for_server(client_url, server_thread)
        window = webview.create_window(
            WINDOW_TITLE,
            url=client_url,
            width=WINDOW_WIDTH,
            height=WINDOW_HEIGHT,
        )

        def _monitor_server() -> None:
            _watch_server(window, server_thread)

        webview.start(_monitor_server)
    finally:
        if server_thread.is_alive():
            _request_server_shutdown(client_url)
            server_thread.join(timeout=2.0)
    if server_thread.error is not None:
        raise server_thread.error


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parent
    if args.open_browser:
        run_browser_app(repo_root, args.host, int(args.port), open_browser=True)
        return 0
    try:
        run_desktop_app(repo_root, args.host, int(args.port))
    except RuntimeError as exc:
        print(f"[GUI] {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
