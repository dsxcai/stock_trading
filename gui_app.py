from __future__ import annotations

import argparse
import importlib
import os
import secrets
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from gui.server import GUI_SESSION_QUERY_PARAM, normalize_gui_host, run_server

WINDOW_TITLE = "Stock Trading GUI"
WINDOW_WIDTH = 1440
WINDOW_HEIGHT = 960
SERVER_START_TIMEOUT_SECONDS = 10.0
SERVER_POLL_INTERVAL_SECONDS = 0.1
_RESTARTED_ENV_VAR = "STOCK_TRADING_GUI_RESTARTED"
_SESSION_TOKEN_ENV_VAR = "STOCK_TRADING_GUI_SESSION_TOKEN"


class ServerLoopThread(threading.Thread):
    def __init__(self, repo_root: Path, host: str, port: int, session_token: str) -> None:
        super().__init__(name="stock-trading-gui-server", daemon=True)
        self.repo_root = repo_root
        self.host = host
        self.port = int(port)
        self.session_token = str(session_token or "").strip()
        self.final_action = "shutdown"
        self.error: BaseException | None = None
        self.finished = threading.Event()

    def run(self) -> None:
        try:
            action = str(
                run_server(
                    self.repo_root,
                    self.host,
                    self.port,
                    open_browser=False,
                    session_token=self.session_token,
                )
                or "shutdown"
            ).strip().lower()
            self.final_action = action or "shutdown"
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
    value = normalize_gui_host(host)
    if value == "::1":
        value = "[::1]"
    elif ":" in value and not value.startswith("["):
        value = f"[{value}]"
    return f"http://{value}:{int(port)}/"


def _build_authenticated_client_url(host: str, port: int, session_token: str) -> str:
    base_url = _build_client_url(host, port)
    query = urllib.parse.urlencode({GUI_SESSION_QUERY_PARAM: str(session_token or "").strip()})
    if not query:
        return base_url
    return f"{base_url}?{query}"


def _get_or_create_session_token() -> str:
    token = str(os.environ.get(_SESSION_TOKEN_ENV_VAR) or "").strip()
    if token:
        return token
    token = secrets.token_urlsafe(24)
    os.environ[_SESSION_TOKEN_ENV_VAR] = token
    return token


def _wait_for_server(client_url: str, session_token: str, server_thread: ServerLoopThread) -> None:
    health_base = urllib.parse.urljoin(client_url, "healthz")
    health_url = f"{health_base}?{urllib.parse.urlencode({GUI_SESSION_QUERY_PARAM: session_token})}"
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


def _restart_current_process() -> None:
    env = os.environ.copy()
    env[_RESTARTED_ENV_VAR] = "1"
    os.execvpe(sys.executable, [sys.executable, *sys.argv], env)


def _watch_server(window, server_thread: ServerLoopThread) -> None:
    server_thread.finished.wait()
    try:
        window.destroy()
    except Exception:
        pass


def run_browser_app(repo_root: Path, host: str, port: int, *, open_browser: bool, session_token: str) -> str:
    return str(
        run_server(
            repo_root,
            host,
            int(port),
            open_browser=bool(open_browser),
            session_token=session_token,
        )
        or "shutdown"
    ).strip().lower()


def run_desktop_app(repo_root: Path, host: str, port: int, session_token: str) -> str:
    webview = _load_webview_module()
    client_url = _build_client_url(host, port)
    authenticated_url = _build_authenticated_client_url(host, port, session_token)
    server_thread = ServerLoopThread(repo_root, host, port, session_token)
    server_thread.start()
    try:
        _wait_for_server(client_url, session_token, server_thread)
        window = webview.create_window(
            WINDOW_TITLE,
            url=authenticated_url,
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
    return str(server_thread.final_action or "shutdown").strip().lower()


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parent
    session_token = _get_or_create_session_token()
    try:
        host = normalize_gui_host(args.host)
    except ValueError as exc:
        print(f"[GUI] {exc}", file=sys.stderr)
        return 1
    if args.open_browser:
        should_open_browser = os.environ.get(_RESTARTED_ENV_VAR) != "1"
        action = run_browser_app(
            repo_root,
            host,
            int(args.port),
            open_browser=should_open_browser,
            session_token=session_token,
        )
        if action == "restart":
            _restart_current_process()
        return 0
    try:
        action = run_desktop_app(repo_root, host, int(args.port), session_token)
    except RuntimeError as exc:
        print(f"[GUI] {exc}", file=sys.stderr)
        return 1
    if action == "restart":
        _restart_current_process()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
