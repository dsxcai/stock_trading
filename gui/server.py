from __future__ import annotations

import cgi
import html
import io
import re
import shutil
import tempfile
import threading
import time
import webbrowser
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from gui.config_view import render_config_panel
from gui.markdown import render_markdown
from gui.services import GuiServices, OperationResult, RuntimeConfigSnapshot, SignalConfigSnapshot

_LOG_HIGHLIGHT_RE = re.compile(r"^\[(?:ERR|ERROR|EXCEPTION|ABORT)\]\s*|^Traceback\b|(?:^|\b)[A-Za-z_]*?(?:Error|Exception):", re.IGNORECASE)


@dataclass
class GuiState:
    selected_report_path: str = ""
    right_tab: str = "report"
    view_mode: str = "rendered"
    last_result: Optional[OperationResult] = None


class GuiApplication:
    def __init__(self, repo_root: Path) -> None:
        self.services = GuiServices(repo_root)
        self.state = GuiState()
        self._lock = threading.RLock()

    def set_selected_report(self, report_path: str) -> None:
        with self._lock:
            self.state.selected_report_path = str(report_path or "").strip()

    def set_view_mode(self, view_mode: str) -> None:
        with self._lock:
            self.state.view_mode = "raw" if str(view_mode or "").strip().lower() == "raw" else "rendered"

    def set_right_tab(self, right_tab: str) -> None:
        with self._lock:
            tab_value = str(right_tab or "").strip().lower()
            self.state.right_tab = tab_value if tab_value in {"report", "status", "config"} else "report"

    def set_last_result(self, result: Optional[OperationResult]) -> None:
        with self._lock:
            self.state.last_result = result
            if result is not None and not result.success:
                self.state.right_tab = "status"

    def ensure_selected_report(self, recent_reports: list) -> None:
        with self._lock:
            selected = str(self.state.selected_report_path or "").strip()
            recent_paths = [str(item.path) for item in recent_reports]
            if selected and selected in recent_paths:
                return
            if selected and Path(selected).exists():
                return
            self.state.selected_report_path = recent_paths[0] if recent_paths else ""

    def snapshot(self) -> GuiState:
        with self._lock:
            return GuiState(
                selected_report_path=self.state.selected_report_path,
                right_tab=self.state.right_tab,
                view_mode=self.state.view_mode,
                last_result=self.state.last_result,
            )

    def render_page(self) -> str:
        recent_reports = self.services.list_recent_reports(limit=20)
        self.ensure_selected_report(recent_reports)
        snapshot = self.snapshot()
        current_report_text = self.services.read_text(snapshot.selected_report_path) if snapshot.selected_report_path else ""
        current_report_html = render_markdown(current_report_text) if current_report_text else ""
        runtime_config = self.services.load_runtime_config_snapshot()
        signal_config = self.services.load_signal_config()
        error_log_text = ""
        if snapshot.last_result and snapshot.last_result.log_path:
            error_log_text = self.services.read_text(snapshot.last_result.log_path)
        selected_info = None
        for item in recent_reports:
            if item.path == snapshot.selected_report_path:
                selected_info = item
                break
        body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Stock Trading GUI</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4f0e8;
      --panel: #fffdf8;
      --panel-2: #f8f4eb;
      --ink: #1e1d1a;
      --muted: #6b655d;
      --line: #d9d0c2;
      --accent: #1d5c63;
      --accent-2: #d08c60;
      --danger: #8b2e22;
      --success: #225f3a;
      --shadow: 0 12px 30px rgba(62, 51, 35, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
      background:
        radial-gradient(circle at top right, rgba(208, 140, 96, 0.16), transparent 26%),
        radial-gradient(circle at top left, rgba(29, 92, 99, 0.14), transparent 28%),
        var(--bg);
      color: var(--ink);
    }}
    .page {{
      display: grid;
      grid-template-columns: 360px 1fr;
      gap: 18px;
      min-height: 100vh;
      padding: 18px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }}
    .sidebar {{
      display: flex;
      flex-direction: column;
      gap: 18px;
      background: transparent;
      box-shadow: none;
      border: none;
      overflow: visible;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: var(--shadow);
      padding: 18px;
    }}
    h1, h2, h3 {{ margin: 0; }}
    h1 {{
      font-size: 1.35rem;
      letter-spacing: 0.01em;
    }}
    h2 {{
      font-size: 1rem;
      margin-bottom: 12px;
    }}
    h3 {{
      font-size: 0.95rem;
      margin-bottom: 8px;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .stack {{
      display: grid;
      gap: 12px;
    }}
    .ops-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
    }}
    .ops-grid.two {{
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }}
    button, select, textarea, input[type="date"], input[type="text"], input[type="file"], input[type="number"] {{
      width: 100%;
      border-radius: 12px;
      border: 1px solid var(--line);
      padding: 10px 12px;
      font: inherit;
      background: white;
      color: var(--ink);
    }}
    textarea {{
      min-height: 110px;
      resize: vertical;
    }}
    button {{
      cursor: pointer;
      background: linear-gradient(180deg, #2d6f78, var(--accent));
      color: white;
      border-color: rgba(0, 0, 0, 0.06);
      font-weight: 600;
      transition: transform 120ms ease, box-shadow 120ms ease, filter 120ms ease, opacity 120ms ease;
      box-shadow: 0 10px 20px rgba(29, 92, 99, 0.18);
    }}
    button:hover {{
      filter: brightness(1.04);
      box-shadow: 0 14px 24px rgba(29, 92, 99, 0.22);
    }}
    button:active, button.is-submitting {{
      transform: translateY(1px) scale(0.988);
      box-shadow: 0 5px 12px rgba(29, 92, 99, 0.22);
      filter: brightness(0.96);
    }}
    button:disabled {{
      cursor: progress;
      opacity: 0.78;
    }}
    button.secondary {{
      background: var(--panel-2);
      color: var(--ink);
      border-color: var(--line);
      box-shadow: 0 8px 18px rgba(62, 51, 35, 0.09);
    }}
    button.ghost {{
      background: white;
      color: var(--accent);
      border-color: var(--accent);
    }}
    button.danger {{
      background: linear-gradient(180deg, #a3483a, var(--danger));
      color: white;
      border-color: rgba(0, 0, 0, 0.06);
      box-shadow: 0 10px 20px rgba(139, 46, 34, 0.18);
    }}
    label {{
      display: grid;
      gap: 6px;
      font-size: 0.92rem;
      color: var(--muted);
    }}
    .inline {{
      display: flex;
      gap: 8px;
      align-items: center;
    }}
    .inline input[type="checkbox"] {{
      width: auto;
    }}
    .report-list {{
      display: grid;
      gap: 8px;
      max-height: 280px;
      overflow: auto;
      padding-right: 2px;
    }}
    .report-actions {{
      margin-top: 10px;
    }}
    .report-row {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: stretch;
    }}
    .report-delete-form {{
      margin: 0;
    }}
    .report-item {{
      display: grid;
      gap: 6px;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: white;
    }}
    .report-item.active {{
      border-color: var(--accent);
      background: #eef7f8;
    }}
    .report-delete {{
      width: 42px;
      min-width: 42px;
      padding: 0;
      border-radius: 12px;
      font-size: 1rem;
      line-height: 1;
    }}
    .report-title {{
      font-weight: 700;
      font-size: 0.95rem;
    }}
    .report-meta {{
      color: var(--muted);
      font-size: 0.84rem;
    }}
    .status {{
      border-radius: 14px;
      padding: 12px 14px;
      border: 1px solid var(--line);
      background: var(--panel-2);
    }}
    .status.ok {{
      border-color: rgba(34, 95, 58, 0.22);
      background: rgba(34, 95, 58, 0.08);
    }}
    .status.err {{
      border-color: rgba(139, 46, 34, 0.22);
      background: rgba(139, 46, 34, 0.08);
    }}
    .status-log {{
      margin: 0;
      max-height: 52vh;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    .log-error-line {{
      display: block;
      margin: 0 -6px;
      padding: 0 6px;
      color: #ffe7e2;
      background: rgba(139, 46, 34, 0.72);
      border-left: 3px solid #ffb2a3;
    }}
    .viewer-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      padding: 18px 20px 14px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(248, 244, 235, 0.8), rgba(255, 253, 248, 0.88));
    }}
    .viewer-body {{
      padding: 20px;
      overflow: auto;
      min-height: 70vh;
    }}
    .viewer-toggle {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .viewer-controls {{
      display: grid;
      gap: 10px;
      justify-items: end;
    }}
    .tab-bar {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .toggle-link {{
      text-decoration: none;
      padding: 8px 10px;
      border-radius: 999px;
      border: 1px solid var(--line);
      color: var(--ink);
      font-size: 0.88rem;
      background: white;
    }}
    .toggle-link.active {{
      border-color: var(--accent);
      background: #eef7f8;
      color: var(--accent);
    }}
    .log-shell {{
      display: grid;
      gap: 14px;
    }}
    .empty {{
      display: grid;
      place-items: center;
      min-height: 52vh;
      color: var(--muted);
      background: linear-gradient(180deg, rgba(248, 244, 235, 0.56), rgba(255, 255, 255, 0.9));
      border-radius: 14px;
      border: 1px dashed var(--line);
      text-align: center;
      padding: 24px;
    }}
    .markdown h1, .markdown h2, .markdown h3 {{
      margin: 1.1em 0 0.45em;
    }}
    .markdown h1:first-child, .markdown h2:first-child, .markdown h3:first-child {{
      margin-top: 0;
    }}
    .markdown p, .markdown ul {{
      margin: 0.7em 0;
      line-height: 1.55;
    }}
    .markdown table {{
      width: 100%;
      border-collapse: collapse;
      margin: 1em 0 1.4em;
      font-size: 0.94rem;
      background: white;
    }}
    .markdown th, .markdown td {{
      border: 1px solid var(--line);
      padding: 8px 10px;
      vertical-align: top;
    }}
    .markdown th {{
      background: #f6efe3;
      text-align: left;
    }}
    .align-right {{ text-align: right; }}
    .align-center {{ text-align: center; }}
    .markdown code {{
      background: #f0ebe4;
      padding: 0.1em 0.35em;
      border-radius: 0.3em;
      font-family: "SFMono-Regular", Menlo, Consolas, monospace;
      font-size: 0.92em;
    }}
    pre {{
      background: #1f2328;
      color: #f5f7fa;
      padding: 14px;
      border-radius: 14px;
      overflow: auto;
    }}
    details {{
      border: 1px solid var(--line);
      border-radius: 12px;
      background: white;
      overflow: hidden;
    }}
    summary {{
      cursor: pointer;
      padding: 12px 14px;
      font-weight: 700;
      background: var(--panel-2);
    }}
    details pre {{
      margin: 0;
      border-radius: 0;
      max-height: 360px;
    }}
    .config-table {{
      width: 100%;
      border-collapse: collapse;
    }}
    .config-table th, .config-table td {{
      padding: 8px 4px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      font-size: 0.9rem;
    }}
    .config-table td.compact {{
      width: 96px;
    }}
    .form-note {{
      color: var(--muted);
      font-size: 0.82rem;
    }}
    .config-shell {{
      display: grid;
      gap: 18px;
    }}
    .config-section {{
      display: grid;
      gap: 14px;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: linear-gradient(180deg, rgba(248, 244, 235, 0.48), rgba(255, 255, 255, 0.94));
    }}
    .config-section-head {{
      display: grid;
      gap: 4px;
    }}
    .config-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}
    .config-subsection {{
      display: grid;
      gap: 12px;
      padding: 16px;
      border: 1px solid rgba(29, 92, 99, 0.12);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.74);
    }}
    .config-subsection-head {{
      display: grid;
      gap: 3px;
    }}
    .config-subsection-head h4 {{
      margin: 0;
      font-size: 0.96rem;
    }}
    .config-span-2 {{
      grid-column: 1 / -1;
    }}
    .raw-report {{
      white-space: pre-wrap;
      word-break: break-word;
      font-family: "SFMono-Regular", Menlo, Consolas, monospace;
      font-size: 0.92rem;
      line-height: 1.5;
      color: var(--ink);
      background: #f8f4eb;
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 18px;
    }}
    .visually-hidden {{
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      border: 0;
    }}
    .busy-overlay {{
      position: fixed;
      inset: 0;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 24px;
      background: rgba(30, 29, 26, 0.28);
      backdrop-filter: blur(3px);
      z-index: 999;
    }}
    body.is-busy .busy-overlay {{
      display: flex;
    }}
    .busy-card {{
      width: min(420px, 100%);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: 0 18px 48px rgba(30, 29, 26, 0.24);
      padding: 22px 20px;
      display: grid;
      gap: 10px;
      text-align: center;
    }}
    .busy-title {{
      font-size: 1.1rem;
      font-weight: 700;
    }}
    .busy-progress-track {{
      width: 100%;
      height: 12px;
      border-radius: 999px;
      overflow: hidden;
      background: #e8dfd3;
      border: 1px solid rgba(29, 92, 99, 0.1);
    }}
    .busy-progress-fill {{
      width: 0%;
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, #2d6f78, #d08c60);
      transition: width 180ms ease;
    }}
    .busy-progress-value {{
      font-size: 1.6rem;
      font-weight: 700;
      line-height: 1;
      color: var(--accent);
    }}
    .busy-spinner {{
      width: 44px;
      height: 44px;
      border-radius: 999px;
      margin: 0 auto;
      border: 4px solid rgba(29, 92, 99, 0.18);
      border-top-color: var(--accent);
      animation: spin 0.85s linear infinite;
    }}
    @keyframes spin {{
      from {{ transform: rotate(0deg); }}
      to {{ transform: rotate(360deg); }}
    }}
    @media (max-width: 1080px) {{
      .page {{
        grid-template-columns: 1fr;
      }}
      .viewer-body {{
        min-height: 50vh;
      }}
      .config-grid {{
        grid-template-columns: 1fr;
      }}
      .config-span-2 {{
        grid-column: auto;
      }}
    }}
  </style>
</head>
<body>
  <div id="busy_overlay" class="busy-overlay" aria-hidden="true">
    <div class="busy-card" role="status" aria-live="assertive">
      <div class="busy-spinner" aria-hidden="true"></div>
      <div class="busy-title">Running</div>
      <div id="busy_message" class="subtle">Please wait. The page will refresh automatically when the operation finishes.</div>
      <div class="subtle">Estimated progress</div>
      <div id="busy_progress_track" class="busy-progress-track" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0">
        <div id="busy_progress_fill" class="busy-progress-fill"></div>
      </div>
      <div id="busy_progress_value" class="busy-progress-value">0%</div>
      <div id="busy_phase" class="subtle">Preparing request...</div>
    </div>
  </div>
  <div id="live_status" class="visually-hidden" aria-live="polite"></div>
  <main class="page">
    <aside class="sidebar">
      <section class="card stack">
        <div class="stack" style="gap: 6px;">
          <h1>Stock Trading GUI</h1>
          <div class="subtle">Run daily workflows, inspect reports, edit tactical SMA settings, and surface logs without using the command line.</div>
        </div>
        <form class="stack" method="post" action="/server-control" data-busy-message="Handling server control, please wait...">
          <div class="ops-grid two">
            <button class="ghost" type="submit" name="server_action" value="restart" data-busy-label="restart-server">Restart Server</button>
            <button class="danger" type="submit" name="server_action" value="shutdown" data-busy-label="shutdown-server">Stop Server</button>
          </div>
          <div class="form-note">Restart keeps the same host and port. Stop exits the current local GUI process.</div>
        </form>
      </section>
      <section class="card stack">
        <h2>Daily Run</h2>
        <form class="stack" method="post" action="/run-mode" data-busy-message="Running the daily workflow, please wait..." data-async-submit="1" data-progress-kind="daily-run">
          <div class="inline">
            <input id="force_mode" type="checkbox" name="force_mode" value="1">
            <label for="force_mode" style="gap:0;">Allow force mode</label>
          </div>
          <div class="inline">
            <input id="allow_incomplete_daily" type="checkbox" name="allow_incomplete_csv_rows" value="1">
            <label for="allow_incomplete_daily" style="gap:0;">Allow incomplete CSV rows</label>
          </div>
          <div class="ops-grid">
            <button type="submit" name="mode" value="Premarket" data-busy-label="Premarket">Premarket</button>
            <button type="submit" name="mode" value="Intraday" data-busy-label="Intraday">Intraday</button>
            <button type="submit" name="mode" value="AfterClose" data-busy-label="AfterClose">AfterClose</button>
          </div>
        </form>
      </section>
      <section class="card stack">
        <h2>Generate Report</h2>
        <form class="stack" method="post" action="/generate-report" data-busy-message="Generating the report, please wait..." data-async-submit="1" data-progress-kind="generate-report">
          <label>Mode
            <select name="mode">
              <option value="Premarket">Premarket</option>
              <option value="Intraday">Intraday</option>
              <option value="AfterClose">AfterClose</option>
            </select>
          </label>
          <label>Report Date
            <input type="date" name="report_date" value="{html.escape(selected_info.report_date if selected_info else '')}">
          </label>
          <div class="inline">
            <input id="allow_incomplete_report" type="checkbox" name="allow_incomplete_csv_rows" value="1">
            <label for="allow_incomplete_report" style="gap:0;">Allow incomplete CSV rows</label>
          </div>
          <button type="submit">Generate Report</button>
        </form>
      </section>
      <section class="card stack">
        <h2>Import Trades</h2>
        <form class="stack" method="post" action="/import-trades" enctype="multipart/form-data" data-busy-message="Importing trades and refreshing the report, please wait..." data-async-submit="1" data-progress-kind="import-trades">
          <label>Capital XLS Upload
            <input type="file" name="capital_xls_file" accept=".xls,.html,.htm">
          </label>
          <label>Or Local File Path
            <input type="text" name="capital_xls_path" placeholder="/path/to/OSHistoryDealAll.xls">
          </label>
          <label>Import Mode
            <select name="trades_import_mode">
              <option value="append">append</option>
              <option value="replace" selected>replace</option>
            </select>
          </label>
          <label>Trade Date From (ET)
            <input type="date" name="trade_date_from">
          </label>
          <label>Trade Date To (ET)
            <input type="date" name="trade_date_to">
          </label>
          <div class="inline">
            <input id="allow_incomplete_import" type="checkbox" name="allow_incomplete_csv_rows" value="1">
            <label for="allow_incomplete_import" style="gap:0;">Allow incomplete CSV rows for report refresh</label>
          </div>
          <div class="form-note">Optional trade-date bounds filter the imported XLS rows before append or replace. On success, the GUI will regenerate the currently selected report when that report can be identified by date and mode.</div>
          <button type="submit">Import Trades</button>
        </form>
      </section>
      <section class="card stack">
        <h2>Cash Adjustment</h2>
        <form class="stack" method="post" action="/cash-adjust" data-busy-message="Recording the cash adjustment and refreshing the report, please wait..." data-async-submit="1" data-progress-kind="cash-adjust">
          <label>Amount (USD)
            <input type="number" name="cash_adjust_usd" step="0.01" placeholder="-3600 for withdrawal, 2000 for deposit">
          </label>
          <label>Note
            <input type="text" name="cash_adjust_note" placeholder="optional note">
          </label>
          <div class="form-note">Use a positive amount for a deposit and a negative amount for a withdrawal. On success, the GUI will update <code>states.json</code> and regenerate the currently selected report when that report can be identified by date and mode.</div>
          <button type="submit">Record Cash Adjustment</button>
        </form>
      </section>
      <section class="card stack">
        <h2>Recent Reports</h2>
        <div class="report-list">
          {self._render_recent_reports(recent_reports, snapshot.selected_report_path)}
        </div>
        <form class="report-actions" method="post" action="/delete-all-reports" data-busy-message="Deleting all reports, please wait..." onsubmit="return window.confirm('Delete all reports under report/?');">
          <button class="danger" type="submit">Delete All Reports</button>
        </form>
      </section>
    </aside>
    <section class="panel">
      <div class="viewer-head">
        <div class="stack" style="gap: 6px;">
          <h2>{html.escape(self._viewer_title(snapshot, selected_info))}</h2>
          <div class="subtle">{html.escape(self._viewer_subtitle(snapshot, selected_info))}</div>
        </div>
        <div class="viewer-controls">
          <div class="tab-bar">
            <a class="toggle-link {'active' if snapshot.right_tab == 'report' else ''}" href="{self._build_home_href(snapshot, tab='report')}">Report</a>
            <a class="toggle-link {'active' if snapshot.right_tab == 'status' else ''}" href="{self._build_home_href(snapshot, tab='status')}">Status</a>
            <a class="toggle-link {'active' if snapshot.right_tab == 'config' else ''}" href="{self._build_home_href(snapshot, tab='config')}">Config</a>
          </div>
          {self._render_view_toggle(snapshot)}
        </div>
      </div>
      <div class="viewer-body">
        {self._render_right_panel(snapshot, current_report_text, current_report_html, error_log_text, runtime_config, signal_config)}
      </div>
    </section>
  </main>
</body>
<script>
  (() => {{
    const overlay = document.getElementById("busy_overlay");
    const busyMessage = document.getElementById("busy_message");
    const liveStatus = document.getElementById("live_status");
    const busyProgressTrack = document.getElementById("busy_progress_track");
    const busyProgressFill = document.getElementById("busy_progress_fill");
    const busyProgressValue = document.getElementById("busy_progress_value");
    const busyPhase = document.getElementById("busy_phase");
    const forms = document.querySelectorAll("form[data-busy-message]");
    if (!overlay || !busyMessage || !liveStatus || !busyProgressTrack || !busyProgressFill || !busyProgressValue || !busyPhase || !forms.length) {{
      return;
    }}
    let progressTimer = null;
    const progressProfiles = {{
      "daily-run": {{
        estimateMs: 18000,
        phases: [
          {{ pct: 5, text: "Preparing run..." }},
          {{ pct: 18, text: "Refreshing market data..." }},
          {{ pct: 42, text: "Repricing positions and totals..." }},
          {{ pct: 68, text: "Computing signals and actions..." }},
          {{ pct: 86, text: "Rendering report output..." }},
          {{ pct: 96, text: "Finalizing files..." }},
        ],
      }},
      "generate-report": {{
        estimateMs: 9000,
        phases: [
          {{ pct: 5, text: "Preparing report inputs..." }},
          {{ pct: 28, text: "Loading market history..." }},
          {{ pct: 56, text: "Computing report sections..." }},
          {{ pct: 84, text: "Rendering markdown output..." }},
          {{ pct: 96, text: "Finalizing files..." }},
        ],
      }},
      "import-trades": {{
        estimateMs: 14000,
        phases: [
          {{ pct: 5, text: "Uploading and parsing trades..." }},
          {{ pct: 24, text: "Normalizing imported trades..." }},
          {{ pct: 48, text: "Updating state and trade files..." }},
          {{ pct: 74, text: "Refreshing the selected report..." }},
          {{ pct: 96, text: "Finalizing files..." }},
        ],
      }},
      "cash-adjust": {{
        estimateMs: 6000,
        phases: [
          {{ pct: 5, text: "Recording cash adjustment..." }},
          {{ pct: 42, text: "Updating state and totals..." }},
          {{ pct: 78, text: "Refreshing the selected report..." }},
          {{ pct: 96, text: "Finalizing files..." }},
        ],
      }},
      "save-config": {{
        estimateMs: 7000,
        phases: [
          {{ pct: 5, text: "Saving config..." }},
          {{ pct: 45, text: "Refreshing the selected report..." }},
          {{ pct: 96, text: "Finalizing files..." }},
        ],
      }},
      "default": {{
        estimateMs: 7000,
        phases: [
          {{ pct: 5, text: "Preparing request..." }},
          {{ pct: 55, text: "Running operation..." }},
          {{ pct: 96, text: "Finalizing..." }},
        ],
      }},
    }};
    const updateProgress = (percent, phaseText) => {{
      const clamped = Math.max(0, Math.min(100, Math.round(percent)));
      busyProgressFill.style.width = clamped + "%";
      busyProgressValue.textContent = clamped + "%";
      busyProgressTrack.setAttribute("aria-valuenow", String(clamped));
      busyPhase.textContent = phaseText || "Running...";
    }};
    const progressProfileFor = (form) => {{
      const kind = (form.dataset.progressKind || "default").trim();
      return progressProfiles[kind] || progressProfiles.default;
    }};
    const progressPercentFor = (estimateMs, elapsedMs) => {{
      const safeEstimate = Math.max(Number(estimateMs) || 1, 1);
      const ratio = Math.min(Math.max(elapsedMs, 0) / safeEstimate, 1);
      const eased = 1 - Math.pow(1 - ratio, 3);
      return Math.min(95, Math.max(4, Math.round(4 + (eased * 91))));
    }};
    const progressPhaseFor = (phases, percent) => {{
      let current = "Running...";
      phases.forEach((phase) => {{
        if (percent >= phase.pct) {{
          current = phase.text;
        }}
      }});
      return current;
    }};
    const stopProgressTimer = () => {{
      if (progressTimer !== null) {{
        window.clearInterval(progressTimer);
        progressTimer = null;
      }}
    }};
    const resetBusyState = () => {{
      stopProgressTimer();
      document.body.classList.remove("is-busy");
      overlay.setAttribute("aria-hidden", "true");
      updateProgress(0, "Preparing request...");
      forms.forEach((form) => {{
        form.dataset.submitting = "0";
      }});
      document.querySelectorAll("button").forEach((button) => {{
        button.disabled = false;
        button.classList.remove("is-submitting");
        button.removeAttribute("aria-disabled");
      }});
    }};
    const startEstimatedProgress = (form) => {{
      stopProgressTimer();
      const profile = progressProfileFor(form);
      const startedAt = Date.now();
      const tick = () => {{
        const elapsedMs = Date.now() - startedAt;
        const percent = progressPercentFor(profile.estimateMs, elapsedMs);
        updateProgress(percent, progressPhaseFor(profile.phases, percent));
      }};
      tick();
      progressTimer = window.setInterval(tick, 180);
    }};
    const waitForPaint = () => new Promise((resolve) => {{
      window.requestAnimationFrame(() => {{
        window.requestAnimationFrame(resolve);
      }});
    }});
    const setBusyState = (form, submitter) => {{
      if (form.dataset.submitting === "1") {{
        return false;
      }}
      form.dataset.submitting = "1";
      const buttons = document.querySelectorAll("button");
      buttons.forEach((button) => {{
        button.classList.remove("is-submitting");
        if (button !== submitter) {{
          button.disabled = true;
        }}
      }});
      if (submitter) {{
        submitter.classList.add("is-submitting");
        submitter.setAttribute("aria-disabled", "true");
      }}
      let message = form.getAttribute("data-busy-message") || "Running, please wait...";
      if (submitter && submitter.dataset && submitter.dataset.busyLabel) {{
        const label = submitter.dataset.busyLabel;
        if (form.getAttribute("action") === "/run-mode") {{
          message = "Running " + label + ", please wait...";
        }} else if (form.getAttribute("action") === "/select-report") {{
          message = "Loading " + label + ", please wait...";
        }} else if (form.getAttribute("action") === "/server-control") {{
          message = label === "shutdown-server" ? "Stopping the server, please wait..." : "Restarting the server, please wait...";
        }}
      }}
      document.body.classList.add("is-busy");
      overlay.setAttribute("aria-hidden", "false");
      busyMessage.textContent = message;
      liveStatus.textContent = message;
      updateProgress(4, "Preparing request...");
      return true;
    }};
    forms.forEach((form) => {{
      form.addEventListener("submit", (event) => {{
        const submitter = event.submitter || null;
        if (form.dataset.asyncSubmit === "1") {{
          event.preventDefault();
          if (!setBusyState(form, submitter)) {{
            return;
          }}
          startEstimatedProgress(form);
          void (async () => {{
            try {{
              await waitForPaint();
              const payload = new FormData(form);
              if (submitter && submitter.name) {{
                payload.append(submitter.name, submitter.value);
              }}
              const response = await window.fetch(form.action, {{
                method: (form.method || "POST").toUpperCase(),
                body: payload,
                redirect: "follow",
                credentials: "same-origin",
              }});
              updateProgress(100, "Loading updated page...");
              liveStatus.textContent = "Operation complete. Loading updated page...";
              window.location.assign((response && response.url) ? response.url : "/");
            }} catch (error) {{
              resetBusyState();
              const message = (error && error.message) ? error.message : "The operation failed before the server responded.";
              liveStatus.textContent = message;
              window.alert(message);
            }}
          }})();
          return;
        }}
        if (!setBusyState(form, submitter)) {{
          event.preventDefault();
        }}
      }});
    }});
  }})();
</script>
</html>"""
        return body

    def _render_recent_reports(self, recent_reports: list, selected_report_path: str) -> str:
        if not recent_reports:
            return '<div class="subtle">No recent reports were found under <code>report/</code>.</div>'
        chunks = []
        for item in recent_reports:
            active = " active" if item.path == selected_report_path else ""
            item_name = html.escape(item.name)
            item_path = html.escape(item.path)
            delete_confirm = html.escape(str(item.name).replace("\\", "\\\\").replace("'", "\\'"))
            chunks.append(
                '<div class="report-row">'
                '<form method="post" action="/select-report" data-busy-message="Loading the report, please wait...">'
                f'<button class="secondary report-item{active}" type="submit" name="report_path" value="{item_path}" data-busy-label="{item_name}">'
                f'<span class="report-title">{item_name}</span>'
                f'<span class="report-meta">{html.escape(item.mode_label)} · {html.escape(item.report_date)} · modified {html.escape(item.modified_at)}</span>'
                "</button>"
                "</form>"
                f'<form class="report-delete-form" method="post" action="/delete-report" data-busy-message="Deleting the report, please wait..." onsubmit="return window.confirm(\'Delete {delete_confirm}?\');">'
                f'<button class="danger report-delete" type="submit" name="report_path" value="{item_path}" aria-label="Delete {item_name}" title="Delete {item_name}">X</button>'
                "</form>"
                "</div>"
            )
        return "\n".join(chunks)

    def _render_right_panel(
        self,
        snapshot: GuiState,
        report_text: str,
        report_html: str,
        error_log_text: str,
        runtime_config: RuntimeConfigSnapshot,
        signal_config: SignalConfigSnapshot,
    ) -> str:
        if snapshot.right_tab == "status":
            return self._render_status_panel(snapshot, error_log_text)
        if snapshot.right_tab == "config":
            return render_config_panel(runtime_config, signal_config)
        return self._render_report_view(snapshot, report_text, report_html)

    def _render_status_panel(self, snapshot: GuiState, error_log_text: str) -> str:
        if snapshot.last_result is None:
            return '<div class="empty">No operations have been run from the GUI yet.</div>'
        css_class = "ok" if snapshot.last_result.success else "err"
        report_line = (
            f'<div class="subtle">Report: {html.escape(snapshot.last_result.report_path)}</div>'
            if snapshot.last_result.report_path
            else ""
        )
        log_line = (
            f'<div class="subtle">Log: {html.escape(snapshot.last_result.log_path)}</div>'
            if snapshot.last_result.log_path
            else ""
        )
        log_body = error_log_text or snapshot.last_result.stdout or snapshot.last_result.command or ""
        return (
            '<div class="log-shell">'
            f'<div class="status {css_class}">'
            f"<strong>{html.escape(snapshot.last_result.name)}</strong>"
            f"<div>{html.escape(snapshot.last_result.message)}</div>"
            f'{report_line}{log_line}<div class="subtle">Exit Code: {snapshot.last_result.returncode}</div>'
            "</div>"
            f'{self._render_status_log(log_body, highlight_errors=not snapshot.last_result.success)}'
            "</div>"
        )

    def _render_status_log(self, log_body: str, *, highlight_errors: bool) -> str:
        text = str(log_body or "")
        if not text.strip():
            return '<div class="empty">The latest operation did not produce any log output.</div>'
        rendered_lines = []
        for line in text.splitlines():
            escaped = html.escape(line)
            if highlight_errors and _LOG_HIGHLIGHT_RE.search(line.strip()):
                rendered_lines.append(f'<span class="log-error-line">{escaped}</span>')
            else:
                rendered_lines.append(escaped)
        joined_lines = "\n".join(rendered_lines)
        return f'<pre class="status-log">{joined_lines}</pre>'

    def _render_report_view(self, snapshot: GuiState, report_text: str, report_html: str) -> str:
        if not snapshot.selected_report_path:
            return '<div class="empty">Select a report from <strong>Recent Reports</strong> to render it on the main screen.</div>'
        if not report_text:
            return '<div class="empty">The selected report file could not be read. It may have been moved or deleted.</div>'
        if snapshot.view_mode == "raw":
            return f'<pre class="raw-report">{html.escape(report_text)}</pre>'
        return f'<article class="markdown">{report_html}</article>'

    def _render_view_toggle(self, snapshot: GuiState) -> str:
        if snapshot.right_tab != "report":
            return ""
        return (
            '<div class="viewer-toggle">'
            f'<a class="toggle-link {"active" if snapshot.view_mode != "raw" else ""}" href="{self._build_home_href(snapshot, tab="report", view="rendered")}">Rendered</a>'
            f'<a class="toggle-link {"active" if snapshot.view_mode == "raw" else ""}" href="{self._build_home_href(snapshot, tab="report", view="raw")}">Raw Markdown</a>'
            "</div>"
        )

    @staticmethod
    def _viewer_title(snapshot: GuiState, selected_info: Optional[object]) -> str:
        if snapshot.right_tab == "status":
            return "Operation Status"
        if snapshot.right_tab == "config":
            return "Runtime Config"
        return selected_info.name if selected_info else "Report Viewer"

    @staticmethod
    def _viewer_subtitle(snapshot: GuiState, selected_info: Optional[object]) -> str:
        if snapshot.right_tab == "status":
            if snapshot.last_result is None:
                return "No GUI operations have been run yet."
            return snapshot.last_result.message
        if snapshot.right_tab == "config":
            return "Edit config.json through structured forms instead of the command line."
        if selected_info:
            return snapshot.selected_report_path
        return "Select a recent report to render it here."

    @staticmethod
    def _build_home_href(snapshot: GuiState, *, tab: Optional[str] = None, view: Optional[str] = None) -> str:
        tab_value = tab or snapshot.right_tab
        view_value = view or snapshot.view_mode
        return f"/?tab={html.escape(tab_value)}&view={html.escape(view_value)}"


class GuiHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass) -> None:
        super().__init__(server_address, RequestHandlerClass)
        self.control_action = "shutdown"


def make_handler(app: GuiApplication):
    class GuiHandler(BaseHTTPRequestHandler):
        server_version = "StockTradingGUI/0.1"

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/healthz":
                return self._send_text("ok\n")
            if parsed.path != "/":
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                return
            params = parse_qs(parsed.query or "", keep_blank_values=True)
            if "view" in params:
                app.set_view_mode((params.get("view") or ["rendered"])[-1])
            if "tab" in params:
                app.set_right_tab((params.get("tab") or ["report"])[-1])
            self._send_html(app.render_page())

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            fields, uploads = self._parse_form_data()
            try:
                if parsed.path == "/select-report":
                    app.set_selected_report(fields.get("report_path", ""))
                    app.set_right_tab("report")
                    return self._redirect_home()
                if parsed.path == "/delete-report":
                    report_path = fields.get("report_path", "")
                    result = app.services.delete_report(report_path)
                    if str(app.snapshot().selected_report_path or "").strip() == str(report_path or "").strip():
                        app.set_selected_report("")
                    app.set_right_tab("report")
                    app.set_last_result(result)
                    return self._redirect_home()
                if parsed.path == "/delete-all-reports":
                    result = app.services.delete_all_reports()
                    app.set_selected_report("")
                    app.set_right_tab("report")
                    app.set_last_result(result)
                    return self._redirect_home()
                if parsed.path == "/server-control":
                    server_action = str(fields.get("server_action", "")).strip().lower()
                    if server_action not in {"restart", "shutdown"}:
                        self.send_error(HTTPStatus.BAD_REQUEST, "Bad Request")
                        return
                    self.server.control_action = server_action
                    self._send_server_control_page(server_action)
                    self._schedule_server_shutdown()
                    return
                if parsed.path == "/run-mode":
                    result = app.services.run_daily_mode(
                        fields.get("mode", ""),
                        force_mode=bool(fields.get("force_mode")),
                        allow_incomplete_csv_rows=bool(fields.get("allow_incomplete_csv_rows")),
                    )
                    if result.success and result.report_path:
                        app.set_selected_report(result.report_path)
                    app.set_last_result(result)
                    return self._redirect_home()
                if parsed.path == "/generate-report":
                    result = app.services.run_generate_report(
                        fields.get("mode", ""),
                        fields.get("report_date", ""),
                        allow_incomplete_csv_rows=bool(fields.get("allow_incomplete_csv_rows")),
                    )
                    if result.success and result.report_path:
                        app.set_selected_report(result.report_path)
                    app.set_last_result(result)
                    return self._redirect_home()
                if parsed.path == "/import-trades":
                    upload_path = ""
                    cleanup_path = None
                    file_item = uploads.get("capital_xls_file")
                    if file_item is not None and getattr(file_item, "filename", ""):
                        suffix = Path(str(file_item.filename)).suffix or ".xls"
                        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="gui_import_") as handle:
                            shutil.copyfileobj(file_item.file, handle)
                            cleanup_path = Path(handle.name)
                            upload_path = str(cleanup_path)
                    if not upload_path:
                        upload_path = fields.get("capital_xls_path", "")
                    try:
                        result = app.services.run_import_trades(
                            upload_path,
                            trades_import_mode=fields.get("trades_import_mode", "replace"),
                            trade_date_from=fields.get("trade_date_from", ""),
                            trade_date_to=fields.get("trade_date_to", ""),
                            selected_report_path=app.snapshot().selected_report_path,
                            allow_incomplete_csv_rows=bool(fields.get("allow_incomplete_csv_rows")),
                        )
                    finally:
                        if cleanup_path is not None:
                            try:
                                cleanup_path.unlink()
                            except OSError:
                                pass
                    if result.success and result.report_path:
                        app.set_selected_report(result.report_path)
                    app.set_last_result(result)
                    return self._redirect_home()
                if parsed.path == "/cash-adjust":
                    result = app.services.run_cash_adjustment(
                        fields.get("cash_adjust_usd", ""),
                        cash_adjust_note=fields.get("cash_adjust_note", ""),
                        selected_report_path=app.snapshot().selected_report_path,
                    )
                    if result.success and result.report_path:
                        app.set_selected_report(result.report_path)
                    app.set_last_result(result)
                    return self._redirect_home()
                if parsed.path == "/save-runtime-config":
                    result = app.services.save_runtime_config(
                        fields,
                        selected_report_path=app.snapshot().selected_report_path,
                    )
                    if result.success and result.report_path:
                        app.set_selected_report(result.report_path)
                    app.set_last_result(result)
                    return self._redirect_home()
                if parsed.path == "/save-signal-config":
                    snapshot = app.services.load_signal_config()
                    selected_windows = {}
                    for ticker in snapshot.candidate_tickers:
                        if fields.get(f"ticker_enabled_{ticker}"):
                            selected_windows[ticker] = int(fields.get(f"ticker_window_{ticker}", "50") or 50)
                    custom_window = int(fields.get("custom_window", "50") or 50)
                    for ticker in self._parse_custom_tickers(fields.get("custom_tickers", "")):
                        selected_windows[ticker] = custom_window
                    result = app.services.save_signal_config(
                        selected_windows,
                        selected_report_path=app.snapshot().selected_report_path,
                    )
                    if result.success and result.report_path:
                        app.set_selected_report(result.report_path)
                    app.set_last_result(result)
                    return self._redirect_home()
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
            except Exception as exc:
                app.set_last_result(
                    OperationResult(
                        name="GUI error",
                        success=False,
                        returncode=1,
                        command="internal",
                        stdout=str(exc),
                        message=str(exc),
                    )
                )
                return self._redirect_home()

        def log_message(self, format: str, *args) -> None:
            return

        def _send_html(self, body: str) -> None:
            payload = body.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _send_text(self, body: str) -> None:
            payload = body.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _redirect_home(self) -> None:
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/")
            self.end_headers()

        def _send_server_control_page(self, server_action: str) -> None:
            if server_action == "restart":
                title = "Restarting Server"
                heading = "Restarting server"
                detail = "The GUI server is restarting. This tab will reconnect automatically."
                script = """
<script>
  (() => {
    const status = document.getElementById("status_text");
    const tryReconnect = () => {
      fetch("/healthz", { cache: "no-store" })
        .then((response) => {
          if (response.ok) {
            window.location.href = "/";
          }
        })
        .catch(() => {});
    };
    window.setInterval(tryReconnect, 700);
    window.setTimeout(tryReconnect, 900);
    if (status) {
      status.textContent = "Waiting for server to come back...";
    }
  })();
</script>
"""
            else:
                title = "Server Stopped"
                heading = "Server stopped"
                detail = "The local GUI process has been stopped. You can close this tab or start the server again from the terminal."
                script = ""
            body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #f4f0e8;
      --panel: #fffdf8;
      --ink: #1e1d1a;
      --muted: #6b655d;
      --line: #d9d0c2;
      --accent: #1d5c63;
    }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
      background:
        radial-gradient(circle at top right, rgba(208, 140, 96, 0.16), transparent 26%),
        radial-gradient(circle at top left, rgba(29, 92, 99, 0.14), transparent 28%),
        var(--bg);
      color: var(--ink);
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
    }}
    .card {{
      width: min(540px, 100%);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 24px;
      box-shadow: 0 18px 48px rgba(30, 29, 26, 0.16);
      display: grid;
      gap: 10px;
    }}
    h1 {{
      margin: 0;
      font-size: 1.4rem;
    }}
    .subtle {{
      color: var(--muted);
      line-height: 1.6;
    }}
    a {{
      color: var(--accent);
    }}
  </style>
</head>
<body>
  <section class="card">
    <h1>{html.escape(heading)}</h1>
    <div class="subtle">{html.escape(detail)}</div>
    <div id="status_text" class="subtle">{'Preparing reconnect…' if server_action == 'restart' else 'The current session has ended.'}</div>
    <div><a href="/">Return to GUI</a></div>
  </section>
  {script}
</body>
</html>"""
            self._send_html(body)

        def _schedule_server_shutdown(self) -> None:
            def _shutdown() -> None:
                time.sleep(0.15)
                try:
                    self.server.shutdown()
                except Exception:
                    pass

            threading.Thread(target=_shutdown, daemon=True).start()

        def _parse_form_data(self) -> Tuple[Dict[str, str], Dict[str, cgi.FieldStorage]]:
            content_type = self.headers.get("Content-Type", "")
            mime_type, _ = cgi.parse_header(content_type)
            if mime_type == "multipart/form-data":
                form = cgi.FieldStorage(
                    fp=self.rfile,
                    headers=self.headers,
                    environ={
                        "REQUEST_METHOD": "POST",
                        "CONTENT_TYPE": content_type,
                    },
                    keep_blank_values=True,
                )
                fields: Dict[str, str] = {}
                uploads: Dict[str, cgi.FieldStorage] = {}
                if form.list:
                    for item in form.list:
                        if item.filename:
                            uploads[item.name] = item
                        else:
                            fields[item.name] = item.value
                return fields, uploads
            length = int(self.headers.get("Content-Length", "0") or 0)
            raw_body = self.rfile.read(length).decode("utf-8")
            parsed = parse_qs(raw_body, keep_blank_values=True)
            return {key: values[-1] for key, values in parsed.items()}, {}

        @staticmethod
        def _parse_custom_tickers(raw_value: str) -> list[str]:
            out = []
            seen = set()
            for part in str(raw_value or "").split(","):
                ticker = str(part or "").upper().strip()
                if ticker and ticker not in seen:
                    seen.add(ticker)
                    out.append(ticker)
            return out

    return GuiHandler


def run_server(repo_root: Path, host: str, port: int, *, open_browser: bool = False) -> str:
    app = GuiApplication(repo_root)
    handler = make_handler(app)
    with GuiHTTPServer((host, port), handler) as server:
        url = f"http://{host}:{port}/"
        print(f"[GUI] serving {url}")
        if open_browser:
            try:
                webbrowser.open(url)
            except Exception:
                pass
        server.serve_forever()
        return str(getattr(server, "control_action", "shutdown") or "shutdown")
