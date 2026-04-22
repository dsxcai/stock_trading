/*
 * Copyright (c) 2026 Sheng-Hsin Tsai
 * SPDX-License-Identifier: MIT
 */

import { useEffect, useState, type FormEvent } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { GuiApiClient, loadDesktopConfig } from "@/api/client";
import type {
  ApiStateResponse,
  DashboardState,
  DesktopShellConfig,
  OperationResult,
  RuntimeConfigSnapshot,
  SignalConfigSnapshot,
} from "@/types";

type TabKey = "report" | "status" | "config";
type ViewMode = "rendered" | "raw";

type ReportFormState = {
  date_mode: "latest" | "selected";
  allow_incomplete_csv_rows: boolean;
  force_mode: boolean;
  mode: string;
  report_date: string;
};

type ImportFormState = {
  capital_xls_path: string;
  trade_date_from: string;
  trade_date_to: string;
  trades_import_mode: "append" | "replace";
};

type CashAdjustmentFormState = {
  cash_adjust_note: string;
  cash_adjust_usd: string;
};

type RuntimeConfigFormState = Record<string, string>;

type SignalRowState = {
  enabled: boolean;
  window: number;
};

type ProgressProfile = {
  estimateMs: number;
  match: (message: string) => boolean;
  phases: Array<{ pct: number; text: string }>;
};

const PRECISION_FIELDS = [
  { key: "usd_amount", label: "USD Amount" },
  { key: "display_price", label: "Display Price" },
  { key: "display_pct", label: "Display Percent" },
  { key: "trade_cash_amount", label: "Trade Cash Amount" },
  { key: "trade_dedupe_amount", label: "Trade Dedupe Amount" },
  { key: "state_selected_fields", label: "State Selected Fields" },
  { key: "backtest_amount", label: "Backtest Amount" },
  { key: "backtest_price", label: "Backtest Price" },
  { key: "backtest_rate", label: "Backtest Rate" },
  { key: "backtest_cost_param", label: "Backtest Cost Param" },
];

const DEFAULT_PROGRESS_PROFILE: ProgressProfile = {
  estimateMs: 9000,
  match: () => true,
  phases: [
    { pct: 6, text: "Preparing request..." },
    { pct: 28, text: "Invoking Python backend..." },
    { pct: 56, text: "Processing runtime state..." },
    { pct: 82, text: "Refreshing desktop state..." },
  ],
};

const PROGRESS_PROFILES: ProgressProfile[] = [
  {
    estimateMs: 12000,
    match: (message) => message.toLowerCase().includes("running") && message.toLowerCase().includes("workflow"),
    phases: [
      { pct: 8, text: "Checking mode and inputs..." },
      { pct: 26, text: "Refreshing CSV and state data..." },
      { pct: 58, text: "Rendering report output..." },
      { pct: 84, text: "Reloading GUI state..." },
    ],
  },
  {
    estimateMs: 6500,
    match: (message) => message.toLowerCase().includes("generating") && message.toLowerCase().includes("report"),
    phases: [
      { pct: 10, text: "Preparing report context..." },
      { pct: 42, text: "Rendering markdown..." },
      { pct: 80, text: "Reloading selected report..." },
    ],
  },
  {
    estimateMs: 8000,
    match: (message) => message.toLowerCase().includes("importing trades"),
    phases: [
      { pct: 10, text: "Reading Capital XLS..." },
      { pct: 38, text: "Merging trades into ledger..." },
      { pct: 70, text: "Refreshing selected report..." },
    ],
  },
  {
    estimateMs: 4500,
    match: (message) => message.toLowerCase().includes("saving") || message.toLowerCase().includes("applying cash"),
    phases: [
      { pct: 12, text: "Validating form data..." },
      { pct: 48, text: "Writing configuration..." },
      { pct: 82, text: "Refreshing desktop state..." },
    ],
  },
  {
    estimateMs: 3500,
    match: (message) => message.toLowerCase().includes("deleting"),
    phases: [
      { pct: 12, text: "Resolving selected artifacts..." },
      { pct: 52, text: "Deleting report files..." },
      { pct: 84, text: "Refreshing report list..." },
    ],
  },
];

function snapshotToRuntimeForm(snapshot: RuntimeConfigSnapshot): RuntimeConfigFormState {
  return {
    doc: snapshot.doc,
    trades_file: snapshot.trades_file,
    cash_events_file: snapshot.cash_events_file,
    buy_fee_rate: String(snapshot.buy_fee_rate),
    sell_fee_rate: String(snapshot.sell_fee_rate),
    core_tickers: snapshot.core_tickers_text,
    tactical_tickers: snapshot.tactical_tickers_text,
    tactical_cash_pool_ticker: snapshot.tactical_cash_pool_ticker,
    tactical_cash_pool_tickers: snapshot.tactical_cash_pool_tickers_text,
    fx_pairs: snapshot.fx_pairs_text,
    csv_sources: snapshot.csv_sources_text,
    closed_days: snapshot.closed_days_text,
    early_close_days: snapshot.early_closes_text,
    keep_prev_trade_days_simplified: String(snapshot.keep_prev_trade_days_simplified),
    ...Object.fromEntries(
      PRECISION_FIELDS.map((field) => [
        field.key,
        String(snapshot.numeric_precision[field.key] ?? 0),
      ]),
    ),
  };
}

function buildSignalRows(snapshot: SignalConfigSnapshot): Record<string, SignalRowState> {
  const rows: Record<string, SignalRowState> = {};
  for (const ticker of snapshot.candidate_tickers) {
    rows[ticker] = {
      enabled: Object.prototype.hasOwnProperty.call(snapshot.selected_windows, ticker),
      window: snapshot.selected_windows[ticker] ?? 50,
    };
  }
  return rows;
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }
  return "The desktop UI could not complete the request.";
}

function parseCustomTickers(rawValue: string): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const fragment of rawValue.split(/[,\s]+/)) {
    const ticker = fragment.trim().toUpperCase();
    if (!ticker || seen.has(ticker)) {
      continue;
    }
    seen.add(ticker);
    out.push(ticker);
  }
  return out;
}

function renderLogText(result: OperationResult | null, errorLogText: string): string {
  if (errorLogText.trim()) {
    return errorLogText.trim();
  }
  if (result?.stdout.trim()) {
    return result.stdout.trim();
  }
  return "No log output captured for the latest operation.";
}

export default function App() {
  const [desktopConfig, setDesktopConfig] = useState<DesktopShellConfig | null>(null);
  const [apiClient, setApiClient] = useState<GuiApiClient | null>(null);
  const [dashboard, setDashboard] = useState<DashboardState | null>(null);
  const [activeTab, setActiveTab] = useState<TabKey>("report");
  const [viewMode, setViewMode] = useState<ViewMode>("rendered");
  const [busyMessage, setBusyMessage] = useState("");
  const [errorMessage, setErrorMessage] = useState("");
  const [reportForm, setReportForm] = useState<ReportFormState>({
    date_mode: "latest",
    allow_incomplete_csv_rows: false,
    force_mode: false,
    mode: "premarket",
    report_date: "",
  });
  const [importForm, setImportForm] = useState<ImportFormState>({
    capital_xls_path: "",
    trade_date_from: "",
    trade_date_to: "",
    trades_import_mode: "replace",
  });
  const [cashAdjustmentForm, setCashAdjustmentForm] = useState<CashAdjustmentFormState>({
    cash_adjust_note: "",
    cash_adjust_usd: "",
  });
  const [runtimeForm, setRuntimeForm] = useState<RuntimeConfigFormState>({});
  const [signalRows, setSignalRows] = useState<Record<string, SignalRowState>>({});
  const [customSignalTickers, setCustomSignalTickers] = useState("");
  const [customSignalWindow, setCustomSignalWindow] = useState("50");
  const [selectedReports, setSelectedReports] = useState<Set<string>>(new Set());
  const [progress, setProgress] = useState(0);
  const [progressPhase, setProgressPhase] = useState("");

  useEffect(() => {
    let cancelled = false;

    async function bootstrap() {
      try {
        const config = await loadDesktopConfig();
        if (cancelled) {
          return;
        }
        const client = new GuiApiClient();
        setDesktopConfig(config);
        setApiClient(client);
        const response = await client.getState();
        if (cancelled) {
          return;
        }
        setDashboard(response.state);
        setErrorMessage(response.error ?? "");
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(getErrorMessage(error));
        }
      }
    }

    void bootstrap();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!dashboard) {
      return;
    }
    const defaultMode = dashboard.modes[0]?.key ?? "premarket";
    setReportForm((current) => ({ ...current, mode: current.mode || defaultMode }));
    setRuntimeForm(snapshotToRuntimeForm(dashboard.runtime_config));
    setSignalRows(buildSignalRows(dashboard.signal_config));
    setCustomSignalTickers("");
    setCustomSignalWindow("50");
  }, [dashboard]);

  useEffect(() => {
    if (reportForm.date_mode === "selected" && reportForm.mode === "intraday") {
      setReportForm((current) => ({
        ...current,
        mode: "premarket",
      }));
    }
  }, [reportForm.date_mode, reportForm.mode]);

  useEffect(() => {
    if (!busyMessage) {
      setProgress(0);
      setProgressPhase("");
      return;
    }
    const profile = PROGRESS_PROFILES.find((item) => item.match(busyMessage)) ?? DEFAULT_PROGRESS_PROFILE;
    const startedAt = Date.now();

    const tick = () => {
      const elapsed = Date.now() - startedAt;
      const ratio = Math.min(elapsed / profile.estimateMs, 1);
      const eased = 1 - Math.pow(1 - ratio, 3);
      const nextProgress = Math.min(95, Math.max(4, Math.round(4 + eased * 91)));
      setProgress(nextProgress);

      let nextPhase = "Processing request...";
      for (const phase of profile.phases) {
        if (nextProgress >= phase.pct) {
          nextPhase = phase.text;
        }
      }
      setProgressPhase(nextPhase);
    };

    tick();
    const timer = window.setInterval(tick, 180);
    return () => window.clearInterval(timer);
  }, [busyMessage]);

  useEffect(() => {
    const reportPaths = new Set(dashboard?.recent_reports.map((report) => report.path) ?? []);
    setSelectedReports((current) => {
      const next = new Set<string>();
      for (const path of current) {
        if (reportPaths.has(path)) {
          next.add(path);
        }
      }
      return next.size === current.size ? current : next;
    });
  }, [dashboard]);

  async function applyResponse(response: ApiStateResponse) {
    setDashboard(response.state);
    setErrorMessage(response.error ?? "");
  }

  async function runAction(
    message: string,
    action: () => Promise<ApiStateResponse>,
    nextTab?: TabKey,
  ) {
    if (!apiClient) {
      setErrorMessage("The desktop API client is not ready yet.");
      return;
    }

    setBusyMessage(message);
    setErrorMessage("");
    try {
      const response = await action();
      await applyResponse(response);
      if (response.state.last_result && !response.state.last_result.success) {
        setActiveTab("status");
      } else if (nextTab) {
        setActiveTab(nextTab);
      }
    } catch (error) {
      setErrorMessage(getErrorMessage(error));
    } finally {
      setBusyMessage("");
    }
  }

  async function handleBrowseCapitalXls() {
    if (!window.desktopApi) {
      return;
    }
    const selectedPath = await window.desktopApi.pickCapitalXls();
    if (!selectedPath) {
      return;
    }
    setImportForm((current) => ({
      ...current,
      capital_xls_path: selectedPath,
    }));
  }

  async function handleSelectReport(reportPath: string) {
    await runAction("Loading the selected report...", () =>
      apiClient!.invokeAction("select-report", { report_path: reportPath }),
    );
  }

  function toggleReportSelection(reportPath: string, checked: boolean) {
    setSelectedReports((current) => {
      const next = new Set(current);
      if (checked) {
        next.add(reportPath);
      } else {
        next.delete(reportPath);
      }
      return next;
    });
  }

  function handleToggleAllReports() {
    const reports = dashboard?.recent_reports ?? [];
    if (reports.length === 0) {
      return;
    }
    setSelectedReports((current) => {
      if (current.size === reports.length) {
        return new Set();
      }
      return new Set(reports.map((report) => report.path));
    });
  }

  async function handleDeleteSelectedReports() {
    const reportPaths = Array.from(selectedReports);
    if (reportPaths.length === 0) {
      window.alert("No reports selected.");
      return;
    }
    if (!window.confirm(`Delete ${reportPaths.length} selected report(s) and matching JSON artifacts?`)) {
      return;
    }

    setBusyMessage("Deleting selected reports...");
    setErrorMessage("");
    try {
      let lastResponse: ApiStateResponse | null = null;
      for (const reportPath of reportPaths) {
        lastResponse = await apiClient!.invokeAction("delete-report", { report_path: reportPath });
      }
      setSelectedReports(new Set());
      if (lastResponse) {
        await applyResponse(lastResponse);
        setActiveTab("report");
      }
    } catch (error) {
      setErrorMessage(getErrorMessage(error));
    } finally {
      setBusyMessage("");
    }
  }

  async function handleGenerateReport(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (reportForm.date_mode === "selected" && !reportForm.report_date.trim()) {
      setErrorMessage("Choose a historical trading day before generating a historical report.");
      return;
    }
    const actionLabel = reportForm.date_mode === "selected" ? "Generating historical report..." : "Generating latest report...";
    await runAction(
      actionLabel,
      () => apiClient!.invokeAction("generate-report", reportForm),
      "report",
    );
  }

  async function handleImportTrades(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await runAction(
      "Importing trades and refreshing the selected report...",
      () => apiClient!.invokeAction("import-trades", importForm),
      "report",
    );
  }

  async function handleCashAdjustment(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await runAction(
      "Applying cash adjustment...",
      () => apiClient!.invokeAction("cash-adjust", cashAdjustmentForm),
      "report",
    );
  }

  async function handleSaveRuntimeConfig(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await runAction(
      "Saving runtime config and refreshing the report...",
      () =>
        apiClient!.invokeAction("save-runtime-config", {
          config_fields: runtimeForm,
        }),
      "config",
    );
  }

  async function handleSaveSignalConfig(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const selectedWindows: Record<string, number> = {};
    for (const [ticker, row] of Object.entries(signalRows)) {
      if (!row.enabled) {
        continue;
      }
      selectedWindows[ticker] = row.window;
    }
    for (const ticker of parseCustomTickers(customSignalTickers)) {
      selectedWindows[ticker] = Number(customSignalWindow) || 50;
    }
    await runAction(
      "Saving signal config and refreshing the report...",
      () =>
        apiClient!.invokeAction("save-signal-config", {
          selected_windows: selectedWindows,
        }),
      "config",
    );
  }

  async function handleReloadApp() {
    if (!window.desktopApi) {
      setErrorMessage("The Electron desktop bridge is unavailable in this renderer.");
      return;
    }
    setBusyMessage("Reloading the desktop application...");
    await window.desktopApi.reloadApplication();
  }

  async function handleCloseApp() {
    if (!window.desktopApi) {
      setErrorMessage("The Electron desktop bridge is unavailable in this renderer.");
      return;
    }
    setBusyMessage("Closing the desktop application...");
    await window.desktopApi.closeApplication();
  }

  function updateRuntimeField(field: string, value: string) {
    setRuntimeForm((current) => ({
      ...current,
      [field]: value,
    }));
  }

  function updateSignalRow(ticker: string, patch: Partial<SignalRowState>) {
    setSignalRows((current) => ({
      ...current,
      [ticker]: {
        ...current[ticker],
        ...patch,
      },
    }));
  }

  if (!dashboard) {
    return (
      <div className="loading-screen">
        <div className="loading-card">
          <div className="eyebrow">Stock Trading Desktop</div>
          <h1>Starting desktop workspace</h1>
          <p>Connecting the React shell to the local Python desktop bridge.</p>
          {errorMessage ? <div className="error-banner">{errorMessage}</div> : null}
        </div>
      </div>
    );
  }

  const currentResult = dashboard.last_result;
  const allReportsSelected =
    dashboard.recent_reports.length > 0 && selectedReports.size === dashboard.recent_reports.length;
  const availableReportModes =
    reportForm.date_mode === "selected"
      ? dashboard.modes.filter((mode) => mode.key !== "intraday")
      : dashboard.modes;
  const renderReport =
    viewMode === "raw" ? (
      <pre className="report-raw">{dashboard.report.text || "No report selected."}</pre>
    ) : (
      <article className="report-rendered markdown-shell">
        <ReactMarkdown remarkPlugins={[remarkGfm]}>
          {dashboard.report.text || "No report selected."}
        </ReactMarkdown>
      </article>
    );

  return (
    <>
      <div className="app-shell">
        <aside className="control-rail">
          <section className="hero-card">
            <div className="eyebrow">Electron + React + TypeScript</div>
            <h1>Stock Trading Desktop</h1>
            <p>
              Python keeps the trading logic and file workflow. React talks to Electron IPC, and Electron
              invokes Python directly without a local web server.
            </p>
            <div className="hero-meta">
              <span>{window.desktopApi ? "Electron shell" : "Renderer only"}</span>
              <span>{desktopConfig?.transport === "ipc" ? "Direct IPC bridge" : "Desktop bridge pending"}</span>
            </div>
            <div className="hero-actions">
              <button type="button" className="secondary-button" onClick={handleReloadApp} disabled={!!busyMessage}>
                Reload
              </button>
              <button type="button" className="danger-button" onClick={handleCloseApp} disabled={!!busyMessage}>
                Close
              </button>
            </div>
          </section>

          <section className="rail-panel">
            <div className="section-head">
              <h2>Generate Report</h2>
              <p>Generate the latest session report or a report for a specified historical trading day.</p>
            </div>
            <form className="stack" onSubmit={handleGenerateReport}>
              <div>
                <div>Report Basis</div>
                <div className="basis-options">
                  <label className="basis-option">
                    <input
                      type="radio"
                      name="report-basis"
                      value="latest"
                      checked={reportForm.date_mode === "latest"}
                      onChange={(event) =>
                        setReportForm((current) => ({
                          ...current,
                          date_mode: event.target.value as "latest" | "selected",
                          force_mode: current.force_mode,
                          report_date: "",
                        }))
                      }
                    />
                    <span>Latest trading session</span>
                  </label>
                  <label className="basis-option">
                    <input
                      type="radio"
                      name="report-basis"
                      value="selected"
                      checked={reportForm.date_mode === "selected"}
                      onChange={(event) =>
                        setReportForm((current) => ({
                          ...current,
                          date_mode: event.target.value as "latest" | "selected",
                          force_mode: false,
                          report_date: current.report_date,
                        }))
                      }
                    />
                    <span>Specified historical trading day</span>
                  </label>
                </div>
              </div>
              {reportForm.date_mode === "selected" ? (
                <label>
                  Trading Day
                  <input
                    type="date"
                    value={reportForm.report_date}
                    onChange={(event) =>
                      setReportForm((current) => ({ ...current, report_date: event.target.value }))
                    }
                  />
                </label>
              ) : null}
              <label>
                Mode
                <select
                  value={reportForm.mode}
                  onChange={(event) =>
                    setReportForm((current) => ({ ...current, mode: event.target.value }))
                  }
                >
                  {availableReportModes.map((mode) => (
                    <option key={mode.key} value={mode.key}>
                      {mode.label}
                    </option>
                  ))}
                </select>
              </label>
              {reportForm.date_mode === "latest" ? (
                <label className="checkbox-row">
                  <input
                    type="checkbox"
                    checked={reportForm.force_mode}
                    onChange={(event) =>
                      setReportForm((current) => ({ ...current, force_mode: event.target.checked }))
                    }
                  />
                  <span>Allow force mode</span>
                </label>
              ) : null}
              <label className="checkbox-row">
                <input
                  type="checkbox"
                  checked={reportForm.allow_incomplete_csv_rows}
                  onChange={(event) =>
                    setReportForm((current) => ({
                      ...current,
                      allow_incomplete_csv_rows: event.target.checked,
                    }))
                  }
                />
                <span>Allow incomplete CSV rows</span>
              </label>
              <button type="submit" disabled={!!busyMessage}>
                Generate Report
              </button>
            </form>
          </section>

          <section className="rail-panel">
            <div className="section-head">
              <h2>Import Trades</h2>
              <p>Import Capital XLS history, then regenerate the currently selected report.</p>
            </div>
            <form className="stack" onSubmit={handleImportTrades}>
              <label className="field-with-action">
                <span>Capital XLS Path</span>
                <div className="input-action-row">
                  <input
                    type="text"
                    value={importForm.capital_xls_path}
                    placeholder="/path/to/OSHistoryDealAll.xls"
                    onChange={(event) =>
                      setImportForm((current) => ({
                        ...current,
                        capital_xls_path: event.target.value,
                      }))
                    }
                  />
                  <button type="button" className="secondary-button" onClick={handleBrowseCapitalXls} disabled={!!busyMessage}>
                    Browse
                  </button>
                </div>
              </label>
              <label>
                Import Mode
                <select
                  value={importForm.trades_import_mode}
                  onChange={(event) =>
                    setImportForm((current) => ({
                      ...current,
                      trades_import_mode: event.target.value as "append" | "replace",
                    }))
                  }
                >
                  <option value="replace">replace</option>
                  <option value="append">append</option>
                </select>
              </label>
              <div className="two-up">
                <label>
                  Trade Date From (ET)
                  <input
                    type="date"
                    value={importForm.trade_date_from}
                    onChange={(event) =>
                      setImportForm((current) => ({
                        ...current,
                        trade_date_from: event.target.value,
                      }))
                    }
                  />
                </label>
                <label>
                  Trade Date To (ET)
                  <input
                    type="date"
                    value={importForm.trade_date_to}
                    onChange={(event) =>
                      setImportForm((current) => ({
                        ...current,
                        trade_date_to: event.target.value,
                      }))
                    }
                  />
                </label>
              </div>
              <button type="submit" disabled={!!busyMessage}>
                Import Trades
              </button>
            </form>
          </section>

          <section className="rail-panel">
            <div className="section-head">
              <h2>Cash Adjustment</h2>
              <p>Write signed USD cash events and refresh the selected report when possible.</p>
            </div>
            <form className="stack" onSubmit={handleCashAdjustment}>
              <label>
                Amount (USD)
                <input
                  type="number"
                  step="0.01"
                  placeholder="-100 = withdraw, 100 = deposit"
                  value={cashAdjustmentForm.cash_adjust_usd}
                  onChange={(event) =>
                    setCashAdjustmentForm((current) => ({
                      ...current,
                      cash_adjust_usd: event.target.value,
                    }))
                  }
                />
              </label>
              <label>
                Note
                <input
                  type="text"
                  value={cashAdjustmentForm.cash_adjust_note}
                  onChange={(event) =>
                    setCashAdjustmentForm((current) => ({
                      ...current,
                      cash_adjust_note: event.target.value,
                    }))
                  }
                />
              </label>
              <button type="submit" disabled={!!busyMessage}>
                Apply Cash Adjustment
              </button>
            </form>
          </section>

          <section className="rail-panel">
            <div className="section-head">
              <h2>Recent Reports</h2>
              <p>Switch the main viewer or clean up old generated artifacts.</p>
            </div>
            <div className="report-list">
              {dashboard.recent_reports.length === 0 ? (
                <div className="empty-list">No standard reports found under `report/`.</div>
              ) : (
                dashboard.recent_reports.map((report) => (
                  <div
                    key={report.path}
                    className={
                      report.path === dashboard.ui.selected_report_path ? "report-item is-active" : "report-item"
                    }
                  >
                    <label className="report-check">
                      <input
                        type="checkbox"
                        checked={selectedReports.has(report.path)}
                        onChange={(event) => toggleReportSelection(report.path, event.target.checked)}
                      />
                    </label>
                    <button type="button" className="report-select" onClick={() => void handleSelectReport(report.path)}>
                      <span className="report-name">{report.name}</span>
                      <span className="report-meta">
                        {report.mode_label} · {report.modified_at}
                      </span>
                    </button>
                  </div>
                ))
              )}
            </div>
            <div className="report-actions">
              <button
                type="button"
                className="secondary-button report-select-toggle"
                onClick={handleToggleAllReports}
                disabled={!!busyMessage || dashboard.recent_reports.length === 0}
              >
                {allReportsSelected ? "Deselect All" : "Select All"}
              </button>
              <button
                type="button"
                className="danger-button"
                onClick={() => void handleDeleteSelectedReports()}
                disabled={!!busyMessage || selectedReports.size === 0}
              >
                Delete All Selects
              </button>
            </div>
          </section>
        </aside>

        <main className="workspace">
          <header className="workspace-head">
            <div>
              <div className="eyebrow">Viewer</div>
              <h2>{dashboard.report.selected?.name ?? "Report Viewer"}</h2>
              <p>{dashboard.report.selected?.path ?? "Select a recent report to inspect generated output."}</p>
            </div>
            <div className="workspace-actions">
              <div className="tab-strip">
                {(["report", "status", "config"] as TabKey[]).map((tab) => (
                  <button
                    key={tab}
                    type="button"
                    className={tab === activeTab ? "tab-button is-active" : "tab-button"}
                    onClick={() => setActiveTab(tab)}
                  >
                    {tab === "report" ? "Report" : tab === "status" ? "Status" : "Config"}
                  </button>
                ))}
              </div>
              {activeTab === "report" ? (
                <div className="tab-strip">
                  <button
                    type="button"
                    className={viewMode === "rendered" ? "tab-button is-active" : "tab-button"}
                    onClick={() => setViewMode("rendered")}
                  >
                    Rendered
                  </button>
                  <button
                    type="button"
                    className={viewMode === "raw" ? "tab-button is-active" : "tab-button"}
                    onClick={() => setViewMode("raw")}
                  >
                    Raw Markdown
                  </button>
                </div>
              ) : null}
            </div>
          </header>

          {errorMessage ? <div className="error-banner">{errorMessage}</div> : null}

          {activeTab === "report" ? (
            <section className="viewer-card">{renderReport}</section>
          ) : null}

          {activeTab === "status" ? (
            <section className="viewer-card status-card">
              <div className="status-summary">
                <span className={currentResult?.success ? "status-pill is-success" : "status-pill is-error"}>
                  {currentResult?.success ? "Success" : "Latest Result"}
                </span>
                <h3>{currentResult?.message ?? "No GUI operation has been run yet."}</h3>
                <p>{currentResult?.name ?? "Run a workflow from the left rail to populate operation details."}</p>
              </div>
              <div className="status-grid">
                <div>
                  <span className="meta-label">Command</span>
                  <code>{currentResult?.command ?? "n/a"}</code>
                </div>
                <div>
                  <span className="meta-label">Exit Code</span>
                  <code>{currentResult?.returncode ?? 0}</code>
                </div>
                <div>
                  <span className="meta-label">Report Path</span>
                  <code>{currentResult?.report_path || "n/a"}</code>
                </div>
                <div>
                  <span className="meta-label">Log Path</span>
                  <code>{currentResult?.log_path || "n/a"}</code>
                </div>
              </div>
              <pre className="status-log">{renderLogText(currentResult, dashboard.report.error_log_text)}</pre>
            </section>
          ) : null}

          {activeTab === "config" ? (
            <section className="config-layout">
              <form className="config-card" onSubmit={handleSaveRuntimeConfig}>
                <div className="section-head">
                  <h3>Runtime Config</h3>
                  <p>These fields write back to `config.json` through the Python backend.</p>
                </div>

                <div className="config-section-grid">
                  <label className="wide-field">
                    Document Title / `meta.doc`
                    <input
                      type="text"
                      value={runtimeForm.doc ?? ""}
                      onChange={(event) => updateRuntimeField("doc", event.target.value)}
                    />
                  </label>
                </div>

                <div className="two-up">
                  <label>
                    Trades File
                    <input
                      type="text"
                      value={runtimeForm.trades_file ?? ""}
                      onChange={(event) => updateRuntimeField("trades_file", event.target.value)}
                    />
                  </label>
                  <label>
                    Cash Events File
                    <input
                      type="text"
                      value={runtimeForm.cash_events_file ?? ""}
                      onChange={(event) => updateRuntimeField("cash_events_file", event.target.value)}
                    />
                  </label>
                </div>

                <div className="two-up">
                  <label>
                    Buy Fee Rate
                    <input
                      type="number"
                      step="0.000001"
                      value={runtimeForm.buy_fee_rate ?? ""}
                      onChange={(event) => updateRuntimeField("buy_fee_rate", event.target.value)}
                    />
                  </label>
                  <label>
                    Sell Fee Rate
                    <input
                      type="number"
                      step="0.000001"
                      value={runtimeForm.sell_fee_rate ?? ""}
                      onChange={(event) => updateRuntimeField("sell_fee_rate", event.target.value)}
                    />
                  </label>
                </div>

                <label>
                  Core Bucket Tickers
                  <textarea
                    value={runtimeForm.core_tickers ?? ""}
                    onChange={(event) => updateRuntimeField("core_tickers", event.target.value)}
                  />
                </label>

                <label>
                  Tactical Bucket Tickers
                  <textarea
                    value={runtimeForm.tactical_tickers ?? ""}
                    onChange={(event) => updateRuntimeField("tactical_tickers", event.target.value)}
                  />
                </label>

                <div className="two-up">
                  <label>
                    Tactical Cash Pool Ticker
                    <input
                      type="text"
                      value={runtimeForm.tactical_cash_pool_ticker ?? ""}
                      onChange={(event) =>
                        updateRuntimeField("tactical_cash_pool_ticker", event.target.value)
                      }
                    />
                  </label>
                  <label>
                    Keep Previous Trade Days
                    <input
                      type="number"
                      min="0"
                      value={runtimeForm.keep_prev_trade_days_simplified ?? ""}
                      onChange={(event) =>
                        updateRuntimeField("keep_prev_trade_days_simplified", event.target.value)
                      }
                    />
                  </label>
                </div>

                <label>
                  Tactical Cash Pool Bucket Tickers
                  <textarea
                    value={runtimeForm.tactical_cash_pool_tickers ?? ""}
                    onChange={(event) =>
                      updateRuntimeField("tactical_cash_pool_tickers", event.target.value)
                    }
                  />
                </label>

                <label>
                  FX Pairs (`alias=ticker`)
                  <textarea
                    value={runtimeForm.fx_pairs ?? ""}
                    onChange={(event) => updateRuntimeField("fx_pairs", event.target.value)}
                  />
                </label>

                <label>
                  CSV Sources (`TICKER=path`)
                  <textarea
                    value={runtimeForm.csv_sources ?? ""}
                    onChange={(event) => updateRuntimeField("csv_sources", event.target.value)}
                  />
                </label>

                <label>
                  Closed Trading Days (`YYYY-MM-DD=Reason`)
                  <textarea
                    value={runtimeForm.closed_days ?? ""}
                    onChange={(event) => updateRuntimeField("closed_days", event.target.value)}
                  />
                </label>

                <label>
                  Early Close Trading Days (`YYYY-MM-DD=HH:MM|Reason`)
                  <textarea
                    value={runtimeForm.early_close_days ?? ""}
                    onChange={(event) => updateRuntimeField("early_close_days", event.target.value)}
                  />
                </label>

                <div className="precision-grid">
                  {PRECISION_FIELDS.map((field) => (
                    <label key={field.key}>
                      {field.label}
                      <input
                        type="number"
                        min="0"
                        value={runtimeForm[field.key] ?? ""}
                        onChange={(event) => updateRuntimeField(field.key, event.target.value)}
                      />
                    </label>
                  ))}
                </div>

                <button type="submit" disabled={!!busyMessage}>
                  Save Runtime Config
                </button>
              </form>

              <form className="config-card" onSubmit={handleSaveSignalConfig}>
                <div className="section-head">
                  <h3>Signal Config</h3>
                  <p>Manage `state_engine.strategy.tactical.indicators` from the desktop UI.</p>
                </div>
                <div className="signal-table">
                  <div className="signal-row signal-header">
                    <span>Ticker</span>
                    <span>Enable</span>
                    <span>SMA</span>
                  </div>
                  {dashboard.signal_config.candidate_tickers.map((ticker) => (
                    <div key={ticker} className="signal-row">
                      <span>{ticker}</span>
                      <label className="signal-check">
                        <input
                          type="checkbox"
                          checked={signalRows[ticker]?.enabled ?? false}
                          onChange={(event) =>
                            updateSignalRow(ticker, { enabled: event.target.checked })
                          }
                        />
                      </label>
                      <select
                        value={String(signalRows[ticker]?.window ?? 50)}
                        onChange={(event) =>
                          updateSignalRow(ticker, { window: Number(event.target.value) })
                        }
                      >
                        <option value="50">SMA50</option>
                        <option value="100">SMA100</option>
                      </select>
                    </div>
                  ))}
                </div>

                <label>
                  Custom Tickers
                  <input
                    type="text"
                    placeholder="TSLA, QQQ, NFLX"
                    value={customSignalTickers}
                    onChange={(event) => setCustomSignalTickers(event.target.value)}
                  />
                </label>
                <label>
                  Custom SMA
                  <select value={customSignalWindow} onChange={(event) => setCustomSignalWindow(event.target.value)}>
                    <option value="50">SMA50</option>
                    <option value="100">SMA100</option>
                  </select>
                </label>

                <button type="submit" disabled={!!busyMessage}>
                  Save Signal Config
                </button>
              </form>
            </section>
          ) : null}
        </main>
      </div>

      {busyMessage ? (
        <div className="busy-overlay">
          <div className="busy-card">
            <div className="busy-spinner" />
            <h3>{busyMessage}</h3>
            <div className="busy-progress-shell">
              <div className="busy-progress-bar" style={{ width: `${progress}%` }} />
            </div>
            <div className="busy-progress-value">{progress}%</div>
            <p>{progressPhase || "Python is applying the request and refreshing desktop state."}</p>
          </div>
        </div>
      ) : null}
    </>
  );
}
