/*
 * Copyright (c) 2026 Sheng-Hsin Tsai
 * SPDX-License-Identifier: MIT
 */

export type ReportInfo = {
  path: string;
  name: string;
  report_date: string;
  mode_key: string;
  mode_label: string;
  modified_at: string;
};

export type OperationResult = {
  name: string;
  success: boolean;
  returncode: number;
  command: string;
  stdout: string;
  message: string;
  log_path: string;
  report_path: string;
  report_json_path: string;
};

export type RuntimeConfigSnapshot = {
  doc: string;
  trades_file: string;
  cash_events_file: string;
  buy_fee_rate: number;
  sell_fee_rate: number;
  core_tickers_text: string;
  tactical_tickers_text: string;
  tactical_cash_pool_ticker: string;
  tactical_cash_pool_tickers_text: string;
  fx_pairs_text: string;
  csv_sources_text: string;
  closed_days_text: string;
  early_closes_text: string;
  numeric_precision: Record<string, number>;
  keep_prev_trade_days_simplified: number;
};

export type SignalConfigSnapshot = {
  selected_windows: Record<string, number>;
  candidate_tickers: string[];
};

export type ModeInfo = {
  key: string;
  label: string;
};

export type DashboardState = {
  ui: {
    selected_report_path: string;
  };
  report: {
    selected: ReportInfo | null;
    text: string;
    error_log_text: string;
  };
  recent_reports: ReportInfo[];
  runtime_config: RuntimeConfigSnapshot;
  signal_config: SignalConfigSnapshot;
  last_result: OperationResult | null;
  modes: ModeInfo[];
};

export type ApiStateResponse = {
  ok: boolean;
  state: DashboardState;
  error?: string;
};

export type DesktopShellConfig = {
  isElectron: boolean;
  transport: "ipc";
};
