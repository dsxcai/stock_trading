# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by Keep a Changelog, and this project uses semantic versioning.

## [Unreleased] - 2026-04-22

### Added
- `download_1y.py --zip`: 下載完成後自動把 `--output-dir` 內所有檔案壓縮成 `{stamp_date}.zip`，stamp date 優先從 `GOOG.csv` 最後一行日期取得，否則 fallback 為 `--end` 日期。取代 `get_rec.sh` 的 zip 步驟。
- `download_1y.py --days-back N`: 將起始日設為 N 天前（優先於 `--start`）。`get_rec.sh` 固定傳入 1200，現在可直接透過此參數傳遞。

### Removed
- `get_rec.sh` 的功能已完全整合進 `download_1y.py`，腳本本身保留但不再需要依賴。等效替代指令：
  ```
  python3 download_1y.py --days-back 1200 --end $(date +%Y-%m-%d) --zip
  ```

### Documentation
- `README.md` 新增 Section 18「Utility scripts」，說明 `download_1y.py` 的參數、使用時機，以及取代 `get_rec.sh` 的等效指令。
- `README.md` Section 8.5 加入 cite，連結至 Section 18.1。

## [1.3] - 2026-04-14

### Breaking changes
- `python3 gui_app.py` now launches the Electron desktop app directly. The old browser-first dashboard flow and the local GUI HTTP server path have been removed.
- Split cash flow history from `states.json` into a new dedicated `cash_events.json` ledger. Legacy cash history is automatically migrated.

### Added
- `desktop/` workspace built with `React + TypeScript + Electron`.
- JSON/stdin Python bridge via `gui_ipc.py` and `gui/desktop_backend.py`, so desktop actions can call `GuiServices` without HTTP transport.
- Desktop viewer tabs for `Report`, `Status`, and structured `Config`, plus app-level `Reload` and `Close` controls.
- Unified report generation flow in the GUI to handle both latest sessions and specific historical dates.
- Trade-date range filter (`Trade Date From` / `Trade Date To`) for Capital XLS imports.
- GUI controls to multi-select and delete generated report artifacts.
- GUI cash-adjustment controls for recording external deposits and withdrawals through `update_states.py --cash-adjust-usd`, with optional notes and selected-report refresh.
- GUI window geometry persistence and state restore via `config.json`.
- GUI contributor-friendly frontend workflow through `python3 gui_app.py --dev`, which runs renderer and Electron watch builds before launching the shell.

### Changed
- GUI transport switched from server-rendered pages to Electron IPC-backed process execution.
- Desktop actions such as mode runs, ad hoc report generation, Capital XLS imports, report cleanup, runtime config edits, and signal config edits now flow through the desktop bridge while reusing the existing Python trading logic.
- Split execution fees into distinct buy and sell fee rates for finer configuration.
- Stabilized GUI busy state and UX for long-running actions.
- Included the tactical cash pool ticker in the signal status report.
- Sorted Signal Status table by `B-A` (SMA - Price) and refined threshold display formats.

### Fixed
- Fixed report regressions, including historical FX look-ahead bias and threshold displays.
- Migrated legacy cash history properly before applying performance updates.
- Refined trade detail cash flow to ensure zero-budget buy actions render correctly.

### Removed
- Browser-based GUI entrypoint via `python3 gui_app.py --open-browser`.
- Local GUI HTTP server and session-handling code from the active desktop path.

## [1.2] - 2026-04-03

### Breaking changes
- None.

### Added
- Local GUI dashboard entrypoint via `python3 gui_app.py --open-browser`.
- Recent report picker with rendered Markdown and raw Markdown views.
- GUI controls for daily mode runs, report generation, Capital XLS import, tactical SMA config editing, and local server restart/stop.
- Status-panel log rendering with inline error highlighting for failed GUI operations.
- Report header timestamp output: `Generated At (ET)`.
- Tactical signal coverage expansion with additional tracked stocks.
- GUI-focused test coverage for report selection, raw view rendering, importer wiring, and server controls.

### Changed
- Report pricing labels now use `Price (Now)` consistently, including the signal table alias and current-position note wording.
- Mode-based market refresh now updates active CSVs through the current ET date, with same-day handling for FX and intraday equities.
- CSV refresh fails on incomplete OHLC rows by default; `--allow-incomplete-csv-rows` can be used to bypass incomplete rows intentionally.
- Added `--force-mode` override handling for explicit mode execution.
- Split backtest configuration from live runtime configuration and refactored the config schema around that separation.
- Repository history no longer tracks `states.json` and `trades.json`; both files are now local-only runtime artifacts ignored by git.

### Fixed
- Imported portfolio rebuild behavior and totals rounding consistency.
- Current-position notes now derive from surviving FIFO lots instead of stale persisted notes.
- Market snapshot rebuild now follows active tickers more reliably for reporting and refresh flows.
- GUI import execution now uses the module entrypoint correctly, avoiding local import-path failures.
- Raw Markdown report view in the GUI now renders with readable text styling.

## [1.1] - 2026-03-20

### Added
- External trades storage via `trades.json` with `--trades-file` support in update/report flows.
- Broker import plumbing with generic `--imported-trades-json` support and a Capital XLS extension entrypoint.
- Tactical backtest entrypoints: `backtest.py` and `backtest_all_in_one.sh`.
- Markdown backtest reports with gross/net results and start-of-period buy-and-hold comparison.
- Reusable tactical/report modules: `core/tactical_engine.py` and `core/report_bundle.py`.
- Fixture refresh helper script: `refresh_test_fixtures.sh`.
- Backtest coverage for `t+1` execution, date-range validation, and markdown output generation.
- Signal, reporting-safety, import, and report-bundle regression tests.

### Changed
- Split report rendering from persisted state updates; report roots and tactical plan tables are now built transiently at render time.
- Kept `states.json` lean by externalizing runtime config to `config.json`, loading market history from CSV each run, and pruning non-deterministic mode snapshot payloads.
- Simplified tactical rules by removing sell protection and holding-day fields; `BUY` / `BUY_MORE` sizing now uses `Close(t) * (1 + fee_rate)`.
- Added tactical backtest controls for `--starting-cash`, `--lookback-trading-days`, and explicit `--start-date` / `--end-date`.
- Simplified trade import/update flows by removing old reconcile-style branches, externalizing the trade ledger, and routing broker-specific parsing through importer extensions.
- Rounded selected persisted numeric output fields consistently with the 4-decimal policy used by the state engine.

### Fixed
- Removed stale `missing root key: config` warnings during report completeness checks.
- Stopped persisting confusing `report_context` / `broker_context` mode data that could drift from the actual render session.
- Backtest date selection now fails fast when requested dates exceed common trading days or required warm-up history.
- Report rendering no longer mutates state rows via `row_computed`.
- Deterministic key ordering for records written to `trades.json`.
