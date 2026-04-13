# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by Keep a Changelog, and this project uses semantic versioning.

## [Unreleased]

### Breaking changes
- `python3 gui_app.py` now launches the Electron desktop app directly. The old browser-first dashboard flow and the local GUI HTTP server path have been removed.

### Added
- `desktop/` workspace built with `React + TypeScript + Electron`.
- JSON/stdin Python bridge via `gui_ipc.py` and `gui/desktop_backend.py`, so desktop actions can call `GuiServices` without HTTP transport.
- Desktop launcher checks in `gui_app.py` for `node` and `npm`, automatic `npm install` when `desktop/node_modules` is missing, and automatic build of `desktop/dist/` and `desktop/dist-electron/` before production launch.
- Desktop viewer tabs for `Report`, `Status`, and structured `Config`, plus app-level `Reload` and `Close` controls.

- GUI cash-adjustment controls for recording external deposits and withdrawals through `update_states.py --cash-adjust-usd`, with optional notes and selected-report refresh.
- GUI contributor-friendly frontend workflow through `python3 gui_app.py --dev`, which runs renderer and Electron watch builds before launching the shell.

### Changed
- GUI transport switched from server-rendered pages to Electron IPC-backed process execution.
- Desktop actions such as mode runs, ad hoc report generation, Capital XLS imports, report cleanup, runtime config edits, and signal config edits now flow through the desktop bridge while reusing the existing Python trading logic.

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
