# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by Keep a Changelog, and this project uses semantic versioning.

## [Unreleased]

- No unreleased changes yet.

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
