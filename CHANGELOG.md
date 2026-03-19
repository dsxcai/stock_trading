# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by Keep a Changelog, and this project uses semantic versioning.

## [1.0.0] - 2026-03-19

### Added
- External trades storage via `trades.json` with `--trades-file` support in `update_states.py` and `generate_report.py`.
- Fixture refresh helper script: `refresh_test_fixtures.sh`.
- New signal behavior tests in `tests/test_state_engine_signals.py`.
- Reporting safety test in `tests/test_reporting_safety.py` to ensure row-computed logic does not mutate source state.

### Changed
- Tactical signal logic:
  - `Buy = (Close(t) > MA(t)) and (Close(t) > Close(t-5))` (no buy-rule relaxation during protection window).
  - `Sell = not Buy` for existing tactical holdings.
  - Recent-buy protection now blocks sell execution (`HOLD`) instead of flipping to buy.
- Buy budget allocation now includes estimated same-cycle sell reclaim amount for non-blocked sells.
- Runtime market history now loads from CSV each run; persistent `history_400d` removed from `states.json`.
- Selected numeric output fields are rounded consistently (4-decimal policy for specified ratios/indicators).

### Fixed
- Report rendering no longer mutates state rows via `row_computed`.
- Deterministic key ordering for records written to `trades.json`.

---

## Unreleased

### Added
- (fill me)

### Changed
- (fill me)

### Fixed
- (fill me)
