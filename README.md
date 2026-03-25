# Stock Trading System User Manual

The purpose of this document is as follows:

- Explain clearly when the system will buy, sell, or add to an existing position.
- Explain how each field in the report is calculated.
- Identify the commands used in routine operation, and the parameters that affect cash, trade records, and report outputs.
- Clarify which operations must be invoked with `--mode` and which may be executed without it.

------

## 1. Core operation of the strategy

The system performs four tasks each day:

1. Read the latest market price data.
2. Re-evaluate whether each tactical stock still satisfies the buy criteria.
3. Determine the recommended action for the next day, or the next trading session.
4. Generate a report listing positions, cash, signals, thresholds, and trade details.

### Core strategic concept

Each tactical stock is evaluated using two signals:

1. Whether the closing price is above its moving average

$$
Close(t) > MA(t)
$$

1. Whether the closing price is above the closing price from 5 trading days earlier

$$
Close(t) > Close(t-5)
$$

A Buy signal is considered valid only when both conditions are satisfied.

------

## 2. How buy, sell, and add-on decisions are made

### 2.1 Buy rule

A stock becomes eligible for purchase if both of the following conditions hold:

- `Close(t) > MA(t)`
- `Close(t) > Close(t-5)`

### 2.2 Sell rule

If a tactical stock is currently held, but its Buy signal is no longer valid, the system action is:

- `SELL_ALL`

At present, the strategy does not support partial reductions or staged exits. Once the signal fails, the entire tactical position is exited.

### 2.3 Add-on rule

As long as a stock has a valid Buy signal, it participates in the current capital allocation cycle, regardless of whether it is:

- not currently held, or
- already held

Accordingly, three positive actions may result:

- `BUY`: not previously held, and allocated shares in this round
- `BUY_MORE`: already held, and allocated additional shares in this round
- `HOLD`: already held, but no additional shares were allocated in this round

------

## 3. How cash is partitioned: deployable cash vs reserve cash

The system divides cash into two categories:

| Type            | Field            | Can be used to buy stocks | Included in total assets |
| --------------- | ---------------- | ------------------------- | ------------------------ |
| Deployable cash | `deployable_usd` | Yes                       | Yes                      |
| Reserve cash    | `reserve_usd`    | No                        | Yes                      |

Total cash is always defined as:

```text
cash.usd = deployable_usd + reserve_usd
```

### 3.1 Which funds go into deployable cash

The following cash flows are routed to `deployable_usd` by default:

- cash proceeds recovered from stock sales
- capital injection or external deposit
- tactical cash inferred from trade history

These funds are therefore treated, by default, as capital available for redeployment into the market.

### 3.2 What reserve cash means

`reserve_usd` is cash intentionally set aside and excluded from the next buy cycle.

It:

- remains part of total assets
- is still included in NAV
- does not participate in tactical buy allocation

### 3.3 Internal transfers: deployable cash <-> reserve cash

The following parameter may be used:

```bash
--cash-transfer-to-reserve-usd <amount>
```

Transfer rules:

- positive value: transfer from `deployable_usd` to `reserve_usd`
- negative value: transfer from `reserve_usd` back to `deployable_usd`

Examples:

- `--cash-transfer-to-reserve-usd 5000`
  means transferring USD 5,000 of deployable cash into reserve cash
- `--cash-transfer-to-reserve-usd -3000`
  means transferring USD 3,000 of reserve cash back into deployable cash

### 3.4 Safety checks

The system performs boundary checks:

- if the positive transfer amount exceeds `deployable_usd`, execution aborts immediately
- if the absolute value of a negative transfer exceeds `reserve_usd`, execution aborts immediately

Upon abort:

- `states.json` is not updated
- no report is generated
- no partial output is left behind

In other words, the entire run is rolled back to its pre-execution state.

------

## 4. How share quantities are allocated for purchases

### 4.1 Which stocks are included in the current allocation round

A stock enters the set of buy candidates if:

- `buy_signal = true`, and
- a valid price is available, meaning `action_price_usd > 0`

This includes:

- stocks not currently held, representing new entries
- stocks already held, representing add-on allocations

### 4.2 Which cash bucket is used

Only the following cash bucket is used in the current purchase allocation round:

- `deployable_usd`

`reserve_usd` does not participate in stock purchases.

In addition, if `SELL_ALL` actions exist in the same round, the system performs a simple estimate and adds projected sale proceeds into the buy funding pool:

```text
estimated_sell_reclaim_usd = Sigma(sold_shares x action_price_usd)
investable_cash_usd = investable_cash_base_usd + estimated_sell_reclaim_usd
```

This estimate is used only for purchase allocation within the current round.

### 4.3 Allocation flow

The allocation logic for `BUY` and `BUY_MORE` does not use `Close(t)` directly. Instead, the per-share purchase cost is scaled to include fees:

```text
buy_sizing_price_usd = Close(t) x (1 + fee_rate)
```

That is, the allocator uses the fee-inclusive per-share cost.

#### Phase A: ensure at least 1 share per selected stock

If capital is sufficient, the system first ensures that each selected stock receives at least 1 share.

If capital is insufficient to buy 1 share of every selected stock:

- stocks are sorted from lowest price to highest price
- only the cheapest affordable prefix set is retained

Operationally, this means that when capital is insufficient to establish an initial position in every candidate, priority is given to the lowest-priced eligible names.

#### Phase B: split remaining cash evenly

After Phase A, the remaining deployable cash is divided equally across the selected stocks.

For each stock, the system computes how many additional shares can be purchased and rounds down to an integer number of shares.

#### Phase C: repeatedly deploy the remainder into the most expensive affordable stock

After equal allocation, if residual cash remains, the system will:

- in each round, identify the most expensive stock that is still affordable
- buy 1 additional share
- repeat
- continue until no stock is affordable anymore

This is intended to utilize `deployable_usd` as efficiently as possible.

------

## 5. How `mode`, snapshots, and `states.json` are now separated

### 5.1 `states.json` no longer stores the selector

`states.json` no longer stores selector-style fields such as:

- `meta.mode`
- `meta.active_mode`
- `meta.last_run`

That means the state file no longer persists which mode is currently active.

### 5.1.1 Responsibility split among `states.json`, `config.json`, and `trades.json`

Trade details are now externalized into `trades.json` by default:

- `config.json` retains strategy configuration, bucket definitions, moving-average rules, trading calendar settings, report document titles, timezone display settings, the external `trades.json` path, and configuration-oriented input source mappings such as `state_engine.csv_sources`
- `states.json` retains compact persistent state such as holdings quantities, cash baselines, external cash-flow history, and performance basis, and no longer stores `meta.trades_file` or `meta.trades_count`
- `trades.json` retains transaction-level trade records
- `report/<DATE>_<mode>.json` retains the fully assembled mode-specific report snapshot used to render markdown output

In addition, historical price data is now loaded directly from `data/*.csv` on each execution, rather than storing a long-lived OHLCV block such as `history_400d` inside `states.json`. This includes configured FX pairs such as `state_engine.fx_pairs.usd_twd -> TWD=X`, which are downloaded alongside equity CSVs and can be used for report-only currency analytics.

Also, `states.json` no longer embeds a `config` block and no longer stores `market.csv_sources`, `meta.doc`, `meta.timezone`, `meta.trades_file`, or `meta.trades_count`. These are loaded from external `config.json` at runtime.

Numeric rounding policy is fully configuration-owned as well. `state_engine.numeric_precision` now centralizes:

- `usd_amount` for persisted cash and other USD runtime amounts
- `display_price` and `display_pct` for rendered output
- `trade_cash_amount` and `trade_dedupe_amount` for imported trades before they are merged into `trades.json`
- `state_selected_fields` for persisted rounded fields in `states.json`
- `backtest_amount`, `backtest_price`, `backtest_rate`, and `backtest_cost_param` for backtest output

### 5.2 Mode context is derived at runtime

Mode-specific report context is no longer persisted in `states.json`.

Fields such as:

- `signal_basis`
- `execution_basis`
- `version_anchor_et`
- `by_mode.*`

are now treated as transient runtime metadata derived from:

- the explicit `--mode`
- the current ET session, or an explicit report date when applicable
- the configured trading calendar

Likewise, `signals`, `thresholds`, `market.signals_inputs`, and `market.next_close_threshold_inputs` are no longer persisted. These are now treated as transient data computed on demand during report rendering.

`states.json` now primarily retains:

- current holdings quantities: `portfolio.positions[*].ticker`, `portfolio.positions[*].shares`
- persistent cash state: `portfolio.cash.usd`, `portfolio.cash.deployable_usd`, `portfolio.cash.reserve_usd`, `portfolio.cash.baseline_usd`, external cash-flow history, and related reconciliation metadata
- persistent performance basis: `portfolio.performance.initial_investment_usd` and `portfolio.performance.baseline.*`

Derived fields such as market prices, totals, signals, thresholds, and per-mode report content are not persisted in `states.json`. Runtime performance outputs such as current total assets and profit are recomputed from the persistent basis plus the latest `trades.json` and `data/*.csv`, then written into `report/<DATE>_<mode>.json` for each mode run.

### 5.3 Why all reports now require an explicit `--mode`

Because the state no longer stores the selector or mode snapshot, any path that generates a report must explicitly specify:

```bash
--mode Premarket
--mode Intraday
--mode AfterClose
```

The report will then derive the corresponding runtime context directly rather than relying on a stale last-used mode stored in state.

------

## 6. How to use the main entry points

### 6.1 Premarket

```bash
./premarket.sh
```

Purpose: update the state before market open and generate the premarket report.
Current-position USD prices and signal inputs stay on the prior NYSE close. `Unrealized PnL (TWD)` still uses the latest available USD/TWD CSV quote and the report marks that as `Estimated Price`.
This entry point does not rewrite the primary `states.json`. It writes `report/<DATE>_premarket.json` and renders the markdown report from that snapshot.

### 6.2 Intraday

```bash
./intraday.sh
```

Purpose: update the state during market hours and generate the intraday report.
If a same-day CSV row is available, `Current Positions` and `Signal Status` use that same-day price in Intraday mode, and the report marks it as `Estimated Price`.
This entry point does not rewrite the primary `states.json`. It writes `report/<DATE>_intraday.json` and renders the markdown report from that snapshot.

### 6.3 AfterClose

```bash
./afterclose.sh
```

Purpose: update the state after market close and generate the after-close report.
This entry point does not rewrite the primary `states.json`. It writes `report/<DATE>_afterclose.json` and renders the markdown report from that snapshot.

### 6.4 Capital XLS extension import

```bash
./update_xml.sh <capital-xls-path> [extra update_states args...]
```

Purpose: run the Capital Securities importer extension, convert `OSHistoryDealAll.xls` into normalized trades JSON, and then call `update_states.py` to synchronize `trades.json` and `states.json`.

This entry point:

- does not require `--mode`
- does not require explicit `--csv-dir`
- if `--csv-dir` is omitted, price CSV files are automatically loaded from `./data`
- preserves `update_states.py` import behavior: default is `append`, and only `--trades-import-mode replace` triggers replace mode

### 6.5 Generate a report only

```bash
python3 generate_report.py --states states.json --trades-file trades.json --schema report_spec.json --mode Premarket
```

Purpose: use an existing snapshot together with `data/*.csv` to compute the tactical plan on demand and generate the report for the specified mode.

This command must explicitly include `--mode`.
When `update_states.py` is run with `--mode`, it now attempts an automatic CSV refresh first. The refresh scope is limited to functionally active tickers: current holdings, strategy tickers, and configured FX pairs such as `TWD=X`. The mode update flow refreshes those active tickers every time instead of trusting the last CSV row to already be a finalized close. For equities, the refresh end date still follows the mode's latest completed NYSE trading day. Configured FX pairs are allowed to refresh through the current ET date so same-day FX quotes can be used when available.
If the input `states.json` is compact, the command reconstructs derived position fields such as bucket and FIFO cost basis from `trades.json` before loading CSV market data.
If a downloaded or local CSV row has incomplete OHLC values, the command fails by default. Use `--allow-incomplete-csv-rows` only when you intentionally want to bypass that failure and skip incomplete rows.

### 6.6 Tactical simulation / backtest

```bash
python3 backtest.py --config config.json --csv-dir data --out-dir backtest
```

Purpose: simulate tactical strategy performance using historical OHLCV data.

Currently implemented rules:

- tactical-only
- by default uses the most recent `252` trading days, plus the required warm-up window
- supports `--lookback-trading-days`
- supports `--start-date` and `--end-date`
- initial capital defaults to top-level `backtest_starting_cash`, but can be overridden using `--starting-cash`
- `BUY`, `BUY_MORE`, and `SELL_ALL` are all executed at `t+1`
- the `t+1` execution price is `(Open(t+1) + Close(t+1)) / 2`
- net results incorporate `fee_rate`, `commission_per_trade`, and `slippage_bps`
- the markdown report lists total return, per-position return, and a comparison against a "buy-and-hold from initial date without selling" benchmark

Period and capital parameters:

- `--starting-cash`
  - overrides `backtest_starting_cash` from `config.json`
- `--lookback-trading-days N`
  - when `--start-date` is not specified, backtests the most recent `N` trading days
  - if `--end-date` is also specified, that date is used as the backward anchor
- `--start-date YYYY-MM-DD`
  - specifies the inclusive start date
  - once specified, `--lookback-trading-days` no longer applies
- `--end-date YYYY-MM-DD`
  - specifies the inclusive end date
  - if omitted, defaults to the last date in the shared trading calendar
  - if the date range falls outside the shared trading calendar, or if there are not enough warm-up trading days before `start-date`, the program fails directly rather than auto-shrinking the window

Output files:

- `summary.json`
- `equity_curve.csv`
- `gross_trades.json`
- `net_trades.json`
- `report.md`

### 6.6.1 All-in-one shell script

```bash
./backtest_all_in_one.sh --starting-cash 100000 --start-date 2025-03-01 --end-date 2026-03-01 --out-dir backtest_phase2
```

This script performs more than report generation alone. It will:

1. accept the provided parameters
2. run the backtest simulation
3. output `summary`, `equity_curve`, and `trades`
4. generate `report.md` at the end

If `--out-dir` is not specified, the system automatically creates a timestamped directory.

------

## 7. How to read the report

### 7.1 Buy and sell trigger status table

This is the primary table for day-to-day decision-making.

| Field                           | Meaning                      | Description                                   |
| ------------------------------- | ---------------------------- | --------------------------------------------- |
| Symbol                          | Ticker                       | For example, GOOG or NVDA                     |
| A: Close(t)                     | Today's close                | Sourced from signals input                    |
| MA rule                         | Moving-average rule          | For example, SMA50 or SMA100                  |
| B: SMA(t)                       | Current moving average value | Computed according to the rule for that stock |
| C: Close(t-5)                   | Close 5 trading days ago     | Always participates in Buy evaluation         |
| A>B                             | Above moving average         | TRUE or FALSE                                 |
| A>C                             | Above close from 5 days ago  | TRUE or FALSE                                 |
| BUY signal                      | Final signal status          | TRUE or FALSE, defined by `A>B && A>C`        |
| Tactical shares (pre-execution) | Shares held before execution | Tactical bucket only                          |
| t+1 action                      | Recommended next action      | BUY, BUY_MORE, HOLD, SELL_ALL, or NO_ACTION   |
| Action shares                   | Recommended order quantity   | 0 if no action is needed                      |

### 7.2 How `A>B` is computed

When:

```text
A > B
```

the field is shown as `TRUE`; otherwise `FALSE`.

### 7.2.1 How `A>C` is computed

When:

```text
A > C
```

the field is shown as `TRUE`; otherwise `FALSE`.

### 7.3 Final determination of the Buy signal

```text
buy_signal = (Close(t) > MA(t)) and (Close(t) > Close(t-5))
```

Sell determination:

```text
sell_signal = (shares_pre > 0) and (not buy_signal)
```

If Sell is true, then `t+1_action = SELL_ALL`.

------

## 8. How other report fields are computed

### 8.1 Current position status table

The report only displays stocks with `shares > 0`.

Positions that have been fully sold are now removed directly from `portfolio.positions`; zero-share remnants are no longer retained.

| Field              | Calculation                                         |
| ------------------ | --------------------------------------------------- |
| Market value       | `shares x price_now`                                |
| Unrealized PnL     | `market_value_usd - cost_usd`                       |
| Unrealized percent | `unrealized_pnl_usd / cost_usd`, blank if cost is 0 |

### 8.2 Total assets and NAV

```text
portfolio.nav_usd = total market value of all holdings + deployable_usd + reserve_usd
```

### 8.3 Return rate

If an initial invested amount has been defined:

```text
effective_capital_base_usd = initial_investment_usd + net_external_cash_flow_usd
profit_usd = current_total_assets_usd - effective_capital_base_usd
profit_rate = profit_usd / effective_capital_base_usd
```

------

## 9. Capital XLS extension behavior and new ticker handling

### 9.1 Capital XLS extension can run without `--mode`

This is intentionally supported because the extension ultimately feeds normalized trades into the core update flow, which can update trades, cash, and positions without recomputing a report mode.

### 9.2 If Capital XLS import introduces a new ticker, the system automatically creates the position

If the Capital XLS export contains a new buy for a stock that does not already exist in `portfolio.positions`, the system automatically creates that ticker entry.

### 9.3 CSV loading and price hydration happen within the same execution

As long as `./data/<TICKER>.csv` exists, the system will complete the following within the same Capital XLS import run:

- CSV import
- `price_now` update
- recomputation of market value and unrealized PnL

Accordingly, no second run is required. A newly imported ticker can be valued immediately within the same execution cycle.

------

## 10. Frequently used parameters

### 10.1 Data and output

| Parameter         | Purpose                                                      |
| ----------------- | ------------------------------------------------------------ |
| `--states`        | Specify the state file                                       |
| `--out`           | Specify the output state file                                |
| `--csv-dir`       | Specify the market price CSV directory; defaults to `./data` if omitted |
| `--allow-incomplete-csv-rows` | Bypass incomplete OHLC rows by skipping them instead of failing |
| `--report-json-out` | Explicitly specify the mode snapshot JSON path; default is `report/<DATE>_<mode>.json` |
| `--report-schema` | Specify the report schema                                    |
| `--report-dir`    | Specify the report output directory                          |
| `--report-out`    | Directly specify the report output filename                  |
| `--log-file`      | Specify the log file; if omitted, a file is automatically created under `logs/` |

### 10.2 Mode and reporting

| Parameter         | Purpose                                                      |
| ----------------- | ------------------------------------------------------------ |
| `--mode`          | Specify `Premarket`, `Intraday`, or `AfterClose`             |
| `--render-report` | Generate the report in the same execution flow after update  |
| `--now-et`        | Override the current ET timestamp for testing mode or session determination |
| `-f`, `--force-mode`   | Bypass the ET/session check and run the requested `--mode` anyway |

### 10.3 Cash and performance

| Parameter                        | Purpose                                                      |
| -------------------------------- | ------------------------------------------------------------ |
| `--initial-investment-usd`       | Specify the initial invested capital                         |
| `--cash-adjust-usd`              | Record external deposit or withdrawal                        |
| `--cash-adjust-note`             | Note for external cash flow                                  |
| `--cash-transfer-to-reserve-usd` | Perform internal transfer between deployable and reserve cash |
| `--tactical-cash-usd`            | Reconcile tactical cash using broker cash snapshot           |

### 10.4 Reconciliation and validation

| Parameter                        | Purpose                                      |
| -------------------------------- | -------------------------------------------- |
| `--broker-investment-total-usd`  | Broker total holdings snapshot               |
| `--broker-investment-total-kind` | Reconcile against cost basis or market value |
| `--verify-tolerance-usd`         | Reconciliation tolerance                     |

### 10.5 Trade import

| Parameter              | Purpose               |
| ---------------------- | --------------------- |
| `--imported-trades-json` | Import normalized trades JSON generated by an external importer |
| `--trades-import-mode` | `append` or `replace` (default: `append`) |

Imported trade rounding is controlled by `config.json` under `state_engine.numeric_precision`. In practice, `trade_cash_amount` controls stored `cash_amount` values and `trade_dedupe_amount` controls the numeric precision used by trade deduplication keys.

After imported trades are merged, `portfolio.positions` is rebuilt from the full trade ledger. Remaining position cost basis follows FIFO. This trade-ledger rebuild applies to holdings only; `market.prices_now` is rebuilt separately from loaded CSV history.
When `states.json` is saved, holdings are persisted as share quantities while cash baselines, external cash-flow history, and performance basis are also preserved; derived FIFO cost basis is reconstructed again on the next runtime from `trades.json`.
Current-position notes shown in reports are derived from the surviving FIFO lots behind each holding, aggregating the unique non-empty trade notes that still compose the remaining shares and appending the remaining share count for each note, such as `AA x2 | BB x7`. They are not persisted in `portfolio.positions`.
The `Current Positions` table also includes `Unrealized PnL (TWD)` and `Unrealized PnL % (TWD)`. They are computed only at report-build time by converting each surviving FIFO buy lot with the USD/TWD close on or before its buy date, then comparing that TWD cost basis with the current position market value translated by the latest available USD/TWD CSV quote. In Premarket mode the report marks that FX translation as `Estimated Price` whenever the FX quote is newer than the prior-close signal basis day.

### 10.6 When `--mode` is mandatory

- general daily update, signal recomputation, or report generation: required
- `--render-report`: must be used together with `--mode`
- Capital XLS extension import only: optional
- external cash adjustment only: optional
- deployable to reserve transfer only: optional
- initial investment update only: optional

When `--mode` is omitted, the system performs only state updates such as imported trades, cash adjustments, or initial investment updates.

It does not recompute report-scoped signals or thresholds, and it does not generate a report.

If `--mode` is present but the current ET session does not normally allow that mode, the command aborts by default. Use `-f` / `--force-mode` only when you intentionally want to run that mode anyway for backfill, testing, or manual scenario generation.

------

## 11. How log files work

### 11.1 `update_states.py` writes logs automatically

If `--log-file` is not explicitly specified, the system automatically creates:

```text
logs/update_states_<timestamp>_<pid>.log
```

### 11.2 `generate_report.py` also writes logs automatically

If `--log-file` is not explicitly specified, the system automatically creates:

```text
logs/generate_report_<timestamp>_<pid>.log
```

### 11.3 Contents of the logs

The log contains sufficient data for troubleshooting, for example:

- the actual command line
- working directory
- parsed parameters
- mode, ET time, and session determination
- which CSV files were imported
- imported trades result
- mismatch or abort reason
- report output path
- traceback

### 11.4 When the logs should be inspected

Logs should be reviewed first in situations such as the following:

- why the report was not generated
- why a mode was determined as invalid or unexpected
- why a ticker has no price
- why share count or cost basis after an importer run does not match expectation
- why a mismatch caused the run to abort

------

## 12. Routine operation examples

### 12.1 Standard premarket update

```bash
./premarket.sh
```

A routine daily update of this kind always includes `--mode`, and updates both the snapshot and the report for that mode.

### 12.2 Premarket update and move USD 3,000 into reserve cash

```bash
./premarket.sh --cash-transfer-to-reserve-usd 3000
```

### 12.3 Move USD 1,500 of reserve cash back into deployable cash

```bash
./premarket.sh --cash-transfer-to-reserve-usd -1500
```

### 12.4 After-close update and reconcile broker total holdings

```bash
./afterclose.sh --broker-investment-total-usd 40490.18
```

### 12.5 Import Capital XLS only, without `--mode`

```bash
./update_xml.sh data/OSHistoryDealAll.xls
```

### 12.6 Record an external deposit or withdrawal only, without `--mode`

```bash
python3 update_states.py --states states.json --out states.json --cash-adjust-usd 2000 --cash-adjust-note "top up"
```

### 12.7 Perform deployable and reserve transfer only, without `--mode`

```bash
python3 update_states.py --states states.json --out states.json --cash-transfer-to-reserve-usd 1500
```

### 12.8 Generate a report for a specific mode only

```bash
python3 generate_report.py --states states.json --schema report_spec.json --mode Intraday --out-dir report
```

If `states.json` is already a reduced snapshot, this command automatically loads CSV files from `./data` to reconstruct the buy/sell trigger table and threshold table required for reporting.

You may also specify `--csv-dir` explicitly.

### 12.9 Run a tactical simulation

```bash
python3 backtest.py --config config.json --csv-dir data --starting-cash 80000 --lookback-trading-days 120 --out-dir backtest_tactical
```

### 12.10 Specify start and end dates and generate a markdown report

```bash
./backtest_all_in_one.sh --starting-cash 80000 --start-date 2025-01-01 --end-date 2025-12-31 --out-dir backtest_2025
```

### 12.11 Specify a custom log file path

```bash
python3 update_states.py --states states.json --out states.json --mode Premarket --csv-dir ./data --log-file logs/manual_premarket.log
```

------

## 13. Key operational points

1. The core Buy signal is: price above the moving average, and above the close from 5 trading days ago.
2. This strategy currently has no new-entry protection. If a position is held and the Buy signal fails, the `t+1` action is `SELL_ALL`.
3. Deployable cash is intended to be used as fully as possible. Reserve cash counts as an asset, but is not used to buy stocks.
4. A stock that is already held and still has a valid signal can also receive add-on allocation.
5. The report only displays current holdings with `shares > 0`.
6. Importer-driven trade updates and cash-related updates may be executed without `--mode`. General report and scenario updates must include it.
7. Standalone report generation always requires an explicit `--mode`.
8. Backtest is currently tactical-only. Initial capital defaults to `backtest_starting_cash`, but can be overridden via `--starting-cash`.
9. Backtest can use `--lookback-trading-days`, or an explicit period via `--start-date` and `--end-date`.
10. The backtest report lists both strategy results and the "buy-and-hold from initial date without selling" benchmark.
11. If any result appears abnormal, inspect `logs/` first.

------

## 14. Test case overview

### 14.1 Run all tests with one command

```bash
./run_tests.sh
```

Current coverage includes the following:

- regression pipeline comparison of `update_states` plus `generate_report` against golden fixtures
- strategy and download utility function tests
- safety test ensuring report `row_computed` fields cannot be written back into state
- tactical signal tests and tests for folding projected sell proceeds into the buy cash pool
- backtest tests covering `t+1` execution, period selection, and markdown report generation

### 14.2 When fixtures should be refreshed

Whenever the following items are modified, `tests/fixtures/*golden*` should generally be refreshed:

- `state_engine` signal logic, cash allocation logic, or rounding output
- report schema or rendering behavior
- trade data structures or output formats for `states.json` or `trades.json`

### 14.3 Refresh regression fixtures with one command

```bash
./refresh_test_fixtures.sh
```

An optional custom time anchor may also be specified:

```bash
./refresh_test_fixtures.sh 2026-03-18T08:00:00-04:00
```

------

## 15. Versioning and changelog policy

- This project is formally designated as `v1.0.0` effective `2026-03-19`.
- From this point forward, whenever functionality, behavior, output format, or test baselines change, `CHANGELOG.md` must be updated.
- Any feature addition, removal, or behavior change must be reflected in `program.md` to keep documentation aligned with implementation.
- Any feature addition, removal, or behavior change must also be accompanied by the corresponding sanity tests or unit tests. Functionality changes without test coverage are not acceptable.
- At minimum, the following should be recorded:
  - Added / Changed / Fixed
  - whether a breaking change exists
  - which commands, fields, fixtures, and tests are affected
