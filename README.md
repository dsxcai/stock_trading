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

2. Whether the closing price is above the closing price from 5 trading days earlier

$$
Close(t) > Close(t-5)
$$

A Buy signal is considered valid only when both conditions are satisfied.

------

## 2. How buy, sell, and add-on decisions are made

### 2.1 Standard buy rule

A stock becomes eligible for purchase if both of the following conditions hold:

- `Close(t) > MA(t)`
- `Close(t) > Close(t-5)`

### 2.2 Five-trading-day protection for newly purchased positions

If a stock was purchased recently and is still currently held, then during the first **5 trading days after purchase**:

- the system **does not relax the Buy condition**; `Close(t) > Close(t-5)` must still hold
- however, if a Sell condition is triggered, sell-block protection is applied

The purpose is straightforward:

> to avoid immediate reversals caused by short-term price fluctuations, such as selling the day after purchase, buying back the following day, and selling again shortly thereafter.

Accordingly, during the protection window:

- Buy must still satisfy both the moving-average condition and the t-5 condition
- if Buy is no longer valid while the position is still held, the condition is still treated as a Sell signal, but the resulting action is converted to `HOLD` by the protection mechanism

### 2.3 Sell rule, including sell blocking during the protection window

If a tactical stock is currently held, but its Buy signal is no longer valid, the system action is:

- `SELL_ALL`

At present, the strategy does not support partial reductions or staged exits. Once the signal fails, the entire tactical position is exited.

However, if the stock is still within the 5-trading-day protection window following a recent purchase, then:

- the Sell signal still exists
- the `t+1` action is changed to `HOLD` to block the sale
- the `T-5 Filter` field in the report will display `SELL_BLOCKED (Xd<=5d)`

### 2.4 Add-on rule

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
| ---------------- | ---------------- | ------------------------- | ------------------------ |
| Deployable cash | `deployable_usd` | Yes | Yes |
| Reserve cash    | `reserve_usd`    | No  | Yes |

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

In addition, if `SELL_ALL` actions exist in the same round and are **not blocked by the protection window**, the system performs a simple estimate and adds projected sale proceeds into the buy funding pool:

```text
estimated_sell_reclaim_usd = Σ(sold shares × action_price_usd)
investable_cash_usd = investable_cash_base_usd + estimated_sell_reclaim_usd
```

This estimate is used only for purchase allocation within the current round. If a sale is marked as `SELL_BLOCKED`, it is not included.

### 4.3 Allocation flow

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

### 5.1.1 Responsibility split between `states.json` and `trades.json`

Trade details are now externalized into `trades.json` by default:

- `states.json` retains summary outputs, snapshots, and `meta.trades_file` / `meta.trades_count`
- `trades.json` retains transaction-level trade records

In addition, historical price data is now loaded directly from `data/*.csv` on each execution, rather than storing a long-lived OHLCV block such as `history_400d` inside `states.json`.

### 5.2 Mode-specific data is stored under `by_mode`

Each mode snapshot is stored separately under the top-level `by_mode` key:

- `by_mode.premarket`
- `by_mode.intraday`
- `by_mode.afterclose`

Each snapshot stores mode-specific information such as:

- `signal_basis`
- `execution_basis`
- `version_anchor_et`
- `version`
- `report_context`
- `broker_context`

### 5.3 Why all reports now require an explicit `--mode`

Because the state no longer stores the selector, any path that generates a report must explicitly specify:

```bash
--mode Premarket
--mode Intraday
--mode AfterClose
```

The report will then directly select the corresponding `by_mode.<mode>` snapshot rather than relying on a stale last-used mode stored in state.

------

## 6. How to use the main entry points

### 6.1 Premarket

```bash
./premarket.sh
```

Purpose: update the state before market open and generate the premarket report.

### 6.2 Intraday

```bash
./intraday.sh
```

Purpose: update the state during market hours and generate the intraday report.

### 6.3 AfterClose

```bash
./afterclose.sh
```

Purpose: update the state after market close and generate the after-close report.

### 6.4 XML import

```bash
./update_xml.sh <xml-path> [extra update_states args...]
```

Purpose: import broker XML trade data into the trade data file, defaulting to `trades.json`, and synchronize `states.json`.

This entry point:

- does not require `--mode`
- does not require explicit `--csv-dir`
- if `--csv-dir` is omitted, price CSV files are automatically loaded from `./data`

### 6.5 Generate a report only

```bash
python3 generate_report.py --states states.json --trades-file trades.json --schema report_spec.json --mode Premarket
```

Purpose: use an existing snapshot to generate the report for the specified mode.

This command must explicitly include `--mode`.

------

## 7. How to read the report

### 7.1 Buy and sell trigger status table

This is the primary table for day-to-day decision-making.

| Field | Meaning | Description |
| --- | --- | --- |
| Symbol | Ticker | For example, GOOG or NVDA |
| Close(t) | Today's close | Sourced from signals input |
| MA rule | Moving-average rule | For example, SMA50 or SMA100 |
| MA(t) | Current moving average value | Computed according to the rule for that stock |
| Close(t-5) | Close 5 trading days ago | Always participates in Buy evaluation |
| Close(t)>MA(t) | Above moving average | `PASS` / `FAIL` |
| Holding days | Number of trading days since the most recent buy date of the current position | Displays `-` if not currently held |
| T-5 Filter | t-5 condition and protection-window status | `PASS` / `FAIL` / `SELL_BLOCKED` |
| Buy signal | Final signal status | Valid / not valid |
| Tactical shares (pre-execution) | Shares held before execution | Tactical bucket only |
| t+1 action | Recommended next action | `BUY` / `BUY_MORE` / `HOLD` / `SELL_ALL` / `NO_ACTION` |
| Action shares | Recommended order quantity | `0` if no action is needed |

### 7.2 How `Close(t)>MA(t)` is computed

When:

```text
Close(t) > MA(t)
```

the field is shown as `PASS`; otherwise `FAIL`.

### 7.3 How `Holding days` is computed

If the stock is currently held, the system will:

1. identify the most recent buy trade date that opened or re-established the position
2. use the current signal day as the reference point
3. count how many trading days have elapsed in between

If the stock is not currently held, the field displays `-`.

### 7.4 How to interpret `T-5 Filter`

This field shows either the result of the t-5 condition or the sell-blocked status caused by the protection window.

Possible values:

- `PASS`: the condition is in effect, and `Close(t) > Close(t-5)` holds
- `FAIL`: the condition is in effect, but `Close(t) <= Close(t-5)`
- `SELL_BLOCKED (Xd<=5d)`: a Sell signal exists, but the sale is blocked by the 5-day protection rule for newly purchased positions

### 7.5 Final determination of the Buy signal

```text
buy_signal = (Close(t) > MA(t)) and (Close(t) > Close(t-5))
```

The same formula applies during the protection window; it is not relaxed into a moving-average-only rule.

Sell determination:

```text
sell_signal = (shares_pre > 0) and (not buy_signal)
```

The protection window changes only the action, not the truth value of the signal. If Sell is true and the position is still within the protection window, then `t+1_action = HOLD`.

------

## 8. How other report fields are computed

### 8.1 Current position status table

The report displays only stocks with `shares > 0`. Positions that have been fully sold but are retained in `states.json` only as historical traces do not appear in the portfolio holdings table of the report.

| Field | Calculation |
| --- | --- |
| Market value | `shares × price_now` |
| Unrealized PnL | `market_value_usd - cost_usd` |
| Unrealized percent | `unrealized_pnl_usd / cost_usd` (blank if cost is 0) |

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

## 9. XML import behavior and new ticker handling

### 9.1 XML import can run without `--mode`

This is intentionally supported because the nature of XML import is to update trade records, not to recompute signals or reports for a specific mode.

### 9.2 If XML introduces a new ticker, the system automatically creates the position

If the XML contains a new buy for a stock that does not already exist in `portfolio.positions`, the system automatically creates that ticker entry.

### 9.3 CSV loading and price hydration happen within the same execution

As long as `./data/<TICKER>.csv` exists, the system will complete the following within the same XML import run:

- CSV import
- `price_now` update
- recomputation of market value and unrealized PnL

Accordingly, no second run is required. A newly imported ticker can be valued immediately within the same execution cycle.

------

## 10. Frequently used parameters

### 10.1 Data and output

| Parameter | Purpose |
| --- | --- |
| `--states` | Specify the state file |
| `--trades-file` | Specify the trade data file, defaulting to `trades.json` |
| `--out` | Specify the output state file |
| `--csv-dir` | Specify the market price CSV directory; defaults to `./data` if omitted |
| `--report-schema` | Specify the report schema |
| `--report-dir` | Specify the report output directory |
| `--report-out` | Directly specify the report output filename |
| `--log-file` | Specify the log file; if omitted, a file is automatically created under `logs/` |

### 10.2 Mode and reporting

| Parameter | Purpose |
| --- | --- |
| `--mode` | Specify `Premarket`, `Intraday`, or `AfterClose` |
| `--render-report` | Generate the report in the same execution flow after update |
| `--now-et` | Override the current ET timestamp for testing mode or session determination |

### 10.3 Cash and performance

| Parameter | Purpose |
| --- | --- |
| `--initial-investment-usd` | Specify the initial invested capital |
| `--cash-adjust-usd` | Record an external cash inflow or outflow |
| `--cash-adjust-note` | Note for external cash flow |
| `--cash-transfer-to-reserve-usd` | Perform an internal transfer between deployable and reserve cash |
| `--tactical-cash-usd` | Correct tactical cash using a broker cash snapshot |

### 10.4 Reconciliation and verification

| Parameter | Purpose |
| --- | --- |
| `--broker-investment-total-usd` | Broker snapshot of total invested holdings |
| `--broker-investment-total-kind` | Whether reconciliation is based on cost basis or market value |
| `--verify-tolerance-usd` | Reconciliation tolerance |
| `--mismatch-policy` | `abort`, `warn`, or `force` when a mismatch occurs |
| `--diagnose-mismatch` | Whether to output a mismatch diagnostic file |

### 10.5 Trade import

| Parameter | Purpose |
| --- | --- |
| `--trades-xml` | Import broker XML |
| `--trades-import-mode` | `append`, `reconcile`, or `replace` |
| `--trade-reconcile-abs-tol-usd` | Absolute tolerance for XML reconciliation |
| `--trade-reconcile-rel-tol` | Relative tolerance for XML reconciliation |

### 10.6 When `--mode` is required

- general daily update, signal recomputation, or report generation: **required**
- `--render-report`: **must be used together with `--mode`**
- XML import only: **may be omitted**
- external cash adjustment only: **may be omitted**
- deployable/reserve transfer only: **may be omitted**
- initial invested capital update only: **may be omitted**

When `--mode` is omitted, the system performs only state updates such as XML import, cash operations, or initial-investment updates. It **does not** recompute report-scoped signals or thresholds, and it does not generate a report.

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

### 11.3 What the logs contain

The logs retain sufficient information for debugging, including:

- the actual command line
- working directory
- parsed arguments
- mode, ET time, and session determination
- which CSV files were imported
- XML import results
- mismatch or abort reasons
- report output path
- traceback

### 11.4 When the logs should be checked

Check the logs first in the following situations:

- why a report was not generated
- why a certain mode was judged to be invalid
- why a certain ticker has no price
- why share count or cost after XML import differs from expectation
- why a mismatch caused an abort

------

## 12. Routine operation examples

### 12.1 Standard premarket update

```bash
./premarket.sh
```

This type of routine update always includes `--mode` and updates both the corresponding mode snapshot and the report.

### 12.2 Premarket update with USD 3,000 moved into reserve cash

```bash
./premarket.sh --cash-transfer-to-reserve-usd 3000
```

### 12.3 Transfer USD 1,500 from reserve cash back into deployable cash

```bash
./premarket.sh --cash-transfer-to-reserve-usd -1500
```

### 12.4 After-close update with broker holdings reconciliation

```bash
./afterclose.sh --broker-investment-total-usd 40490.18
```

### 12.5 Import broker XML only, without `--mode`

```bash
./update_xml.sh data/OSHistoryDealAll.xml
```

### 12.6 Record external cash inflow or outflow only, without `--mode`

```bash
python3 update_states.py --states states.json --out states.json --cash-adjust-usd 2000 --cash-adjust-note "top up"
```

### 12.7 Perform a deployable/reserve transfer only, without `--mode`

```bash
python3 update_states.py --states states.json --out states.json --cash-transfer-to-reserve-usd 1500
```

### 12.8 Generate a report only for a specified mode

```bash
python3 generate_report.py --states states.json --schema report_spec.json --mode Intraday --out-dir report
```

### 12.9 Specify a custom log file

```bash
python3 update_states.py --states states.json --out states.json --mode Premarket --csv-dir ./data --log-file logs/manual_premarket.log
```

------

## 13. Key points to remember

1. **The Buy signal is fundamentally defined by two conditions: above the moving average, and above the close from 5 days earlier.**
2. **The 5-day protection rule for newly purchased positions blocks sales only; it does not convert an invalid Buy into a valid one.**
3. **Deployable cash is intended to be used as fully as possible; reserve cash remains part of assets but is not used for purchases.**
4. **A stock that is already held can still receive an add-on allocation if its signal remains valid.**
5. **The report displays only current holdings with `shares > 0`.**
6. **XML import and cash-related updates may run without `--mode`; ordinary report or scenario updates must include it.**
7. **Standalone report generation must always specify `--mode` explicitly.**
8. **If any result appears abnormal, inspect `logs/` first.**

------

## 14. Test case overview

### 14.1 Run the entire test suite in one command

```bash
./run_tests.sh
```

Current coverage includes:

- regression pipeline validation by comparing `update_states` + `generate_report` outputs against golden fixtures
- strategy and download utility function tests
- safety tests ensuring `report` `row_computed` fields cannot write back into state
- tests for the new tactical signal rules, including sell blocking during the protection window and recycling sale proceeds into the buy cash pool

### 14.2 When fixtures should be refreshed

Whenever changes are made to any of the following areas, `tests/fixtures/*golden*` normally needs to be refreshed:

- signal logic, cash allocation logic, or rounding outputs in `state_engine`
- report schema or rendering behavior
- output formats of trade-related structures such as `states.json` and `trades.json`

### 14.3 Refresh regression fixtures in one command

```bash
./refresh_test_fixtures.sh
```

A custom time anchor may also be specified:

```bash
./refresh_test_fixtures.sh 2026-03-18T08:00:00-04:00
```

------

## 15. Versioning and changelog policy

- This project has been formally defined as **v1.0.0** starting from **2026-03-19**.
- From this point forward, every change to functionality, behavior, output format, or test baseline should be recorded in `CHANGELOG.md`.
- At a minimum, it is recommended to record:
  - Added / Changed / Fixed
  - whether a breaking change exists
  - which commands, fields, fixtures, or tests are affected
