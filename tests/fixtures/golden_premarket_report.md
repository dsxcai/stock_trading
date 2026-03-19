# Daily Investment Report (Premarket)

- Version: v2026-03-18-004
- Signal Basis: t=2026-03-17 (NYSE Close)
- Execution Basis: t+1=2026-03-18 (NYSE Trading Day)

## Performance Summary
| Item | Value |
| --- | ---: |
| Initial Investment | $40,490.18 |
| Current Holdings Market Value | $31,122.84 |
| Cash | $0.00 |
| Current Total Assets | $31,122.84 |
| Net External Cash Flow | $-7,725.46 |
| Effective Capital Base | $32,764.72 |
| Total Profit | $-1,641.88 |
| Total Return | -5.01% |

## Current Positions
| Bucket | Ticker | Shares | Cost (USD) | Price(now) | Market Value (USD) | Unrealized PnL (USD) | Unrealized PnL % | Notes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Core | ARKQ | 68 | $8,473.60 | 118.9700 | $8,089.96 | $-383.64 | -4.53% | Added 37 shares intraday on 03/02 |
| Core | SPY | 17 | $11,618.41 | 659.8000 | $11,216.60 | $-401.81 | -3.46% | Added 6 shares intraday on 03/02 |
| Tactical | NVDA | 33 | $6,102.30 | 178.5600 | $5,892.48 | $-209.82 | -3.44% |  |
| Tactical | SMH | 15 | $5,968.56 | 394.9200 | $5,923.80 | $-44.76 | -0.75% |  |
| Core Subtotal | - | - | $20,092.01 | - | $19,306.56 | $-785.45 | -3.91% | - |
| Tactical Subtotal (incl. Cash Pool) | - | - | $12,070.86 | - | $11,816.28 | $-254.58 | -2.11% | - |
| Cash POOL_CASH | - | - | - | - | $0.00 | - | - | - |
| Portfolio Total | - | - | $32,162.87 | - | $31,122.84 | $-1,040.03 | -3.23% | - |

## Signal Status
| Ticker | Close(t) | MA Rule | MA(t) | Close(t-5) | Close(t)>MA(t) | Holding Days | T-5 Filter | Buy Signal | Tactical Shares (Pre-Execution) | t+1 Action | Action Shares |
| --- | ---: | --- | ---: | ---: | --- | ---: | --- | --- | ---: | --- | ---: |
| GOOG | 305.7300 | SMA50 | 318.5090 | 303.2100 | FAIL | - | PASS | FAIL | 0 | NO_ACTION | 0 |
| NVDA | 178.5600 | SMA50 | 184.9286 | 183.1400 | FAIL | 1 | SELL_BLOCKED (1d<=5d) | FAIL | 33 | HOLD | 0 |
| SMH | 394.9200 | SMA100 | 378.2351 | 388.1300 | PASS | 1 | PASS | PASS | 15 | HOLD | 0 |

## Hypothetical t+1 Close Threshold to Trigger Buy/Sell (P_min)
| Ticker | MA Rule | SUM_{n-1} | Close(t-5) | SMA-Equivalent Threshold (strict >) | P_min (strict >) | Display |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| GOOG | SMA50 | $15,603.02 | 301.4600 | 318.4290 | 318.4290 | 318.43+ |
| NVDA | SMA50 | $9,057.32 | 180.2500 | 184.8433 | 184.8433 | 184.84+ |
| SMH | SMA100 | $37,472.37 | 387.3300 | 378.5088 | 387.3300 | 387.33+ |

## Trade Details
### Trade Date (ET): 2026-03-16
| Trade ID | Ticker | Side | Time (TW) | Price | Shares | Gross | Fee | Cash Amount | Cash Basis | Fee Rate | Notes |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |
| 1 | NVDA | BUY | 2026/03/16 23:15:59 | 184.5490 | 33 | $6,090.12 | $12.18 | $6,102.30 | - | - | Imported from OSHistoryDealAll (NVDA) |
| 2 | SMH | BUY | 2026/03/16 23:15:33 | 397.1099 | 15 | $5,956.65 | $11.91 | $5,968.56 | - | - | Imported from OSHistoryDealAll (SMH) |

### Trade Date (ET): 2026-03-13
| Trade ID | Ticker | Side | Time (TW) | Shares | Amount |
| ---: | --- | --- | --- | ---: | ---: |
| 3 | NVDA | SELL | 2026/03/14 00:07:02 | 54 | $9,772.78 |
| 4 | SMH | SELL | 2026/03/14 00:06:37 | 24 | $9,305.59 |

### Trade Date (ET): 2026-03-10
| Trade ID | Ticker | Side | Time (TW) | Shares | Amount |
| ---: | --- | --- | --- | ---: | ---: |
| 5 | SMH | BUY | 2026/03/10 22:59:57 | 24 | $9,659.36 |
| 6 | NVDA | BUY | 2026/03/10 22:58:45 | 54 | $10,040.77 |
| 7 | META | SELL | 2026/03/10 22:54:37 | 31 | $20,388.14 |

### Trade Date (ET): 2026-03-02
| Trade ID | Ticker | Side | Time (TW) | Shares | Amount |
| ---: | --- | --- | --- | ---: | ---: |
| 8 | META | BUY | 2026/03/03 00:43:57 | 31 | $20,409.91 |
| 9 | ARKQ | BUY | 2026/03/03 00:03:41 | 37 | $4,600.73 |
| 10 | SPY | BUY | 2026/03/03 00:03:08 | 6 | $4,112.44 |

### Trade Date (ET): 2026-02-27
| Trade ID | Ticker | Side | Time (TW) | Shares | Amount |
| ---: | --- | --- | --- | ---: | ---: |
| 11 | SMH | SELL | 2026/02/28 04:22:32 | 71 | $28,793.86 |
| 12 | SMH | BUY | 2026/02/27 22:37:22 | 30 | $12,169.58 |
| 13 | NVDA | SELL | 2026/02/27 22:34:37 | 69 | $12,407.56 |

### Trade Date (ET): 2026-02-26
| Trade ID | Ticker | Side | Time (TW) | Shares | Amount |
| ---: | --- | --- | --- | ---: | ---: |
| 14 | NVDA | BUY | 2026/02/26 23:09:28 | 7 | $1,322.14 |
| 15 | SMH | BUY | 2026/02/26 23:06:39 | 2 | $827.39 |
