# Daily Investment Report (Premarket)
- Signal Basis: t=2026-03-17 (NYSE Close)
- Execution Basis: t+1=2026-03-18 (NYSE Trading Day)
- Estimated Price: Premarket Unrealized PnL (TWD) uses the latest TWD=X CSV quote from 2026-03-25.

## Performance Summary
| Item | Value |
| --- | ---: |
| Initial Investment | $40,490.18 |
| Current Holdings Market Value | $31,442.45 |
| Cash Position | $73.35 |
| Current Total Assets | $31,515.80 |
| Net External Cash Flow | $-7,725.46 |
| Effective Capital Base | $32,764.72 |
| Cumulative Profit | $-1,248.92 |
| Cumulative Return | -3.81% |

## Current Positions
| Bucket | Ticker | Shares | Cost (USD) | Price (Now) | Market Value (USD) | Unrealized PnL (USD) | Unrealized PnL (TWD) | Unrealized PnL % | Unrealized PnL % (TWD) | Notes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Core | ARKQ | 70 | $8,698.20 | 121.8500 | $8,529.50 | $-168.70 | -576.81 | -1.94% | -0.21% | Imported from Capital XLS (ARKQ ARKQUS) x70 |
| Core | SPY | 17 | $11,618.54 | 670.7900 | $11,403.43 | $-215.11 | -878.97 | -1.85% | -0.24% | Imported from Capital XLS (SPY SPDR標普500ETF) x17 |
| Tactical | SMH | 29 | $11,494.64 | 396.8800 | $11,509.52 | $14.88 | -232.39 | 0.13% | -0.06% | Imported from Capital XLS (SMH VanEck半導體ETF) x29 |
| Core Subtotal | - | - | $20,316.74 | - | $19,932.93 | $-383.81 | -1,455.77 | -1.89% | -0.23% | - |
| Tactical Subtotal (incl. Cash Pool) | - | - | $11,494.64 | - | $11,509.52 | $14.88 | -232.39 | 0.13% | -0.06% | - |
| Cash Pool | - | - | - | - | $73.35 | - | - | - | - | - |
| Portfolio Total | - | - | $31,811.38 | - | $31,515.80 | $-368.93 | -1,688.16 | -1.16% | -0.17% | - |

## Signal Status
| Ticker | A: Close(t) | MA Rule | B: SMA(t) | C: Close(t-5) | A>B | A>C | Buy Signal | Tactical Shares (Pre-Execution) | t+1 Action | Action Shares |
| --- | ---: | --- | ---: | ---: | --- | --- | --- | ---: | --- | ---: |
| GOOG | 309.4100 | SMA50 | 318.9058 | 306.9300 | FALSE | TRUE | FALSE | 0 | NO_ACTION | 0 |
| NVDA | 181.9300 | SMA50 | 185.2566 | 184.7700 | FALSE | FALSE | FALSE | 0 | NO_ACTION | 0 |
| SMH | 396.8800 | SMA100 | 377.1832 | 397.3300 | TRUE | FALSE | FALSE | 29 | SELL_ALL | 29 |

## t+1 Hypothetical Trigger Close Threshold (P_min)
| Ticker | MA Rule | SUM_{n-1} | Close(t-5) | SMA-Equivalent Threshold (strict >) | P_min (strict >) | Display |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| GOOG | SMA50 | $15,627.97 | 308.4200 | 318.9382 | 318.9382 | 318.94+ |
| NVDA | SMA50 | $9,074.71 | 186.0300 | 185.1982 | 186.0300 | 186.03+ |
| SMH | SMA100 | $37,379.72 | 401.0300 | 377.5729 | 401.0300 | 401.03+ |

## Trade Details
### Trade Date (ET): 2026-03-20
| Trade ID | Ticker | Side | Time (TW) | Price | Shares | Gross | Fee | Cash Amount | Cash Basis | Fee Rate | Notes |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |
| 103 | ARKQ | BUY | 2026/03/20 21:36:32 | 118.0000 | 2 | $236.00 | $0.47 | $236.47 | - | - | Imported from Capital XLS (ARKQ ARKQUS) |
| 102 | SMH | BUY | 2026/03/20 21:34:37 | 393.9320 | 14 | $5,515.05 | $11.03 | $5,526.08 | - | - | Imported from Capital XLS (SMH VanEck半導體ETF) |
| 101 | NVDA | SELL | 2026/03/20 21:31:18 | 177.2000 | 33 | $5,847.60 | $11.70 | $5,835.90 | - | - | Imported from Capital XLS (NVDA 輝達) |

### Trade Date (ET): 2026-03-16
| Trade ID | Ticker | Side | Time (TW) | Shares | Amount |
| ---: | --- | --- | --- | ---: | ---: |
| 100 | NVDA | BUY | 2026/03/16 23:15:59 | 33 | $6,102.30 |
| 99 | SMH | BUY | 2026/03/16 23:15:33 | 15 | $5,968.56 |

### Trade Date (ET): 2026-03-13
| Trade ID | Ticker | Side | Time (TW) | Shares | Amount |
| ---: | --- | --- | --- | ---: | ---: |
| 98 | NVDA | SELL | 2026/03/14 00:07:02 | 54 | $9,772.78 |
| 97 | SMH | SELL | 2026/03/14 00:06:37 | 24 | $9,305.59 |

### Trade Date (ET): 2026-03-10
| Trade ID | Ticker | Side | Time (TW) | Shares | Amount |
| ---: | --- | --- | --- | ---: | ---: |
| 96 | SMH | BUY | 2026/03/10 22:59:57 | 24 | $9,659.36 |
| 95 | NVDA | BUY | 2026/03/10 22:58:45 | 54 | $10,040.77 |
| 94 | META | SELL | 2026/03/10 22:54:37 | 31 | $20,388.14 |

### Trade Date (ET): 2026-03-02
| Trade ID | Ticker | Side | Time (TW) | Shares | Amount |
| ---: | --- | --- | --- | ---: | ---: |
| 93 | META | BUY | 2026/03/03 00:43:57 | 31 | $20,409.91 |
| 92 | ARKQ | BUY | 2026/03/03 00:03:41 | 37 | $4,600.73 |
| 91 | SPY | BUY | 2026/03/03 00:03:08 | 6 | $4,112.44 |

### Trade Date (ET): 2026-02-27
| Trade ID | Ticker | Side | Time (TW) | Shares | Amount |
| ---: | --- | --- | --- | ---: | ---: |
| 90 | SMH | SELL | 2026/02/28 04:22:32 | 71 | $28,793.86 |
| 89 | SMH | BUY | 2026/02/27 22:37:22 | 30 | $12,169.58 |
| 88 | NVDA | SELL | 2026/02/27 22:34:37 | 69 | $12,407.56 |
