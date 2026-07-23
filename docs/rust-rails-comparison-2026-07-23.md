# Rust versus Rails: 24-hour production comparison

Status: immutable production evidence
Window: `[2026-07-22T07:01:40Z, 2026-07-23T07:01:40Z)`
Created: 2026-07-23

This report freezes the larger equal-window comparison used to review the Rust
production architecture. It is evidence, not a runtime configuration source.
The architectural decisions derived from it live in
[`rust-production-architecture.md`](rust-production-architecture.md).

## Population and method

- Rails: pair `trading_pair_id = 3`, strategy `profit_token_a`, results joined
  to `arbitrage_trades` so completed and failed DEX attempts are separated.
- Rust: pair `world-chain-usdc-wld`, `dex_first`, live
  `arbitrage_result`/execution telemetry only.
- Both sides use the same half-open UTC interval.
- The Rust result population has one source revision:
  `e381dff246e806b86ca9cf3de4a5c805204cb86c`.
- The standard comparison was run with:

```bash
scripts/compare-arbitrage-results \
  2026-07-22T07:01:40Z \
  2026-07-23T07:01:40Z
```

The script's Rails `profitable_trades` column means `estimated_profit >= 0`.
That includes zero-PnL failed DEX attempts. The stricter figures below count
only `estimated_profit > 0` and separately report completed trades.

## Headline result

| Metric | Rails | Rust |
| --- | ---: | ---: |
| Result rows | 670 | 317 |
| Completed/balanced results | 550 completed | 317 balanced |
| Strictly profitable | 529 completed | 114 balanced |
| Profit rate in completed/balanced population | 96.2% | 36.0% |
| Comparable PnL | **+19.647121 USDC** | **+1.575745 USDC** |
| Average PnL per result row | +0.029324 USDC | +0.004971 USDC |
| Absolute WLD residual | 9.7877896249 WLD | 1.591945 WLD |

Rails produced 2.11 times as many result rows, 12.47 times the total PnL, and
5.90 times the average PnL per result row. Rust was profitable over the full
window and retained materially less residual WLD, but it did not reach Rails
execution quality.

Rails completed trades contributed `+19.679493 USDC`; 120 DEX-failed rows
contributed `-0.032372 USDC`. The standard script reported 582 nonnegative Rails
rows because 53 failed attempts had exactly zero reported PnL.

## Hourly comparison

PnL includes every result row in the hour. Rails `positive` counts strictly
positive completed trades; Rust `positive` counts `balanced_profit`.

| UTC hour | Rails rows | Rails completed | Rails positive | Rails PnL | Rust results | Rust positive | Rust PnL |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-07-22 08:00 | 3 | 1 | 0 | -0.053199 | 4 | 0 | -0.013482 |
| 2026-07-22 09:00 | 20 | 16 | 16 | +0.402006 | 2 | 0 | -0.033128 |
| 2026-07-22 10:00 | 81 | 71 | 70 | +1.927296 | 17 | 2 | -0.076672 |
| 2026-07-22 11:00 | 1 | 1 | 0 | -0.007254 | 0 | 0 | 0 |
| 2026-07-22 12:00 | 8 | 0 | 0 | -0.001550 | 7 | 1 | +0.003119 |
| 2026-07-22 13:00 | 13 | 8 | 8 | +0.211890 | 17 | 1 | -0.143678 |
| 2026-07-22 14:00 | 67 | 58 | 58 | +1.995937 | 21 | 4 | -0.125484 |
| 2026-07-22 15:00 | 0 | 0 | 0 | 0 | 1 | 0 | -0.027082 |
| 2026-07-22 16:00 | 266 | 251 | 249 | +11.468304 | 121 | 58 | +0.784836 |
| 2026-07-22 17:00 | 59 | 33 | 33 | +1.121856 | 53 | 14 | -0.257908 |
| 2026-07-22 18:00 | 5 | 1 | 0 | -0.026417 | 5 | 1 | +0.001686 |
| 2026-07-22 19:00 | 10 | 3 | 1 | -0.017171 | 10 | 0 | -0.038446 |
| 2026-07-22 20:00 | 3 | 1 | 0 | -0.014458 | 0 | 0 | 0 |
| 2026-07-22 21:00 | 5 | 3 | 2 | -0.021766 | 3 | 0 | -0.017967 |
| 2026-07-22 22:00 | 34 | 29 | 26 | +1.785848 | 19 | 12 | +0.911959 |
| 2026-07-22 23:00 | 1 | 0 | 0 | 0 | 1 | 0 | -0.005127 |
| 2026-07-23 00:00 | 5 | 0 | 0 | -0.001559 | 5 | 0 | -0.007942 |
| 2026-07-23 01:00 | 3 | 2 | 1 | +0.001069 | 0 | 0 | 0 |
| 2026-07-23 02:00 | 16 | 14 | 13 | +0.252441 | 6 | 6 | +0.201438 |
| 2026-07-23 03:00 | 60 | 56 | 52 | +0.692038 | 11 | 10 | +0.288544 |
| 2026-07-23 04:00 | 8 | 1 | 0 | -0.048460 | 13 | 4 | +0.093357 |
| 2026-07-23 06:00 | 2 | 1 | 0 | -0.019729 | 1 | 1 | +0.037722 |

The main absolute gap was the high-volume 16:00 UTC hour: Rails made
`+11.468304 USDC` while Rust made `+0.784836 USDC`.

## Rust execution funnel

Rust emitted 9,314 threshold-crossing direction events. That counter is not the
top of the admission funnel: one market update can emit an opportunity event
for each direction, while admission selects at most one candidate. Separately,
the engine recorded 2,820 candidate admission rejections for
`dex_settlement_waiting`.

```text
6,872 plans admitted by the engine
  ├─ 6,307 pending plans superseded by a newer plan
  ├─   215 pending plans invalidated by DEX settlement
  └─   350 plans received by the live task
       ├─ 25 entry-preflight rejections
       └─ 325 durable coordinator admissions
            ├─ 317 balanced results
            └─   8 isolated unknown outcomes
```

The entry-preflight rejections were:

| Reason | Plans |
| --- | ---: |
| `cex_top_quantity_below_admission` | 10 |
| `dex_pool_changed_after_quote` | 8 |
| `cex_price_moved_against_admission` | 7 |

The eight unknown outcomes did not recreate the former global dead end. Seven
were followed by another admission in less than one second. The remaining
event occurred during a period with no subsequent opportunity for about 59
minutes; trading later resumed without operator reconciliation. Each unknown
reservation continued to hold only its own claimed inventory.

## Rust admission quality

Of 6,872 admitted plans:

- 4,360 (63.4%) had positive expected primary profit after venue fees and the
  DEX execution reserve;
- 1,877 (27.3%) had positive expected profit after the maximum gas budget;
- the sum of expected primary profit across all admissions was
  `+178.996183 USDC`.

Of the 317 plans that actually reached a balanced result:

- only 95 (30.0%) had positive expected primary profit;
- only 14 (4.4%) had positive expected profit after the maximum gas budget;
- their summed expected primary profit was only `+1.120706 USDC`.

The newest-wins mailbox therefore prevented stale execution, but the executed
cohort was economically much weaker than the complete admitted population.
Admission count is not execution quality.

## Rust DEX, Binance, and recovery cohorts

Of 317 balanced results:

- 265 had a filled DEX leg;
- 52 had a known failed DEX leg;
- 187 primary Binance IOC orders produced at least one fill;
- 78 primary IOC orders produced no fill;
- 86 plans required one MARKET recovery, including eight partial primary
  fills.

The cohort PnL was:

| Cohort | Results | PnL |
| --- | ---: | ---: |
| DEX filled, primary CEX sufficient, no recovery | 179 | +2.011869 USDC |
| DEX filled, MARKET recovery required | 86 | -0.410810 USDC |
| DEX failed with known zero-token outcome | 52 | -0.025314 USDC |

Recovery remains a material loss source, but unlike the initial 5.5-hour
sample it did not consume the entire primary profit. The no-recovery cohort was
net profitable, with 99 profitable and 80 losing results.

## Rails execution cohorts

Rails completed 550 trades:

| Final CEX order type | Completed | Strictly positive | PnL |
| --- | ---: | ---: | ---: |
| LIMIT | 498 | 485 | +18.182582 USDC |
| LIMIT+MARKET | 2 | 2 | +0.072328 USDC |
| MARKET | 50 | 42 | +1.424583 USDC |

Rails' final CEX record does not always preserve whether a MARKET result came
from an expired primary IOC or a later retry, so the table must not be read as
an exact primary-fill funnel. It does show that Rails' completed LIMIT cohort
was both large and consistently profitable.

## DEX settlement evidence

Rust reconciled 265 filled DEX plans:

- 11 were already represented by a newer prepared pool generation when the
  terminal event arrived;
- 252 receipt catch-up attempts were deferred because `eth_getLogs` did not
  return the receipt Swap;
- 2 additional catch-up attempts failed at the RPC request;
- no catch-up was applied through the HTTP proof path in this window;
- the remaining updates were reconciled through the WebSocket pool log path.

Settlement duration was:

| Percentile | Duration |
| --- | ---: |
| p50 | 1.155 s |
| p95 | 1.739 s |
| max | 20.004 s |

During settlement, 2,820 candidate admission attempts were rejected.
Requiring `eth_getLogs` to reproduce a Swap already present in a canonical
receipt is therefore a measurable throughput bottleneck.

## Architectural conclusions

1. Market-data ingestion and local CLMM calculation are not the limiting
   stages. Rust observed far more signals than it could execute.
2. The old global `BlockedUnknown` dead end is fixed. Unknown work now isolates
   its own reservation while later plans continue.
3. The current latest-only mailbox is correct as an anti-staleness boundary,
   but the plan selected when the lane becomes available must pass stronger
   economic and stability review; raw admission volume is misleading.
4. DEX settlement must consume the receipt Swap immediately and use the
   WebSocket stream for canonical ordering/reorg correction, rather than
   requiring a second HTTP copy before updating the local pool.
5. The Binance hedge needs an explicit post-receipt decision based on the
   latest top of book and the immutable recovery envelope. The current frozen
   admission price leaves too many filled DEX legs requiring MARKET recovery.
6. Rust's lower residual inventory and exact multi-venue reservation remain
   deliberate advantages and must not be removed to imitate Rails throughput.
