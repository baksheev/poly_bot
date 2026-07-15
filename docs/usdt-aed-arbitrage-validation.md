# USDT/AED executable arbitrage validation

Status: rejected as the next implementation target; retained as research  
Last reviewed: 2026-07-15

## Question

Do UAE crypto venues offer an executable USDT/AED price that can be hedged
through a global stablecoin market and a real USD/AED banking route for a
positive return after every fee, spread, transfer, and inventory cost?

Comparing a displayed USDT/AED price with the interbank USD/AED midpoint is not
enough. USDT is not USD, the accessible bank FX price is not necessarily the
central-bank intervention rate, and fiat/crypto settlement is not atomic.

## Primary venue: OKX UAE

OKX Middle East Fintech FZE has an active VARA licence that includes Exchange
Services. Eligible UAE customers can deposit and withdraw AED through local
bank transfer. Product availability remains account-specific.

Official references:

- [OKX VARA register entry](https://www.vara.ae/en/licenses-and-register/public-register/okx-middle-east-fintech-fze/)
- [OKX AED bank deposit](https://www.okx.com/en-gb/help/how-do-i-deposit-aed-with-bank-transfer)
- [OKX AED bank withdrawal](https://www.okx.com/en-ae/help/how-do-i-withdraw-aed-with-bank-transfer)
- [OKX V5 API](https://www.okx.com/docs-v5/en/)

The public instrument endpoint confirms `USDT-AED` is a live SPOT instrument,
with AED as the quote currency. This is a true orderbook API suitable for a
read-only collector, unlike an indicative conversion page.

## Initial public observations

### OKX

On 2026-07-15 the public OKX API returned approximately:

| Market | Best bid | Best ask | Top size context |
| --- | ---: | ---: | --- |
| USDT/AED | 3.6660 | 3.6670 | more than 100k USDT near each top side |
| USDT/USD | 0.9993 | 0.9994 | more than 2.8m/8.1m USDT at top |

The apparent discount to the AED peg is not the complete edge because USDT was
also trading below USD. For the potentially profitable direction:

```text
buy 1 USDT at the OKX USDT/AED ask             = 3.6670000 AED
sell 1 USDT at the OKX USDT/USD bid            = 0.9993 USD
sell USD at optimistic CBUAE reference 3.672   = 3.6694296 AED

gross profit before all fees                   = 0.0024296 AED
gross edge on AED cost                          = about 6.6 bps
```

There is no public `USD-AED` spot instrument on OKX, so the last conversion is
a bank/treasury leg, not an atomic third order on the exchange.

OKX's current UAE fee publications make the observed snapshot unprofitable for
a regular account:

- AED spot pairs: approximately 10 bps maker / 13.5 bps taker under the
  published Middle East framework;
- USDT/USD stablecoin pair: maker 0 bps / taker 4 bps after the July 2026
  update.

An all-taker cycle therefore starts around 17.5 bps of trading fees against a
6.6 bps gross edge, before bank spread and transfers. A maker AED fill plus a
maker USDT/USD hedge still starts around 10 bps of fees and adds fill/adverse
selection risk. The monitor must load the authenticated account-specific fee
tier; published schedules can change and must not be hardcoded as permanent.

Sources:

- [live OKX USDT/AED orderbook](https://www.okx.com/api/v5/market/books?instId=USDT-AED&sz=20)
- [live OKX USDT/USD orderbook](https://www.okx.com/api/v5/market/books?instId=USDT-USD&sz=20)
- [OKX UAE fee framework](https://www.okx.com/en-ae/help/updates-to-uae-wholesale-fee-framework)
- [July 2026 USDT/USD fee update](https://www.okx.com/en-ae/help/fee-update-for-spot-trading-pair-usdt-usds-uae)

Wider temporary dislocations, better VIP or market-maker fees, or resting
orders at better prices could still be profitable. The observed edge does not
justify diverting implementation from the already profitable `arb_bot` clone,
so no OKX collector or execution component is planned now. Any future revisit
must measure the signal net of actual account fees before it is
called an opportunity.

### BitOasis comparison

On 2026-07-15 the public BitOasis API returned approximately:

| Market | Best bid | Best ask |
| --- | ---: | ---: |
| USDT/AED | 3.67507 | 3.67544 |
| USDT/USD | 1.0007 | 1.0008 |

The AED peg translation explains these values:

```text
1.0007 × 3.6725 = 3.67507075 -> 3.67507
1.0008 × 3.6725 = 3.67543800 -> 3.67544
```

The public books also showed matching quantities at corresponding price
levels. This is consistent with BitOasis showing partner liquidity translated
into AED, rather than an independent local USDT/AED orderbook imbalance.

Sources:

- [BitOasis API documentation](https://api.bitoasis.net/doc/)
- [live USDT/AED orderbook](https://api.bitoasis.net/v1/exchange/order-book/USDT-AED?bids_limit=50&asks_limit=50)
- [live USDT/USD orderbook](https://api.bitoasis.net/v1/exchange/order-book/USDT-USD?bids_limit=50&asks_limit=50)
- [BitOasis orderbook and liquidity-partner explanation](https://support.bitoasis.net/en/support/solutions/articles/29000042697-bitoasis-trading-operations-order-book-transparency-and-liquidity-partner-collaboration)

The Central Bank of the UAE states that it intervenes at USD/AED 3.672 when
buying USD and 3.673 when selling USD. Those are market-intervention rates, not
a promise that our bank or broker will execute any size for us at those prices:
[CBUAE domestic market operations](https://centralbank.ae/en/our-operations/monetary-policy-and-domestic-markets/domestic-market-operations/).

### BitOasis interpretation

Relative to the 3.6725 peg midpoint, the BitOasis snapshot contained roughly a
7 bps bid premium and an 8 bps ask premium for USDT. That is a stablecoin/USD
basis translated into AED. It is not yet evidence of profitable AED arbitrage.

Even using the CBUAE sell-USD intervention rate of 3.673 as an optimistic
replenishment reference, selling USDT locally at 3.67507 leaves only about 5.6
bps gross before local trading fees, global hedge fees, bank spread, wires,
crypto transfers, capital cost, and failed/rejected settlement. The actual
account-specific cost stack is likely decisive.

## Correct comparison

Define prices for a requested USDT size `q`:

- `local_bid(q)`: AED received per USDT when selling into local depth;
- `local_ask(q)`: AED paid per USDT when buying from local depth;
- `global_bid(q)`: USD received per USDT at the hedge venue;
- `global_ask(q)`: USD paid per USDT at the hedge venue;
- `fx_bid(q)`: AED received when selling USD through the accessible bank route;
- `fx_ask(q)`: AED paid when buying USD through that route.

All book prices are size-weighted executable VWAP, not top-of-book midpoint.

### Sell USDT locally, replenish globally

```text
local_net_aed = local_bid(q) × q × (1 - local_sell_fee)

replenishment_aed =
  global_ask(q) × q × (1 + global_buy_fee) × fx_ask(q)

profit_aed =
  local_net_aed
  - replenishment_aed
  - bank_fees
  - crypto_transfer_fees
  - rebalance_slippage
  - inventory_and_failure_reserve
```

This is the plausible direction when USDT trades at a local premium. With
pre-funded inventory, the two market trades can be simultaneous, but the cycle
still ends long AED locally and short USD/stablecoin liquidity globally. The
inventories must later be rebalanced.

### Buy USDT locally, sell globally

```text
local_cost_aed = local_ask(q) × q × (1 + local_buy_fee)

global_net_aed =
  global_bid(q) × q × (1 - global_sell_fee) × fx_bid(q)

profit_aed =
  global_net_aed
  - local_cost_aed
  - bank_fees
  - crypto_transfer_fees
  - rebalance_slippage
  - inventory_and_failure_reserve
```

A local bid being below the USD/AED reference does not make this reverse path
profitable; it is the local ask that must be low enough.

## What the monitor must capture

### Local venue

- complete executable USDT/AED depth and event/source time;
- USDT/USD and USDC/AED books when offered, to identify synthetic translation;
- trades, book age, venue status, and minimum/order increments;
- account-specific maker/taker tier and actual charged fees;
- AED deposit/withdrawal rules, cutoffs, limits, holds, and charges;
- USDT networks, confirmation rules, withdrawal fee, and transfer limits.

Start with BitOasis because it has a documented public orderbook API. Add M2,
Rain, OKX UAE, and other venues only where an executable quote/API and a legal
account/fiat route are available. P2P and cash/OTC are separate strategies with
counterparty, fraud, AML, chargeback, and non-atomic settlement risk; they
should not be mixed into the first exchange-orderbook study.

### Global hedge venue

- the exact market used to replenish or sell USDT, not a generic price index;
- full depth, fees, deposit/withdrawal networks, and account limits;
- USDT/USD where real USD rails exist, or the complete USDT/USDC plus USDC/USD
  conversion path if USDC is used as the funding asset;
- stablecoin depeg and venue-credit reserves.

Binance can provide the liquid crypto leg, but a Binance USDT/USDC price does
not by itself close the USD/AED banking loop.

### FX and banking

- streaming or requested executable USD/AED bid/ask from the actual bank,
  broker, or treasury provider;
- tier/size, settlement date, value-date cutoff, transfer and correspondent
  fees;
- evidence from completed conversions to validate quoted versus realized FX;
- account limits, compliance review time, and whether crypto-related flows are
  accepted.

If no programmable quote exists, begin with timestamped manual quotes or
realized bank statements. Using 3.6725 as if it were executable would overstate
the edge.

## Measurement design

For each venue and both directions, calculate at least these USDT notionals:

```text
1,000 / 10,000 / 50,000 / 100,000 / 500,000
```

Record:

- raw levels and size-aware VWAP;
- gross basis versus the peg and versus the actual FX route;
- every fee/cost component in AED and bps;
- net edge and maximum profitable size;
- opportunity start/end, duration, and required reaction time;
- inventory consumed on each venue;
- estimated time and cost to restore starting inventories;
- data freshness and missing/unexecutable reasons.

Use a configurable cost model with `unknown` distinct from zero. Missing bank
spread or fee data must make net profitability unknown, never optimistically
profitable.

## Proposed read-only service

```text
OKX depth WebSocket ────────┐
OKX private fee/account ────┤
BitOasis comparison REST ───┼─> normalized books ─> cost engine ─> ClickHouse
bank/FX quote adapter ──────┤                         │
fee and rail config ────────┘                         └─> opportunity alerts
```

The first version has no authenticated trading endpoints and no keys capable
of moving money. Rust is still useful because the same typed books,
fixed-point math, replay, health, and telemetry can later support execution and
the broader `arb_bot` clone.

Suggested ClickHouse datasets:

- `market_book_snapshots` with raw and normalized levels;
- `fx_quotes` with provider, size, direction, and value date;
- `cost_model_versions` without secrets;
- `opportunity_snapshots` for every size/direction, including negative edges;
- `rail_observations` for actual transfer/conversion duration and cost.

## Evidence threshold before execution work

Collect at least 7-14 continuous days and require:

- positive edge after account-specific trading, banking, transfer, and
  inventory costs;
- enough depth and duration to execute both market legs;
- repeatable opportunity frequency and daily capacity;
- profitability under conservative stablecoin, venue, rejection, and
  rebalance reserves;
- a feasible legal/compliance and bank-account operating model;
- deterministic replay of the cost calculation.

If positive edge exists only before fees or only against the 3.6725 reference,
the hypothesis is rejected.

## Live shape if evidence is positive

Use pre-funded, isolated inventories rather than waiting for each transfer:

- USDT and AED at the UAE venue;
- USD/USDC and USDT at the global hedge venue;
- dedicated bank/broker balances for periodic AED/USD rebalancing.

Market legs can then be placed together, while slow fiat and blockchain moves
restore inventory afterward. Hard limits must cover venue credit risk,
stablecoin depeg, bank holds, transfer delays, failed hedges, and one-sided
fills.

Maker execution may be evaluated if taker fees consume the small basis, but it
introduces queue-position, fill-probability, and adverse-selection risk. A
displayed maker spread cannot be counted as earned until fills and hedge costs
are measured live at tiny size.

## Regulatory checkpoint

For Dubai, the current VARA FAQ distinguishes proprietary trading with own
funds/no clients from providing virtual-asset services, but also describes NOC
and high-volume registration requirements. The exact entity, jurisdiction,
activity, banking flow, and volume should be confirmed with VARA and qualified
UAE counsel before live operation: [VARA licensing FAQ](https://www.vara.ae/en/faq/).

Do not accept third-party funds or provide exchange/remittance services under a
proprietary-trading design.

## Information still required

To determine whether the observed opportunity is real, obtain:

1. the actual OKX UAE account tier and authenticated maker/taker fee response;
2. whether both AED and USD deposit/withdrawal rails are enabled on that
   account, with their limits and fees;
3. the bank or broker that can convert USD/AED, including executable bid/ask;
4. available USD, AED, USDT, and USDC balances/rails and their limits;
5. target trade sizes and acceptable inventory duration;
6. any additional UAE venues to use as comparisons after OKX and BitOasis.
