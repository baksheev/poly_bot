# Rails Binance capital failure lessons

Status: encoded in the Rust read-only capital recovery hydrator
Reviewed: 2026-07-16

The Rails application remains a behavioral specification only. This review
covered `BinanceService`, `ProcessBinanceDepositJob`,
`ProcessBinanceWithdrawalJob`, their fixtures and specs, and the Binance
rebalance fix history in `/Users/baksheev/code/arb_bot`.

## Invariants retained in Rust

| Rails behavior | Failure that it prevents | Rust behavior |
|---|---|---|
| Deposit history uses the on-chain `txId` | Crediting an unrelated Binance deposit | Parse an EVM hash, compare it case-insensitively, and reject duplicate matches |
| Withdrawal recovery searches `withdrawOrderId` | Confusing the numeric Travel Rule `trId`, Binance UUID, and client id | Fetch capital history and match the deterministic client id locally |
| Ordinary `withdraw` first posts to `sapi/v1/capital/withdraw/apply`; Binance may synchronously require the Travel Rule flow for that exact withdrawal | Hard-coding either endpoint globally, or guessing Binance's amount/risk threshold locally | Journal the standard request first; only the exact synchronous `-4104` response may route the same durable request to `localentity/withdraw/apply`, using the address-verification metadata already accepted by Binance |
| Deposit statuses `1` and `6` are credited | Waiting forever after funds are already credited but temporarily locked for withdrawal | Expose `Credited` and `CreditedWithdrawalLocked` as separate typed states |
| Empty history causes another observation | Treating Binance indexing lag as a failed transfer and resubmitting | Return an empty matching set; absence never authorizes a retry |
| Travel Rule questionnaire state is retained | Declaring a restricted deposit fully usable | Preserve `requireQuestionnaire` and `travelRuleReqStatus` separately from credit state |

## Intentional hardening over Rails

- Rust selects the exact requested coin and network, requires deposits to be
  enabled, and resolves multiple addresses only through one explicit default.
  It never takes the first response entry.
- The rebalance scope supports EVM routes only, so tagged, zero, malformed, or
  non-EVM deposit addresses are rejected.
- Amounts and withdrawal fees use `Decimal`; recovery performs no `to_f`
  conversion.
- Every matching deposit and withdrawal must return the requested coin and
  network. Duplicate identity matches fail closed.
- A standard withdrawal can create a failed Travel Rule history row before the
  local-entity request is sent. Restart recovery therefore permits multiple
  failed rows for one deterministic ID, selects at most one viable submission,
  and never replays an ambiguous POST.
- Transport, HTTP, authentication, and schema failures remain errors and are
  distinct from a valid empty history response.

## Remaining work

- Store the verified Binance deposit address and deterministic withdrawal ID
  in the durable operation journal before the first external mutation.
- Revalidate an address immediately before signing, while retaining the
  journaled address as the identity of an already-submitted transfer.
- Add bounded polling and final source/destination balance reconciliation in
  the paper executor.
