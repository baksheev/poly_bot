# PancakeSwap V3 ARB/USDC P8 live pre-deploy review

P8 enables direct full-live selection of the canonical PancakeSwap V3
ARB/native-USDC fee-500 pool. There is no paper phase. The first candidate that
clears the existing 20 bps gate may submit a real DEX-first production swap and
the existing Binance hedge/recovery flow.

## Canonical route

- Pool: `0x9ffca51d23ac7f7df82da414865ef1055e5afcc3`
- Factory: `0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865`
- QuoterV2: `0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997`
- V3-only router: `0x1b81D678ffb9C0263b24A97847620C99d213eB14`
- Token0/token1: ARB/native USDC
- Fee/tick spacing: 500/10
- Router selector: `0x414bf389`, the reviewed eight-word
  `exactInputSingle` tuple with a deadline.

The compiled pool lifecycle must be `execution_eligible`. The source switch is
`selection_enabled=true`; Uniswap V3 fee-500 and fee-3000 pools remain eligible
and the existing deterministic economic ranking chooses among all three.

## Allowance and gas authority

Startup prepares exactly two Pancake router allowances under provider-specific
durable operation identities before locking allowance mutations:

- native USDC `0xaf88d065e77c8cc2239327c5edb3a432268e5831` -> Pancake router,
  `uint256::MAX`;
- ARB `0x912ce59144191c1204e64559fe8253a0e49e6548` -> Pancake router,
  `uint256::MAX`.

The shared Arbitrum nonce/journal owner serializes these approvals with every
other EVM mutation. Approval operation IDs are distinct from swap parent IDs.
After preparation, the executor cannot create or change allowances.

Pancake uses the reviewed V3 gas policy: twice the RPC estimate and a 250,000
gas no-estimate fallback. Historical router receipts sampled during P5 ranged
from 130,932 to 179,165 gas, leaving at least 39% fallback headroom. Arbitrum
maximum fee uses the existing 12,000 bps headroom and zero priority fee.

## P7 evidence

The production report is
[`pancakeswap-v3-arb-usdc-p7-production.json`](../performance/pancakeswap-v3-arb-usdc-p7-production.json).
It contains 1,315 background evaluations and 2,630 Pancake shadow items with
21 us p95 / 26 us p99 calculation time, zero Pancake admissions/executions and
zero telemetry drops. Only ten live pool events occurred, so this review makes
no 1,000-event live-tail claim. Two target-C4 100,000-ARBUSDC replays supplement
the low-activity live pool: median decision p99 is 1.0304x control and median
pool-build p99 is 1.1992x control, within the frozen 1.05x/1.20x gates and all
hard ceilings.

The previous production source is
`arb-bot-production-usdc-arb-arbitrum-v3-pancakeswap-v3-shadow` at commit
`c3ce70501ceac1914b10bbce5430b26982b6c781`. The current rollback image is
`sha256:c14e6c0203e8c5cace1262dc858eb9bb21d5f38ae841334dbd63a890f55115fe`.

## P8 local performance gate

The release-mode report is
[`pancakeswap-v3-arb-usdc-p8-local.json`](../performance/pancakeswap-v3-arb-usdc-p8-local.json).
Each provider benchmark executes 8,388,608 operations. Pancake/Uniswap p95 and
p99 ratios pass the 1.10 ceiling for event decode, receipt proof, execution-plan
materialization, and calldata construction. The closest result is event-decode
p99 at 1.0975. The common 20-pair replay remains at 84 ns decision p99 and
18,000 ns pool-build p99 with zero route failures, dependency faults, network
I/O, or external mutations.

## Reconciliation and rollback

Every swap keeps the existing durable EVM intent, nonce, broadcast, receipt,
positional pool-Swap proof, local mirror update and prepared-curve rebuild.
Unknown EVM outcomes remain quarantined for receipt/transaction
reconciliation; known reverts release the lane and may enqueue only bounded
diagnostics. After a proven DEX fill, the existing immutable Binance IOC and
bounded MARKET recovery semantics apply unchanged.

Rollback is a new reviewed `main` commit restoring the v3 shadow source and a
normal `Deploy GKE` run. It disables Pancake selection immediately in the new
runtime artifact; the already granted router allowances remain inert and
locked. GCE stays terminated and is not part of routine rollback.
