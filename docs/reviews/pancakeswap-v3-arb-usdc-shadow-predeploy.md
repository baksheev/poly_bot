# PancakeSwap V3 ARB/USDC P7 pre-deploy review

The P7 artifact hydrates and mirrors PancakeSwap V3 ARB/USDC on Arbitrum while
keeping it outside opportunity selection and execution. Uniswap remains the
only execution-eligible ARB/USDC provider.

## Reviewed identity

- Pool: `0x9ffca51d23ac7f7df82da414865ef1055e5afcc3`
- Factory: `0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865`
- Quoter: `0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997`
- Router: `0x1b81D678ffb9C0263b24A97847620C99d213eB14`
- Pair: ARB/native USDC, fee 500, tick spacing 10
- Compiled lifecycle: `validated`
- Source switch: `selection_enabled=false`

Startup does not create Pancake router allowances while the pool is
observe-only. Allowance preparation is tied to the later reviewed revision
that changes `selection_enabled` to true.

## Local performance evidence

The machine-readable report is
[`pancakeswap-v3-arb-usdc-p7-local.json`](../performance/pancakeswap-v3-arb-usdc-p7-local.json).
The provider suite uses 32 alternating rounds and 1,048,576 operations per
provider. All paired p95 and p99 ratios pass the 1.10 ceiling. The maximum
observed p95 ratio is 1.0505 for receipt proof; the maximum p99 ratio is 1.0028
for plan materialization.

The 2,000,000-frame capacity replay includes 100,000 ARBUSDC frames and 25
pools. Decision p99 is 84 ns, unchanged from the frozen control; pool-build
p99 is 14,084 ns. The replay performed no network I/O or external mutations.

The immutable-image C4 replay and production observation cohort remain P7
deployment gates. P8 live enablement is not authorized by this review.
