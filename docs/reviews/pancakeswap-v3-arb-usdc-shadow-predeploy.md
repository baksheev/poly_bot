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

The first immutable-image C4 replay passed in workflow run `31006763857` for
digest `sha256:cef2e05ade044c732fa0270836048fb41be62ef8d473bb801b8a977c9e2d2063`.
Decision p99 was 97 ns versus 99 ns for the previous production image. Pool
build p99 was 30,829 ns versus 28,888 ns (+6.72%, within the 1.20 target-runtime
gate and the 200 us hard ceiling). Throughput was 7,021,091 frames/s versus
6,994,407 (+0.38%). The 100,000-frame ARBUSDC cohort had zero route failures,
dependency faults, network I/O, or external mutations.

The production observation cohort remains the final P7 gate. P8 live
enablement is not authorized by this review.
