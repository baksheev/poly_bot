# GKE production deployment

Status: GKE network prepared; the old GCE VM is stopped and retained as a
rollback target; the first GKE application revision is not deployed yet

## Topology

Production uses a zonal GKE Standard cluster named `arb-bot` in
`asia-southeast1-b`. Every release uses a dedicated fixed-size node pool with
one `c4-highcpu-8` node, Container-Optimized OS, and private networking. Cluster
Autoscaler is disabled. The sole pool is not tainted: GKE DNS, connectivity,
metrics, logging, and storage system Pods must share that node. The application
is pinned to the active release pool with a node selector.

Steady state contains one application Pod, one node, and one node pool. For a
release, GitHub Actions explicitly creates a new one-node pool named from the
tested source SHA. The Deployment uses `Recreate`: the previous process must
terminate before the replacement can attach the single-writer persistent disk
and start. After startup recovery and hydration remain Ready for 20 seconds,
the workflow deletes the previous pool. Two nodes can exist during replacement,
but never two application processes.

The runtime has Guaranteed QoS with equal requests and limits of six exclusive
CPUs and 10 GiB memory. A C4-8 node exposes about 7.91 CPUs and 12.96 GiB as
allocatable; required GKE system Pods currently reserve about one CPU and 1.53
GB, leaving a safe scheduling and runtime margin without moving to C4-16.

If image startup or readiness fails, the previous pool remains intact, the
Deployment is restored to its previous revision, and the failed release pool
is deleted. If old-pool cleanup fails after a successful rollout, the healthy
new revision remains active and a later workflow retry removes orphaned
`arb-*` pools.

The zonal ReadWriteOnce disk stores the durable rebalance journal and provides
a second single-writer boundary. Recreate rollout plus the journal file lock
prevent two processes from owning the same canary operation.

## Networking and secrets

- Nodes have no public IP addresses.
- The control plane exposes only its IAM-authenticated DNS endpoint.
- Pod egress passes through `arb-bot-gke-nat` and the reserved
  `arb-bot-gce-egress` address (`34.21.220.162`).
- The GKE Secret Manager add-on mounts six runtime secrets directly as
  in-memory files. GitHub Actions and Kubernetes Secret objects never contain
  their values.
- The runtime Kubernetes service account receives accessor permission only for
  those six secrets.
- The namespace denies all inbound connections to the runtime Pod.

Cloud NAT deliberately reuses the static IP that was previously attached to
the GCE VM, so the Binance API-key allowlist does not change. A static address
cannot be attached to a VM and Cloud NAT simultaneously: the VM must remain
stopped and without an external access config while GKE owns the address.

The project-wide and regional C4 quotas must cover the existing eight-vCPU VM
plus two fixed GKE nodes during a controlled replacement: 24 vCPU during
coexistence. Steady-state GKE usage is eight vCPU; release-time GKE usage is
sixteen. Bootstrap and the release helper both explicitly set `--num-nodes=1`.

## One-time bootstrap

Use the repository-local gcloud configuration and an ignored production env
file:

```bash
ENV_FILE=.env.production scripts/create-gke-runtime
```

The script keeps both Cloud SDK state and its generated kubeconfig under the
ignored repository-local `.gcloud/` directory; it does not read or modify the
global gcloud or kubeconfig state.

The idempotent script creates or configures:

1. secondary Pod and Service ranges on the existing isolated subnet;
2. the retained GCE static egress IP, Cloud Router, and Cloud NAT;
3. the private zonal GKE cluster and a fixed one-node bootstrap C4 pool (the
   temporary two-vCPU default pool is removed first);
4. Workload Identity access to Secret Manager;
5. the GitHub OIDC provider, a least-privilege deploy service account, and a
   custom role limited to node-pool create/get/list/delete operations;
6. the namespace, runtime configuration, CSI secret provider, PDB, and RBAC.

It prints the NAT IP and these GitHub `production` environment variables:

- `GCP_PROJECT_ID`
- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_DEPLOY_SERVICE_ACCOUNT`

Configure a required reviewer on the GitHub `production` environment. The
workflow is triggered only after `CI` succeeds on `main`, and can also be
started manually from `main`.

## Deployment and rollback

`.github/workflows/deploy-gke.yml` performs the release:

1. checks out the exact CI-tested source SHA;
2. authenticates with a short-lived GitHub OIDC credential;
3. builds and pushes a SHA-tagged image;
4. resolves the immutable Artifact Registry digest;
5. creates a fixed one-node pool named `arb-<source-sha-prefix>`;
6. targets the new Deployment revision exclusively at that pool;
7. waits up to 20 minutes for readiness;
8. deletes every previous release/bootstrap pool only after success;
9. restores the previous Deployment and deletes the new pool on failure.

Live-capable releases accept deployment downtime: preserving single ownership
is more important than overlap availability. No Cluster Autoscaler participates
in the release.

Every operator-visible release must update `CHANGELOG.md`. GitHub Actions also
records the source SHA, digest, cluster, and zone in the workflow summary;
Kubernetes retains five Deployment revisions.

## First cutover

1. Confirm that `34.21.220.162` remains in the Binance API-key allowlist.
2. Configure the three GitHub production environment variables and reviewer.
3. Run the workflow and verify startup, Binance freshness, DEX heads, balances,
   ClickHouse telemetry, and decision latency using the GKE engine identity.
4. Observe at least one reconnect and one controlled rollout.
5. Keep the stopped VM, its digest, and configuration as the rollback target
   until the GKE observation window is complete.

Do not start the old VM while its static IP is assigned to Cloud NAT. A rollback
must first move the address from NAT back to the VM.
