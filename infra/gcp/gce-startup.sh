#!/bin/bash
set -euo pipefail

metadata_header="Metadata-Flavor: Google"
metadata_root="http://metadata.google.internal/computeMetadata/v1"

metadata() {
  curl --fail --silent --show-error \
    --header "${metadata_header}" \
    "${metadata_root}/instance/attributes/$1"
}

metadata_or_default() {
  local key="$1"
  local default_value="$2"
  local value
  value="$(curl --silent \
    --header "${metadata_header}" \
    "${metadata_root}/instance/attributes/${key}" || true)"
  if [[ -n "${value}" ]]; then
    printf '%s' "${value}"
  else
    printf '%s' "${default_value}"
  fi
}

project_id="$(curl --fail --silent --show-error \
  --header "${metadata_header}" \
  "${metadata_root}/project/project-id")"
image="$(metadata arb-bot-image)"
engine_id="$(metadata arb-bot-engine-id)"
region="$(metadata arb-bot-region)"
wallet_address="$(metadata arb-bot-wallet-address)"
domain_config_path="$(metadata_or_default arb-bot-domain-config-path config/strategies/usdc-wld-world-chain.v4.json)"
arbitrage_execution_mode="$(metadata_or_default arb-bot-arbitrage-execution-mode disabled)"
arbitrage_live_confirmation="$(metadata_or_default arb-bot-arbitrage-live-confirmation '')"
arbitrage_max_plan_cost="$(metadata_or_default arb-bot-arbitrage-max-plan-cost 0)"
arbitrage_max_recovery_loss="$(metadata_or_default arb-bot-arbitrage-max-recovery-loss 0)"
arbitrage_max_cumulative_loss="$(metadata_or_default arb-bot-arbitrage-max-cumulative-loss 0)"
arbitrage_max_cumulative_recovery_loss="$(metadata_or_default arb-bot-arbitrage-max-cumulative-recovery-loss 0)"
arbitrage_max_total_entries="$(metadata_or_default arb-bot-arbitrage-max-total-entries 0)"
arbitrage_max_entries_per_minute="$(metadata_or_default arb-bot-arbitrage-max-entries-per-minute 0)"

if [[ ! "${arbitrage_execution_mode}" =~ ^(disabled|paper_dex_first|paper_concurrent_hedged|full_live)$ ]]; then
  echo "invalid arb-bot-arbitrage-execution-mode metadata" >&2
  exit 1
fi
if [[ ! "${domain_config_path}" =~ ^config/strategies/[a-z0-9.-]+\.json$ ]]; then
  echo "invalid arb-bot-domain-config-path metadata" >&2
  exit 1
fi
for value in \
  "${arbitrage_max_plan_cost}" \
  "${arbitrage_max_recovery_loss}" \
  "${arbitrage_max_cumulative_loss}" \
  "${arbitrage_max_cumulative_recovery_loss}" \
  "${arbitrage_max_total_entries}" \
  "${arbitrage_max_entries_per_minute}"; do
  if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
    echo "invalid numeric arbitrage risk metadata" >&2
    exit 1
  fi
done
if [[ "${arbitrage_execution_mode}" == "full_live" ]]; then
  if [[ "${domain_config_path}" != "config/strategies/usdc-wld-world-chain.v5.json" ]]; then
    echo "full_live requires the reviewed v5 domain artifact" >&2
    exit 1
  fi
  if [[ "${arbitrage_live_confirmation}" != "ENABLE_FULL_LIVE_ARBITRAGE" ]]; then
    echo "full_live metadata confirmation is missing" >&2
    exit 1
  fi
  if (( arbitrage_max_plan_cost == 0 \
    || arbitrage_max_recovery_loss == 0 \
    || arbitrage_max_cumulative_loss == 0 \
    || arbitrage_max_cumulative_recovery_loss == 0 \
    || arbitrage_max_total_entries == 0 \
    || arbitrage_max_total_entries > 100 \
    || arbitrage_max_entries_per_minute == 0 \
    || arbitrage_max_entries_per_minute > 10 )); then
    echo "full_live arbitrage risk metadata is outside the launch envelope" >&2
    exit 1
  fi
fi
if [[ "${arbitrage_execution_mode}" == "full_live" ]]; then
  arbitrage_trade_journal="/var/lib/arb-bot/arbitrage-live-trades.jsonl"
else
  arbitrage_trade_journal="/var/lib/arb-bot/arbitrage-paper-trades.jsonl"
fi

token_response="$(curl --fail --silent --show-error \
  --header "${metadata_header}" \
  "${metadata_root}/instance/service-accounts/default/token")"
access_token="$(printf '%s' "${token_response}" \
  | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
if [[ -z "${access_token}" ]]; then
  echo "failed to obtain instance service-account token" >&2
  exit 1
fi

fetch_secret() {
  local name="$1"
  local response encoded
  response="$(curl --fail --silent --show-error \
    --header "Authorization: Bearer ${access_token}" \
    "https://secretmanager.googleapis.com/v1/projects/${project_id}/secrets/${name}/versions/latest:access")"
  encoded="$(printf '%s' "${response}" \
    | sed -n 's/.*"data"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
  if [[ -z "${encoded}" ]]; then
    echo "secret payload is empty: ${name}" >&2
    exit 1
  fi
  printf '%s' "${encoded}" | base64 --decode
}

install -d -m 0700 /etc/arb-bot
install -d -m 0700 -o 65532 -g 65532 /var/lib/arb-bot
env_file=/etc/arb-bot/runtime.env
umask 077
{
  printf 'SERVICE_NAME=arb-bot-rust-shadow\n'
  printf 'ENGINE_ID=%s\n' "${engine_id}"
  printf 'GCP_PROJECT_ID=%s\n' "${project_id}"
  printf 'GCP_REGION=%s\n' "${region}"
  printf 'BINANCE_WS_BASE_URL=wss://stream.binance.com:9443/ws\n'
  printf 'BINANCE_REST_BASE_URL=https://api.binance.com\n'
  printf 'BINANCE_WS_API_URL=wss://ws-api.binance.com:443/ws-api/v3\n'
  printf 'DOMAIN_CONFIG_PATH=%s\n' "${domain_config_path}"
  printf 'MARKET_DATA_MAX_AGE_MS=5000\n'
  printf 'DEX_EVENT_CHANNEL_CAPACITY=8192\n'
  printf 'DEX_HEAD_MAX_AGE_MS=10000\n'
  printf 'BALANCE_SYNC_INTERVAL_MS=1000\n'
  printf 'BALANCE_MAX_AGE_MS=5000\n'
  printf 'BALANCE_EVENT_CHANNEL_CAPACITY=16\n'
  printf 'ARBITRAGE_EXECUTION_MODE=%s\n' "${arbitrage_execution_mode}"
  printf 'ARBITRAGE_LIVE_CONFIRMATION=%s\n' "${arbitrage_live_confirmation}"
  printf 'ARBITRAGE_TRADE_JOURNAL_PATH=%s\n' "${arbitrage_trade_journal}"
  printf 'ARBITRAGE_WALLET_JOURNAL_PATH=/var/lib/arb-bot/arbitrage-wallet.jsonl\n'
  printf 'ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH=/var/lib/arb-bot/arbitrage-binance-orders.jsonl\n'
  printf 'ARBITRAGE_MAX_PLAN_COST_TOKEN_A_BASE_UNITS=%s\n' "${arbitrage_max_plan_cost}"
  printf 'ARBITRAGE_MAX_RECOVERY_LOSS_TOKEN_A_BASE_UNITS=%s\n' "${arbitrage_max_recovery_loss}"
  printf 'ARBITRAGE_MAX_CUMULATIVE_LOSS_TOKEN_A_BASE_UNITS=%s\n' "${arbitrage_max_cumulative_loss}"
  printf 'ARBITRAGE_MAX_CUMULATIVE_RECOVERY_LOSS_TOKEN_A_BASE_UNITS=%s\n' "${arbitrage_max_cumulative_recovery_loss}"
  printf 'ARBITRAGE_MAX_TOTAL_ENTRIES=%s\n' "${arbitrage_max_total_entries}"
  printf 'ARBITRAGE_MAX_ENTRIES_PER_MINUTE=%s\n' "${arbitrage_max_entries_per_minute}"
  printf 'ARBITRAGE_ENTRY_STOP_FILE=/var/lib/arb-bot/arbitrage-entry.stop\n'
  printf 'REBALANCE_EXECUTION_MODE=disabled\n'
  printf 'EVM_WALLET_ADDRESS=%s\n' "${wallet_address}"
  printf 'CLICKHOUSE_DATABASE=arb_bot_prod\n'
  printf 'CLICKHOUSE_USER=default\n'
  printf 'TELEMETRY_CHANNEL_CAPACITY=8192\n'
  printf 'TELEMETRY_BATCH_SIZE=200\n'
  printf 'TELEMETRY_FLUSH_INTERVAL_MS=100\n'
  printf 'RUST_LOG=arb_bot=info\n'
  printf 'CLICKHOUSE_URL='
  fetch_secret CLICKHOUSE_URL
  printf '\nCLICKHOUSE_PASSWORD='
  fetch_secret CLICKHOUSE_PASSWORD
  printf '\nALCHEMY_WORLDCHAIN_RPC_URL='
  fetch_secret ALCHEMY_WORLDCHAIN_RPC_URL
  printf '\nALCHEMY_WORLDCHAIN_WS_URL='
  fetch_secret ALCHEMY_WORLDCHAIN_WS_URL
  printf '\nBINANCE_API_KEY='
  fetch_secret BINANCE_API_KEY
  printf '\nBINANCE_SECRET_KEY='
  fetch_secret BINANCE_SECRET_KEY
  if [[ "${arbitrage_execution_mode}" == "full_live" ]]; then
    printf '\nEVM_WALLET_PRIVATE_KEY='
    fetch_secret EVM_WALLET_PRIVATE_KEY
  fi
  printf '\n'
} >"${env_file}"
chmod 0600 "${env_file}"

registry="${region}-docker.pkg.dev"
export DOCKER_CONFIG=/run/arb-bot/docker
install -d -m 0700 "${DOCKER_CONFIG}"
printf '%s' "${access_token}" \
  | docker login --username oauth2accesstoken --password-stdin \
    "https://${registry}"
docker pull "${image}"
docker logout "https://${registry}"
rm -rf "${DOCKER_CONFIG}"

cat >/etc/systemd/system/arb-bot.service <<EOF
[Unit]
Description=Low-latency arbitrage worker
After=docker.service network-online.target
Requires=docker.service
Wants=network-online.target

[Service]
Type=simple
ExecStartPre=-/usr/bin/docker rm --force arb-bot-rust-shadow
ExecStart=/usr/bin/docker run --rm --name arb-bot-rust-shadow --network host --stop-signal SIGINT --env-file ${env_file} --volume /var/lib/arb-bot:/var/lib/arb-bot --log-driver journald ${image} run
ExecStop=/usr/bin/docker stop --time 20 arb-bot-rust-shadow
Restart=always
RestartSec=1
TimeoutStopSec=30
LimitNOFILE=1048576
Nice=-10
OOMScoreAdjust=-900

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now arb-bot.service
