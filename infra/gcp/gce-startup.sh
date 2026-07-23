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
domain_config_path="$(metadata_or_default arb-bot-domain-config-path config/strategies/usdc-wld-world-chain.v6.json)"
rebalance_execution_mode="$(metadata_or_default arb-bot-rebalance-execution-mode disabled)"
rebalance_live_confirmation="$(metadata_or_default arb-bot-rebalance-live-confirmation '')"
rebalance_max_wld_amount="$(metadata_or_default arb-bot-rebalance-max-wld-amount 0)"
rebalance_max_usdc_amount="$(metadata_or_default arb-bot-rebalance-max-usdc-amount 0)"
rebalance_binance_withdrawal_api_mode="$(metadata_or_default arb-bot-rebalance-binance-withdrawal-api-mode standard)"

if [[ ! "${rebalance_execution_mode}" =~ ^(disabled|full_live)$ ]]; then
  echo "invalid arb-bot-rebalance-execution-mode metadata" >&2
  exit 1
fi
if [[ ! "${rebalance_binance_withdrawal_api_mode}" =~ ^(standard|travel_rule)$ ]]; then
  echo "invalid arb-bot-rebalance-binance-withdrawal-api-mode metadata" >&2
  exit 1
fi
if [[ ! "${domain_config_path}" =~ ^config/strategies/[a-z0-9.-]+\.json$ ]]; then
  echo "invalid arb-bot-domain-config-path metadata" >&2
  exit 1
fi
if [[ "${domain_config_path}" != "config/strategies/usdc-wld-world-chain.v6.json" ]]; then
  echo "production requires the reviewed v6 domain artifact" >&2
  exit 1
fi
if [[ "${rebalance_execution_mode}" == "full_live" ]]; then
  if [[ "${rebalance_live_confirmation}" != "ENABLE_FULL_REBALANCE" ]]; then
    echo "full_live rebalance metadata confirmation is missing" >&2
    exit 1
  fi
  if [[ ! "${rebalance_max_wld_amount}" =~ ^[0-9]+([.][0-9]+)?$ || ! "${rebalance_max_usdc_amount}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "full_live rebalance maximum metadata is invalid" >&2
    exit 1
  fi
  if [[ "${rebalance_max_wld_amount}" == "0" || "${rebalance_max_usdc_amount}" == "0" ]]; then
    echo "full_live rebalance requires positive maximum metadata" >&2
    exit 1
  fi
fi
arbitrage_trade_journal="/var/lib/arb-bot/arbitrage-live-trades.jsonl"

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
  printf 'GAS_PRICE_MAX_TRANSPORT_SILENCE_MS=5000\n'
  printf 'DEX_EVENT_CHANNEL_CAPACITY=8192\n'
  printf 'DEX_HEAD_MAX_AGE_MS=10000\n'
  printf 'BALANCE_SYNC_INTERVAL_MS=1000\n'
  printf 'BALANCE_MAX_AGE_MS=5000\n'
  printf 'BALANCE_EVENT_CHANNEL_CAPACITY=16\n'
  printf 'ARBITRAGE_EXECUTION_MODE=full_live\n'
  printf 'ARBITRAGE_TRADE_JOURNAL_PATH=%s\n' "${arbitrage_trade_journal}"
  printf 'ARBITRAGE_WALLET_JOURNAL_PATH=/var/lib/arb-bot/arbitrage-wallet.jsonl\n'
  printf 'ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH=/var/lib/arb-bot/arbitrage-binance-orders.jsonl\n'
  printf 'ARBITRAGE_ENTRY_STOP_FILE=/var/lib/arb-bot/arbitrage-entry.stop\n'
  printf 'REBALANCE_EXECUTION_MODE=%s\n' "${rebalance_execution_mode}"
  printf 'REBALANCE_EXECUTOR_JOURNAL_PATH=/var/lib/arb-bot/rebalance-executor.jsonl\n'
  printf 'REBALANCE_EXECUTOR_TIMEOUT_SECONDS=1800\n'
  printf 'REBALANCE_MAX_WLD_AMOUNT=%s\n' "${rebalance_max_wld_amount}"
  printf 'REBALANCE_MAX_USDC_AMOUNT=%s\n' "${rebalance_max_usdc_amount}"
  printf 'REBALANCE_LIVE_CONFIRMATION=%s\n' "${rebalance_live_confirmation}"
  printf 'REBALANCE_BINANCE_WITHDRAWAL_API_MODE=%s\n' "${rebalance_binance_withdrawal_api_mode}"
  printf 'EVM_WALLET_JOURNAL_PATH=/var/lib/arb-bot/rebalance-wallet.jsonl\n'
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
  if [[ "${rebalance_execution_mode}" == "full_live" ]]; then
    printf '\nALCHEMY_OPTIMISM_RPC_URL='
    fetch_secret ALCHEMY_OPTIMISM_RPC_URL
  fi
  printf '\nBINANCE_API_KEY='
  fetch_secret BINANCE_API_KEY
  printf '\nBINANCE_SECRET_KEY='
  fetch_secret BINANCE_SECRET_KEY
  if [[ "${rebalance_execution_mode}" == "full_live" ]]; then
    printf '\nBINANCE_TREASURY_API_KEY='
    fetch_secret BINANCE_TREASURY_API_KEY
    printf '\nBINANCE_TREASURY_SECRET_KEY='
    fetch_secret BINANCE_TREASURY_SECRET_KEY
    printf '\nBINANCE_SUBACCOUNT_EMAIL='
    fetch_secret BINANCE_SUBACCOUNT_EMAIL
  fi
  printf '\nEVM_WALLET_PRIVATE_KEY='
  fetch_secret EVM_WALLET_PRIVATE_KEY
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
ExecStart=/usr/bin/docker run --rm --name arb-bot-rust-shadow --network host --stop-signal SIGINT --env-file ${env_file} --volume /var/lib/arb-bot:/var/lib/arb-bot --log-driver gcplogs --log-opt gcp-project=${project_id} ${image} run
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
systemctl enable arb-bot.service
systemctl restart arb-bot.service
