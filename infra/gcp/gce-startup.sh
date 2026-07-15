#!/bin/bash
set -euo pipefail

metadata_header="Metadata-Flavor: Google"
metadata_root="http://metadata.google.internal/computeMetadata/v1"

metadata() {
  curl --fail --silent --show-error \
    --header "${metadata_header}" \
    "${metadata_root}/instance/attributes/$1"
}

project_id="$(curl --fail --silent --show-error \
  --header "${metadata_header}" \
  "${metadata_root}/project/project-id")"
image="$(metadata arb-bot-image)"
engine_id="$(metadata arb-bot-engine-id)"
region="$(metadata arb-bot-region)"

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
env_file=/etc/arb-bot/runtime.env
umask 077
{
  printf 'SERVICE_NAME=arb-bot-rust-shadow\n'
  printf 'ENGINE_ID=%s\n' "${engine_id}"
  printf 'GCP_PROJECT_ID=%s\n' "${project_id}"
  printf 'GCP_REGION=%s\n' "${region}"
  printf 'BINANCE_WS_BASE_URL=wss://stream.binance.com:9443/ws\n'
  printf 'DOMAIN_CONFIG_PATH=config/strategies/usdc-wld-world-chain.v2.json\n'
  printf 'MARKET_DATA_MAX_AGE_MS=5000\n'
  printf 'DEX_EVENT_CHANNEL_CAPACITY=8192\n'
  printf 'DEX_HEAD_MAX_AGE_MS=10000\n'
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
  printf '\n'
} >"${env_file}"
chmod 0600 "${env_file}"

registry="${region}-docker.pkg.dev"
printf '%s' "${access_token}" \
  | docker login --username oauth2accesstoken --password-stdin \
    "https://${registry}"
docker pull "${image}"

cat >/etc/systemd/system/arb-bot.service <<EOF
[Unit]
Description=Low-latency read-only arbitrage shadow worker
After=docker.service network-online.target
Requires=docker.service
Wants=network-online.target

[Service]
Type=simple
ExecStartPre=-/usr/bin/docker rm --force arb-bot-rust-shadow
ExecStart=/usr/bin/docker run --rm --name arb-bot-rust-shadow --network host --stop-signal SIGINT --env-file ${env_file} --log-driver journald ${image} run
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

