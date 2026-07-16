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

install -d -m 0700 /var/lib/arb-bot-test
env_file=/var/lib/arb-bot-test/binance.env
umask 077
{
  printf 'SERVICE_NAME=arb-bot-binance-test\n'
  printf 'ENGINE_ID=arb-bot-binance-test\n'
  printf 'GCP_PROJECT_ID=%s\n' "${project_id}"
  printf 'GCP_REGION=%s\n' "${region}"
  printf 'BINANCE_WS_BASE_URL=wss://stream.binance.com:9443/ws\n'
  printf 'BINANCE_REST_BASE_URL=https://api.binance.com\n'
  printf 'BINANCE_WS_API_URL=wss://ws-api.binance.com:443/ws-api/v3\n'
  printf 'DOMAIN_CONFIG_PATH=config/strategies/usdc-wld-world-chain.v2.json\n'
  printf 'RUST_LOG=arb_bot=info\n'
  printf 'BINANCE_API_KEY='
  fetch_secret BINANCE_API_KEY
  printf '\nBINANCE_SECRET_KEY='
  fetch_secret BINANCE_SECRET_KEY
  printf '\n'
} >"${env_file}"
chmod 0600 "${env_file}"

registry="${region}-docker.pkg.dev"
export DOCKER_CONFIG=/run/arb-bot-test/docker
install -d -m 0700 "${DOCKER_CONFIG}"
printf '%s' "${access_token}" \
  | docker login --username oauth2accesstoken --password-stdin \
    "https://${registry}"
docker pull "${image}"
docker logout "https://${registry}"
rm -rf "${DOCKER_CONFIG}"

cat >/var/lib/arb-bot-test/arb-bot-test <<'EOF'
#!/bin/bash
set -euo pipefail

case "${1:-}" in
  binance-account|binance-capital|binance-recent-validation-orders|binance-withdrawal-status|binance-travel-rule-withdrawal-status)
    ;;
  *)
    echo "only read-only Binance diagnostic commands are permitted" >&2
    exit 2
    ;;
esac

metadata_header="Metadata-Flavor: Google"
metadata_url="http://metadata.google.internal/computeMetadata/v1/instance/attributes/arb-bot-image"
image="$(curl --fail --silent --show-error --header "${metadata_header}" "${metadata_url}")"

exec docker run --rm \
  --network host \
  --env-file /var/lib/arb-bot-test/binance.env \
  "${image}" \
  "$@"
EOF
chmod 0755 /var/lib/arb-bot-test/arb-bot-test

echo "Binance test VM is ready; use sudo /bin/bash /var/lib/arb-bot-test/arb-bot-test <read-only-command>"
