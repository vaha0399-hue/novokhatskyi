#!/usr/bin/env bash
set -euo pipefail

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_RUNTIME="$(mktemp -d /tmp/football-analytics-live-rest.XXXXXX)"
REDIS_SOCKET="${TEST_RUNTIME}/redis.sock"
POSTGRES_SOCKET_DIR="${TEST_RUNTIME}/postgres-socket"
REDIS_PID=""

source "$REPOSITORY_ROOT/scripts/lib/isolated-test-resource.sh"

cleanup() {
  if ! fa_assert_cleanup_resource; then return; fi
  if [[ -n "${REDIS_PID}" ]]; then
    kill "${REDIS_PID}" 2>/dev/null || true
    wait "${REDIS_PID}" 2>/dev/null || true
  fi
  rm -rf "${TEST_RUNTIME}"
}
trap cleanup EXIT

mkdir -p "$POSTGRES_SOCKET_DIR"
fa_initialize_test_resource "$TEST_RUNTIME" "$POSTGRES_SOCKET_DIR" 55445 "$REDIS_SOCKET" fa_live_rest_guard

redis-server \
  --save "" \
  --appendonly no \
  --port 0 \
  --unixsocket "${REDIS_SOCKET}" \
  --unixsocketperm 700 \
  --logfile "${TEST_RUNTIME}/redis.log" &
REDIS_PID=$!

for _ in {1..50}; do
  if [[ -S "${REDIS_SOCKET}" ]]; then
    break
  fi
  sleep 0.1
done

if [[ ! -S "${REDIS_SOCKET}" ]]; then
  if [[ -f "${TEST_RUNTIME}/redis.log" ]]; then
    sed -n '1,120p' "${TEST_RUNTIME}/redis.log" >&2
  fi
  echo "temporary Redis did not create its socket" >&2
  exit 1
fi
fa_assert_redis_url "unix://${REDIS_SOCKET}"

cd "${REPOSITORY_ROOT}/backend"
env -u PGHOST -u PGHOSTADDR -u PGPORT -u PGDATABASE -u PGUSER -u PGPASSWORD -u PGPASSFILE -u PGSERVICE -u PGSERVICEFILE -u PGOPTIONS \
  FA_TEST_RESOURCE_MANIFEST="$FA_TEST_RESOURCE_MANIFEST" \
  LIVE_REDIS_TEST_URL="unix://${REDIS_SOCKET}" \
  uv run pytest -q tests/test_live_store_integration.py tests/test_web_live_api_integration.py
