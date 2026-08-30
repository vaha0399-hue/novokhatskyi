#!/usr/bin/env bash
set -euo pipefail

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_RUNTIME="$(mktemp -d /tmp/football-live-rest.XXXXXX)"
REDIS_SOCKET="${TEST_RUNTIME}/redis.sock"
REDIS_PID=""

cleanup() {
  if [[ -n "${REDIS_PID}" ]]; then
    kill "${REDIS_PID}" 2>/dev/null || true
    wait "${REDIS_PID}" 2>/dev/null || true
  fi
  rm -rf "${TEST_RUNTIME}"
}
trap cleanup EXIT

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

cd "${REPOSITORY_ROOT}/backend"
LIVE_REDIS_TEST_URL="unix://${REDIS_SOCKET}" \
  uv run pytest -q tests/test_live_store_integration.py tests/test_web_live_api_integration.py
