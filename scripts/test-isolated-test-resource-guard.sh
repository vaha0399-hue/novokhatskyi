#!/usr/bin/env bash
set -euo pipefail

# Shell regression checks. They call no database or Redis client: all negative
# cases must fail inside the guard before a connection-capable wrapper runs.

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly WORK_DIR="$(mktemp -d /tmp/football-analytics-isolated-guard-XXXXXX)"
readonly SOCKET_DIR="$WORK_DIR/postgres-socket"
readonly REDIS_SOCKET="$WORK_DIR/redis.sock"
readonly DATABASE="fa_iso_shell_guard"

source "$ROOT_DIR/scripts/lib/isolated-test-resource.sh"

cleanup() {
  fa_assert_cleanup_resource || return 0
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR"
fa_initialize_test_resource "$WORK_DIR" "$SOCKET_DIR" 55490 "$REDIS_SOCKET" "$DATABASE"
readonly ORIGINAL_MANIFEST="$WORK_DIR/manifest.original"
cp "$FA_TEST_RESOURCE_MANIFEST" "$ORIGINAL_MANIFEST"

FA_GUARD_CALL_LOG="$WORK_DIR/guard-calls"
export FA_GUARD_CALL_LOG
printf '%s\n' '#!/usr/bin/env bash' 'printf client >>"$FA_GUARD_CALL_LOG"' >"$WORK_DIR/mock-client"
chmod 700 "$WORK_DIR/mock-client"

expect_failure() {
  local label="$1"
  shift
  if "$@"; then
    printf 'expected guard failure: %s\n' "$label" >&2
    return 1
  fi
}

cleanup_calls=""
redis-cli() { cleanup_calls+=" redis"; }
kill() { cleanup_calls+=" kill"; }
sudo() { cleanup_calls+=" sudo"; }
rm() { cleanup_calls+=" rm"; }
cleanup_probe() {
  if ! fa_assert_cleanup_resource; then return 1; fi
  redis-cli -s "$REDIS_SOCKET" SHUTDOWN NOSAVE
  kill 99999
  sudo true
  rm -rf "$WORK_DIR"
}

assert_tampered_manifest_refusal() {
  local label="$1"
  cleanup_calls=""
  command rm -f "$FA_GUARD_CALL_LOG"
  expect_failure "$label resource" fa_assert_test_resource
  expect_failure "$label cleanup" cleanup_probe
  expect_failure "$label PostgreSQL wrapper" fa_pg_client "$WORK_DIR/mock-client"
  expect_failure "$label Redis wrapper" fa_redis_client "$WORK_DIR/mock-client"
  if [[ -n "$cleanup_calls" || -e "$FA_GUARD_CALL_LOG" ]]; then
    printf 'manifest refusal invoked a cleanup or client command: %s\n' "$label" >&2
    exit 1
  fi
  cp "$ORIGINAL_MANIFEST" "$FA_TEST_RESOURCE_MANIFEST"
}

expect_failure direct fa_assert_pg_database fa_iso_not_created
if fa_assert_pg_database fa_iso_not_created; then
  printf 'guard succeeded inside if\n' >&2
  exit 1
fi
if ! fa_assert_pg_database fa_iso_not_created; then :; else
  printf 'guard succeeded under !\n' >&2
  exit 1
fi
if value="$(fa_pg_url fa_iso_not_created)"; then
  printf 'guard succeeded in command substitution: %s\n' "$value" >&2
  exit 1
fi

printf '{broken\n' >"$FA_TEST_RESOURCE_MANIFEST"
assert_tampered_manifest_refusal corrupted-manifest
: >"$FA_TEST_RESOURCE_MANIFEST"
assert_tampered_manifest_refusal truncated-manifest
manifest_payload="$(<"$ORIGINAL_MANIFEST")"
printf '%s\n' "${manifest_payload/55490/55491}" >"$FA_TEST_RESOURCE_MANIFEST"
assert_tampered_manifest_refusal changed-manifest
fa_assert_test_resource

readonly MANIFEST_DIGEST_FILE="$WORK_DIR/.fa-test-resource-manifest.sha256"
mv "$MANIFEST_DIGEST_FILE" "$WORK_DIR/manifest-digest.saved"
expect_failure missing-manifest-digest fa_assert_test_resource
mv "$WORK_DIR/manifest-digest.saved" "$MANIFEST_DIGEST_FILE"
sha256sum() { return 1; }
expect_failure manifest-digest-command-failure fa_assert_test_resource
unset -f sha256sum

mv "$FA_TEST_RESOURCE_MANIFEST" "$WORK_DIR/manifest.saved"
expect_failure missing-manifest fa_assert_test_resource
mv "$WORK_DIR/manifest.saved" "$FA_TEST_RESOURCE_MANIFEST"

printf '%s\n' wrong-token >"$WORK_DIR/.fa-test-resource"
expect_failure wrong-marker fa_assert_pg_database "$DATABASE"
printf '%s\n' "$FA_TEST_RESOURCE_TOKEN" >"$WORK_DIR/.fa-test-resource"

if (PGHOSTADDR=198.51.100.10 fa_assert_clean_libpq_environment); then
  printf 'guard accepted inherited PGHOSTADDR\n' >&2
  exit 1
fi
if (PGSERVICE=foreign-service fa_assert_clean_libpq_environment); then
  printf 'guard accepted inherited PGSERVICE\n' >&2
  exit 1
fi

command rm "$WORK_DIR/.fa-test-resource"
expect_failure missing-marker fa_assert_pg_database "$DATABASE"
if fa_assert_cleanup_resource; then
  printf 'cleanup guard accepted a missing marker\n' >&2
  exit 1
fi

expect_failure cleanup-refusal cleanup_probe
if [[ -n "$cleanup_calls" ]]; then
  printf 'cleanup invoked a destructive command after guard refusal:%s\n' "$cleanup_calls" >&2
  exit 1
fi
unset -f redis-cli kill sudo rm
printf '%s\n' "$FA_TEST_RESOURCE_TOKEN" >"$WORK_DIR/.fa-test-resource"

if env PGHOSTADDR=198.51.100.10 uv run --directory "$ROOT_DIR/backend" pytest -q tests/test_health.py >/dev/null 2>&1; then
  printf 'direct pytest accepted inherited PGHOSTADDR\n' >&2
  exit 1
fi

printf 'Shell guard regression checks passed.\n'
