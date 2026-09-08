#!/usr/bin/env bash
set -euo pipefail

# P02: every connection is a private local cluster under /tmp.  No .env,
# Supabase project, API-Football endpoint, service, or production config is read.
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly PG_BIN="/usr/lib/postgresql/18/bin"
readonly WORK_DIR="$(mktemp -d /tmp/football-analytics-competition-sync-policies.XXXXXX)"
readonly DATA_DIR="$WORK_DIR/data"
readonly SOCKET_DIR="$WORK_DIR/socket"
readonly PORT="55460"
readonly REDIS_SOCKET="$WORK_DIR/redis.sock"
readonly CLEAN_DB="fa_competition_sync_policies_clean"
readonly UPGRADE_DB="fa_competition_sync_policies_upgrade"
readonly POLICY_MIGRATION="$ROOT_DIR/supabase/migrations/20260908010000_competition_sync_policies.sql"

source "$ROOT_DIR/scripts/lib/isolated-test-resource.sh"

cleanup() {
  if ! fa_assert_cleanup_resource; then return; fi
  if [[ -f "$DATA_DIR/postmaster.pid" ]]; then
    sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -m fast stop >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

psql_db() {
  local database="$1"
  shift
  fa_assert_pg_database "$database"
  fa_pg_client "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d "$database" "$@"
}

apply_prior_migrations() {
  local database="$1"
  local migration
  while IFS= read -r migration; do
    psql_db "$database" -f "$migration" >/dev/null
  done < <(find "$ROOT_DIR/supabase/migrations" -maxdepth 1 -type f -name '*.sql' ! -name '20260908010000_competition_sync_policies.sql' | sort)
}

apply_policy_and_assertions() {
  local database="$1"
  psql_db "$database" -f "$POLICY_MIGRATION" >/dev/null
  psql_db "$database" -f "$ROOT_DIR/supabase/tests/competition_sync_policies_assertions.sql" >/dev/null
}

mkdir -p "$SOCKET_DIR"
fa_initialize_test_resource "$WORK_DIR" "$SOCKET_DIR" "$PORT" "$REDIS_SOCKET" "$CLEAN_DB" "$UPGRADE_DB"
chown postgres:postgres "$WORK_DIR"
install -d -o postgres -g postgres "$DATA_DIR" "$SOCKET_DIR"
sudo -u postgres "$PG_BIN/initdb" -D "$DATA_DIR" --auth=trust --no-locale --encoding=UTF8 >/dev/null
{
  printf "unix_socket_directories = '%s'\n" "$SOCKET_DIR"
  printf "listen_addresses = ''\n"
  printf "port = %s\n" "$PORT"
} >>"$DATA_DIR/postgresql.conf"
sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$WORK_DIR/postgres.log" start >/dev/null
fa_pg_client "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d postgres -c 'CREATE ROLE anon NOLOGIN' >/dev/null
fa_pg_client "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d postgres -c 'CREATE ROLE authenticated NOLOGIN' >/dev/null

printf 'P02 clean migration on an empty database...\n'
fa_pg_client "$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres "$CLEAN_DB"
apply_prior_migrations "$CLEAN_DB"
apply_policy_and_assertions "$CLEAN_DB"

printf 'P02 additive upgrade from the previous schema...\n'
fa_pg_client "$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres "$UPGRADE_DB"
apply_prior_migrations "$UPGRADE_DB"
apply_policy_and_assertions "$UPGRADE_DB"

readonly TEST_DB_URL="postgresql://postgres@/$UPGRADE_DB?host=$SOCKET_DIR&port=$PORT"
env -u PGHOST -u PGHOSTADDR -u PGPORT -u PGDATABASE -u PGUSER -u PGPASSWORD -u PGPASSFILE -u PGSERVICE -u PGSERVICEFILE -u PGOPTIONS \
  FA_TEST_RESOURCE_MANIFEST="$FA_TEST_RESOURCE_MANIFEST" \
  COMPETITION_SYNC_POLICIES_TEST_DB_URL="$TEST_DB_URL" \
  uv run --directory "$ROOT_DIR/backend" pytest -q tests/test_sync_policies_integration.py

printf 'Competition sync policy P02 migration validation passed.\n'
