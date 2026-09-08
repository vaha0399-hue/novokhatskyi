#!/usr/bin/env bash
set -euo pipefail

# Full disposable-environment gate. It never reads .env, uses Docker, invokes
# API-Football, or opens a TCP listener. Every database client call is guarded
# by a run-owned Unix socket, nonce marker, exact port, and DB allowlist.

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly PG_BIN="/usr/lib/postgresql/18/bin"
readonly WORK_DIR="$(mktemp -d /tmp/football-analytics-isolated-XXXXXX)"
readonly DATA_DIR="$WORK_DIR/postgres-data"
readonly SOCKET_DIR="$WORK_DIR/postgres-socket"
readonly REDIS_SOCKET="$WORK_DIR/redis.sock"
readonly RUN_ID="$(basename "$WORK_DIR" | tr -cd 'a-z0-9')"
readonly PORT="$((56000 + RANDOM % 900))"
readonly CLEAN_DB="fa_iso_${RUN_ID}_clean"
readonly UPGRADE_DB="fa_iso_${RUN_ID}_upgrade"
readonly RESTORE_DB="fa_iso_${RUN_ID}_restore"
readonly BACKUP_FILE="$WORK_DIR/upgrade.backup"
REDIS_PID=""

source "$ROOT_DIR/scripts/lib/isolated-test-resource.sh"

cleanup() {
  local cleanup_failed=0
  if ! fa_assert_cleanup_resource; then
    printf 'refusing cleanup: test resource marker no longer matches %s\n' "$WORK_DIR" >&2
    return
  fi
  if [[ -n "$REDIS_PID" ]] && kill -0 "$REDIS_PID" 2>/dev/null; then
    if [[ -S "$REDIS_SOCKET" ]]; then
      fa_redis_client redis-cli -s "$REDIS_SOCKET" SHUTDOWN NOSAVE >/dev/null 2>&1 || true
    fi
    for _ in {1..20}; do
      kill -0 "$REDIS_PID" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$REDIS_PID" 2>/dev/null; then
      if tr '\0' ' ' <"/proc/$REDIS_PID/cmdline" 2>/dev/null | grep -Fq -- "$REDIS_SOCKET"; then
        kill "$REDIS_PID" >/dev/null 2>&1 || cleanup_failed=1
        wait "$REDIS_PID" >/dev/null 2>&1 || true
      else
        printf 'refusing to signal Redis PID %s: socket identity does not match\n' "$REDIS_PID" >&2
        cleanup_failed=1
      fi
    fi
  fi
  if [[ -f "$DATA_DIR/postmaster.pid" ]]; then
    if ! sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -m fast stop >/dev/null 2>&1 || [[ -f "$DATA_DIR/postmaster.pid" ]]; then
      printf 'PostgreSQL did not stop; retaining test runtime for inspection: %s\n' "$WORK_DIR" >&2
      cleanup_failed=1
    fi
  fi
  if (( cleanup_failed )); then
    return
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR"
fa_initialize_test_resource "$WORK_DIR" "$SOCKET_DIR" "$PORT" "$REDIS_SOCKET" \
  "$CLEAN_DB" "$UPGRADE_DB" "$RESTORE_DB"

pg_control() {
  if ! fa_assert_pg_control_connection; then return 1; fi
  fa_pg_client "$@" -h "$SOCKET_DIR" -p "$PORT" -U postgres
}

psql_db() {
  local database="$1"
  shift
  local database_url
  if ! fa_assert_pg_database "$database"; then return 1; fi
  if ! database_url="$(fa_pg_url "$database")"; then return 1; fi
  fa_pg_client "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 "$database_url" "$@"
}

create_database() {
  local database="$1"
  fa_assert_pg_database "$database"
  pg_control "$PG_BIN/createdb" "$database"
}

apply_migration() {
  local database="$1"
  local migration="$2"
  psql_db "$database" -f "$migration" >/dev/null
}

apply_all_migrations() {
  local database="$1"
  local migration
  while IFS= read -r migration; do
    printf '  apply %s\n' "$(basename "$migration")"
    apply_migration "$database" "$migration"
  done < <(find "$ROOT_DIR/supabase/migrations" -maxdepth 1 -type f -name '*.sql' -print | LC_ALL=C sort)
}

expect_guard_rejects_foreign_destinations() {
  local own_url
  own_url="$(fa_pg_url "$CLEAN_DB")"
  if fa_assert_redis_url 'redis://127.0.0.1:6379/0'; then
    printf 'guard accepted TCP Redis before connection\n' >&2
    return 1
  fi
  if fa_assert_redis_url 'unix:///tmp/foreign/redis.sock'; then
    printf 'guard accepted foreign Redis socket before connection\n' >&2
    return 1
  fi
  if [[ "$own_url" == *'/tmp/foreign/'* ]]; then
    printf 'unexpected runner URL construction\n' >&2
    return 1
  fi
  if fa_assert_pg_database 'fa_iso_not_created'; then
    printf 'guard accepted uncreated database before connection\n' >&2
    return 1
  fi
  printf 'Negative destination guard checks passed before connection.\n'
}

start_postgres() {
  chown postgres:postgres "$WORK_DIR"
  install -d -o postgres -g postgres "$DATA_DIR" "$SOCKET_DIR"
  sudo -u postgres "$PG_BIN/initdb" -D "$DATA_DIR" --auth=trust --no-locale --encoding=UTF8 >/dev/null
  {
    printf "unix_socket_directories = '%s'\n" "$SOCKET_DIR"
    printf "listen_addresses = ''\n"
    printf "port = %s\n" "$PORT"
  } >>"$DATA_DIR/postgresql.conf"
  sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$WORK_DIR/postgres.log" start >/dev/null
  pg_control "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -d postgres -c 'CREATE ROLE anon NOLOGIN' >/dev/null
  pg_control "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -d postgres -c 'CREATE ROLE authenticated NOLOGIN' >/dev/null
}

start_redis() {
  redis-server --save '' --appendonly no --port 0 --unixsocket "$REDIS_SOCKET" \
    --unixsocketperm 700 --logfile "$WORK_DIR/redis.log" &
  REDIS_PID=$!
  for _ in {1..50}; do
    [[ -S "$REDIS_SOCKET" ]] && break
    sleep 0.1
  done
  [[ -S "$REDIS_SOCKET" ]]
  fa_assert_redis_url "unix://$REDIS_SOCKET"
  fa_redis_client redis-cli -s "$REDIS_SOCKET" PING | grep -qx PONG
}

assert_clean_schema() {
  psql_db "$CLEAN_DB" -Atc "SELECT CASE WHEN to_regclass('football.fixture_statistics_coverage') IS NOT NULL AND to_regclass('ops.provider_daily_request_usage') IS NOT NULL AND to_regclass('source.fixture_status_code_mappings') IS NOT NULL THEN 'clean-schema-ok' ELSE 'clean-schema-failed' END" | grep -qx clean-schema-ok
}

assert_upgrade_preserves_synthetic_data() {
  psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/stage_3d_upgrade_assertions.sql" >/dev/null
  psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/historical_lineups_upgrade_assertions.sql" >/dev/null
  printf 'Synthetic previous-schema preservation assertions passed.\n'
}

schema_fingerprint() {
  local database="$1"
  local database_url
  if ! fa_assert_pg_database "$database"; then return 1; fi
  if ! database_url="$(fa_pg_url "$database")"; then return 1; fi
  fa_pg_client "$PG_BIN/pg_dump" --schema-only --no-owner --no-privileges "$database_url" \
    | sed -e '/^\\connect /d' -e '/^\\restrict /d' -e '/^\\unrestrict /d' \
    | sha256sum | awk '{print $1}'
}

key_data_fingerprint() {
  local database="$1"
  psql_db "$database" -Atc "SELECT md5(coalesce(string_agg(entry, E'\\n' ORDER BY entry), '')) FROM (SELECT 'league:' || row_to_json(row)::text AS entry FROM football.leagues row UNION ALL SELECT 'team:' || row_to_json(row)::text FROM football.teams row UNION ALL SELECT 'fixture:' || row_to_json(row)::text FROM football.fixtures row UNION ALL SELECT 'fixture-statistics:' || row_to_json(row)::text FROM football.fixture_team_statistics row UNION ALL SELECT 'league-ref:' || row_to_json(row)::text FROM source.league_provider_refs row UNION ALL SELECT 'season-ref:' || row_to_json(row)::text FROM source.season_provider_refs row UNION ALL SELECT 'team-ref:' || row_to_json(row)::text FROM source.team_provider_refs row UNION ALL SELECT 'fixture-ref:' || row_to_json(row)::text FROM source.fixture_provider_refs row UNION ALL SELECT 'provider-fetch:' || row_to_json(row)::text FROM source.provider_fetches row UNION ALL SELECT 'standing:' || row_to_json(row)::text FROM football.standings_snapshots row) key_rows"
}

constraint_fingerprint() {
  local database="$1"
  psql_db "$database" -Atc "SELECT md5(coalesce(string_agg(n.nspname || '.' || c.relname || ':' || pg_get_constraintdef(k.oid), E'\\n' ORDER BY n.nspname,c.relname,k.conname), '')) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('football','source','ops')"
}

verify_backup_restore() {
  fa_assert_pg_database "$UPGRADE_DB"
  fa_assert_pg_database "$RESTORE_DB"
  printf 'Create custom-format backup...\n'
  fa_pg_client "$PG_BIN/pg_dump" --format=custom --file="$BACKUP_FILE" "$(fa_pg_url "$UPGRADE_DB")"
  [[ -s "$BACKUP_FILE" ]]
  printf 'Restore backup into a second disposable database...\n'
  create_database "$RESTORE_DB"
  fa_pg_client "$PG_BIN/pg_restore" --exit-on-error --no-owner --no-privileges -d "$(fa_pg_url "$RESTORE_DB")" "$BACKUP_FILE"
  printf 'Compare restored schema...\n'
  [[ "$(schema_fingerprint "$UPGRADE_DB")" == "$(schema_fingerprint "$RESTORE_DB")" ]]
  printf 'Compare restored key records...\n'
  [[ "$(key_data_fingerprint "$UPGRADE_DB")" == "$(key_data_fingerprint "$RESTORE_DB")" ]]
  printf 'Compare restored constraints...\n'
  [[ "$(constraint_fingerprint "$UPGRADE_DB")" == "$(constraint_fingerprint "$RESTORE_DB")" ]]
  printf 'Backup/restore schema, key records, and constraints match.\n'
}

run_safe_integrations() {
  local database_url redis_url
  database_url="$(fa_pg_url "$UPGRADE_DB")"
  redis_url="unix://$REDIS_SOCKET"
  fa_assert_redis_url "$redis_url"
  env -u API_FOOTBALL_KEY -u SUPABASE_DB_URL -u REDIS_URL \
    -u PGHOST -u PGHOSTADDR -u PGPORT -u PGDATABASE -u PGUSER -u PGPASSWORD \
    -u PGPASSFILE -u PGSERVICE -u PGSERVICEFILE -u PGOPTIONS \
    FA_TEST_RESOURCE_MANIFEST="$FA_TEST_RESOURCE_MANIFEST" \
    ANALYTICS_TEST_DB_URL="$database_url" \
    READ_API_TEST_DB_URL="$database_url" \
    CURRENT_SEASON_STATISTICS_TEST_DB_URL="$database_url" \
    API_FOOTBALL_BUDGET_TEST_DB_URL="$database_url" \
    LIVE_REDIS_TEST_URL="$redis_url" \
    uv run --directory "$ROOT_DIR/backend" pytest -q \
      tests/test_analytics_repository_integration.py \
      tests/test_web_read_dependencies_integration.py \
      tests/test_scanner_repository_integration.py \
      tests/test_live_store_integration.py \
      tests/test_web_live_api_integration.py \
      tests/test_api_football_budget_integration.py
  printf '%s\n' 'Skipped test_web_read_api_integration.py: the synthetic upgrade fixture has no standings snapshot and cannot satisfy that development-data contract.'
}

run_backend_suite() {
  local redis_url
  redis_url="unix://$REDIS_SOCKET"
  fa_assert_redis_url "$redis_url"
  env -u ACTIVE_SEASON_TEST_DB_URL -u ANALYTICS_TEST_DB_URL -u BACKFILL_TEST_DB_URL \
    -u CURRENT_SEASON_STATISTICS_TEST_DB_URL -u HISTORICAL_LINEUPS_TEST_DB_URL \
    -u LIVE_DOMAIN_TEST_DB_URL -u LIVE_REDIS_TEST_URL -u LIVE_WORKER_TEST_DB_URL \
    -u READ_API_TEST_DB_URL -u SEASON_BOOTSTRAP_TEST_DB_URL -u API_FOOTBALL_KEY \
    -u SUPABASE_DB_URL -u PGHOST -u PGHOSTADDR -u PGPORT -u PGDATABASE -u PGUSER \
    -u PGPASSWORD -u PGPASSFILE -u PGSERVICE -u PGSERVICEFILE -u PGOPTIONS \
    FA_TEST_RESOURCE_MANIFEST="$FA_TEST_RESOURCE_MANIFEST" REDIS_URL="$redis_url" \
    uv run --directory "$ROOT_DIR/backend" pytest -q
}

expect_guard_rejects_foreign_destinations
start_postgres
start_redis

printf 'Apply all migrations to empty disposable database...\n'
create_database "$CLEAN_DB"
apply_all_migrations "$CLEAN_DB"
assert_clean_schema

printf 'Upgrade synthetic previous schema through all migrations...\n'
create_database "$UPGRADE_DB"
apply_migration "$UPGRADE_DB" "$ROOT_DIR/supabase/migrations/20260821193000_stage_3b_core_schema.sql"
apply_migration "$UPGRADE_DB" "$ROOT_DIR/supabase/migrations/20260822010000_fix_standings_child_guard.sql"
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/stage_3d_upgrade_seed.sql" >/dev/null
while IFS= read -r migration; do
  [[ "$(basename "$migration")" > "20260822010000_fix_standings_child_guard.sql" ]] || continue
  printf '  upgrade %s\n' "$(basename "$migration")"
  apply_migration "$UPGRADE_DB" "$migration"
done < <(find "$ROOT_DIR/supabase/migrations" -maxdepth 1 -type f -name '*.sql' -print | LC_ALL=C sort)
assert_upgrade_preserves_synthetic_data
verify_backup_restore

printf 'Run selected integration tests with owned resource manifest...\n'
run_safe_integrations
printf 'Run complete backend suite with integration destinations unset and Redis isolated...\n'
run_backend_suite
printf 'Isolated environment validation passed.\n'
