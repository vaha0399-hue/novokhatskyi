#!/usr/bin/env bash
set -euo pipefail

# P02 proof for Q02: clean install, then additive upgrade with a pre-Q02 row.
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly PG_BIN="/usr/lib/postgresql/18/bin"
readonly WORK_DIR="$(mktemp -d /tmp/football-analytics-repeatable-queue.XXXXXX)"
readonly DATA_DIR="$WORK_DIR/data"
readonly SOCKET_DIR="$WORK_DIR/socket"
readonly PORT="55462"
readonly CLEAN_DB="fa_repeatable_queue_clean"
readonly UPGRADE_DB="fa_repeatable_queue_upgrade"
readonly MIGRATION="$ROOT_DIR/supabase/migrations/20260908020000_repeatable_sync_queue.sql"
readonly LEASE_MIGRATION="$ROOT_DIR/supabase/migrations/20260908030000_repeatable_sync_work_item_leases.sql"
source "$ROOT_DIR/scripts/lib/isolated-test-resource.sh"

cleanup() {
  fa_assert_cleanup_resource || return
  [[ ! -f "$DATA_DIR/postmaster.pid" ]] || sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -m fast stop >/dev/null 2>&1 || true
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

psql_db() { local database="$1"; shift; fa_assert_pg_database "$database"; fa_pg_client "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d "$database" "$@"; }
apply_before_q02() { local database="$1" migration; while IFS= read -r migration; do psql_db "$database" -f "$migration" >/dev/null; done < <(find "$ROOT_DIR/supabase/migrations" -maxdepth 1 -type f -name '*.sql' ! -name '20260908020000_repeatable_sync_queue.sql' ! -name '20260908030000_repeatable_sync_work_item_leases.sql' | LC_ALL=C sort); }
apply_q02() { local database="$1"; psql_db "$database" -f "$MIGRATION" >/dev/null; psql_db "$database" -f "$LEASE_MIGRATION" >/dev/null; psql_db "$database" -f "$ROOT_DIR/supabase/tests/repeatable_sync_queue_assertions.sql" >/dev/null; }

mkdir -p "$SOCKET_DIR"
fa_initialize_test_resource "$WORK_DIR" "$SOCKET_DIR" "$PORT" "$WORK_DIR/redis.sock" "$CLEAN_DB" "$UPGRADE_DB"
chown postgres:postgres "$WORK_DIR"
install -d -o postgres -g postgres "$DATA_DIR" "$SOCKET_DIR"
sudo -u postgres "$PG_BIN/initdb" -D "$DATA_DIR" --auth=trust --no-locale --encoding=UTF8 >/dev/null
printf "unix_socket_directories = '%s'\nlisten_addresses = ''\nport = %s\n" "$SOCKET_DIR" "$PORT" >> "$DATA_DIR/postgresql.conf"
sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$WORK_DIR/postgres.log" start >/dev/null
fa_pg_client "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d postgres -c 'CREATE ROLE anon NOLOGIN; CREATE ROLE authenticated NOLOGIN' >/dev/null

fa_pg_client "$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres "$CLEAN_DB"
apply_before_q02 "$CLEAN_DB"; apply_q02 "$CLEAN_DB"
fa_pg_client "$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres "$UPGRADE_DB"
apply_before_q02 "$UPGRADE_DB"
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/repeatable_sync_queue_upgrade_seed.sql" >/dev/null
apply_q02 "$UPGRADE_DB"
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/repeatable_sync_queue_upgrade_assertions.sql" >/dev/null

readonly TEST_DB_URL="postgresql://postgres@/$UPGRADE_DB?host=$SOCKET_DIR&port=$PORT"
env -u PGHOST -u PGHOSTADDR -u PGPORT -u PGDATABASE -u PGUSER -u PGPASSWORD -u PGPASSFILE -u PGSERVICE -u PGSERVICEFILE -u PGOPTIONS \
  FA_TEST_RESOURCE_MANIFEST="$FA_TEST_RESOURCE_MANIFEST" REPEATABLE_SYNC_QUEUE_TEST_DB_URL="$TEST_DB_URL" \
  uv run --directory "$ROOT_DIR/backend" pytest -q tests/test_repeatable_sync_queue_integration.py
printf 'Repeatable sync queue P02 migration validation passed.\n'
