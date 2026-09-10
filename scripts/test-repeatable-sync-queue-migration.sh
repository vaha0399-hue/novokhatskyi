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
readonly HARDENING_MIGRATION="$ROOT_DIR/supabase/migrations/20260908040000_q03_lease_hardening.sql"
readonly Q05_MIGRATION="$ROOT_DIR/supabase/migrations/20260909010000_q05_scheduler_checkpoints.sql"
readonly Q05_POLICY_LOCK_MIGRATION="$ROOT_DIR/supabase/migrations/20260909020000_q05_scheduler_policy_lock.sql"
readonly Q05_WINDOW_MIGRATION="$ROOT_DIR/supabase/migrations/20260909030000_q05_scheduler_window_checkpoint_contract.sql"
readonly Q05_EVENT_MIGRATION="$ROOT_DIR/supabase/migrations/20260909040000_q05_scheduler_event_checkpoints.sql"
readonly Q05_ANALYTICS_MIGRATION="$ROOT_DIR/supabase/migrations/20260909050000_q05_analytics_enqueue.sql"
readonly Q05_ANALYTICS_WINDOW_MIGRATION="$ROOT_DIR/supabase/migrations/20260909180000_q05_analytics_window_checkpoints.sql"
readonly Q05_LATE_ANALYTICS_WINDOW_MIGRATION="$ROOT_DIR/supabase/migrations/20260909200000_q05_late_analytics_windows.sql"
readonly Q05_ANALYTICS_DEADLINE_MIGRATION="$ROOT_DIR/supabase/migrations/20260909210000_q05_analytics_deadlines.sql"
readonly Q05_FIXTURE_SCHEDULE_OBSERVATION_MIGRATION="$ROOT_DIR/supabase/migrations/20260910003852_q05_fixture_fetch_observations.sql"
source "$ROOT_DIR/scripts/lib/isolated-test-resource.sh"

cleanup() {
  fa_assert_cleanup_resource || return
  [[ ! -f "$DATA_DIR/postmaster.pid" ]] || sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -m fast stop >/dev/null 2>&1 || true
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

psql_db() { local database="$1"; shift; fa_assert_pg_database "$database"; fa_pg_client "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d "$database" "$@"; }
apply_before_q02() { local database="$1" migration; while IFS= read -r migration; do psql_db "$database" -f "$migration" >/dev/null; done < <(find "$ROOT_DIR/supabase/migrations" -maxdepth 1 -type f -name '*.sql' ! -name '20260908020000_repeatable_sync_queue.sql' ! -name '20260908030000_repeatable_sync_work_item_leases.sql' ! -name '20260908040000_q03_lease_hardening.sql' ! -name '20260909010000_q05_scheduler_checkpoints.sql' ! -name '20260909020000_q05_scheduler_policy_lock.sql' ! -name '20260909030000_q05_scheduler_window_checkpoint_contract.sql' ! -name '20260909040000_q05_scheduler_event_checkpoints.sql' ! -name '20260909050000_q05_analytics_enqueue.sql' ! -name '20260909180000_q05_analytics_window_checkpoints.sql' ! -name '20260909200000_q05_late_analytics_windows.sql' ! -name '20260909210000_q05_analytics_deadlines.sql' ! -name '20260910003852_q05_fixture_fetch_observations.sql' | LC_ALL=C sort); }
apply_q03() { local database="$1"; psql_db "$database" -f "$MIGRATION" >/dev/null; psql_db "$database" -f "$LEASE_MIGRATION" >/dev/null; }
apply_hardening() { local database="$1"; psql_db "$database" -f "$HARDENING_MIGRATION" >/dev/null; psql_db "$database" -f "$ROOT_DIR/supabase/tests/repeatable_sync_queue_assertions.sql" >/dev/null; }
apply_q05() { local database="$1"; psql_db "$database" -f "$Q05_MIGRATION" >/dev/null; psql_db "$database" -f "$ROOT_DIR/supabase/tests/q05_scheduler_assertions.sql" >/dev/null; }
apply_q05_policy_lock() { local database="$1"; psql_db "$database" -f "$Q05_POLICY_LOCK_MIGRATION" >/dev/null; psql_db "$database" -f "$ROOT_DIR/supabase/tests/q05_scheduler_policy_lock_assertions.sql" >/dev/null; }
apply_q05_window_contract() { local database="$1"; psql_db "$database" -f "$Q05_WINDOW_MIGRATION" >/dev/null; psql_db "$database" -f "$ROOT_DIR/supabase/tests/q05_scheduler_window_assertions.sql" >/dev/null; }
apply_q05_event_checkpoint() { local database="$1"; psql_db "$database" -f "$Q05_EVENT_MIGRATION" >/dev/null; }
apply_q05_analytics() { local database="$1"; psql_db "$database" -f "$Q05_ANALYTICS_MIGRATION" >/dev/null; }
apply_q05_analytics_window() { local database="$1"; psql_db "$database" -f "$Q05_ANALYTICS_WINDOW_MIGRATION" >/dev/null; }
apply_q05_late_analytics_window() { local database="$1"; psql_db "$database" -f "$Q05_LATE_ANALYTICS_WINDOW_MIGRATION" >/dev/null; }
apply_q05_analytics_deadline() { local database="$1"; psql_db "$database" -f "$Q05_ANALYTICS_DEADLINE_MIGRATION" >/dev/null; }
apply_q05_fixture_schedule_observation() { local database="$1"; psql_db "$database" -f "$Q05_FIXTURE_SCHEDULE_OBSERVATION_MIGRATION" >/dev/null; }

mkdir -p "$SOCKET_DIR"
fa_initialize_test_resource "$WORK_DIR" "$SOCKET_DIR" "$PORT" "$WORK_DIR/redis.sock" "$CLEAN_DB" "$UPGRADE_DB"
chown postgres:postgres "$WORK_DIR"
install -d -o postgres -g postgres "$DATA_DIR" "$SOCKET_DIR"
sudo -u postgres "$PG_BIN/initdb" -D "$DATA_DIR" --auth=trust --no-locale --encoding=UTF8 >/dev/null
printf "unix_socket_directories = '%s'\nlisten_addresses = ''\nport = %s\n" "$SOCKET_DIR" "$PORT" >> "$DATA_DIR/postgresql.conf"
sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$WORK_DIR/postgres.log" start >/dev/null
fa_pg_client "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d postgres -c 'CREATE ROLE anon NOLOGIN; CREATE ROLE authenticated NOLOGIN' >/dev/null

fa_pg_client "$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres "$CLEAN_DB"
apply_before_q02 "$CLEAN_DB"; apply_q03 "$CLEAN_DB"; apply_hardening "$CLEAN_DB"; apply_q05 "$CLEAN_DB"; apply_q05_policy_lock "$CLEAN_DB"; apply_q05_window_contract "$CLEAN_DB"; apply_q05_event_checkpoint "$CLEAN_DB"
apply_q05_analytics "$CLEAN_DB"
apply_q05_analytics_window "$CLEAN_DB"
apply_q05_late_analytics_window "$CLEAN_DB"
apply_q05_analytics_deadline "$CLEAN_DB"
apply_q05_fixture_schedule_observation "$CLEAN_DB"
psql_db "$CLEAN_DB" -f "$ROOT_DIR/supabase/tests/q05_fixture_schedule_observations_assertions.sql" >/dev/null
fa_pg_client "$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres "$UPGRADE_DB"
apply_before_q02 "$UPGRADE_DB"
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/repeatable_sync_queue_upgrade_seed.sql" >/dev/null
apply_q03 "$UPGRADE_DB"
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/repeatable_sync_queue_q03_upgrade_seed.sql" >/dev/null
apply_hardening "$UPGRADE_DB"
apply_q05 "$UPGRADE_DB"
apply_q05_policy_lock "$UPGRADE_DB"
apply_q05_window_contract "$UPGRADE_DB"
apply_q05_event_checkpoint "$UPGRADE_DB"
apply_q05_analytics "$UPGRADE_DB"
apply_q05_analytics_window "$UPGRADE_DB"
apply_q05_late_analytics_window "$UPGRADE_DB"
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/q05_analytics_deadline_upgrade_seed.sql" >/dev/null
apply_q05_analytics_deadline "$UPGRADE_DB"
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/q05_analytics_deadline_upgrade_assertions.sql" >/dev/null
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/q05_fixture_schedule_observations_upgrade_seed.sql" >/dev/null
apply_q05_fixture_schedule_observation "$UPGRADE_DB"
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/q05_fixture_schedule_observations_upgrade_assertions.sql" >/dev/null
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/repeatable_sync_queue_upgrade_assertions.sql" >/dev/null
psql_db "$UPGRADE_DB" -f "$ROOT_DIR/supabase/tests/repeatable_sync_queue_q03_upgrade_assertions.sql" >/dev/null

readonly TEST_DB_URL="postgresql://postgres@/$UPGRADE_DB?host=$SOCKET_DIR&port=$PORT"
env -u PGHOST -u PGHOSTADDR -u PGPORT -u PGDATABASE -u PGUSER -u PGPASSWORD -u PGPASSFILE -u PGSERVICE -u PGSERVICEFILE -u PGOPTIONS \
  FA_TEST_RESOURCE_MANIFEST="$FA_TEST_RESOURCE_MANIFEST" REPEATABLE_SYNC_QUEUE_TEST_DB_URL="$TEST_DB_URL" \
  uv run --directory "$ROOT_DIR/backend" pytest -q tests/test_repeatable_sync_queue_integration.py
printf 'Repeatable sync queue P02 migration validation passed.\n'
