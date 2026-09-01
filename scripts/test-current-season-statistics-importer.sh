#!/usr/bin/env bash
set -euo pipefail

# Disposable integration gate.  It never reads .env, calls API-Football, or
# touches the linked Supabase project.
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly PG_BIN="/usr/lib/postgresql/18/bin"
readonly WORK_DIR="$(mktemp -d /tmp/football-analytics-current-season-statistics.XXXXXX)"
readonly DATA_DIR="$WORK_DIR/data"
readonly SOCKET_DIR="$WORK_DIR/socket"
readonly PORT="55451"
readonly DATABASE="fa_current_season_statistics_test"

cleanup() {
  if [[ -f "$DATA_DIR/postmaster.pid" ]]; then
    sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -m fast stop >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

psql_db() {
  "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d "$DATABASE" "$@"
}

chown postgres:postgres "$WORK_DIR"
install -d -o postgres -g postgres "$DATA_DIR" "$SOCKET_DIR"
sudo -u postgres "$PG_BIN/initdb" -D "$DATA_DIR" --auth=trust --no-locale --encoding=UTF8 >/dev/null
{
  printf "unix_socket_directories = '%s'\n" "$SOCKET_DIR"
  printf "listen_addresses = ''\n"
  printf "port = %s\n" "$PORT"
} >>"$DATA_DIR/postgresql.conf"
sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$WORK_DIR/postgres.log" start >/dev/null

"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres postgres >/dev/null 2>&1 || true
"$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d postgres -c 'CREATE ROLE anon NOLOGIN' >/dev/null
"$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d postgres -c 'CREATE ROLE authenticated NOLOGIN' >/dev/null

prepare_database() {
  "$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres "$DATABASE"
  for migration in \
    "$ROOT_DIR/supabase/migrations/20260821193000_stage_3b_core_schema.sql" \
    "$ROOT_DIR/supabase/migrations/20260822010000_fix_standings_child_guard.sql" \
    "$ROOT_DIR/supabase/tests/stage_3d_upgrade_seed.sql"; do
    psql_db -f "$migration" >/dev/null
  done
  psql_db -c "UPDATE football.fixtures SET result_finalized_at='2025-06-01 00:00:00+00'" >/dev/null
  for migration in \
    "$ROOT_DIR/supabase/migrations/20260822210000_multi_competition_foundation.sql" \
    "$ROOT_DIR/supabase/migrations/20260823010000_historical_lineups_foundation.sql" \
    "$ROOT_DIR/supabase/migrations/20260829010000_sync_control_plane_foundation.sql" \
    "$ROOT_DIR/supabase/migrations/20260831224435_batch_fixture_statistics_and_rolling_metrics.sql" \
    "$ROOT_DIR/supabase/migrations/20260901085117_finalize_completed_season_fixture.sql"; do
    psql_db -f "$migration" >/dev/null
  done
}

prepare_database

CURRENT_SEASON_STATISTICS_TEST_DB_URL="postgresql://postgres@/$DATABASE?host=$SOCKET_DIR&port=$PORT" \
  uv run --directory "$ROOT_DIR/backend" pytest -q tests/test_scanner_sample_count_migration.py

"$PG_BIN/dropdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres "$DATABASE"
prepare_database
psql_db -f "$ROOT_DIR/supabase/migrations/20260901193000_scanner_metric_sample_counts.sql" >/dev/null

CURRENT_SEASON_STATISTICS_TEST_DB_URL="postgresql://postgres@/$DATABASE?host=$SOCKET_DIR&port=$PORT" \
  uv run --directory "$ROOT_DIR/backend" pytest -q \
    tests/test_current_season_statistics_integration.py \
    tests/test_scanner_repository_integration.py
printf 'Current-season statistics importer integration validation passed.\n'
