#!/usr/bin/env bash
set -euo pipefail

# Disposable PostgreSQL gate for active-season schedule refreshes. It never
# reads .env, contacts API-Football, or connects to Supabase production.
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly PG_BIN="/usr/lib/postgresql/18/bin"
readonly WORK_DIR="$(mktemp -d /tmp/football-analytics-active-season.XXXXXX)"
readonly DATA_DIR="$WORK_DIR/data"
readonly SOCKET_DIR="$WORK_DIR/socket"
readonly PORT="55454"
readonly DATABASE="fa_active_season_test"

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

"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres "$DATABASE"
psql_db -c "CREATE ROLE anon NOLOGIN" >/dev/null
psql_db -c "CREATE ROLE authenticated NOLOGIN" >/dev/null
for migration in \
  "$ROOT_DIR/supabase/migrations/20260821193000_stage_3b_core_schema.sql" \
  "$ROOT_DIR/supabase/migrations/20260822010000_fix_standings_child_guard.sql"
do
  psql_db -f "$migration" >/dev/null
done
psql_db -c "INSERT INTO source.providers(code,name) VALUES ('api-football','API-Football')" >/dev/null
for migration in \
  "$ROOT_DIR/supabase/migrations/20260822210000_multi_competition_foundation.sql" \
  "$ROOT_DIR/supabase/migrations/20260823010000_historical_lineups_foundation.sql" \
  "$ROOT_DIR/supabase/migrations/20260902014134_add_postponed_fixture_status_mapping.sql" \
  "$ROOT_DIR/supabase/migrations/20260902014917_allow_unknown_postponed_kickoffs.sql"
do
  psql_db -f "$migration" >/dev/null
done

ACTIVE_SEASON_TEST_DB_URL="postgresql://postgres@/$DATABASE?host=$SOCKET_DIR&port=$PORT" \
  uv run --directory "$ROOT_DIR/backend" pytest -q \
    tests/test_active_season.py \
    tests/test_active_season_integration.py
printf 'Active-season importer integration validation passed.\n'
