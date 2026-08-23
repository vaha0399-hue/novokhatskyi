#!/usr/bin/env bash
set -euo pipefail

# Disposable integration gate for the Python historical-lineup importer.
# It never reads .env, reaches Supabase, or calls API-Football.
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly PG_BIN="/usr/lib/postgresql/18/bin"
readonly WORK_DIR="$(mktemp -d /tmp/football-analytics-lineup-importer.XXXXXX)"
readonly DATA_DIR="$WORK_DIR/data"
readonly SOCKET_DIR="$WORK_DIR/socket"
readonly PORT="55448"
readonly DATABASE="fa_historical_lineups_importer_test"

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
cat >>"$DATA_DIR/postgresql.conf" <<EOF
unix_socket_directories = '$SOCKET_DIR'
listen_addresses = ''
port = $PORT
EOF
sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$WORK_DIR/postgres.log" start >/dev/null

"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres postgres >/dev/null 2>&1 || true
"$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d postgres -c 'CREATE ROLE anon NOLOGIN' >/dev/null
"$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d postgres -c 'CREATE ROLE authenticated NOLOGIN' >/dev/null
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres "$DATABASE"

for migration in \
  "$ROOT_DIR/supabase/migrations/20260821193000_stage_3b_core_schema.sql" \
  "$ROOT_DIR/supabase/migrations/20260822010000_fix_standings_child_guard.sql" \
  "$ROOT_DIR/supabase/tests/stage_3d_upgrade_seed.sql"; do
  printf '  apply %s\n' "$(basename "$migration")"
  psql_db -f "$migration" >/dev/null
done

# Seed fixtures are completed but deliberately unfinalized.  Give all 380 a
# final result before Stage 3D immutability is installed, mirroring the remote
# EPL 2024 prerequisite without mutating any finalized row later in tests.
psql_db -c "UPDATE football.fixtures SET result_finalized_at='2025-06-01 00:00:00+00'" >/dev/null

for migration in \
  "$ROOT_DIR/supabase/migrations/20260822210000_multi_competition_foundation.sql" \
  "$ROOT_DIR/supabase/migrations/20260823010000_historical_lineups_foundation.sql"; do
  printf '  apply %s\n' "$(basename "$migration")"
  psql_db -f "$migration" >/dev/null
done

cd "$ROOT_DIR/backend"
HISTORICAL_LINEUPS_TEST_DB_URL="postgresql://postgres@/$DATABASE?host=$SOCKET_DIR&port=$PORT" \
  uv run pytest -q tests/test_historical_lineups_importer.py tests/test_historical_lineups_importer_integration.py
printf 'Historical lineups importer integration validation passed.\n'
