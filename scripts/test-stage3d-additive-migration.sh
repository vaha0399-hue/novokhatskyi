#!/usr/bin/env bash
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly PG_BIN="/usr/lib/postgresql/18/bin"
readonly WORK_DIR="$(mktemp -d /tmp/football-analytics-stage3d.XXXXXX)"
readonly DATA_DIR="$WORK_DIR/data"
readonly SOCKET_DIR="$WORK_DIR/socket"
readonly PORT="55446"
readonly BASE_MIGRATION="$ROOT_DIR/supabase/migrations/20260821193000_stage_3b_core_schema.sql"
readonly FIX_MIGRATION="$ROOT_DIR/supabase/migrations/20260822010000_fix_standings_child_guard.sql"
readonly ADDITIVE_MIGRATION="$ROOT_DIR/supabase/migrations/20260822210000_multi_competition_foundation.sql"

cleanup() {
  if [[ -f "$DATA_DIR/postmaster.pid" ]]; then
    sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -m fast stop >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

chown postgres:postgres "$WORK_DIR"
install -d -o postgres -g postgres "$DATA_DIR" "$SOCKET_DIR"
sudo -u postgres "$PG_BIN/initdb" -D "$DATA_DIR" --auth=trust --no-locale --encoding=UTF8 >/dev/null
cat >>"$DATA_DIR/postgresql.conf" <<EOF
unix_socket_directories = '$SOCKET_DIR'
listen_addresses = ''
port = $PORT
EOF
sudo -u postgres "$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$WORK_DIR/postgres.log" start >/dev/null

psql_db() {
  local database="$1"
  shift
  "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d "$database" "$@"
}

apply_file() {
  local database="$1"
  local file="$2"
  printf '  apply %s\n' "$(basename "$file")"
  psql_db "$database" -f "$file" >/dev/null
}

"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres postgres >/dev/null 2>&1 || true
psql_db postgres -c "CREATE ROLE anon NOLOGIN" >/dev/null
psql_db postgres -c "CREATE ROLE authenticated NOLOGIN" >/dev/null

printf 'Clean database migration...\n'
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres fa_stage3d_clean
apply_file fa_stage3d_clean "$BASE_MIGRATION"
apply_file fa_stage3d_clean "$FIX_MIGRATION"
apply_file fa_stage3d_clean "$ADDITIVE_MIGRATION"
apply_file fa_stage3d_clean "$ROOT_DIR/supabase/tests/stage_3d_clean_assertions.sql"

printf 'Upgrade migration with EPL 2024 preservation fingerprints...\n'
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres fa_stage3d_upgrade
apply_file fa_stage3d_upgrade "$BASE_MIGRATION"
apply_file fa_stage3d_upgrade "$FIX_MIGRATION"
apply_file fa_stage3d_upgrade "$ROOT_DIR/supabase/tests/stage_3d_upgrade_seed.sql"
apply_file fa_stage3d_upgrade "$ADDITIVE_MIGRATION"
apply_file fa_stage3d_upgrade "$ROOT_DIR/supabase/tests/stage_3d_upgrade_assertions.sql"

printf 'Atomic migration failure/rollback...\n'
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres fa_stage3d_failure
apply_file fa_stage3d_failure "$BASE_MIGRATION"
apply_file fa_stage3d_failure "$FIX_MIGRATION"
psql_db fa_stage3d_failure -c "INSERT INTO football.leagues (name) VALUES ('Unreviewed Competition')" >/dev/null
if psql_db fa_stage3d_failure -f "$ADDITIVE_MIGRATION" >/dev/null 2>&1; then
  printf 'Expected migration failure did not occur\n' >&2
  exit 1
fi
psql_db fa_stage3d_failure -Atc \
  "SELECT CASE WHEN to_regclass('football.countries') IS NULL AND (SELECT count(*) FROM football.leagues) = 1 THEN 'rollback-ok' ELSE 'rollback-failed' END" \
  | grep -qx 'rollback-ok'

printf 'Stage 3D local migration validation passed.\n'
