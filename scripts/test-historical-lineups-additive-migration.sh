#!/usr/bin/env bash
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly PG_BIN="/usr/lib/postgresql/18/bin"
readonly WORK_DIR="$(mktemp -d /tmp/football-analytics-historical-lineups.XXXXXX)"
readonly DATA_DIR="$WORK_DIR/data"
readonly SOCKET_DIR="$WORK_DIR/socket"
readonly PORT="55447"
readonly BASE_MIGRATION="$ROOT_DIR/supabase/migrations/20260821193000_stage_3b_core_schema.sql"
readonly FIX_MIGRATION="$ROOT_DIR/supabase/migrations/20260822010000_fix_standings_child_guard.sql"
readonly STAGE_3D_MIGRATION="$ROOT_DIR/supabase/migrations/20260822210000_multi_competition_foundation.sql"
readonly ADDITIVE_MIGRATION="$ROOT_DIR/supabase/migrations/20260823010000_historical_lineups_foundation.sql"

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
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres fa_historical_lineups_clean
apply_file fa_historical_lineups_clean "$BASE_MIGRATION"
apply_file fa_historical_lineups_clean "$FIX_MIGRATION"
apply_file fa_historical_lineups_clean "$STAGE_3D_MIGRATION"
apply_file fa_historical_lineups_clean "$ADDITIVE_MIGRATION"
apply_file fa_historical_lineups_clean "$ROOT_DIR/supabase/tests/historical_lineups_clean_assertions.sql"

printf 'Upgrade migration with EPL 2024 preservation fingerprints...\n'
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres fa_historical_lineups_upgrade
apply_file fa_historical_lineups_upgrade "$BASE_MIGRATION"
apply_file fa_historical_lineups_upgrade "$FIX_MIGRATION"
apply_file fa_historical_lineups_upgrade "$ROOT_DIR/supabase/tests/stage_3d_upgrade_seed.sql"
apply_file fa_historical_lineups_upgrade "$STAGE_3D_MIGRATION"
apply_file fa_historical_lineups_upgrade "$ADDITIVE_MIGRATION"
# This assertion evaluates the original before/after fingerprint after the new
# migration, then supplies its independent Stage 3D contract checks.
apply_file fa_historical_lineups_upgrade "$ROOT_DIR/supabase/tests/stage_3d_upgrade_assertions.sql"
apply_file fa_historical_lineups_upgrade "$ROOT_DIR/supabase/tests/historical_lineups_upgrade_assertions.sql"

printf 'Deferred count-guard rollback...\n'
if psql_db fa_historical_lineups_upgrade \
  -f "$ROOT_DIR/supabase/tests/historical_lineups_expected_count_failure.sql" >/dev/null 2>&1; then
  printf 'Expected deferred historical lineup count failure did not occur\n' >&2
  exit 1
fi
psql_db fa_historical_lineups_upgrade -Atc \
  "SELECT CASE WHEN (SELECT count(*) FROM football.fixture_historical_lineup_snapshots) = 2 AND NOT EXISTS (SELECT 1 FROM source.provider_fetches WHERE content_sha256 = decode(repeat('de', 32), 'hex')) THEN 'deferred-rollback-ok' ELSE 'deferred-rollback-failed' END" \
  | rg -qx 'deferred-rollback-ok'

printf 'Atomic migration failure/rollback...\n'
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PORT" -U postgres fa_historical_lineups_failure
apply_file fa_historical_lineups_failure "$BASE_MIGRATION"
apply_file fa_historical_lineups_failure "$FIX_MIGRATION"
apply_file fa_historical_lineups_failure "$STAGE_3D_MIGRATION"
psql_db fa_historical_lineups_failure -c \
  "CREATE TABLE football.fixture_historical_lineup_snapshots (id bigint PRIMARY KEY)" >/dev/null
if psql_db fa_historical_lineups_failure -f "$ADDITIVE_MIGRATION" >/dev/null 2>&1; then
  printf 'Expected migration failure did not occur\n' >&2
  exit 1
fi
psql_db fa_historical_lineups_failure -Atc \
  "SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM pg_enum value JOIN pg_type type ON type.oid = value.enumtypid JOIN pg_namespace namespace ON namespace.oid = type.typnamespace WHERE namespace.nspname = 'source' AND type.typname = 'fetch_purpose' AND value.enumlabel = 'historical_backfill') AND to_regclass('football.fixture_historical_lineups') IS NULL AND to_regclass('football.fixture_historical_lineup_players') IS NULL THEN 'rollback-ok' ELSE 'rollback-failed' END" \
  | rg -qx 'rollback-ok'

printf 'Historical lineups additive migration validation passed.\n'
