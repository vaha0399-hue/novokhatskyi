#!/usr/bin/env bash
set -euo pipefail

readonly CLI_VERSION="2.115.0"
readonly MIGRATION_VERSION="20260821193000"
readonly MIGRATION_FILE="supabase/migrations/${MIGRATION_VERSION}_stage_3b_core_schema.sql"
readonly MIGRATION_SHA256="6cc1c8638dca57ed80c7bb15b7577e9ac6f663df1f2a5d818e31d777fba0d225"
readonly APPLY_CONFIRMATION="deploy-stage-3c-to-development"
readonly ENV_FILE="${STAGE3C_ENV_FILE:-.env}"

mode="dry-run"
if [[ "${1:-}" == "--apply" ]]; then
  mode="apply"
elif [[ $# -ne 0 ]]; then
  printf 'Usage: %s [--apply]\n' "$0" >&2
  exit 2
fi

fail() {
  printf 'Stage 3C preflight failed: %s\n' "$1" >&2
  exit 1
}

[[ "$(git branch --show-current)" == "develop" ]] || fail "current branch is not develop"
git diff --quiet || fail "tracked working-tree changes exist"
git diff --cached --quiet || fail "staged changes exist"

[[ -f "$ENV_FILE" ]] || fail "$ENV_FILE is missing"
[[ "$(stat -c '%a' "$ENV_FILE")" == "600" ]] || fail "$ENV_FILE permissions must be 600"
git check-ignore -q "$ENV_FILE" || fail "$ENV_FILE is not ignored by Git"
if git ls-files --error-unmatch "$ENV_FILE" >/dev/null 2>&1; then
  fail "$ENV_FILE is tracked by Git"
fi

# The file is trusted operator input and must use shell-compatible KEY='value' syntax.
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

for variable in \
  SUPABASE_ACCESS_TOKEN \
  SUPABASE_DB_PASSWORD \
  SUPABASE_PROJECT_REF \
  SUPABASE_EXPECTED_PROJECT_NAME; do
  [[ -n "${!variable:-}" ]] || fail "$variable is not set"
done

[[ -f "$MIGRATION_FILE" ]] || fail "migration file is missing"
actual_sha256="$(sha256sum "$MIGRATION_FILE" | awk '{print $1}')"
[[ "$actual_sha256" == "$MIGRATION_SHA256" ]] || fail "migration checksum changed"

if [[ "$mode" == "apply" ]]; then
  [[ "${STAGE3C_APPLY_CONFIRM:-}" == "$APPLY_CONFIRMATION" ]] || \
    fail "STAGE3C_APPLY_CONFIRM must equal $APPLY_CONFIRMATION"
fi

tmp_projects="$(mktemp /tmp/football-analytics-stage3c-projects.XXXXXX.json)"
tmp_dump="$(mktemp /tmp/football-analytics-stage3c-schema.XXXXXX.sql)"
tmp_history="$(mktemp /tmp/football-analytics-stage3c-history.XXXXXX.txt)"
trap 'rm -f "$tmp_projects" "$tmp_dump" "$tmp_history"' EXIT

printf 'Stage 3C: verifying Supabase CLI and development target...\n'
[[ "$(npx --yes "supabase@${CLI_VERSION}" --version)" == "$CLI_VERSION" ]] || \
  fail "unexpected Supabase CLI version"

npx --yes "supabase@${CLI_VERSION}" projects list --output-format json >"$tmp_projects"
python3 - "$tmp_projects" <<'PY'
import json
import os
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)

projects = payload.get("projects", []) if isinstance(payload, dict) else payload
if not isinstance(projects, list) or not all(isinstance(project, dict) for project in projects):
    raise SystemExit("Stage 3C preflight failed: unexpected projects-list response shape")

expected_ref = os.environ["SUPABASE_PROJECT_REF"]
expected_name = os.environ["SUPABASE_EXPECTED_PROJECT_NAME"]
matches = [project for project in projects if project.get("ref", project.get("id")) == expected_ref]
if len(matches) != 1:
    raise SystemExit("Stage 3C preflight failed: project ref is not uniquely present in this account")

project = matches[0]
if project.get("name") != expected_name:
    raise SystemExit("Stage 3C preflight failed: project name does not match the expected development target")

print(f"Development target verified: {project['name']} ({expected_ref})")
PY

# The CLI reads SUPABASE_ACCESS_TOKEN and SUPABASE_DB_PASSWORD from the environment.
# Secrets are deliberately not placed in command arguments.
npx --yes "supabase@${CLI_VERSION}" link --project-ref "$SUPABASE_PROJECT_REF"

printf '\nStage 3C: current remote migration history:\n'
npx --yes "supabase@${CLI_VERSION}" migration list --linked

printf '\nStage 3C: migration dry-run:\n'
npx --yes "supabase@${CLI_VERSION}" db push --linked --dry-run --skip-vault

if [[ "$mode" == "dry-run" ]]; then
  printf '\nDry-run complete. No migration was applied.\n'
  exit 0
fi

printf '\nStage 3C: applying migration to the verified development project...\n'
npx --yes "supabase@${CLI_VERSION}" db push --linked --skip-vault

printf '\nStage 3C: verifying remote migration history...\n'
npx --yes "supabase@${CLI_VERSION}" migration list --linked | tee "$tmp_history"
grep -q "$MIGRATION_VERSION" "$tmp_history" || \
  fail "migration version is absent from remote history"

printf '\nStage 3C: exporting deployed schemas for structural verification...\n'
npx --yes "supabase@${CLI_VERSION}" db dump \
  --linked \
  --schema source,football,ml,ops \
  --file "$tmp_dump"

python3 - "$MIGRATION_FILE" "$tmp_dump" <<'PY'
import re
import sys

local_sql = open(sys.argv[1], encoding="utf-8").read()
remote_sql = open(sys.argv[2], encoding="utf-8").read()

local_tables = set(re.findall(
    r"^CREATE TABLE (source|football|ml|ops)\.([a-z_]+) \(",
    local_sql,
    re.MULTILINE,
))
remote_tables = set(re.findall(
    r'^CREATE TABLE IF NOT EXISTS "(source|football|ml|ops)"\."([a-z_]+)" \(',
    remote_sql,
    re.MULTILINE,
))

if len(local_tables) != 32:
    raise SystemExit(f"Stage 3C preflight failed: local migration table count is {len(local_tables)}, expected 32")
if remote_tables != local_tables:
    missing = sorted(local_tables - remote_tables)
    extra = sorted(remote_tables - local_tables)
    raise SystemExit(f"Stage 3C preflight failed: remote table mismatch; missing={missing}, extra={extra}")
if {schema for schema, _ in remote_tables} != {"source", "football", "ml", "ops"}:
    raise SystemExit("Stage 3C preflight failed: remote schema set differs from source/football/ml/ops")

rls_relations = set(re.findall(
    r'^ALTER TABLE(?: ONLY)? "(source|football|ml|ops)"\."([a-z_]+)" ENABLE ROW LEVEL SECURITY;$',
    remote_sql,
    re.MULTILINE,
))
if rls_relations != remote_tables:
    missing_rls = sorted(remote_tables - rls_relations)
    raise SystemExit(f"Stage 3C preflight failed: RLS is missing for {missing_rls}")

for role in ("anon", "authenticated"):
    if re.search(rf'^GRANT .* TO "?{role}"?;$', remote_sql, re.MULTILINE):
        raise SystemExit(f"Stage 3C preflight failed: direct grant to {role} found in remote dump")

for marker in (
    'CREATE OR REPLACE FUNCTION "ml"."guard_prediction_insert"',
    'CREATE OR REPLACE FUNCTION "ml"."assert_prediction_commit_valid"',
    'CREATE OR REPLACE FUNCTION "football"."guard_prematch_snapshot"',
    'CREATE OR REPLACE FUNCTION "football"."guard_fixture_statistics"',
    'CREATE OR REPLACE FUNCTION "source"."guard_safe_request_params"',
    'CREATE OR REPLACE FUNCTION "ops"."finalize_fixture_result"',
    'TRIGGER "predictions_insert_guard"',
    'TRIGGER "predictions_immutable_guard"',
):
    if marker not in remote_sql:
        raise SystemExit(f"Stage 3C preflight failed: critical remote marker absent: {marker}")

if any(re.search(r"(^|_)(live|event_stream|minute_by_minute)($|_)", name) for _, name in remote_tables):
    raise SystemExit("Stage 3C preflight failed: live-specific table detected")

print("Remote structure verified: 4 schemas, 32 exact tables, RLS on all tables, critical guards present")
PY

printf '\nStage 3C deployment and structural verification completed successfully.\n'
