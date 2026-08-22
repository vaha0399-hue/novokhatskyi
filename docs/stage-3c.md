# Stage 3C — development Supabase deployment canon

## Goal and stop condition

Deploy the already reviewed Stage 3B migration to one explicitly identified **development**
Supabase project, verify the remote migration history and deployed structure, and stop before
building the importer or frontend integration.

This stage does not change the approved data model and does not add API-Football endpoints,
tables, Auth flows, importer jobs, frontend code, live ingestion, Realtime subscriptions, cron,
or production deployment.

## Fixed inputs

- Migration: `supabase/migrations/20260821193000_stage_3b_core_schema.sql`
- Approved SHA-256: `6cc1c8638dca57ed80c7bb15b7577e9ac6f663df1f2a5d818e31d777fba0d225`
- Supabase CLI: `2.115.0`, executed through pinned `npx`
- Branch: `develop`
- Target environment: development only
- Realtime: disabled in `supabase/config.toml`
- Seed execution: disabled; Stage 3C deploys schema only

## Required protected environment variables

Store these only in `/opt/football-analytics/.env`, which must remain Git-ignored and mode `600`:

```dotenv
SUPABASE_ACCESS_TOKEN='personal access token from the Supabase account settings'
SUPABASE_DB_PASSWORD='database password for the development project'
SUPABASE_PROJECT_REF='development project reference'
SUPABASE_EXPECTED_PROJECT_NAME='exact development project name shown in Supabase'
```

For the actual apply step, pass this one-time explicit gate only to the deployment process:

```bash
STAGE3C_APPLY_CONFIRM='deploy-stage-3c-to-development' \
  ./scripts/stage3c-supabase-deploy.sh --apply
```

Do not store the account password, anon key, service-role key, database URL, auth headers, or
API-Football key in Supabase config or migrations. Stage 3C does not need Supabase anon or
service-role keys.

## Safety gates

`scripts/stage3c-supabase-deploy.sh` refuses to continue unless all of these are true:

1. the current branch is `develop`;
2. tracked and staged changes are absent;
3. `.env` exists, has mode `600`, is ignored, and is not tracked;
4. all required Supabase variables exist without being printed;
5. the migration checksum still matches the reviewed Stage 3B artifact;
6. the authenticated account contains exactly the configured project reference;
7. its returned project name exactly matches `SUPABASE_EXPECTED_PROJECT_NAME`;
8. the pinned CLI version is the expected version;
9. the CLI link and migration dry-run succeed;
10. the apply confirmation string is present for a real deployment.

The script contains no `set -x` and never passes the access token or database password as command
arguments. The CLI reads them from the environment.

## Controlled procedure

### 1. Read-only target check and migration dry-run

```bash
./scripts/stage3c-supabase-deploy.sh
```

Expected result: project identity is printed, the CLI link succeeds, remote/local migration
history is shown, and `db push --dry-run --skip-vault` lists only the approved Stage 3B migration.
No schema change is made.

Stop immediately if the target identity differs, the remote history is unexpected, the dry-run
contains another migration, or the remote project is not the intended development project.

### 2. Apply only after the dry-run is reviewed

```bash
./scripts/stage3c-supabase-deploy.sh --apply
```

The script repeats every preflight, repeats the dry-run, then executes `db push --linked
--skip-vault`. It verifies the migration version in remote history and exports only `source`,
`football`, `ml`, and `ops` for structural checks. The temporary dump is deleted on exit.

## Remote acceptance criteria

- migration `20260821193000` appears in remote migration history;
- schemas `source`, `football`, `ml`, and `ops` exist;
- exactly 32 approved base tables are present in those schemas;
- critical tables `football.fixtures` and `ml.predictions` exist;
- RLS declarations exist in the remote schema dump;
- no secret is present in migrations, config, logs, or Git diff;
- no importer, frontend integration, live pipeline, Realtime subscription, or new domain entity is
  introduced.

The Stage 3B local PostgreSQL constraint suite remains the authoritative behavioral verification.
Stage 3C adds remote deployment-history and structural verification without inserting persistent
test data into the development project.

## Failure and rollback policy

- Do not run `supabase db reset --linked`.
- Do not drop schemas or tables automatically.
- Do not repair remote migration history automatically.
- If the dry-run or push fails, preserve the error output after checking it for secrets and diagnose
  the exact failure before retrying.
- If migration history and deployed objects disagree, stop. Any corrective migration or destructive
  rollback requires a separate reviewed plan and explicit approval.

## Handoff

After successful remote verification, stop Stage 3C and report the target project name/reference,
migration version, dry-run result, remote history, structural verification, and secrets/scope checks.
The importer starts only after separate confirmation.

## Deployment record

Stage 3C completed at `2026-08-22T00:36:25Z` against the explicitly verified development target:

- project name: `vaha0399-hue's Project`;
- project ref: `ymjeddffmxhxfagjkoxh`;
- migration: `20260821193000_stage_3b_core_schema.sql`;
- initial remote migration history: empty;
- initial dry-run: exactly one migration, no seeds, no roles;
- apply: successful and recorded remotely as `20260821193000`;
- repeat dry-run/apply verification: remote database reported up to date and applied no changes;
- remote dump: four approved schemas, exactly 32 matching base tables, RLS enabled on every
  base table, critical prediction/snapshot/statistics/security guards present, and no direct
  grants to `anon` or `authenticated`;
- remote lint: no errors; one `warning extra` for intentionally unread local variable
  `fixture_row` in `ops.finalize_fixture_result`, where `SELECT ... FOR UPDATE` is used to lock the
  fixture row before final reconciliation. This does not change the approved schema or runtime
  behavior and does not justify rewriting an already deployed MVP migration.

No importer, frontend integration, live pipeline, Realtime subscription, seed data, or additional
domain entity was deployed.

### Corrective migration discovered by the importer rehearsal

The canary import rollback rehearsal found that the shared
`football.guard_standings_snapshot_children()` trigger referenced `NEW.team_id` while executing for
`football.standings_snapshot_groups`, which has no such column. The rehearsal transaction rolled
back before any API call or persistent data write.

With explicit approval, `20260822010000_fix_standings_child_guard.sql` was applied. It only moves
the team-membership check into the `standings_snapshot_rows` branch; it adds no entity, column,
permission, or live behavior. Both migrations passed the complete disposable PostgreSQL constraint
suite, the remote migration history contains both versions, and the corrected remote rollback
rehearsal passed before the canary used API quota.
