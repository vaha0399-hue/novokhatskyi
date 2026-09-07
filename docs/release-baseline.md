# Release baseline — 2026-09-07

This document records the reproducibility baseline prepared on 2026-09-07.
Use the local Git commit(s) created with this document as the source snapshot;
no database migration was applied, no database was changed, and no service was
restarted during this baseline procedure.

## Source snapshot

- Branch at inspection: `develop`.
- Pre-baseline HEAD: `a714615ba8336c6c61c68a792496095bb7cdc0bc`.
- Refreshed remote state: `origin/develop` was `5e961b9`; the local branch was
  33 commits ahead. `origin/main` was `feb87ad`. No push was performed.
- The baseline introduces cup-season import/queue support, API-Football
  classification and import scripts, corresponding tests, and the three
  migrations listed below.

## Toolchain and dependency locks

Observed host tools:

| Tool | Observed version | Project constraint / lock |
| --- | --- | --- |
| Python | 3.14.4 | backend requires `>=3.11`; `backend/uv.lock` |
| uv | 0.11.28 | no project pin |
| Node.js | v22.23.2 | frontend requires `>=22.0.0`; `frontend/package-lock.json` |
| npm | 10.9.8 | `package-lock.json` lockfile version 3 |

Neither Python/uv nor Node/npm is exactly pinned by repository configuration.
Use compatible versions above, or record the versions used by the deployment
environment before releasing. `pnpm` and `yarn` are not used.

Restore dependencies from a clean checkout using the lock files:

```bash
git checkout <baseline-sha>
cd backend && uv sync --locked
cd ../frontend && npm ci
```

The baseline validation repeats these commands in an isolated temporary Git
worktree. Do not copy `.env` files into Git; create local environment files
from the tracked `*.env.example` templates and supply secrets through the
deployment secret store.

## Supabase migrations (lexical apply order)

```text
20260821193000_stage_3b_core_schema.sql
20260822010000_fix_standings_child_guard.sql
20260822210000_multi_competition_foundation.sql
20260823010000_historical_lineups_foundation.sql
20260829010000_sync_control_plane_foundation.sql
20260831224435_batch_fixture_statistics_and_rolling_metrics.sql
20260901085117_finalize_completed_season_fixture.sql
20260901193000_scanner_metric_sample_counts.sql
20260902014134_add_postponed_fixture_status_mapping.sql
20260902014917_allow_unknown_postponed_kickoffs.sql
20260904000000_catalogue_bootstrap_daily_quota.sql
20260904010000_add_terminal_fixture_status_mappings.sql
20260905022146_fixture_statistics_unavailable_state.sql
20260905024714_add_live_and_abandoned_fixture_status_mappings.sql
20260906010000_allow_partial_standings_home_away_breakdowns.sql
```

Applying migrations is an environment operation and was intentionally outside
this baseline. Consult `supabase/migrations/README.md` before any such action.

## Local operational data deliberately excluded from Git

The following existing local data is preserved in the working directory but
ignored so it cannot be included by an ordinary commit:

- `artifacts/` (classification output, checkpoints, and provider raw responses;
  approximately 7.9 MB at inspection);
- `samples/api-football/epl-2026-refresh-2026-08-31T1225Z/`;
- `samples/api-football/predictions-2026-08-29T0653Z/`.

They contain provider-derived raw material and/or operational state. They are
not required to restore application dependencies. `.env` files remain ignored;
the tracked example templates contain no secret values.

## Runtime observation

The sandbox process listing found no project process. No matching user timer
was listed. Host systemd/Docker visibility was unavailable from this sandbox,
so this is evidence only for the current namespace, not a claim about the
host. No process or timer was restarted or changed.
