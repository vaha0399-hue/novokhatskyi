# Batch current-season statistics: approved implementation plan

## Scope and stop condition

Implement and verify one backend-only smoke path for Premier League 2026/27.
Do not start a multi-league campaign or a 100--500 league backfill in this
change. The smoke run ends with a compact report; it never exposes provider
credentials or raw payloads to the frontend.

## Provider transport correction

The batch transport is `GET /fixtures?ids=<id-id-...>`, up to 20 fixture IDs,
not repeated calls to `/fixtures/statistics?fixture=<id>`. Each returned
fixture is normalised independently from its embedded `statistics` blocks.
The existing single-fixture statistics endpoint remains supported by the
historical backfill importer.

Before the first production write path is enabled, a bounded EPL canary proves
the provider contract at the configured maximum chunk size. A missing or
partial statistics block is retained as fixture-local evidence and skipped for
that run; it must not discard the other fixtures in the raw batch response.
It remains eligible for a later retry because no synthetic zero pair is stored.

## Reused components

- `app.api_football.APIFootballClient`: one reusable async HTTP pool and safe
  quota headers.
- `app.importer.statistics_backfill.map_statistics_block`: strict metric types,
  nullable values, and lossless `extra_metrics`.
- canonical fixtures/provider refs, `source.provider_fetches`, and immutable
  raw payload storage.
- `source.provider_rate_limit_state` for the latest safe provider quota
  observation. The raw fetch ledger remains the durable request counter.
- `app.analytics` history query for xGA pairing and scope semantics.

## Additive data model

1. Add `source.provider_fetch_fixture_subjects(fetch_id, fixture_id)` to bind
   one raw `/fixtures?ids` response to each canonical fixture it contains.
   Existing `subject_fixture_id` remains untouched for one-fixture endpoints.
2. Extend the statistics provenance guard to accept an observed `/fixtures`
   response only when that many-to-many binding exists and the provider/season
   identities agree.
3. Add private `football.team_rolling_metrics`. Its identity is
   `(team_id, season_id, scope, window_size)`; league is derived through the
   canonical season and is intentionally not duplicated.
4. Current-season batch rows remain updateable (`finalized_at IS NULL`) during
   this slice. A later, separate finalisation policy may make them immutable
   after an explicit correction window. No existing finalised historical row is
   changed.

## One-league smoke sequence

1. Fetch current completed EPL fixtures (`FT-AET-PEN`), retain the discovery
   body, and update only their already-canonical fixture mappings. An unknown
   fixture, identity mismatch, or conflict with an immutable final result stops
   safely; schedule creation remains the active-season importer's responsibility.
2. Locally order each participating team’s completed 2026/27 fixtures by
   kickoff descending; independently choose its last 10 overall, home, and
   away fixtures, then union and deduplicate provider IDs. This makes
   `home last 10` and `away last 10` scanner windows complete rather than
   merely deriving them from an overall ten-match slice.
3. Exclude already complete, valid statistics pairs; split remaining IDs into
   chunks of at most 20.
4. Fetch one raw `/fixtures?ids` payload per chunk; persist raw provenance and
   all fetch-to-fixture bindings before normalisation.
5. Reuse the statistics mapper for each fixture; bulk UPSERT up to two team
   rows per valid fixture. xGA is read from the opponent’s xG during metric
   aggregation.
6. Recalculate only affected teams for overall/home/away windows 5 and 10,
   including a per-metric known-value sample count for nullable averages
   (xG, xGA, shots, shots on goal, corners, possession).
   `window_size=0` is reserved in the schema but is not published until a full
   season statistics history has been loaded.
7. Repeat the same run to prove idempotency: no duplicate statistics rows, no
   duplicate metrics, and no provider requests for already complete targets.

## Error, quota, and reporting contract

- A batch has at most 20 provider IDs; invalid/empty statistics are recorded
  per fixture from the retained raw payload.
- `429` stops the run. Transient errors are checkpointed and retried later with
  bounded exponential backoff. The worker stops before its configured daily
  budget (initially 5,500) is exhausted.
- The final report contains discovered completed fixtures, selected unique
  fixtures, batch requests, normalised fixtures, rows written, affected teams,
  quota use, and fixture-local errors/skips.

## Required tests

- chunking and ID deduplication (including 20-ID boundary);
- batch envelope and per-fixture normalisation;
- missing xG/partial statistics without cross-fixture data loss;
- last 5/10 selection, home/away scopes, xGA, BTTS, and over thresholds;
- fewer than 5/10 matches;
- bulk UPSERT/rerun idempotency and raw provenance bindings;
- incremental recomputation limited to affected teams.

## Verified checkpoint — 2026-09-01

- Remote migration history is aligned through `20260831224435`; the
  historical-lineups schema was already present and was history-repaired
  without DDL.
- A real API-Football canary returned 19/19 requested EPL fixtures, all `FT`,
  with two statistics blocks per fixture.
- The bounded EPL 2026/27 write smoke used two provider requests (discovery +
  one batch), normalized 20 fixtures, wrote 40 team-statistics rows, and
  recalculated 120 rolling rows for 20 teams.
- A rerun used one discovery request, zero batch requests, and wrote zero new
  statistics rows; duplicate statistics and rolling-metric keys were zero.
- The first pre-fix smoke fetch remains as retained raw evidence and is marked
  `provider_error`; no canonical or raw payload rows were deleted.
- No multi-league or 100--500 league backfill was started.

## Incremental worker checkpoint — 2026-09-01

The next slice keeps exactly two long-lived backend workers:

1. **Live Worker** polls the configured live competition every 25 seconds and
   owns Redis current state.
2. **Completed/Statistics Worker** runs on its own cadence with one reusable
   API-Football connection pool. It performs terminal discovery, finalizes
   only provider-terminal fixtures after the canonical `kickoff + 3 hours`
   window, fetches missing statistics in chunks of at most 20 fixture IDs, and
   recalculates rolling metrics only for affected teams.

The completed worker never writes live Redis state. Final result and exact
provider status are written through the schema-owned
`ops.finalize_season_discovery_fixture_result` function. Postponed or
rescheduled provider responses are not inferred as completed. Statistics,
rolling metrics, provenance bindings, and the normalized fetch marker are
committed atomically per batch so a failed aggregation is recoverable on the
next run.

Implementation files:

- `backend/app/importer/incremental_statistics.py`
- `backend/app/importer/current_season_statistics.py`
- `supabase/migrations/20260901085117_finalize_completed_season_fixture.sql`
- `backend/tests/test_incremental_statistics.py`

Validation completed:

- disposable PostgreSQL integration with the new migration: `6 passed`;
- backend suite (excluding the unrelated untracked `season_sync` test):
  `241 passed, 46 skipped`;
- worker/live targeted tests: `23 passed`.

Migration `20260901085117_finalize_completed_season_fixture` was applied to
remote Supabase and verified in migration history. Two bounded real EPL
2026/27 worker runs then completed successfully: each used one terminal
discovery request, selected zero incomplete statistics targets, made zero
batch statistics requests, wrote zero rows, and reported no errors. The
second run proves the current state is idempotent. The statistics worker is
deployed and verified as a single hardened VPS systemd timer: EPL provider
scope `39:2026:20`, one run every 15 minutes, a 10-minute execution timeout,
an isolated worker-owned virtual environment, and root-only credentials. Its
first scheduled run completed successfully with one discovery request and no
errors. The timer is temporarily paused while the additive scanner schema
migration remains unapplied, preventing a new code/schema mismatch; it resumes
only after that migration is physically verified.

## Scanner data-contract and REST checkpoint — 2026-09-01

The scanner is a read-only layer over materialized `team_rolling_metrics`; it
does not call API-Football, Redis, or the live worker. Before exposing it, the
data contract was tightened so that venue windows are complete and nullable
averages retain the number of known source values.

- Additive migration `20260901193000_scanner_metric_sample_counts` adds
  `xg`, `xga`, `shots`, `shots_on_goal`, `corners`, and `possession` sample
  counts and derives them for existing rows from finalized canonical history.
- `POST /web/v1/scanner/matches` accepts an IANA user timezone, calendar date,
  internal `league_ids`, window `5` or `10`, a minimum venue sample of matches,
  and `AND`-combined allowlisted `home`/`away` numerical filters using
  `>`, `>=`, `<`, or `<=`.
- It returns only future canonical `scheduled` fixtures. Home filters use the
  home venue row; away filters use the away venue row. Both overall and venue
  snapshots are returned with their metric-specific sample counts.
- A metric source cutoff must precede the target fixture kickoff, preventing
  historical leakage. NULL values never satisfy numeric filters.

The migration and endpoint are verified locally with 248 backend tests and an
eight-test disposable PostgreSQL importer/scanner gate. The new migration has
not been applied to remote Supabase and no scanner backend deployment has been
performed yet.
