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
   kickoff descending; choose last 10, union, and deduplicate provider IDs.
3. Exclude already complete, valid statistics pairs; split remaining IDs into
   chunks of at most 20.
4. Fetch one raw `/fixtures?ids` payload per chunk; persist raw provenance and
   all fetch-to-fixture bindings before normalisation.
5. Reuse the statistics mapper for each fixture; bulk UPSERT up to two team
   rows per valid fixture. xGA is read from the opponent’s xG during metric
   aggregation.
6. Recalculate only affected teams for overall/home/away windows 5 and 10.
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
