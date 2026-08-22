# Controlled Premier League 2024 fixture backfill

This manual development job imports only the 380 completed Premier League
fixtures from research season 2024. Production season remains 2026.

## Provider budget

The logical request is fixed:

```text
GET /fixtures?league=39&season=2024
```

- completed reusable raw payload: 0 API attempts;
- expected fresh run: 1 API attempt;
- hard cap: 3 physical attempts, including transient failures;
- HTTP 429 and all other non-transient/provider-contract errors stop the run;
- unexpected pagination stops after page 1 and never requests page 2.

The Stage 3B schema has no column for provider response headers. The job
therefore allow-lists rate-limit headers and includes the sanitized observation
only in its controlled JSON run report. Authentication headers and credentials
are never persisted or printed.

## Safety and recovery

The job requires the Stage 3C canary provider, league, season and 20 team
mappings. The existing canary fixture must already be completed, observed and
finalized; it is a strict no-op.

Successful raw bytes and typed season provenance are committed before domain
normalization. All 380 fixtures are then validated before DML and processed as
eight internal chunks (`7 x 50 + 30`) inside one PostgreSQL transaction. A
failure rolls back every normalized change while retaining the raw payload, so
the next run resumes with zero API calls.

Historical results use conservative availability:

```text
terminal_status_observed_at = kickoff_at + 3 hours
result_available_at         = kickoff_at + 3 hours
availability_basis          = reconstructed_conservative
result_finalized_at         = provider response time
```

No statistics, standings, injury/lineup snapshots, predictions, live state or
reconciliation jobs are created or changed.

## Manual command

Run only from the backend with server-side environment variables loaded:

```bash
uv run python -m app.importer.season_backfill
```

The command prints only a sanitized JSON report with request accounting,
normalization counts and remote verification results.

## Development execution evidence — 2026-08-22

The approved controlled run completed against development Supabase:

- API attempts: `1`;
- persisted season fetch ID: `19`;
- provider results/paging: `380`, page `1/1`;
- created fixtures: `379` plus the unchanged canary fixture;
- final fixture/provider mapping count: `380/380`;
- schedule validation: 20 teams, 38 matches each, 19 home and 19 away;
- orphan mappings and conservative-availability errors: `0`;
- out-of-scope table counts: unchanged;
- direct `anon`/`authenticated` DML grants: `0`;
- sanitized quota observation after the call: daily `93/100`, minute `9/10`.

The immediate replay reused fetch `19`, made `0` API calls, created `0` rows,
and repeated all remote verification checks successfully.
