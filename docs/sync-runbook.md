# Sync and import diagnostics (Q07)

This runbook observes the opt-in Q01–Q06 control plane. It does not start a
worker, call API-Football, write a provider credential, reserve budget, or
repair queue rows. D01–D09 handlers are not wired by Q07; their absence is
reported as an input/handler limitation, never as a claim that a provider was
late or had no data.

## Safe inputs and access

Use an operations role that has read access to `ops`, `source`, and `football`.
Do not put database URLs or credentials in shell history, tickets, or logs.
Pass a preconfigured **read-only** connection through the environment:

```bash
psql "$SYNC_READONLY_DATABASE_URL" -X -v ON_ERROR_STOP=1 \
  -f backend/app/sync/diagnostics.sql
```

The file has only `SELECT` statements. For an explicit transaction boundary:

```bash
psql "$SYNC_READONLY_DATABASE_URL" -X -v ON_ERROR_STOP=1 <<'SQL'
BEGIN READ ONLY;
\i backend/app/sync/diagnostics.sql
ROLLBACK;
SQL
```

The second command intentionally rolls back even though the reports are
read-only. Neither command calls `ops.reserve_api_football_request` or
`ops.observe_api_football_budget`.

## Trace one job

The `app.sync.lifecycle` logger emits one JSON object per line. Search by the
numeric `job_id`; do not paste the log line into a public issue because scope
can identify a competition/fixture.

An application hosting `RepeatableSyncWorker` must configure this logger once
at process startup, before it runs work. This is a library call, not a new CLI:

```python
from app.sync.diagnostics import configure_lifecycle_logging

configure_lifecycle_logging()
```

The call is idempotent and the supported `scheduler_main` entrypoint already
makes it. A separate Q03 worker host must make the same call; without it,
normal Python logging configuration may not emit the JSON lifecycle stream.

Expected successful sequence:

1. `scheduler_enqueue_committed` (Q05)
2. `job_claimed`
3. `job_execution_started`
4. `job_apply_started`
5. `job_completed`

`job_completed` is emitted only after the transaction containing the fenced
apply, dependent enqueue, and `complete_repeatable_sync_work_item` has
committed. `duration_ms` is process-monotonic elapsed time; it is not a wall
clock timestamp. Each event contains `attempts` and a reduced scope with known
provider/season/fixture/team identifiers where available. It never logs raw
request parameters, headers, raw responses, or exception text.

For a queue record, correlate separately with:

```sql
SELECT id, run_id, status, attempts, job_type, available_at, started_at,
       finished_at, lease_expires_at
FROM ops.sync_work_items
WHERE id = :job_id;
```

Run the placeholder query only in a client that binds `:job_id`; never build
SQL by interpolating a request parameter. The trace and bundled report
intentionally do **not** select `last_error` or `quarantine_reason`, because
historical callers may have stored exception text there.

## Interpret failures and delay

| Signal | Meaning and response |
| --- | --- |
| `job_lease_lost` + `heartbeat_lost` | The separate heartbeat connection failed or the lease was no longer current. Do not apply/replay manually; inspect the queue report for `stale_lease`, then let normal recovery reclaim it. |
| `job_deferred` + `budget_*` | Q04 denied/unavailable capacity. `budget_daily_limit`, `budget_minute_limit`, provider caps, and `budget_cooldown` are distinct safe reason codes. Consult `api_budget`; do not reset counters. |
| `job_retry_scheduled` + `provider_http_5xx` or `provider_http_0` | Transport/provider delay after an attempted request. The retry remains bounded by Q03 attempts. |
| `provider_empty_response_observed` | A successful season-scoped response explicitly reported zero results. This is provider evidence, not a successful statistics-completeness claim. |
| `unknown_no_season_scoped_fetch` | Q07 has no usable season-scoped provider observation. It cannot tell provider absence from an unwired handler, a skipped policy, or a delayed request. |
| `pending_due` with an increasing `oldest_queue_age_seconds` | Active, due handler backlog. `pending_scheduled` is future work and is not backlog; `active_lease` is currently owned; `stale_lease` has expired. |
| `overdue_football_lifecycle` | Known scheduled/in-progress fixtures more than 15 minutes after kickoff, grouped by football lifecycle. This is not `stale_lease` and Q07 does not repair it or make a provider request. |
| `job_quarantined` | Contract/policy/handler failure. The safe event reason is a category; inspect privileged durable evidence under the incident procedure rather than exposing raw exception content. |

`provider_http_429` follows the budget-defer path because Q04 records the
shared cooldown. A `budget_unavailable` event means fail-closed accounting,
not permission to make an unmetered request.

## Coverage report

`metric_coverage` is the completeness report. It is deliberately per selected
metric (`total_shots`, `shots_on_goal`, `corner_kicks`, `yellow_cards`, and
`expected_goals`), provider, and season.

- `fixtures_without_statistics`, `fixtures_with_one_team_statistics`, and
  `coverage_empty`/`coverage_partial` distinguish no statistics from a partial
  pair.
- `observed_metric_team_pairs` counts a numeric zero as observed.
  `null_metric_team_pairs` counts an explicit NULL separately.
- `complete_team_pair_missing_selected_metric` means both participant rows
  exist, but at least one selected metric is missing. It is not hidden by a
  high number of statistics rows.
- Counts are derived from a one-row-per-fixture pair before metric expansion,
  filtered through the active provider/season policy. They do not mix teams or
  seasons and do not multiply fixture counts through joins.

Q07 does not infer freshness for D01–D09 data handlers that do not yet exist.
Provider freshness is one row per provider/season/**endpoint**: its latest
outcome, result count, and normalization timestamp come from the same stored
fetch record. Lifetime success/failure counters are separate context and never
override that latest state. An endpoint therefore cannot make statistics look
fresh merely because an unrelated endpoint succeeded. Use scheduler preview to
see `handler_unavailable` or `input_unavailable`. `freshness_age_seconds` is
the age of that endpoint's newest stored response, not an SLA verdict and not
evidence that an unwired handler should have fetched it.

## Budget report

`api_budget` reads the current Q04 config/state fields: local daily/minute
usage, per-consumer usage, protected reserve, provider-reported remaining caps,
and cooldown. `accounting_status` is descriptive only and does not repair,
clear, or hide Q04's known accounting conditions. The reservation function
remains authoritative at request time; a report can be stale immediately after
it is read.

## Local verification

The repository’s disposable gate provisions only local Unix-socket PostgreSQL
and Redis; it does not source `.env` or call API-Football:

```bash
bash scripts/test-repeatable-sync-queue-migration.sh
bash scripts/test-isolated-environment.sh
```

The first gate includes the Q07 diagnostic SQL integration case. It verifies
that the reports run against the migrated schema and that a partial team pair,
NULL metric, and numeric zero remain distinguishable.
