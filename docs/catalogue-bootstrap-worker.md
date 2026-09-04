# Autonomous regular-league catalogue bootstrap

`python -m app.importer.season_sync --catalogue` is a backend-only worker. It
does not use Codex, OpenAI, an HTTP endpoint, or user input at runtime.

For each selected eligible provider `League` with one current season and
standings coverage, it performs:

```text
API-Football → durable VPS raw capture → strict validation
             → atomic canonical Supabase import → verification
```

The raw capture is transient and stored at
`/var/lib/football-analytics/catalogue-bootstrap` by default. It contains only
response bytes and safe request metadata (endpoint, parameters, timestamps,
status, size and SHA-256); no API key or request headers are written. A crash
reuses completed endpoint captures. A partial endpoint capture is discarded
and only that endpoint is fetched again.

Eligible regular leagues require one standings group and a complete
double-round-robin fixture catalogue. If the standings group already contains
every season team but the fixture catalogue is temporarily incomplete, the
worker imports the known teams, standings and fixtures as `imported_partial`.
Its checkpoint records `calendar_complete: false`, observed/expected fixture
counts and raw provenance; a later catalogue/fixtures sync may add only new
provider fixture IDs or update existing kickoff/status values. It never deletes
known fixtures. A standings response that does not yet cover every season team
remains `pending_not_published`. Cups, multi-group, split and playoff formats
are recorded as deferred without canonical writes until a deterministic format
adapter is added.

The worker uses `ops.sync_runs` and `ops.sync_work_items` for restart-safe
leases and checkpoints. `ops.provider_daily_request_usage` reserves an
API-Football request before network I/O, so daily quota survives restarts.

## Default approved queue: 50 next leagues

With no `CATALOGUE_BOOTSTRAP_LEAGUE_IDS` environment override, the worker is
restricted to the next approved tranche of **50** competitions from the
retained scanner-candidate snapshot. It is not permitted to enumerate and
import the provider's whole catalogue by default.

The tranche is all remaining regular candidates from the 2026-09-01 snapshot
after the 24 scopes already imported before catalogue bootstrap, excluding
provider `1032` (Copa de la Liga Profesional) and `254` (NWSL Women), whose
format handling is deferred. The exact versioned provider IDs live in
`backend/app/importer/catalogue_bootstrap.py` as
`DEFAULT_CATALOGUE_BOOTSTRAP_LEAGUE_IDS`:

```text
72, 80, 82, 89, 98, 114, 119, 128, 134, 144, 145, 169, 172, 179,
197, 207, 210, 233, 235, 236, 239, 242, 244, 250, 252, 253, 262,
265, 271, 281, 283, 286, 292, 301, 305, 307, 323, 327, 344, 345,
357, 363, 383, 421, 475, 479, 549, 624, 813, 1104
```

An explicit `CATALOGUE_BOOTSTRAP_LEAGUE_IDS=39,218` replaces this default for
a narrowly scoped smoke or recovery run. A candidate is still not guaranteed
to import: the worker must validate teams, a complete regular calendar,
standings, mappings and statistics. Unsupported/split formats are safely
deferred, and incomplete calendars are retried at their checkpoint time.

## Deployment order

1. Apply `20260904000000_catalogue_bootstrap_daily_quota.sql`.
2. Provision the existing worker-owned `/var/lib/football-analytics` state
   directory and install the catalogue systemd service/timer.
3. Run one smoke invocation with a deliberately small run cap.
4. Inspect its `ops` report and raw/canonical verification before enabling the
   hourly timer.

Match statistics are intentionally a separate completed-fixture pipeline.
