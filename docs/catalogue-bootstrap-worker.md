# Autonomous regular-league catalogue bootstrap

`python -m app.importer.season_sync --catalogue` is a backend-only worker. It
does not use Codex, OpenAI, an HTTP endpoint, or user input at runtime.

For each eligible provider `League` with one current season and standings
coverage, it performs:

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
double-round-robin fixture catalogue. A temporarily incomplete calendar is
`pending_not_published`; its `ops.sync_work_items.available_at` is the
authoritative next-check time. Cups, multi-group, split and playoff formats
are recorded as deferred without canonical writes until a deterministic format
adapter is added.

The worker uses `ops.sync_runs` and `ops.sync_work_items` for restart-safe
leases and checkpoints. `ops.provider_daily_request_usage` reserves an
API-Football request before network I/O, so daily quota survives restarts.

## Deployment order

1. Apply `20260904000000_catalogue_bootstrap_daily_quota.sql`.
2. Provision the existing worker-owned `/var/lib/football-analytics` state
   directory and install the catalogue systemd service/timer.
3. Run one smoke invocation with a deliberately small run cap.
4. Inspect its `ops` report and raw/canonical verification before enabling the
   hourly timer.

Match statistics are intentionally a separate completed-fixture pipeline.
