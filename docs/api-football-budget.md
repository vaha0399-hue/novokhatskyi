# Q04 shared API-Football budget

`ops.reserve_api_football_request` is the only authorization point for a
physical API-Football HTTP attempt.  It locks one durable PostgreSQL state row,
commits the debit, and returns before HTTP begins.  The debit is never returned
after a timeout or process crash.  If that database call cannot complete, the
client fails closed and makes no HTTP request.

The configurable default is 6,000/day and 300/minute.  Of the daily cap,
2,000 is operations, 90 history, 1,000 legacy/manual, and 2,910 remains a
protected reserve; the reserve is not automatically spendable.  UTC calendar
days and server-clock minute windows are shared by every process and survive
restarts.  Response headers never credit capacity.  API-Football documents
daily `x-ratelimit-requests-*`, minute `X-RateLimit-*`, and 429 as a
per-minute over-limit signal; 429 therefore applies a shared cooldown until at
least the next server minute.  A numeric `Retry-After`, when present, can only
extend that cooldown.

The client retries transport failures and 5xx at most twice with exponential
backoff plus jitter.  Every retry calls the reserve function anew.  Existing
caller-level retries remain bounded and also pass through this same client.

Covered entrypoints are the live worker; catalogue/season sync; incremental
and current-season statistics; historical statistics backfill; season backfill;
canary/season-canary/historical-lineups; Cup runner; and every listed manual
collector/classifier.  They all construct `APIFootballClient.from_environment`,
which always installs the PostgreSQL adapter.  Direct construction has no
unmetered default and fails closed on `get` unless a budget adapter is supplied.
The only unrelated `httpx` use is logo-asset downloading, which does not call
API-Football.  Q05's scheduler is intentionally not introduced by Q04.
