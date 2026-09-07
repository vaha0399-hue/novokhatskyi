# Isolated test environment

This document describes the reproducible test environment validated against
baseline commit `34fdb52c8ae4629d6165eb76531ea1671944f106` and the local
isolated-environment commits `e4f076e` and `cb1f44a` on 2026-09-07.
It does not use `.env`, a Supabase connection, a production dump, raw provider
data, Docker, or a TCP listener.

## Tool versions

| Tool | Version observed |
| --- | --- |
| Host Python | 3.14.4 |
| Python selected by `uv` for backend tests | 3.12.13 |
| uv | 0.11.28 |
| PostgreSQL server/client | 18.6 |
| Redis | 8.0.5 |
| Node.js | v22.23.2 |
| npm | 10.9.8 |

Backend dependencies come from `backend/uv.lock`; frontend dependencies come
from `frontend/package-lock.json`. The repository constrains Python to `>=3.11`
and Node.js to `>=22`, but does not pin exact tool versions.

## Run the gate

```bash
cd /opt/football-analytics
bash scripts/test-isolated-environment.sh
cd frontend && npm test && npm run typecheck && npm run build
```

The script follows the existing disposable-test pattern in `scripts/test-*.sh`:
it uses `initdb`, a new data directory, a private Unix-socket directory, and
`listen_addresses=''`. Redis is started with `--port 0`, a Unix socket under the
same runtime, and persistence disabled. It does not read `.env` or start,
stop, or inspect application services.

## Connection protection

Before PostgreSQL or Redis is contacted, every runner creates a nonce marker and
`test-resource.json` in its own `/tmp/football-analytics-*` directory.
The manifest records the one PostgreSQL socket directory, port, allowed database
names, and the one Redis socket.

`backend/tests/conftest.py` invokes
`app.testing.isolated_resources.validate_test_environment` before pytest starts
when any database or Redis URL is set, including direct `SUPABASE_DB_URL` and
`REDIS_URL`. It accepts only PostgreSQL URLs with the exact
private socket, port, and run-created database allowlist; it accepts only the
exact private `unix://` Redis URL. TCP Redis, a foreign Unix socket, a remote
PostgreSQL host, and a database name absent from the run allowlist fail before
any client connection. `backend/tests/test_isolated_resources.py` supplies the
negative tests for those cases.

The shell guard returns a non-zero status explicitly after every failed check,
including when it is invoked in `if`, under `!`, or through command
substitution. Cleanup verifies the same marker before it can call Redis,
signal a process, stop PostgreSQL, or remove a file. PostgreSQL wrappers clear
`PGHOST`, `PGHOSTADDR`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`,
`PGPASSFILE`, `PGSERVICE`, `PGSERVICEFILE`, and `PGOPTIONS`; pytest rejects a
non-empty inherited value before collection, so a service file or credentials
cannot redirect psycopg to another destination.

A database name containing `test` is never accepted as proof of safety. The
instance socket and nonce-bearing manifest prove ownership instead.

## Checks and results

`scripts/test-isolated-environment.sh` performs the following checks.

| Check | Result |
| --- | --- |
| Positive guard unit tests | 17 passed: valid owned PostgreSQL/Redis manifest and URLs accepted |
| Negative shell guard tests | passed: missing/wrong marker, missing manifest, direct/`if`/`!`/command-substitution failures, `PGHOSTADDR`, `PGSERVICE`, and cleanup mocks; no client was invoked |
| Negative Python guard tests | included in the 17: missing marker/manifest, foreign PostgreSQL/Redis URL, absent manifest, inherited `PGHOSTADDR`, `PGSERVICE`, `PGSERVICEFILE`, `PGPASSFILE`, and `PGPASSWORD` |
| Negative destination guards in full runner | passed; rejected URLs were stopped before connection |
| All 15 migrations on an empty database | passed |
| Upgrade from the earlier synthetic schema | passed; existing Stage 3D and historical-lineup preservation assertions passed |
| Backup and restore | passed: custom `pg_dump`, `pg_restore --exit-on-error`, equal schema, key-record, and constraint fingerprints |
| Selected isolated PostgreSQL/Redis integration tests | 5 passed |
| Full backend suite with all database integration URLs unset | 325 passed, 50 skipped, 0 failed |
| Legacy PostgreSQL integration scripts | passed: active season 22; current statistics 1 then 8; historical lineups 25; live worker 1; season bootstrap 14 then 3 |
| Legacy Redis integration script | passed: live REST 2 |
| Legacy migration scripts | passed: Stage 3D additive; historical lineups additive |
| Frontend `npm test` | 35 passed, 0 failed, 0 skipped |
| Frontend typecheck and production build | passed |

The 50 backend skips are integration suites that require an explicitly supplied
database URL; the runner unsets every such variable unless it has prepared a
compatible synthetic fixture. `test_web_read_api_integration.py` is explicitly
skipped by the selected integration gate: the current synthetic upgrade data
has no standings snapshot, which that development-data contract requires. An
exploratory run confirms this fixture limitation; no business logic or old
migration was changed to make it pass.

No API-Football request is made. Tests that require provider raw data outside
the checked-in synthetic fixtures remain excluded.

## Safe cleanup

The runner installs an `EXIT` trap that first verifies its nonce marker. It
stops Redis through its private socket and only signals the saved PID after
its command line proves the same socket identity. It stops PostgreSQL only
through the `postmaster.pid` inside that runtime. It removes only its own
`/tmp/football-analytics-isolated-*` directory after both shutdowns are
confirmed; otherwise it retains the marked directory for inspection. The
custom backup is stored there and disappears only after successful cleanup. If
a failed shell is left open, inspect the exact directory name and its marker
before manually running the same cleanup; do not use broad `/tmp` deletion
commands.

## Limits

- The backup check proves the local mechanism with synthetic data, not recovery
  of a production backup.
- Docker socket and host systemd access were unavailable from this environment;
  local PostgreSQL/Redis were used instead. Process inspection applies only to
  the current namespace.
- The existing project migration README is historically stale about the number
  of migrations; this gate discovers actual `*.sql` files lexically rather
  than relying on that text.
