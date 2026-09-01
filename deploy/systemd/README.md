# Completed statistics worker — systemd deployment

These templates run one bounded EPL 2026/27 completed/statistics job every
15 minutes. They do not start the live worker, expose an HTTP endpoint, or
enable any other league.

Before enabling the timer, the host must have:

- the repository at `/opt/football-analytics`;
- a `football-analytics` system user/group able to read, but not write, the
  root/deploy-owned repository;
- `/opt/football-analytics/.env` with `SUPABASE_DB_URL` and
  `API_FOOTBALL_KEY`, owned by root and mode `0600`;
- `uv` available at `/usr/local/bin/uv`.

Create the worker-owned runtime directories before provisioning its virtual
environment:

```bash
sudo install -d -o football-analytics -g football-analytics -m 0750 \
  /var/cache/football-analytics /var/lib/football-analytics
```

Provision the worker-owned virtual environment once after each dependency
change. This keeps the service independent from a developer's local `.venv`
or home directory:

```bash
sudo -u football-analytics env \
  UV_CACHE_DIR=/var/cache/football-analytics \
  UV_PROJECT_ENVIRONMENT=/var/lib/football-analytics/venv \
  /usr/local/bin/uv sync --directory /opt/football-analytics/backend --frozen --no-dev
```

The unit creates `/var/cache/football-analytics` and
`/var/lib/football-analytics` with systemd-managed ownership.

Install the two unit files under `/etc/systemd/system/`, then run:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now football-analytics-statistics.timer
sudo systemctl list-timers football-analytics-statistics.timer
```

The worker uses a PostgreSQL advisory lock per league/season, so a second
manual invocation cannot concurrently process the same EPL 2026/27 scope.
Inspect each run with:

```bash
sudo journalctl -u football-analytics-statistics.service -n 100 --no-pager
```
