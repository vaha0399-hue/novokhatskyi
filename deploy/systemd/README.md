# Completed statistics worker — systemd deployment

These templates run one bounded EPL 2026/27 completed/statistics job every
15 minutes. They do not start the live worker, expose an HTTP endpoint, or
enable any other league.

Before enabling the timer, the host must have:

- the repository at `/opt/football-analytics`;
- a `football-analytics` system user/group able to read the repository;
- `/opt/football-analytics/.env` with `SUPABASE_DB_URL` and
  `API_FOOTBALL_KEY`;
- `uv` available in the systemd service PATH.

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
