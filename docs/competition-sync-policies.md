# Q01 competition sync policies

`ops.competition_sync_policies` is a reviewed, disabled-by-default permission
registry for a canonical `(provider_id, season_id)` pair.  The composite FK to
`source.season_provider_refs` prevents a policy for an unmapped provider or a
non-canonical season; it intentionally permits multiple seasons of one league.

The policy keeps explicit allowed work types, a per-work-type `coverage` map
with `unknown` / `covered` / `not_covered` and an observation date, and a
per-work-type `refresh_intervals` map with a numeric value plus explicit unit.
It also has priority, history depth, a monotonic version, and an optional timed
pause.
`anon` and `authenticated` hold no table privileges.

Future adapters use `SyncPolicyGate` through `PolicyCheckedEnqueuer` before an
enqueue callback and `PolicyCheckedExecutor` immediately before an executor
callback.  The second call rereads the policy and requires the enqueue version,
so a changed, disabled, or paused policy cannot authorize stale work. Ordinary
work requires its own coverage to be `covered`; `coverage_refresh` is the
explicit allowed freshness-check exception. Unknown is never coerced to true.

Q01 does not switch existing production entrypoints and adds no scheduler,
queue, quota governor, or automatic pilot activation.  Run its disposable P02
gate with `bash scripts/test-competition-sync-policies-migration.sh`.
