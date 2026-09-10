# Repeatable sync queue (Q02)

`ops.sync_work_items` remains the only queue.  Periodic and recalculation keys
are compact canonical JSON arrays: `["periodic", type, provider, season,
entity, window-start, window-end]` and `["recalculation", type, provider,
season, entity, input-version]`.  Component boundaries are therefore
unambiguous. The provider and canonical season are always included, so a
completed key is permanent history and can never be enqueued again.  A later
window or input version has a new key and therefore creates a new row.

Periodic windows must be ordered, timezone-aware datetimes and are serialized
in UTC, so equivalent offsets cannot create different identities.  Legacy
run-scoped producers remain compatible: a trigger derives unique `legacy:<id>`
keys for their omitted fields, while the Q02 repository creates repeatable
work with explicit durable identities. `legacy` and `legacy:*` identities are
reserved for those paths and excluded from the repeatable claim. Repeatable
identities cannot be updated or deleted, and runs owning work cannot be deleted
(`ON DELETE RESTRICT`), preserving durable-key history.

The Q02 repository checks Q01 policy before its database call.  Its default
execution group is `entity:provider:season:entity`; a reviewed caller can
supply a narrower conflict group.  Only one item in an execution group may be
`running`; blocked versions stay `pending` and are claimed after the current
writer finishes.

Ready work is ordered by `priority + whole minutes waited`, then due time and
ID.  Future work is never claimed.  This aging prevents a continuous stream of
higher-priority work from starving an old ready item.  Scheduling missed time
windows is intentionally Q05's responsibility; Q02 only deduplicates windows
given to it and does not generate per-second catch-up work.

## Q05.1 deterministic scheduler preview

The first scheduler slice calculates `calendar_refresh` and `standings_refresh`
only. It receives `now`, Q01 policies, and saved schedule state explicitly; it
performs no database write, enqueue, API call, or budget reservation.

Saved state means "scheduled", not "successfully processed": an adapter will
save it atomically with enqueue in a later task, while preview never advances
it. State retains both the immutable end of the last enqueued window and its
first next deadline. Thus a policy interval change keeps the old first overdue
deadline and every segment after the saved boundary.

Periodic identities remain Q02 `PeriodicWork` identities. Windows use UTC
closed boundaries, so keys do not contain the scheduler launch instant and DST
cannot change them. After downtime, the scheduler emits one continuous window
from the saved boundary through the latest closed boundary; its deadline is the
first missed deadline and its scope records `coalesced_windows`. Once that
candidate and state are saved, the next candidate starts at its exact end, so
it cannot expand or overlap the immutable enqueued window. API cost is
`unknown` per planned item until an executor selects a physical request.

The opt-in Q05 producer stores this state in `ops.sync_scheduler_checkpoints`.
For each due candidate it evaluates Q01 again, calls the existing Q02 enqueue
function, and advances that checkpoint in one short transaction. A policy
denial, unavailable Q03-compatible handler, stale checkpoint, or transaction
rollback advances neither queue nor state. The checkpoint is intentionally not
an execution-success record; Q03 owns that lifecycle on the work item.

The atomic function locks the policy row with `FOR UPDATE` before it touches
Q02 or the checkpoint. It compares the policy instance and version stored in
the materialized candidate scope with the locked row, then repeats the Q01
permissions check. A concurrent policy change therefore invalidates the old
calculation: it enqueues no item and advances no checkpoint; the next
scheduler run must calculate a replacement candidate.

The same transition parses the immutable Q02 window from the candidate scope.
For an existing checkpoint, the queued window must start exactly at the saved
boundary and its end must equal the new checkpoint boundary. A stale expected
checkpoint is a no-op; a backward or mismatched window is rejected before Q02
can enqueue it.

Section-7 work types whose fixture/reconciliation/input-version reader is not
yet connected remain visible in preview as `input_unavailable`; arbitrary
policy names use `not_implemented`. Neither result causes enqueue or a state
advance. In particular, time alone never supplies the football conditions for
result finalization.

`python -m app.sync.scheduler_main --database-url <url>` is the explicit
preview entrypoint. It reads policies, checkpoints, saved fixtures and the Q04
budget/cooldown state in one read-only transaction, then renders that same
materialized snapshot. `--enqueue --run-id <id>` uses that snapshot but ships
with no handlers; deployment must inject reviewed Q03 handler pairs before it
can enqueue. The entrypoint never calls the budget reservation function.

## Q05 scheduler verification matrix

This matrix covers calculation and Q02 enqueue eligibility only. It does not
claim that a D/A handler exists: a missing Q03-compatible handler leaves the
candidate visible as `handler_unavailable`, and missing saved input is
`input_unavailable`.

| Section 7 row | Calculation | Exact regression |
|---|---|---|
| catalogue/season discovery | weekly policy interval; daily inside the saved preseason horizon | `test_discovery_uses_weekly_policy_interval_outside_preseason`; `test_discovery_quality_and_standings_modes_calculate_from_saved_season_inputs` |
| schedule near | policy-aligned calendar window for a saved fixture within seven days | `test_fixture_snapshot_calculates_section_7_deadlines_without_a_handler`; `test_fixture_periodic_keys_are_stable_inside_a_policy_interval` |
| schedule far | policy-aligned window for a saved fixture beyond seven days | `test_schedule_far_and_postponed_fixture_candidates_keep_their_event_deadlines` |
| prematch check | T−60 and T−10 deadlines from the saved kickoff; a matching successful normalized `/fixtures` observation inside inclusive `[deadline − policy interval, now]` produces `fresh_input` without queue/checkpoint writes, while absent proof retains the Q01 → handler → Q02 path. Open blocker: the deadline-only Q02 identity collides when a +50-minute reschedule makes old T−10 equal new T−60 | `test_fixture_snapshot_calculates_section_7_deadlines_without_a_handler`; `test_q05_prematch_db_boundaries_are_inclusive_for_t60_and_t10`; `test_q05_prematch_materialized_snapshot_preserves_ordinary_enqueue_path`; `test_q05_prematch_reschedule_and_restart_do_not_false_skip_or_duplicate`; `test_q05_prematch_fresh_input_survives_restart_after_one_day`; `test_q05_prematch_without_evidence_still_obeys_policy_and_handler_gates`; `test_q05_prematch_reader_query_explain_analyzes_realistic_history` |
| live | policy interval only while the saved lifecycle is active | `test_live_finalization_and_correction_deadlines_require_saved_football_conditions`; `test_fixture_periodic_keys_are_stable_inside_a_policy_interval` |
| overdue status check | kickoff +15 minutes for a nonterminal saved lifecycle | `test_fixture_snapshot_calculates_section_7_deadlines_without_a_handler` |
| result finalization | terminal observation plus the kickoff +3-hour football condition | `test_live_finalization_and_correction_deadlines_require_saved_football_conditions` |
| statistics retry | eligibility offsets 0, 15m, 1h, 6h, 24h; capped retry becomes `retry_exhausted` | `test_statistics_retry_follows_all_configured_offsets_and_signals_exhaustion` |
| correction check | immutable first-terminal +24h and +72h event windows | `test_live_finalization_and_correction_deadlines_require_saved_football_conditions` |
| standings | daily control away from a matchday; policy interval on a matchday | `test_discovery_quality_and_standings_modes_calculate_from_saved_season_inputs`; `test_standings_uses_hourly_policy_interval_on_a_matchday` |
| analytics/scanner | fixture-write trigger records the latest version in each fixed 60-second window; an event exactly on a boundary starts the next window; enqueue atomically records Q02 work, version identity, and window acceptance | `test_q05_analytics_windows_keep_their_first_deadline_across_delay_and_restart`; `test_q05_analytics_event_on_a_window_boundary_waits_for_the_next_close`; `test_q05_two_scheduler_connections_accept_one_analytics_window_once` |
| quality sweep | daily policy work from the saved season input | `test_discovery_quality_and_standings_modes_calculate_from_saved_season_inputs` |

The repository boundary is covered by real isolated PostgreSQL sessions:
`test_q05_two_scheduler_processes_do_not_duplicate_analytics_version`,
`test_q05_analytics_checkpoint_failure_rolls_back_enqueue_and_retry_is_safe`,
`test_q05_analytics_no_handler_and_stale_policy_write_nothing`, and
`test_q05_read_only_snapshot_is_consistent_and_next_snapshot_sees_update`.
`test_scheduler_cli_passes_materialized_season_and_analytics_inputs` verifies
that both preview and `--enqueue` receive the same materialized season and
analytics inputs.

Analytics windows begin only when the additive fixture trigger observes a new
saved `last_source_fetch_id`. Existing fixtures without an observed window are
reported as `analytics_window_history_unavailable`; the scheduler does not
invent a historical input sequence from the current latest value.
If a version arrives for an already accepted source window, the trigger keeps
that accepted row immutable and stores the latest late version in the next free
durable identity. The persisted deadline remains the close of the source
60-second window: identity collisions may move the storage key, but never delay
enqueue eligibility or overwrite another version.

## Q03 leases

The opt-in repeatable worker receives a globally increasing `lease_token` on
every claim. Heartbeat, checkpoint, requeue and completion require the same
owner, token, running status and unexpired lease. Expired work becomes pending;
attempt exhaustion and contract errors are quarantined, and explicit retry
issues a new token and a fresh bounded attempt budget while retaining the
historical attempt total. A repeatable worker requires a separate heartbeat
connection; Fetch/HTTP occurs outside a transaction. Result writes,
dependent enqueue calls and completion use one connection and one transaction:
the lease guard locks and validates the token before the first domain write.
Canonical writers used there must accept that connection and must not commit or
open a separate connection.
