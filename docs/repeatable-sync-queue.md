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

Section-7 work types whose fixture/reconciliation/input-version reader is not
yet connected remain visible in preview as `input_unavailable`; arbitrary
policy names use `not_implemented`. Neither result causes enqueue or a state
advance. In particular, time alone never supplies the football conditions for
result finalization.

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
