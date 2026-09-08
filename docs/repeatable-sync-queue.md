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

## Q03 leases

The opt-in repeatable worker receives a globally increasing `lease_token` on
every claim. Heartbeat, checkpoint, requeue and completion require the same
owner, token, running status and unexpired lease. Expired work becomes pending;
attempt exhaustion and contract errors are quarantined, and explicit retry
issues a new token. Fetch/HTTP occurs outside a transaction. Result writes,
dependent enqueue calls and completion use one connection and one transaction:
the lease guard locks and validates the token before the first domain write.
Canonical writers used there must accept that connection and must not commit or
open a separate connection.
