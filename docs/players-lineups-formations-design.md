# Players, historical lineups, and formations: design proposal

**Status:** migration drafted and under local validation. Remote Supabase
deployment, importer changes, API-Football requests, and Supabase DML remain
out of scope until separately approved.

## Purpose and non-goals

This proposal adds a canonical, provenance-backed representation of **actual
historical lineups** for completed fixtures. It enables future factual views
and aggregates such as a team's historical formation usage. It does not add
player statistics, events, live data, odds, predictions, or a frontend.

The product is a football analytics platform. A lineup retrieved after a
historical match is evidence of the final/historical lineup, **not** evidence
that it was known before kickoff. It must therefore remain structurally
separate from the existing pre-match snapshot model.

## Evidence audited

The proposal is based on the real saved response
`samples/api-football/lineups.raw.json` for
`GET /fixtures/lineups?fixture=1208021`, together with the existing schema and
canary importer.

Observed, contract-relevant facts:

- wrapper `results` is `2`, with one entry for each fixture team; response
  ordering is not a home/away contract;
- each team entry has `team.id`, nullable-tolerant `coach`, a provider
  `formation` string, `startXI`, and `substitutes`;
- starter and substitute players have provider `id`, `name`, `number`, `pos`,
  and nullable `grid` (`grid` was populated for starters and `null` for the
  observed substitutes);
- no appearance minutes, captain marker, substitution events, player match
  statistics, or pre-match availability time is supplied;
- the response has no fixture identifier inside the body, so the typed source
  fetch subject is required provenance.

The authoritative raw contract details and its limitations remain in
[`api-football/lineups.md`](api-football/lineups.md).

## Existing model retained unchanged

The following tables already exist and must not be repurposed:

- `football.players` and `source.player_provider_refs`;
- `football.coaches` and `source.coach_provider_refs`;
- `football.fixture_lineup_snapshots`, `football.fixture_lineups`, and
  `football.fixture_lineup_players`.

The latter three are append-only, actual **pre-kickoff** observations. Their
database guards require a successful `prematch` `/fixtures/lineups` fetch,
timestamps before kickoff, and an in-transaction immutable child set. They
are correctly empty in development today. Historical backfill must never write
to them.

The observed player contract justifies the existing durable fields
`display_name` and `photo_url`; the observed coach contract justifies the
existing `display_name` and `photo_url`. No age, nationality, current club,
birth data, player statistics, or coach-tenure fields are justified by this
endpoint and none are proposed.

## Proposed additive canonical model

### Fetch-purpose distinction

Add `historical_backfill` to `source.fetch_purpose`. It means a deliberate
post-completion retrieval for factual historical data. It is distinct from
`prematch`, which is reserved for knowledge observed before kickoff.

### `football.fixture_historical_lineup_snapshots`

One immutable **historical/reconstructed** snapshot represents the complete
response to a post-match lineup retrieval for one fixture. Its name is
intentionally distinct from `football.fixture_lineup_snapshots`, which remains
the pre-match-only lane.

| Column | Type | Rule / purpose |
| --- | --- | --- |
| `id` | `bigint GENERATED ALWAYS AS IDENTITY` | Primary key. |
| `fixture_id` | `bigint NOT NULL` | FK to `football.fixtures`; fixture requested from provider. |
| `source_fetch_id` | `bigint NOT NULL` | FK to `source.provider_fetches`; exact raw/provenance source. |
| `content_sha256` | `bytea NOT NULL` | Same 32-byte content hash as the source fetch; enables duplicate-content protection. |
| `captured_at` | `timestamptz NOT NULL` | Exactly the fetch's `response_received_at`; it is retrieval time, not a claimed pre-match availability time. |
| `available_at` | `timestamptz NOT NULL` | Equal to `captured_at`; never moved back toward kickoff. |
| `availability_basis` | `football.availability_basis NOT NULL` | Fixed to `reconstructed_conservative`, never `observed`. |
| `coverage_state` | `football.snapshot_coverage_state NOT NULL` | `complete`, `partial`, `empty`, or documented `unknown`; assigned only after wrapper and participant validation. |
| `team_count` | `smallint NOT NULL` | `0..2`; verified against child rows at commit. |
| `mapping_version` | `text NOT NULL` | Non-blank parser/normalization contract version. |
| `ingest_txid` | `bigint NOT NULL DEFAULT txid_current()` | Binds child insertion to this snapshot's transaction. |
| `created_at` | `timestamptz NOT NULL DEFAULT clock_timestamp()` | Database audit time. |

Constraints and indexes:

- `PRIMARY KEY (id)`;
- `UNIQUE (source_fetch_id)`;
- `UNIQUE (fixture_id, content_sha256)`;
- `CHECK (octet_length(content_sha256) = 32)`;
- `CHECK (available_at = captured_at)`;
- `CHECK (availability_basis = 'reconstructed_conservative')`;
- `CHECK (btrim(mapping_version) <> '')`;
- `CHECK (team_count BETWEEN 0 AND 2)`;
- `CHECK (coverage_state <> 'complete' OR team_count = 2)`;
- `CHECK (coverage_state <> 'empty' OR team_count = 0)`;
- `CHECK (coverage_state <> 'partial' OR team_count = 1)`;
- index `(fixture_id, captured_at DESC, id DESC)` for factual fixture views and
  explicit latest-observation selection.

`unknown` is reserved only for a retained raw response whose wrapper can be
validated but whose coverage cannot be classified by an explicitly documented
future provider-contract case. It is not a fallback for an importer error. The
current observed contract is classified as `complete`, `partial`, or `empty`.

This is an append-only historical snapshot log, not a mutable `current lineup`
row.
If the provider later corrects a lineup with a different payload, it creates a
new observation. A consumer must make an explicit, documented choice of which
post-match observation to display (normally the latest complete one). An
identical raw payload for the same fixture cannot create a duplicate canonical
observation.

### `football.fixture_historical_lineups`

One row per team included in an historical observation.

| Column | Type | Rule / purpose |
| --- | --- | --- |
| `snapshot_id` | `bigint NOT NULL` | FK to the historical snapshot. |
| `team_id` | `bigint NOT NULL` | FK to `football.teams`; must be a home or away participant of the fixture. |
| `coach_id` | `bigint NULL` | FK to `football.coaches`; provider coach identity when present. |
| `formation` | `text NULL` | Provider-supplied, extensible tactical formation such as `4-2-3-1`. |
| `starter_count` | `smallint NOT NULL` | `>= 0`; validated against `starter` rows at commit. |
| `substitute_count` | `smallint NOT NULL` | `>= 0`; validated against `substitute` rows at commit. |

`PRIMARY KEY (snapshot_id, team_id)` prevents a duplicate team lineup.
`formation` stays plain text (with a non-blank check when non-null), not an
enum or a separate formation catalogue. The real contract proves a string,
not a closed vocabulary. Add an index on `(team_id, formation)` where formation
is non-null for future factual formation analysis, and `(coach_id, snapshot_id)`
where `coach_id` is non-null for future coach-scoped factual history.

### `football.fixture_historical_lineup_players`

One named player in a particular team lineup observation.

| Column | Type | Rule / purpose |
| --- | --- | --- |
| `snapshot_id`, `team_id` | `bigint NOT NULL` | Composite FK to the team-lineup row. |
| `player_id` | `bigint NOT NULL` | FK to `football.players`. |
| `lineup_role` | `football.lineup_role NOT NULL` | Existing `starter` or `substitute` type. |
| `position` | `text NULL` | Provider compact position (observed: `G`, `D`, `M`, `F`); extensible. |
| `shirt_number` | `smallint NULL` | `0..199`, preserving null rather than inventing zero. |
| `grid` | `text NULL` | Provider grid; null is meaningful for observed substitutes. |

Constraints and indexes:

- `PRIMARY KEY (snapshot_id, team_id, player_id)`;
- `UNIQUE (snapshot_id, player_id)`: a player cannot appear in both fixture
  teams in the same observation;
- composite FK `(snapshot_id, team_id)` to
  `fixture_historical_lineups`;
- `CHECK` constraints for valid shirt range and non-blank values where textual
  attributes are present;
- index `(player_id, snapshot_id DESC)` for future factual player history.

The provider response array order is retained in raw JSON but is not normalized
as a canonical analytic field. The real sample does not establish that this
ordering has stable domain semantics. Neither exact starter/substitute counts
nor the position/grid vocabulary are treated as a closed provider contract.

The database can prove that the provider team is a fixture participant and
that one player is not assigned to both lineups. It cannot prove the player's
then-current club from this endpoint: no historical roster contract has been
accepted. The importer must therefore validate each response `team.id` against
the fixture and use the team association from that provider response, never a
player's current club.

## Provenance, validation, and immutability

Only narrow relational triggers are proposed; no trigger parses or interprets
raw API-Football JSON.

1. A historical-snapshot guard requires a successful `historical_backfill`
   `/fixtures/lineups` fetch, matching `subject_fixture_id`, non-null response
   timestamp and content SHA-256, a non-purged raw payload, and equality of
   `captured_at`/`available_at` to that fetch's `response_received_at`.
2. The guard requires the fixture to be `completed`, have a non-null
   `result_finalized_at`, and have `captured_at`/`available_at >=
   result_finalized_at` (therefore strictly after the historical match has
   reached our finalized state). Thus this table cannot become a live or
   pre-match data path.
3. A generic fixture-participant guard verifies each team row is the fixture's
   home or away team. This is a domain invariant, not a provider JSON rule.
4. Parent and child rows are insert-only. Child guards require the parent
   `ingest_txid` to equal `txid_current()`, so later transactions cannot append
   players to an existing snapshot. A deferred commit trigger verifies
   `team_count`, `starter_count`, and `substitute_count` against the rows
   inserted in the same transaction.
5. Existing pre-match lineup tables and their guards remain untouched. Future
   pre-kickoff data can only go through their `prematch` path; historical
   snapshots are explicitly `reconstructed_conservative`, have `available_at =
   captured_at` after kickoff, and live in separate tables. They cannot be
   selected as a pre-match input by accident.
6. Extend `source.guard_referenced_provider_fetch_response` so a fetch
   referenced by a historical-lineup observation receives the same immutability
   protection as coverage and exact-status provenance. It must additionally
   block changes to `purpose`, `request_params`, `request_params_sha256`, and
   `subject_team_id`, alongside the already protected provider, endpoint,
   response timestamp, HTTP status/outcome, content hash, and fixture/season
   subjects. Updating `normalized_at` remains permitted.
7. Team-lineup coach IDs and lineup player IDs must each have a provider mapping
   for the provenance fetch's provider. Names and current-club data are never
   used as identity evidence.

The importer/service layer, not SQL, validates wrapper shape, integer provider
IDs, array membership, nullable values, `results`/coverage classification, raw
hash computation, and response-team membership before it starts the database
transaction.

## Normalization and replay strategy

For one fixture, use deliberately separate transaction boundaries:

1. **Transaction A — raw evidence:** persist the successful source fetch and
   raw payload with typed `subject_fixture_id`, parameters hash, content hash,
   and bounded retention metadata, then commit. This persistence requires only
   safe transport/wrapper decoding, not a successful lineup normalization.
2. Validate the retained bytes and their SHA-256 outside the normalization
   transaction: requested fixture binding, wrapper shape, coverage
   classification, participant team IDs, player/coach IDs, duplicate players,
   and nullable field types.
3. **Transaction B — canonical normalization:** resolve player and coach
   provider mappings using the existing `resolve_entity` pattern; insert one
   immutable historical snapshot, its zero-to-two participant team rows and its
   starter/substitute rows; set the source fetch `normalized_at` **before the
   same transaction commits**. Deferred checks run at `COMMIT`, so any failure
   rolls back the canonical rows and `normalized_at` atomically.
4. If validation or Transaction B fails, roll back Transaction B only. Keep
   Transaction A's raw evidence and fetch metadata. In a separate short
   transaction, classify a safe `sanitized_error_class`/text and, where
   appropriate, promote its bounded raw retention class to `anomaly` for
   review. It remains replayable without another provider call.

Safe resume/replay works without a new provider call whenever a raw payload is
already stored. Replaying the same fetch is a no-op by its unique key. A second
fetch with byte-identical content for the same fixture cannot create a second
historical snapshot because of `(fixture_id, content_sha256)`. A distinct correction is
retained as a new immutable historical snapshot, rather than deleting or overwriting
the earlier audit record.

Raw bytes use the existing bounded `standard` retention policy. If that body is
purged, the source-fetch metadata and SHA-256 remain linked to the canonical
observation, but a byte-for-byte replay requires a newly collected provider
response. Retention is therefore explicit provenance policy, not a reason to
retain credentials or unbounded payloads.

Empty and partial responses are valid provenance outcomes. They create a
historical snapshot with `coverage_state = empty`/`partial` and a verified team
count;
they do not fabricate missing lineups or players. A malformed response,
non-participant team, duplicate player, or contract mismatch rolls back its
normalization transaction, retains its raw evidence as a controlled anomaly,
and stops the job for review.

## Migration and backfill acceptance tests

Before any remote deployment, the future migration/importer stage must prove:

1. migration succeeds on a clean disposable database and over a copy of the
   current development schema/data;
2. fingerprints, IDs, raw payload hashes, fixtures, standings, and all 760
   existing `fixture_team_statistics` rows remain unchanged;
3. a future importer contract-integration test using the saved real lineup
   sample creates one historical snapshot, two participant team rows, 40 player
   rows, and only canonical player/coach mappings. The migration suite itself
   uses minimal provider-shaped synthetic raw solely to prove relational
   invariants; it does not claim to parse the provider body;
4. replay of the same stored response makes no duplicate rows or provider API
   request;
5. invalid source endpoint/purpose/request fixture binding/fixture
   subject/timestamp/finalization bound, a non-participant team, a player or
   coach lacking a provider mapping, a duplicate player, and an invalid player
   count are all rejected;
6. `complete`/`empty`/`partial` coverage states reject an incompatible team
   count, while `unknown` is accepted only for its documented contract case;
7. observations and children are immutable; their referenced source fetch
   response identity (including purpose and request hash) is immutable; a valid
   later correction is append-only;
8. historical rows cannot be inserted into any pre-match snapshot table;
9. `anon` and `authenticated` retain no direct table DML or readable base-table
   access, with RLS enabled for all new tables;
10. a read-only remote verification checks orphans, duplicate provider mappings,
   duplicate players per observation, team/fixture mismatches, counts, raw
   provenance, and no mutation to analytics or existing fixture statistics.

## Explicit exclusions

This stage does not add player statistics, events/timeline, substitutions as
actual match events, captaincy, team rosters/transfers, odds, provider
predictions, live snapshots, a cache, a frontend endpoint, or any prediction/
ML feature. Those require separate real-contract evidence and separate
approval.

## Proposed execution order

1. Independently review this design against the saved raw sample and current
   Stage 3B safeguards.
2. Prepare a single additive migration plus local migration tests; do not apply
   it to development Supabase yet.
3. Review migration/security and show the exact remote deployment plan.
4. Only after explicit approval, apply the migration remotely and verify all
   preservation fingerprints.
5. Only after a separate approval, implement a controlled historical-lineup
   canary/backfill.
