# Match data contract

Status: proposed contract for future importers and scanners.  It is not wired
into the active-season importer, live worker, analytics, database, or API
responses.  Verification date: **2026-09-08**.

## Sources and evidence boundary

Provider facts below come from the official [Fixtures reference](https://www.api-football.com/documentation-v3#tag/Fixtures/operation/get-fixtures), [fixture statistics reference](https://www.api-football.com/documentation-v3#tag/Fixtures/operation/get-fixtures-statistics), and [fixture events reference](https://www.api-football.com/documentation-v3#tag/Fixtures/operation/get-fixtures-events). The provider's [2024 release note](https://www.api-football.com/news/post/api-football-new-release-available) documents `fixture.status.extra` and the statistics `half` query; its [completed/live fixture guide](https://www.api-football.com/news/post/how-to-get-all-fixtures-data-from-one-league) uses `FT-AET-PEN` as completed and `1H-HT-2H-ET-BT-P` as in-progress.

The status table in the Fixtures reference is controlling. A March 2026
provider guide calls the list “16” statuses while its canonical table contains
the 19 codes listed here. `status.long` is display text, not a stable machine
contract. A newly observed code is therefore retained as raw evidence and put
into `unknown`; it is never guessed from its display text.

**Provider facts:** `score` exposes `halftime`, `fulltime`, `extratime`, and
`penalty` pairs; fixture responses also expose `goals`; statistics can be null;
event details include Yellow Card, Red Card, and Yellow-Red Card. The Fixtures
reference was rechecked on 2026-09-08. It does not explicitly define
`score.fulltime` as the 90-minute-plus-stoppage result for every fixture case,
does not define whether every score field is cumulative or incremental, does
not promise a non-null `fulltime` for every terminal record, does not define
an extra-time-only/shootout statistics partition, and does not document whether
team-card statistics include bench/staff cards. It exposes venue id, name, and
city, but no neutral-venue boolean.

Everything labelled **product rule** is our choice. Everything labelled
**unknown** must remain nullable/provenanced and must not become zero.

## Results and advancement

`score.fulltime` is retained only as `provider_fulltime`. Its period semantics
are **unconfirmed**, so it cannot fill `regulation_90` or automatically enter
90-minute analytics. `goals` must never substitute for a missing
`provider_fulltime`. `score.halftime`, `score.extratime`, `score.penalty`, and
`goals` retain their provider-field labels rather than being arithmetically
combined.

| Example | Provider fields | Contract result |
| --- | --- | --- |
| FT | `fulltime=2–1`, `goals=2–1` | retain `provider_fulltime=2–1`; 90-minute period remains unconfirmed and ineligible for automatic 90-minute analytics |
| AET | `fulltime=2–2`, `extratime=3–2`, `goals=3–2` | retain provider-labelled pairs separately; no automatic 90-minute result |
| PEN | `fulltime=1–1`, `penalty=5–4`, `goals=1–1` | retain provider-labelled pairs separately; shootout is not a goal total |
| missing FT | `fulltime=null`, `goals=4–0` | `provider_fulltime` is unknown, not 4–0; no automatic 90-minute analytics |
| AWD or WO | any supplied score | administrative outcome; never automatically a played match |

**Product rule:** `AET` and `PEN` establish that the fixture completed, but do
not by themselves identify the team advancing in a round. Two-legged ties,
group rules, replays, and competition-specific regulations can decide
advancement. Store `advancement=unknown` until a provider field or reviewed
competition rule proves it.

## Provider status map

| Provider code | Internal state | Allowed action |
| --- | --- | --- |
| TBD | scheduled | retain schedule; kickoff is unknown |
| NS | scheduled | retain/refresh schedule |
| 1H, 2H, ET, P, LIVE | in progress | track live only; do not finalize |
| HT, BT | paused | retain live snapshot and recheck |
| SUSP | suspended | preserve state and recheck; may be rescheduled |
| INT | interrupted | preserve state and recheck; may resume shortly |
| FT | completed | reconcile a played result while retaining `provider_fulltime` separately |
| AET | completed | reconcile a played result; preserve provider fulltime and extra-time fields separately |
| PEN | completed | reconcile a played result; preserve provider fulltime and shootout fields separately |
| PST | postponed | retain fixture, await a new confirmed kickoff/NS response |
| CANC | cancelled | terminal non-played outcome; do not aggregate |
| ABD | abandoned | terminal uncertain outcome; preserve raw data and require review before aggregation |
| AWD | administrative | technical loss, not automatically played; review |
| WO | administrative | walkover/forfeit, not automatically played; review |
| any other or malformed code | unknown | preserve raw response and stop automatic normalization |

`TBD` may have no known kickoff. `PST` may also have no replacement kickoff;
that absence is unknown, not a fake timestamp. The home/away slots and venue
object do not prove neutral or non-neutral venue. **Product rule:** use
`neutral_venue=unknown` until a reviewed source establishes it.

## Transitions, delayed responses, and corrections

Football phase validity and periodic-poll freshness are separate decisions.
Neither kickoff time, provider time, nor response-arrival order proves that a
snapshot is newer.

**Product rule:** assign each poll request a monotonically increasing local
`request_sequence` when it is sent, per fixture/projection. Store
`received_at` separately for audit and latency analysis, but never use it to
order projections. A response with a lower `request_sequence` is ignored even
when it arrives later. This contract does not introduce a queue, worker, or
storage implementation.

Each retained per-fixture snapshot has an immutable SHA-256 content fingerprint
covering its status and every retained score/period field (normally the
canonical raw fixture object). The pure helper explicitly includes the status
code in its hash input. Fingerprints are compared, never recreated from the
current projection. The decision order is:

1. Lower local request sequence: ignore the older request response and retain
   the current watermark.
2. Same fingerprint: `NO_CHANGE`, but advance the processed-request watermark.
3. Same sequence but different fingerprint: review conflict and advance the
   watermark; retain raw evidence.
4. Newer, different content: apply the football phase graph below and advance
   the watermark whether it is applied or held for review.

Thus a repeated identical `FT` is `NO_CHANGE`; an `FT` with a changed score is
a result correction, not a no-op merely because the status code matches.

Usual allowed forward paths are `TBD/NS → 1H → HT → 2H → FT`, with
`2H → ET → BT → ET/P → AET/PEN`; `PST → NS` is expected after a new date is
published. Polling can miss intermediate phases, so `NS → HT` and `NS → FT`
are valid forward observations. Precise phase regressions such as `2H → 1H`
and `ET → 2H` require review and are never applied automatically. Repeated
`ET`, and `ET → BT → ET`, are valid. A projection retains the last precise
phase across `LIVE`, `SUSP`, `INT`, and `BT`: for example, `2H → LIVE → 1H`
is a regression requiring review, not an automatic rollback. `LIVE` means
in-progress with an unspecified phase: it may refresh a nonterminal snapshot,
but cannot erase a previously known precise phase. A move to `PST` after
`SUSP` or `INT` is permitted and clears the retained precise phase because a
replacement fixture time is pending.

`SUSP` and `INT` may move to live, postponed, cancelled, abandoned, or a
provider-confirmed completed state. A newer terminal snapshot with changed
content is a result correction. A terminal-to-live/suspended response is a
phase conflict pending reviewed repair; raw evidence is retained. Unknown
codes and other phase conflicts require review, never a guessed transition.

## Statistics, corners, and cards

Statistics are nullable. **Product rule:** missing, null, or unsupported is
`unknown`, never zero. A statistic enters 90-minute team aggregates only when
its provenance explicitly identifies `regulation_90`. First-half, second-half,
extra-time, shootout, and unknown-period observations remain separate; they
must not be combined with 90-minute corners, cards, possession, or shots.

The provider's `half` parameter supports halftime statistics, but its public
documentation does not define a 90-minute-only total when extra time occurs.
Thus a full-fixture statistics payload for an AET/PEN match has
`period=unknown` unless a future collection contract proves its period.

For event-derived cards, preserve provider event type/detail and subject role.
A `Yellow-Red Card` is a second-booking dismissal: it is one dismissal event,
but it must not fabricate a second yellow count when the first booking is not
separately observed. Bench/staff cards stay separate from player cards and are
excluded from team-card aggregates unless a future reviewed provider rule
proves their inclusion. Corners follow the same period/provenance rule.

## Executable boundary and current gaps

`backend/app/importer/match_data_contract.py` is the executable pure-function
form of this document. Its table tests cover all 19 documented codes, FT/AET/
PEN/admin examples, missing fulltime, unconfirmed provider-fulltime semantics,
period isolation, skipped phases, phase regressions, repeated ET around BT,
ambiguous LIVE, local request ordering and watermarks, identical observations,
and terminal corrections.

Current behavior differs intentionally until a separately approved integration:

- `active_season.py` accepts a subset and currently maps `AWD` to `completed`;
  this contract marks it administrative and non-played by default.
- `live/normalizer.py` supports only `1H`, `HT`, `2H`, and `FT`.
- `current_season_statistics.py` accepts `FT/AET/PEN`, but its parser permits
  nullable `score.fulltime`; this contract retains a non-null value only as
  provider-labelled data and does not infer a 90-minute result.
- `fixture_status_contract.py` validates raw response integrity and membership,
  but has no status-transition/result-period policy.
- Existing team statistics and analytics preserve nullable metric values and
  sample counts, but do not carry a period/bench-card provenance dimension.
