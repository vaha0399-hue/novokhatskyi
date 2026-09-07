# Match data contract

Status: proposed contract for future importers and scanners.  It is not wired
into the active-season importer, live worker, analytics, database, or API
responses.  Verification date: **2026-09-07**.

## Sources and evidence boundary

Provider facts below come from the official [Fixtures reference](https://www.api-football.com/documentation-v3#tag/Fixtures/operation/get-fixtures), [fixture statistics reference](https://www.api-football.com/documentation-v3#tag/Fixtures/operation/get-fixtures-statistics), and [fixture events reference](https://www.api-football.com/documentation-v3#tag/Fixtures/operation/get-fixtures-events). The provider's [2024 release note](https://www.api-football.com/news/post/api-football-new-release-available) documents `fixture.status.extra` and the statistics `half` query; its [completed/live fixture guide](https://www.api-football.com/news/post/how-to-get-all-fixtures-data-from-one-league) uses `FT-AET-PEN` as completed and `1H-HT-2H-ET-BT-P` as in-progress.

The status table in the Fixtures reference is controlling. A March 2026
provider guide calls the list “16” statuses while its canonical table contains
the 19 codes listed here. `status.long` is display text, not a stable machine
contract. A newly observed code is therefore retained as raw evidence and put
into `unknown`; it is never guessed from its display text.

**Provider facts:** `score` exposes `halftime`, `fulltime`, `extratime`, and
`penalty` pairs; fixture responses also expose `goals`; statistics can be null;
event details include Yellow Card, Red Card, and Yellow-Red Card. The public
reference does not define whether every score field is cumulative or incremental,
does not promise a non-null `fulltime` for every terminal record, does not
define an extra-time-only/shootout statistics partition, and does not document
whether team-card statistics include bench/staff cards. It exposes venue id,
name, and city, but no neutral-venue boolean.

Everything labelled **product rule** is our choice. Everything labelled
**unknown** must remain nullable/provenanced and must not become zero.

## Results and advancement

**Product rule:** `score.fulltime` is the result after 90 minutes plus
stoppage time. It alone can fill `regulation_90`; `goals` must never substitute
for a missing `fulltime`. `score.halftime`, `score.extratime`, `score.penalty`,
and `goals` retain their provider-field labels rather than being arithmetically
combined.

| Example | Provider fields | Contract result |
| --- | --- | --- |
| FT | `fulltime=2–1`, `goals=2–1` | played regulation result 2–1; analytics eligible after normal result finalization |
| AET | `fulltime=2–2`, `extratime=3–2`, `goals=3–2` | 90-minute result remains 2–2; extra-time pair is separate; played match eligible after finalization |
| PEN | `fulltime=1–1`, `penalty=5–4`, `goals=1–1` | 90-minute result remains 1–1; shootout pair is separate and is not a goal total |
| missing FT | `fulltime=null`, `goals=4–0` | regulation result is unknown, not 4–0; no played-match analytics |
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
| FT | completed | reconcile a played regulation result only when `fulltime` exists |
| AET | completed | reconcile a played result; preserve 90-minute and extra-time fields separately |
| PEN | completed | reconcile a played result; preserve shootout separately |
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

**Product rule:** retain each raw observation with its provider observation
time. An observation older than the current projection is stale and does not
change it. Equal timestamps with different status codes require review.

Usual allowed forward paths are `TBD/NS → 1H → HT → 2H → FT`, with
`2H → ET → BT → ET/P → AET/PEN`; `PST → NS` is expected after a new date is
published. `SUSP` and `INT` may move to live, postponed, cancelled, abandoned,
or a provider-confirmed completed state. A later authoritative response may
correct a terminal status or result, including terminal-to-live/suspended;
reconcile the current projection while retaining prior raw evidence. Unknown
or impossible equal-time conflicts require review, never a guessed transition.

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
PEN/admin examples, missing fulltime, period isolation, stale responses, and
terminal corrections.

Current behavior differs intentionally until a separately approved integration:

- `active_season.py` accepts a subset and currently maps `AWD` to `completed`;
  this contract marks it administrative and non-played by default.
- `live/normalizer.py` supports only `1H`, `HT`, `2H`, and `FT`.
- `current_season_statistics.py` accepts `FT/AET/PEN`, but its parser permits
  nullable `score.fulltime`; this contract would keep such a result unresolved.
- `fixture_status_contract.py` validates raw response integrity and membership,
  but has no status-transition/result-period policy.
- Existing team statistics and analytics preserve nullable metric values and
  sample counts, but do not carry a period/bench-card provenance dimension.
