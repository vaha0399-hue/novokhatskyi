# Preliminary data-model notes from real samples

## Status of this document

These are evidence-backed storage recommendations for the future Stage 3
design discussion. They are **not** a final PostgreSQL/Supabase schema, DDL,
migration, importer specification, or authorisation to create tables.

Evidence comes from Premier League research season 2024. Production season
2026 remains the target and still needs future/pre-match samples when plan
access becomes available.

## Stable external ID candidates

The samples consistently link entities by provider numeric IDs:

- `fixture_id` — fixture identity and parent for statistics/lineups/injuries;
- `league_id` — `39` for the Premier League sample;
- `team_id` — shared by teams, fixtures, standings, statistics, injuries and lineups;
- `player_id` — shared by injuries and lineup players;
- `venue_id` — present in teams and fixtures;
- `coach_id` — present in lineups;
- `season` — integer season-start year; part of league-season context, not a
  globally unique entity ID by itself.

Names, codes, URLs, rank, formation, shirt number and provider descriptions are
attributes, not identity keys. The observed `(fixture_id, player_id)` injury
pair is unique in this snapshot, but availability text can change; a snapshot
timestamp is required before treating repeated retrievals as distinct facts.

## A. Must retain in normalized product data

1. **Fixture core**: external fixture ID, league ID, season, kickoff date/time,
   timezone/source timestamp, status values, round, home/away team IDs,
   venue ID, and fetched/updated context.
2. **Fixture outcome components**: home/away goals; halftime, fulltime,
   extra-time and penalty score components with explicit nullability. Do not
   coerce null to zero.
3. **Team identity and season participation**: team ID and season/league
   membership; current display name/code/logo are mutable attributes.
4. **Venue identity**: venue ID and descriptive attributes, while avoiding an
   unproven permanent one-team/one-venue constraint.
5. **Home/away roles**: preserve roles explicitly. A fixture is not an
   unordered team pair.
6. **Source lifecycle timestamps**: application `fetched_at` and provider
   timestamps where supplied, so pre-match knowledge is not overwritten by a
   later state without auditability.
7. **Raw external IDs needed for joins**: player and coach IDs whenever lineup
   or availability data becomes an accepted product input.

## B. Possibly retain, depending on V1 feature scope

1. **Fixture-statistic metrics** such as shots, possession, passing and xG.
   They are not derivable from final score. Values require a tolerant
   representation because the same `value` field is integer, percent string,
   decimal string or null, and metric labels are provider strings.
2. **Fixture-specific availability snapshots** from injuries. Retain
   `fixture_id`, `team_id`, `player_id`, provider `type`/`reason`, and
   `fetched_at` if these inputs affect predictions. Reason text is not a stable
   taxonomy.
3. **Lineup snapshots**: fixture/team/player/coach IDs, starter-vs-bench role,
   formation, position, number and nullable grid, timestamped at collection.
   This is valuable only if a near-kickoff model or UI uses it.
4. **Standings snapshots** for source validation/audit. Rank, points,
   descriptions and form are time-dependent; they must not become identity.
5. **Provider team-statistics snapshots** for short-term validation against
   locally calculated aggregates, not as the authoritative feature source.
6. **Mutable media/display attributes** such as logos, photos and colours when
   the website actually needs them; update them separately from identity.

## C. Calculate from our own database

Once completed fixtures and scores are canonical, calculate with an explicit
cutoff timestamp:

- Last 5 form;
- Last 10 form;
- H2H over a defined window;
- home and away form/performance;
- average goals scored;
- goals conceded;
- home/away/overall win rate;
- draw rate;
- loss rate;
- clean-sheet and failed-to-score rates;
- table strength and opponent-strength features;
- current points/table aggregates;
- goal difference and season-to-date W/D/L totals.

Do not use the provider compact `form` string or upstream team aggregates as
the sole feature source. Local calculation gives deterministic cutoffs,
prevents future information leakage, supports model versioning, and avoids an
API call per user request.

Minute buckets, cards, xG, possession, passing, shots, availability, and actual
lineups cannot be reconstructed from the current fixture/score data alone.
They require their relevant source endpoint if selected for V1.

## D. Do not retain as normalized domain data

- repeated team/league names and logos copied into every fixture/statistic
  record when they can be resolved through external IDs;
- API request authentication headers or API keys;
- rate-limit headers as football-domain facts (they belong only in operational
  request accounting);
- provider `results` count as a business entity attribute;
- response array order as home/away or identity semantics;
- `rank`, display name, URL, lineup array index, or shirt number as stable IDs;
- coerced substitutes for null, such as `0`, empty string, or invented objects;
- a second authoritative copy of Last 5/10, H2H or home/away form fetched on
  every user request.

## E. Temporarily retain as raw/debug evidence

- exact raw API response body;
- safe request endpoint/parameters and collection timestamp;
- wrapper `errors`, `paging`, `results` and provider response-shape anomalies;
- original fixture-statistic labels and mixed-type values before a mapping is
  proven;
- full team-statistics response while validating local calculations;
- historical source samples used to build parser regression tests;
- rejected/empty pre-match responses when season 2026 access later permits
  collection, provided they contain no secrets.

Raw payload retention should be bounded by an explicit operational policy in a
later stage; it is not a substitute for normalized canonical data.

## Candidate entity boundaries for Stage 3 discussion

The samples justify discussing these conceptual boundaries later:

- league-season context;
- teams and season participation;
- venues;
- fixtures with explicit home/away relationships and score/status lifecycle;
- players and coaches only if injury/lineup inputs are accepted;
- fixture statistic observations;
- timestamped availability and lineup snapshots;
- optional standings/source-validation snapshots;
- raw ingestion audit records.

No column set, SQL type, uniqueness constraint or migration should be finalised
until future/pre-match season 2026 samples are obtained and update/idempotency
behaviour is tested.

## Critical evidence gaps before final schema approval

1. Future `NS` fixture nullability and fields absent before kickoff.
2. Postponed/cancelled/live status shapes and rescheduling behaviour.
3. Empty vs partial pre-match lineup responses and their update timing.
4. Injury changes across multiple retrieval timestamps for the same fixture.
5. Fixture-statistic label/value changes across multiple matches.
6. Cross-season team/venue changes and promoted/relegated teams.
7. API error and multi-page response samples under production access.

