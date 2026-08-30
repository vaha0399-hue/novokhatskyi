# Product Scope Canon

Status: **approved**
Effective date: **2026-08-29**

This document is the canonical product scope for Football Analytics. If an
older document conflicts with the live or provider-prediction delivery order,
this document takes precedence.

## Product direction

Football Analytics is a **web football analytics platform**. It gives users
historical match data, calculated statistical indicators, team comparisons,
analytical views, and backend-synchronised live match state. The user makes
their own decisions from factual data.

The approved delivery order adds a narrowly scoped Premier League live
pipeline first. Provider-derived fixture predictions are a separate, later
T-60 workflow: they are stored in Supabase before kickoff, delivered through
FastAPI, and never treated as current live state. Their presentation contract
is defined with that separate workflow.

Existing inactive `ml` database structures must not be assumed suitable for
the later provider-prediction workflow. Reusing, extending, or removing them
requires a separately reviewed and approved migration.

## Product boundary

Only a responsive website is being developed:

- Next.js;
- TypeScript;
- App Router;
- desktop and mobile browsers.

Do not develop a mobile application, desktop application, browser extension,
or standalone public API for external clients.

The initial competition is the Premier League. Architecture, backend DTOs,
analytics, and frontend routes must remain league- and season-independent so
that additional leagues and seasons can be added through data rather than an
application rewrite.

## Target architecture

```text
API-Football
    -> Importer
    -> Supabase PostgreSQL
    -> Analytics Engine
    -> FastAPI
    -> Next.js
    -> User

API-Football
    -> central Live Worker (every 25 seconds)
    -> Live Domain / normalizer
    -> Redis current live state
    -> FastAPI `GET /web/v1/live`
    -> Next.js
```

Authentication flow:

```text
Supabase Auth
    -> Next.js session
    -> FastAPI JWT verification
    -> Free / Premium access policy
```

Browser data flow:

```text
Browser -> Next.js -> FastAPI -> Supabase PostgreSQL
```

Architecture invariants:

1. API-Football is an upstream source used only by controlled backend jobs.
2. User page requests never initiate API-Football requests.
3. The browser never queries `football.*`, `source.*`, `ops.*`, or other base
   tables directly.
4. Next.js is a thin BFF/UI layer. Football business logic and aggregates live
   in FastAPI and the Analytics Engine.
5. FastAPI returns stable DTOs, not PostgreSQL rows copied one-to-one.
6. Historical completed-match data is the foundation for analytics.
7. Live data is collected only by one backend worker. The browser and its
   requests never initiate an API-Football call.
8. Redis holds only current live state. Supabase remains authoritative for
   canonical fixtures and final results.
9. Provider predictions are a separate pre-kickoff Supabase workflow, not
   Redis live state, and are immutable after kickoff.
10. Calendar-day reads use the user's browser-resolved IANA timezone. The
    frontend sends it explicitly; neither Next.js nor FastAPI hardcodes a
    product timezone.

## FastAPI web read contract

The planned internal browser-facing read contract is:

```text
GET /web/v1/leagues
GET /web/v1/leagues/{league_id}/seasons

GET /web/v1/matches/leagues?date={date}&timezone={iana_timezone}
GET /web/v1/matches?date={date}&league_id={league_id}&timezone={iana_timezone}

GET /web/v1/seasons/{season_id}/standings
GET /web/v1/seasons/{season_id}/fixtures

GET /web/v1/fixtures/{fixture_id}
GET /web/v1/fixtures/{fixture_id}/statistics
GET /web/v1/fixtures/{fixture_id}/analytics
GET /web/v1/live

GET /web/v1/teams/{team_id}
GET /web/v1/teams/{team_id}/fixtures
GET /web/v1/teams/{team_id}/analytics

GET /web/v1/me
```

`GET /web/v1/live` returns Redis-backed current state as a stable
`LiveFixtureDTO`; it must not call API-Football. IDs in this contract identify
internal domain entities unless the DTO explicitly states otherwise. No route
may hardcode Premier League or season 2024. A prediction read contract follows
only after the separate T-60 prediction workflow is implemented.

## Analytics Engine scope

The Analytics Engine calculates values from PostgreSQL and does not call
API-Football while serving users.

Minimum team analytics:

- Last 5, Last 10, and Last 20 when enough history exists;
- overall, home-only, and away-only splits;
- wins, draws, losses, points per game, and current streak;
- goals scored/conceded and their averages;
- xG and xGA;
- shots and shots on target;
- possession, corners, and cards;
- clean sheets and failed-to-score rate;
- BTTS and historical over/under rates;
- current table position.

Fixture analytics compares the home team's overall/home form with the away
team's overall/away form. It may generate factual observations such as
"scored in 9 of the last 10 home matches", but must not convert observations
into a predicted result or artificial probability.

Later, after data quality and history are sufficient, the engine may add H2H,
strength of opposition, injuries, lineups, and further derived metrics.
Retrospectively collected injuries or lineups must not be represented as
historically known pre-match information.

## Historical data foundation

The Premier League 2024 historical fixture-statistics backfill is complete:

- 380 completed fixtures;
- 760 `football.fixture_team_statistics` rows;
- exactly two finalized, provenance-linked team-statistics rows for every
  fixture;
- no duplicate or orphan statistics rows.

This is **team-level fixture statistics**, not player statistics. Canonical
players, lineups, formations, and odds snapshots remain separate future data
stages. Imported Supabase data is environment state and is never committed to
Git; only importer code, migrations, tests, and documentation are versioned.

## Website scope

MVP routes:

```text
/
/matches?date={date}&timezone={iana_timezone}
/matches/leagues/{league_id}?date={date}&timezone={iana_timezone}
/leagues
/leagues/{league}
/leagues/{league}/seasons/{season}
/fixtures/{fixture_id}
/teams/{team_id}
/analytics
/predictions
/favorites
/login
/register
/forgot-password
/account
```

The approved global navigation order is **Матчи | Лиги | Аналитика | Прогнозы
| Избранное**. Routes may initially expose empty or staged product sections;
the visual navigation is implemented only in its separately approved UI step.
Future routes may include `/pricing` and `/saved-filters`.

The Matches route keeps the selected date and timezone in the URL. Its date
strip covers ±7 days, labels the current user-local day as «Сегодня», supports
range arrows, arbitrary calendar selection, and horizontal mobile swipe. Date
selection first loads only leagues with fixtures and their counts; selecting a
league then loads only that league's fixtures for the day. Selecting a fixture
opens `/fixtures/{fixture_id}`. This two-step contract avoids preloading every
match on narrow mobile screens.

The season page shows the league, dynamic season selection, standings,
rounds, fixtures, results, and links to fixtures and teams.

The fixture page shows fixture metadata, final score for completed matches,
table positions, factual match statistics, team form, home/away splits,
xG/xGA, attacking and defensive metrics, and later H2H. A future fixture may
show pre-match analytics calculated only from information available before
kickoff. The first live UI is deliberately deferred until the backend live
slice is stable; it will read FastAPI's Redis-backed live DTO. Provider-derived
predictions, when the later T-60 workflow exists, remain separate from that
live state.

The team page shows a selected season, match history, overall/home/away and
Last N views, aggregate metrics, and trends. The frontend consumes aggregates
from FastAPI and does not calculate domain metrics from raw fixture arrays.

## Authentication and access

Supabase Auth owns user identity, passwords, sessions, email confirmation,
password recovery, and password reset. Application code must not store user
passwords.

The browser bundle may contain only:

- `NEXT_PUBLIC_SUPABASE_URL`;
- `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY`.

It must never contain API-Football credentials, database URLs/passwords,
Supabase access tokens, service-role/secret keys, JWT signing secrets, or any
backend/admin credential.

The first auth milestone includes signup, login, logout, email confirmation,
password recovery/reset, session handling, and a protected account page.

The API must be able to distinguish future public, free-account, and premium
access without implementing subscriptions or payments now. Public data may
include results, standings, and basic statistics; deeper history and analytics
can later be gated. Payment and subscription implementation is a separate
future stage.

## Caching boundary

- Finalized completed-fixture data and statistics may use long-lived caching.
- Standings and history use controlled revalidation.
- Account, session, and user-specific responses are private and `no-store`.
- Authenticated responses must never be mixed into public ISR/shared caches.

## Approved development order

The current technical checkpoint and its acceptance criteria are in
[`2026-08-29-live-pipeline-plan.md`](2026-08-29-live-pipeline-plan.md).

1. Verify and bootstrap Premier League 2026/27 in the existing canonical
   model: 20 teams and their provider mappings, full fixture schedule,
   standings, and provider-to-internal fixture-ID resolution.
2. Deliver the backend-only live score/status slice: a reusable async provider
   client, central 25-second worker, `app/live` normaliser, Redis current
   state, and `GET /web/v1/live`.
3. Add live fixture-statistics polling only after score/status is reliable;
   it uses a separate, configurable cadence and active/hot fixtures only.
4. Add the separate T-60 prediction workflow after the first live slice. It
   persists provider predictions in Supabase, never Redis, and does not mutate
   them after kickoff.
5. Add a minimal Next.js live UI after the backend checkpoint. It consumes
   FastAPI and never calls API-Football.

No step above authorises a broad refactor of the existing analytics, importer,
or web modules.
