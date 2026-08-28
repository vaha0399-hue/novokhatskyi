# Product Scope Canon

Status: **approved**
Effective date: **2026-08-22**

This document is the canonical product scope for Football Analytics. If an
older document describes the product as a prediction service, this document
takes precedence.

## Product direction

Football Analytics is a **web football analytics platform**. It gives users
historical match data, calculated statistical indicators, team comparisons,
and analytical views. The user makes their own decisions from factual data.

Football Analytics is **not a prediction service**. The current product and
roadmap exclude:

- a Prediction Engine or ML prediction workflow;
- HOME/DRAW/AWAY probabilities or win probability;
- prediction API endpoints and prediction UI;
- statements that a team will win;
- live scores, live polling, live timelines, minute-by-minute events,
  WebSocket/SSE ingestion, or calculations during a match.

Existing inactive `ml` database structures are not part of the active product
roadmap. They must not be used or extended. Removing them would require a
separately reviewed and approved migration.

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
7. There is no live pipeline. A completed fixture may receive a bounded final
   reconciliation, but this is not live ingestion.

## FastAPI web read contract

The planned internal browser-facing read contract is:

```text
GET /web/v1/leagues
GET /web/v1/leagues/{league_id}/seasons

GET /web/v1/seasons/{season_id}/standings
GET /web/v1/seasons/{season_id}/fixtures

GET /web/v1/fixtures/{fixture_id}
GET /web/v1/fixtures/{fixture_id}/statistics
GET /web/v1/fixtures/{fixture_id}/analytics

GET /web/v1/teams/{team_id}
GET /web/v1/teams/{team_id}/fixtures
GET /web/v1/teams/{team_id}/analytics

GET /web/v1/me
```

There is no prediction endpoint. IDs in this contract identify internal
domain entities unless the DTO explicitly states otherwise. No route may
hardcode Premier League or season 2024.

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
/leagues/{league}
/leagues/{league}/seasons/{season}
/fixtures/{fixture_id}
/teams/{team_id}
/login
/register
/forgot-password
/account
```

Future routes may include `/pricing`, `/favorites`, and `/saved-filters`.

The season page shows the league, dynamic season selection, standings,
rounds, fixtures, results, and links to fixtures and teams.

The fixture page shows fixture metadata, final score for completed matches,
table positions, factual match statistics, team form, home/away splits,
xG/xGA, attacking and defensive metrics, and later H2H. A future fixture may
show pre-match analytics calculated only from information available before
kickoff. It never shows live data or predictions.

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

### Checkpoint

1. Check `git status` and create a dedicated Git checkpoint for any code,
   documentation, and tests produced during the backfill work.
2. Confirm Stage 3D is present in `origin/develop` and that no secret or local
   environment file is staged. Supabase-imported data itself is not committed.

### Analytics foundation before UI

3. Implement **Analytics Engine v1** from canonical PostgreSQL data only:
   - Last 5 / 10 / 15 / 20;
   - overall, home-only, and away-only splits;
   - W/D/L, PPG, goals scored/conceded, xG/xGA;
   - shots, shots on target, possession, corners, and cards;
   - clean sheets, failed to score, BTTS, over/under 0.5 / 1.5 / 2.5 / 3.5,
     and streaks.
4. Implement a historical SQL validation layer for every metric. For target
   fixture **N**, its analytics input must include only fixtures whose
   `kickoff_at` is strictly before fixture N's kickoff. This anti-leakage rule
   applies to overall, home, away, Last N, streak, and rate calculations.
5. Design and implement stable FastAPI read DTOs/contracts over the validated
   Analytics Engine: team analytics, season fixtures, fixture analytics, and
   later scanner endpoints.

### Subsequent data stages and UI

6. Add Players + Lineups + Formations in a separately reviewed additive data
   stage, then add timestamped Odds snapshots in another stage.
7. After those contracts are stable, import EPL 2025/26, then EPL 2026/27,
   then the remaining Top-5 leagues.
8. The stable Analytics Engine and FastAPI Read API are now available, so an
   initial **visual Next.js frontend and Supabase Auth foundation are approved
   now**. They must consume existing DTOs and may not dictate database,
   importer, or analytical-calculation changes.
9. Add backend JWT verification, account/free access policy, premium design,
   and finally subscriptions/payments in later independent stages.

The frontend began only after the Analytics Engine, its historical validation
layer, and FastAPI read contracts became stable. The statistics importer must
not be changed merely because of frontend requirements.
