# API-Football Pro seasonal contract canary: EPL 2026/27

## Scope and controls

On 2026-08-29, a controlled read-only canary queried the Premier League
(`league=39`) 2026/27 season (`season=2026`). It made exactly **13
authenticated GET requests**, with no retries, database writes, migrations,
backend edits, production tests, or workaround calls. The API key was read
from `/opt/football-analytics/.env` in-process and is not present in any
artifact.

Evidence is stored under
[`samples/api-football/pro-canary-2026-08-29-seasonal/`](../../samples/api-football/pro-canary-2026-08-29-seasonal/).
Each response is preserved as an unmodified `*.raw.json` body. Matching
`*.request.json` files contain endpoint parameters, timestamps, HTTP status,
safe quota headers, result/paging/error summaries, byte counts, and SHA-256
hashes. Authentication material is excluded.

Fixture identifiers were selected from the existing EPL 2026 fixture sample:
completed fixture `1557367` (Arsenal 42 vs Coventry 1346, `FT`) and scheduled
fixture `1557383` (Liverpool 40 vs Nottingham Forest 65, `NS`).

## Call results

| # | Endpoint | Parameters | HTTP | Results | Provider errors |
|---:|---|---|---:|---:|---|
| 1 | `/players` | `league=39`, `season=2026`, `page=1` | 200 | 20 | none |
| 2 | `/players` | `league=39`, `season=2026`, `page=2` | 200 | 20 | none |
| 3 | `/players/topscorers` | `league=39`, `season=2026` | 200 | 20 | none |
| 4 | `/players/topassists` | `league=39`, `season=2026` | 200 | 20 | none |
| 5 | `/players/topcards` | `league=39`, `season=2026` | 200 | 0 | `The Players/topcards endpoint does not exist.` |
| 6 | `/injuries` | `league=39`, `season=2026` | 200 | 304 | none |
| 7 | `/teams/statistics` | `league=39`, `season=2026`, `team=42` | 200 | 11 | none |
| 8 | `/fixtures/events` | `fixture=1557367` | 200 | 14 | none |
| 9 | `/fixtures/lineups` | `fixture=1557367` | 200 | 2 | none |
| 10 | `/fixtures/statistics` | `fixture=1557367` | 200 | 2 | none |
| 11 | `/fixtures/players` | `fixture=1557367` | 200 | 2 | none |
| 12 | `/transfers` | `team=42` | 200 | 302 | none |
| 13 | `/predictions` | `fixture=1557383` | 200 | 1 | none |

All calls returned HTTP 200. The `/players/topcards` response is retained as
provider-error evidence; no alternate endpoint or retry was attempted.

## Coverage observations

- Player pagination returned page 1 and page 2, each with 20 results; the
  provider reported 20 total pages.
- Top-scorer and top-assist lists returned 20 results each.
- Injuries, one team-statistics request, completed-fixture events/lineups/
  statistics/players, one EPL-team transfer request, and scheduled-fixture
  predictions returned non-empty responses.
- The provider's season coverage metadata advertised top cards, but the
  requested `/players/topcards` endpoint returned an explicit endpoint-not-
  found error. This is recorded as a contract/coverage discrepancy, not
  inferred as an application defect.

## Artifact integrity and safety

The manifest records `physical_api_calls_this_campaign: 13`, safe quota headers
for each request, and matching raw/request artifact names. A repository scan of
the new directory found no API-key, authorization, bearer-token, or equivalent
secret material.

Coordination protocol: coordinated - existing fixture samples and artifact
conventions were checked; the scoped handoff is the new seasonal sample
directory plus this report, with no shared code or database surface changed.
