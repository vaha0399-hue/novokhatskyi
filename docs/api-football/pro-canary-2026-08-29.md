# API-Football Pro contract canary: EPL 2026/27 (retry)

## Scope

On 2026-08-29, a controlled read-only retry queried the Premier League
(`league=39`) 2026/27 season (`season=2026`) using exactly four authenticated
API-Football GET requests. No database writes, imports, migrations, or
unrelated API calls were made.

Evidence is stored under
[`samples/api-football/pro-canary-2026-08-29/`](../../samples/api-football/pro-canary-2026-08-29/).
Each response is preserved as an unmodified `*.raw.json` body. Matching
`*.request.json` files contain only endpoint parameters, timestamps, HTTP
status, safe quota headers, byte counts, and SHA-256 hashes. Authentication
material is excluded.

## Results

| Endpoint | Parameters | Results |
| --- | --- | ---: |
| `/leagues` | `id=39`, `season=2026` | 1 |
| `/teams` | `league=39`, `season=2026` | 20 |
| `/fixtures` | `league=39`, `season=2026` | 380 |
| `/standings` | `league=39`, `season=2026` | 1 group / 20 rows |

All four calls returned HTTP 200 and empty provider error objects. Each response
reported one page. The final safe quota metadata is retained in the request
artifact for each call.

## Standings shape

The response contains one league group. Group fields are `country`, `flag`, `id`,
`logo`, `name`, `season`, and `standings`. Each of the 20 rows contains
`all`, `away`, `description`, `form`, `goalsDiff`, `group`, `home`, `points`,
`rank`, `status`, `team`, and `update`.
