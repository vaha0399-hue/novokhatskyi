# Current-season import progress — 2026-09-01

This register records only format-canary outcomes for the API-Football current
season candidate set. A row marked **Imported** passed the complete calendar
contract before one atomic canonical import, then received batch statistics
for every completed fixture selected by the current-season backfill.

Raw provider responses remain in `source.provider_fetches`; this document is
the human-readable checkpoint, not a replacement for database provenance.

| Provider ID | Competition | Season | Status | Teams | Fixtures | Completed / future | Statistics rows | Notes |
| ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | --- |
| 39 | Premier League | 2026 | Imported | 20 | 380 | 20 / 360 | 40 | Existing canonical base; rechecked during this campaign. |
| 140 | La Liga | 2026 | Imported | 20 | 380 | 30 / 350 | 60 | Complete regular-season calendar. |
| 141 | Segunda División | 2026 | Imported | 22 | 462 | 33 / 429 | 66 | Complete regular-season calendar. |
| 135 | Serie A | 2026 | Imported | 20 | 380 | 20 / 360 | 40 | Complete regular-season calendar. |
| 136 | Serie B | 2026 | Imported | 20 | 380 | 20 / 360 | 40 | Complete regular-season calendar. |
| 78 | Bundesliga | 2026 | Imported | 18 | 306 | 9 / 297 | 18 | Provider fixture venue ID `0` is normalized as unmapped. |
| 79 | 2. Bundesliga | 2026 | Imported | 18 | 306 | 27 / 279 | 54 | Complete regular-season calendar. |
| 61 | Ligue 1 | 2026 | Imported with reviewed overrides | 18 | 306 | 18 / 288 | 36 | Two immutable fixture policies correct provider home/away and venue defects while preserving raw payloads. |
| 62 | Ligue 2 | 2026 | Imported | 18 | 306 | 36 / 270 | 72 | Complete regular-season calendar. |
| 71 | Brazil Serie A | 2026 | Imported with postponed fixtures | 20 | 380 | 245 completed / 131 scheduled / 4 postponed | 418 | `PST` fixtures are retained as canonical `postponed`; their scheduled replacements will refresh the same provider fixture mapping. |
| 40 | Championship | 2026 | Imported | 24 | 552 | 44 / 508 | 88 | Explicit England/Wales team-country policy for Cardiff, Swansea and Wrexham. |
| 41 | League One | 2026 | Imported with postponed fixtures | 24 | 552 | 42 completed / 509 scheduled / 1 postponed | 84 | `PST` fixture Oxford United—Reading is retained rather than blocking the league import. |
| 42 | League Two | 2026 | Imported | 24 | 552 | 48 / 504 | 96 | Explicit England/Wales team-country policy for Newport County. |
| 88 | Eredivisie | 2026 | Imported | 18 | 306 | 33 / 273 | 66 | Complete regular-season calendar. |
| 89 | Eerste Divisie | 2026 | Not ready | — | — | — | — | Provider standings contain multiple groups; no canonical data written pending an explicit multi-group policy. |
| 106 | Ekstraklasa | 2026 | Imported | 18 | 306 | 47 / 259 | 94 | Complete regular-season calendar. |
| 203 | Süper Lig | 2026 | Imported | 18 | 306 | 27 / 279 | 54 | Complete regular-season calendar. |
| 204 | 1. Lig | 2026 | Imported | 20 | 380 | 44 / 336 | 88 | Complete regular-season calendar. |
| 235 | Russian Premier League | 2026 | Not ready | — | — | — | — | Provider schedule has a duplicate directed pairing; no canonical data written without a reviewed correction. |
| 288 | Premier Soccer League | 2026 | Imported | 16 | 240 | 34 / 206 | 68 | Complete regular-season calendar. |
| 307 | Saudi Pro League | 2026 | Not ready | — | — | — | — | Provider labels Al Khaleej Saihat as United-Arab-Emirates while the league is Saudi-Arabia; no canonical data written. |
| 301 | UAE Pro League | 2026 | Not ready | — | — | — | — | Provider published only 35 of the expected 182 regular-season fixtures; no canonical data written. |
| 94 | Primeira Liga | 2026 | Imported with postponed fixtures | 18 | 306 | 33 completed / 272 scheduled / 1 postponed | 66 | `PST` fixture Braga—Gil Vicente is retained rather than blocking the league import. |
| 95 | Segunda Liga | 2026 | Imported | 18 | 306 | 36 / 270 | 72 | Complete regular-season calendar. |
| 103 | Eliteserien | 2026 | Imported | 16 | 240 | 144 / 96 | 288 | Complete regular-season calendar. |
| 113 | Allsvenskan | 2026 | Imported | 16 | 240 | 150 / 90 | 300 | Complete regular-season calendar. |
| 116 | Belarus Premier League | 2026 | Imported with postponed fixtures | 16 | 240 | 152 completed / 83 scheduled / 5 postponed | 304 | `PST` fixtures are retained as canonical `postponed`; their scheduled replacements will refresh the same provider fixture mapping. |

## Rules

- A failed or not-ready format canary writes **no** canonical league data.
- Future fixtures are imported as canonical `scheduled` fixtures. Provider
  `PST` is a non-terminal canonical `postponed` fixture: it remains in the
  schedule even when the provider has not supplied a replacement kickoff
  (`kickoff_at = null`), and is excluded from date views and scanner results
  until a later `NS` response supplies its confirmed time.
- Match statistics are requested only for provider-completed fixtures.
- Statistics transport uses one discovery request plus fixture-ID batches of at
  most 20; each response produces up to two team-statistics rows per fixture.
- Do not promote a candidate to a scheduled worker merely because it appears
  in `scanner_league_candidates.py`; it must have an **Imported** row here and
  a reviewed operational policy.
