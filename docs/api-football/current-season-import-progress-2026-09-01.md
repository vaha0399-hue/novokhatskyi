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
| 78 | Bundesliga | 2026 | Imported | 18 | 306 | 9 / 297 | 18 | Provider fixture venue ID `0` is normalized as unmapped. |
| 79 | 2. Bundesliga | 2026 | Imported | 18 | 306 | 27 / 279 | 54 | Complete regular-season calendar. |
| 61 | Ligue 1 | 2026 | Imported with reviewed overrides | 18 | 306 | 18 / 288 | 36 | Two immutable fixture policies correct provider home/away and venue defects while preserving raw payloads. |
| 40 | Championship | 2026 | Imported | 24 | 552 | 44 / 508 | 88 | Explicit England/Wales team-country policy for Cardiff, Swansea and Wrexham. |
| 88 | Eredivisie | 2026 | Imported | 18 | 306 | 33 / 273 | 66 | Complete regular-season calendar. |
| 203 | Süper Lig | 2026 | Imported | 18 | 306 | 27 / 279 | 54 | Complete regular-season calendar. |
| 307 | Saudi Pro League | 2026 | Not ready | — | — | — | — | Provider labels Al Khaleej Saihat as United-Arab-Emirates while the league is Saudi-Arabia; no canonical data written. |
| 94 | Primeira Liga | 2026 | Not ready | — | — | — | — | One fixture is `PST` (Braga—Gil Vicente); no canonical data written pending explicit postponed/reschedule lifecycle support. |

## Rules

- A failed or not-ready format canary writes **no** canonical league data.
- Future fixtures are imported as canonical `scheduled` fixtures; match
  statistics are requested only for provider-completed fixtures.
- Statistics transport uses one discovery request plus fixture-ID batches of at
  most 20; each response produces up to two team-statistics rows per fixture.
- Do not promote a candidate to a scheduled worker merely because it appears
  in `scanner_league_candidates.py`; it must have an **Imported** row here and
  a reviewed operational policy.
