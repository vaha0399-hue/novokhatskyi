# Scanner league candidates — API-Football snapshot 2026-09-01

## Purpose and boundary

This is the retained discovery snapshot for expansion beyond EPL. It contains
the provider ID, name, country, and provider-current season for every response
that met all of these provider-side conditions on 2026-09-01:

- league.type = League;
- a provider-current season exists;
- coverage.standings = true;
- coverage.fixtures.statistics_fixtures = true.

The source of truth for worker code is
backend/app/importer/scanner_league_candidates.py. This table is the human
review copy of the same data.

It is a **candidate allow-list**, not an operational import allow-list. No
candidate is fetched, imported, or scheduled merely by appearing here. Before
promotion, each competition needs a format canary (teams, complete schedule,
provider mappings, standings and fixture-statistics validation). This matters
because API-Football's own League type can include competition names such as
Copa de la Liga Profesional; provider classification alone is not proof that
the competition follows the regular-season format used by the current
bootstrap importer.

The discovery call returned 781 current League entries. 651 reported
standings coverage, 77 reported fixture-statistics coverage, and 76 satisfied
both coverage requirements.

## Candidates

| Provider ID | League | Country | Current season |
| ---: | --- | --- | ---: |
| 1032 | Copa de la Liga Profesional | Argentina | 2024 |
| 128 | Liga Profesional Argentina | Argentina | 2026 |
| 134 | Torneo Federal A | Argentina | 2026 |
| 421 | Division di Honor | Aruba | 2025 |
| 218 | Bundesliga | Austria | 2026 |
| 116 | Premier League | Belarus | 2026 |
| 145 | Challenger Pro League | Belgium | 2026 |
| 144 | Jupiler Pro League | Belgium | 2026 |
| 344 | Primera División | Bolivia | 2026 |
| 624 | Carioca - 1 | Brazil | 2026 |
| 475 | Paulista - A1 | Brazil | 2026 |
| 71 | Serie A | Brazil | 2026 |
| 72 | Serie B | Brazil | 2026 |
| 172 | First League | Bulgaria | 2026 |
| 813 | Elite Two | Cameroon | 2026 |
| 479 | Canadian Premier League | Canada | 2026 |
| 265 | Primera División | Chile | 2026 |
| 169 | Super League | China | 2026 |
| 239 | Primera A | Colombia | 2026 |
| 210 | HNL | Croatia | 2026 |
| 345 | Czech Liga | Czech-Republic | 2026 |
| 119 | Superliga | Denmark | 2026 |
| 242 | Liga Pro | Ecuador | 2026 |
| 233 | Premier League | Egypt | 2026 |
| 40 | Championship | England | 2026 |
| 41 | League One | England | 2026 |
| 42 | League Two | England | 2026 |
| 39 | Premier League | England | 2026 |
| 363 | Premier League | Ethiopia | 2025 |
| 244 | Veikkausliiga | Finland | 2026 |
| 61 | Ligue 1 | France | 2026 |
| 62 | Ligue 2 | France | 2026 |
| 327 | Erovnuli Liga | Georgia | 2026 |
| 1104 | Liga 3 | Georgia | 2026 |
| 79 | 2. Bundesliga | Germany | 2026 |
| 80 | 3. Liga | Germany | 2026 |
| 78 | Bundesliga | Germany | 2026 |
| 82 | Frauen Bundesliga | Germany | 2026 |
| 197 | Super League 1 | Greece | 2026 |
| 271 | NB I | Hungary | 2026 |
| 323 | Indian Super League | India | 2025 |
| 357 | Premier Division | Ireland | 2026 |
| 383 | Ligat Ha'al | Israel | 2026 |
| 135 | Serie A | Italy | 2026 |
| 136 | Serie B | Italy | 2026 |
| 98 | J1 League | Japan | 2027 |
| 262 | Liga MX | Mexico | 2026 |
| 89 | Eerste Divisie | Netherlands | 2026 |
| 88 | Eredivisie | Netherlands | 2026 |
| 103 | Eliteserien | Norway | 2026 |
| 250 | Division Profesional - Apertura | Paraguay | 2026 |
| 252 | Division Profesional - Clausura | Paraguay | 2026 |
| 281 | Primera División | Peru | 2026 |
| 106 | Ekstraklasa | Poland | 2026 |
| 94 | Primeira Liga | Portugal | 2026 |
| 95 | Segunda Liga | Portugal | 2026 |
| 305 | Stars League | Qatar | 2026 |
| 283 | Liga I | Romania | 2026 |
| 236 | First League | Russia | 2026 |
| 235 | Premier League | Russia | 2026 |
| 307 | Pro League | Saudi-Arabia | 2026 |
| 179 | Premiership | Scotland | 2026 |
| 286 | Super Liga | Serbia | 2026 |
| 288 | Premier Soccer League | South-Africa | 2026 |
| 292 | K League 1 | South-Korea | 2026 |
| 140 | La Liga | Spain | 2026 |
| 141 | Segunda División | Spain | 2026 |
| 113 | Allsvenskan | Sweden | 2026 |
| 549 | Damallsvenskan | Sweden | 2026 |
| 114 | Superettan | Sweden | 2026 |
| 207 | Super League | Switzerland | 2026 |
| 204 | 1. Lig | Turkey | 2026 |
| 203 | Süper Lig | Turkey | 2026 |
| 301 | Pro League | United-Arab-Emirates | 2026 |
| 253 | Major League Soccer | USA | 2026 |
| 254 | NWSL Women | USA | 2026 |
