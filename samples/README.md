# API Response Samples

This directory contains sanitised raw response samples collected during the
API-Football contract-research and canonical-canary stages. It includes the
original Premier League 2024 research dataset, the reviewed Premier League
2026/27 canonical canary, and dated live-contract samples.

`api-football/live-fixtures-2026-08-29T0646Z/` is a one-request global
`GET /fixtures?live=all` sample. It proves the provider shape for live status,
current goals, elapsed minute, and added time; it is not an EPL-only dataset
and is not canonical match data. See each directory's `manifest.json` for
exact request accounting and `docs/api-football/` for analysis.

Raw response bodies are stored separately from safe request metadata. Samples
must never contain API keys, authentication headers, or other secrets.
