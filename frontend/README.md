# Frontend

This directory is reserved for the responsive Next.js + TypeScript website.

The main frontend starts only after the data pipeline and prediction engine are
working. Stage 1 intentionally contains no JavaScript dependencies, UI, API
client, mobile app, desktop app, browser extension, or public API SDK.

When implemented, the frontend will communicate only with the Football
Analytics FastAPI backend. It must never call API-Football directly or contain
server-side credentials in browser bundles.
