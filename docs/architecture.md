# Architecture Boundaries

## MVP scope

Football Analytics is currently a web-only MVP. The user-facing application
will be a responsive Next.js + TypeScript website supporting desktop and mobile
browsers.

The first supported competition will be the Premier League. Broader league
support is not part of the initial MVP.

## Target data flow

```text
API-Football
    -> FastAPI backend
    -> normalisation
    -> Supabase PostgreSQL
    -> Feature Engine
    -> Prediction Engine
    -> Supabase PostgreSQL
    -> FastAPI backend
    -> Next.js website
    -> User
```

This is a directional boundary, not a Stage 1 implementation specification.
Concrete database tables and payload models must not be designed until real
API-Football responses have been collected and analysed in a later stage.

## Invariants

1. FastAPI is the central application and orchestration layer.
2. API-Football is an upstream data source, not a browser-facing service.
3. The frontend must never receive `API_FOOTBALL_KEY` or other server secrets.
4. The frontend must never call API-Football directly.
5. User requests must not trigger live API-Football requests.
6. Football data must be synchronised to our database ahead of user requests.
7. Database changes must be versioned as migrations in
   `supabase/migrations/` after source-data analysis.
8. Imports must eventually be idempotent and must not create duplicates.
9. Pre-match predictions must eventually be stored before kickoff with their
   model version and calculation timestamp.
10. No mobile app, desktop app, browser extension, or standalone public API is
    being designed at the current stage.

## Stage discipline

Stage 1 implements only the minimal FastAPI health contract and repository
foundation. API-Football and Supabase are deliberately absent. Their future
presence in the target flow does not authorise schemas, clients, credentials,
or integrations in the current stage.
