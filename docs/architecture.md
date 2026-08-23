# Architecture Boundaries

## Canon

The approved product scope is recorded in
[`product-scope.md`](product-scope.md). It supersedes earlier references to a
prediction product.

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
    -> Analytics Engine
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
9. Analytics must report factual and derived historical indicators without
   producing outcome predictions or win probabilities.
10. No mobile app, desktop app, browser extension, or standalone public API is
    being designed at the current stage.

## Stage discipline

The EPL 2024 historical fixture-statistics backfill is complete and provides
380 fixtures with 760 team-level statistics rows. The immediate next step is a
dedicated Git checkpoint for the associated code, tests, and documentation;
Supabase-imported data itself is not committed.

After that checkpoint, implement the Analytics Engine and its historical SQL
validation layer before the FastAPI read contract. For a target fixture, every
derived metric must use only fixtures with an earlier kickoff. Frontend and
authentication integration begin only after those backend contracts are stable.
The canonical detailed roadmap is in [`product-scope.md`](product-scope.md).
