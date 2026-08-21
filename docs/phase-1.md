# Stage 1 Canon: Minimal Project Foundation

Status: **approved for implementation on 2026-08-21**

This document is the canonical source of truth for Stage 1. If implementation
choices conflict with this document, this document wins until it is explicitly
revised.

## Product boundary

The current product is a responsive website for desktop and mobile browsers.
Its planned frontend stack is Next.js and TypeScript.

The following products are not in scope:

- mobile applications;
- desktop applications;
- browser extensions;
- a standalone public API for external clients.

All current architecture must optimise for the web MVP.

## Goal

Create the smallest clean repository foundation that proves the Python backend
can run and be tested, without prematurely implementing data or product layers.

## Required repository structure

```text
backend/
  app/
    __init__.py
    main.py
  tests/
    test_health.py
  README.md
  pyproject.toml
  uv.lock
frontend/
  README.md
supabase/
  README.md
  migrations/
    README.md
samples/
  README.md
docs/
  architecture.md
  phase-1.md
README.md
.gitignore
```

Marker documentation is used instead of empty directories so that Git retains
the structure and the boundary of each future area remains explicit.

## Backend contract

The FastAPI application exposes one endpoint:

```http
GET /health
```

Successful response:

```json
{
  "status": "ok"
}
```

The response status must be HTTP 200 and the contract must be covered by an
automated pytest test.

## Explicit exclusions

Stage 1 must not include:

- API-Football integration or real API calls;
- API-Football response samples;
- Supabase connectivity, database tables, or schema design;
- authentication;
- importer or normalisation pipelines;
- feature or prediction engines;
- production deployment;
- the main Next.js application or UI;
- mobile, desktop, extension, or public-client API work.

## Implementation sequence

1. Record the approved scope and architectural boundaries.
2. Create the minimal directory structure.
3. Implement the FastAPI application and `GET /health`.
4. Add an isolated test for the endpoint contract.
5. Extend repository ignores for Python, environment secrets, and future
   Next.js artifacts.
6. Run the backend tests and basic static/import checks.
7. Inspect the final diff for secrets and scope violations.

## Acceptance criteria

Stage 1 is complete only when:

- the repository remains on `develop`;
- the required structure exists;
- the FastAPI application imports successfully;
- `GET /health` returns HTTP 200 and `{"status":"ok"}`;
- the endpoint test passes;
- `.env` and common local/build artifacts are ignored;
- no API-Football or Supabase implementation/schema has been added;
- no secret is present in the tracked diff;
- verification results are reported before work begins on Stage 2.

## Delegation decision

The backend implementation and its focused tests are a bounded, independently
verifiable task and may be delegated to an implementation agent. Repository
documentation, integration, final scope review, and release-quality verification
remain the tech lead's responsibility.
