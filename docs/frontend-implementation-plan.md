# Frontend implementation plan

## Implemented visual MVP

1. **Data boundary** — Next.js Server Components call FastAPI read DTOs through
   the server-only `BACKEND_INTERNAL_URL`; no browser call reaches API-Football
   or `football.*` tables.
2. **Responsive information architecture** — home, league archive, season
   table/results, fixture match-centre, and team analytics routes are rendered
   from canonical internal IDs and are not EPL/year hard-coded.
3. **Visual system** — premium dark editorial layout, lime data accent,
   desktop comparison surfaces, and compact mobile layouts.
4. **Auth foundation** — official Supabase SSR cookie session flow, signup,
   email confirmation, login, recovery, logout, and protected account page.

## Next controlled iterations

1. **Interactive preview acceptance**: configure the two public Supabase
   values and Dashboard redirects; start backend and frontend locally; complete
   one real signup, confirmation, login, logout, and recovery flow.
2. **Visual QA**: review desktop and mobile screenshots against the actual
   loaded development dataset; adjust hierarchy, density, empty states and
   accessibility without changing analytics calculations.
3. **Local logo delivery** — when a league/team has a canonical provider logo
   URL already stored in the backend, a controlled operator job may cache the
   PNG locally. The browser requests only same-origin Next.js `/media/...`
   routes; the backend serves only cached files and never contacts a provider
   during a page request. Bundesliga 2025/26 is the first visual target.
4. **Backend access-control stage**: verify Supabase JWT in FastAPI and add
   public/free/premium policy gates. Do not put authorization logic into React.
5. **Product expansion**: favourites/saved filters, player/lineup analytics,
   odds snapshots, and premium UI are separate reviewed stages. Payments remain
   out of scope.
