# Design

## Source of truth

- Status: Draft
- Last refreshed: 2026-08-30
- Primary product surfaces: global navigation, match discovery, league and
  fixture pages, team analytics, authentication and account pages.
- Evidence reviewed: `docs/product-scope.md`,
  `docs/frontend-implementation-plan.md`, `frontend/app/globals.css`,
  `frontend/app/layout.tsx`, `frontend/components/app-header.tsx`, current App
  Router pages and frontend tests.

## Brand

- Personality: premium sports analytics, precise, dense, controlled and calm.
- Trust signals: canonical data, explicit timestamps and states, clear source
  boundaries, consistent comparisons and restrained emphasis.
- Avoid: betting aesthetics, excessive glow, decorative dashboards without
  information value, noisy gradients and treating every metric as positive.

## Product goals

- Goals: make fixtures, leagues and factual analytics fast to scan on desktop
  and mobile; keep navigation stable as leagues and product sections grow.
- Non-goals: a native mobile app, browser-to-provider calls, visual prediction
  claims before their approved workflow, or a broad frontend rewrite.
- Success signals: users can identify the active product section, reach a
  fixture with few steps and use the same hierarchy at narrow widths.

## Personas and jobs

- Primary personas: football followers comparing fixtures, teams and form;
  returning authenticated users tracking preferred content.
- User jobs: find matches for a local calendar day, narrow by league, open a
  fixture, inspect factual analytics and return to saved areas.
- Key contexts of use: dense desktop review and one-handed mobile browsing.

## Information architecture

- Primary navigation: Матчи, Лиги, Аналитика, Прогнозы, Избранное.
- Core routes/screens: `/matches`, `/leagues`, `/analytics`, `/predictions`,
  `/favorites`, canonical league/season/team/fixture routes, auth and account.
- Content hierarchy: global section first; selected date second; leagues with
  matches third; fixtures within the selected league fourth.

## Design principles

- Density with hierarchy: compact controls, generous grouping boundaries and
  one obvious active state.
- Progressive disclosure: lightweight league counts before match lists; match
  details only after fixture selection.
- Product-wide consistency: the same navigation, surface and state rules apply
  to match, league, team and analytics screens.
- Tradeoffs: preserve direct access and legibility before decorative motion or
  unusually compact layouts.

## Visual language

- Color: dark, cool, layered sports-analytics direction. Exact palette values
  are intentionally not canonical until the user approves the visual preview.
- Typography: high-contrast primary text, quieter metadata and compact labels;
  tabular data must remain easy to compare.
- Spacing/layout rhythm: compact navigation and data rows with clearer spacing
  between content groups.
- Shape/radius/elevation: thin borders, restrained depth and minimal glow.
- Motion: short state transitions only; no decorative continuous animation.
- Imagery/iconography: functional marks and team/league assets only.

## Components

- Existing components to reuse: `AppHeader`, `Brand`, auth/account controls,
  `LeagueBadge`, `TeamLink` and current shell layout.
- New/changed components: responsive primary navigation within `AppHeader`.
- Variants and states: default, hover, keyboard focus, active route and
  horizontally scrollable mobile navigation.
- Token/component ownership: existing global variables remain in force for the
  preview. Candidate palette values must not be promoted to shared tokens yet.

## Accessibility

- Target standard: WCAG 2.2 AA for navigation and primary interactions.
- Keyboard/focus behavior: every destination is a real link with a visible
  focus state; active section uses `aria-current="page"`.
- Contrast/readability: text meaning cannot depend on glow or color alone.
- Screen-reader semantics: one labelled primary navigation landmark.
- Reduced motion and sensory considerations: respect the existing
  `prefers-reduced-motion` rule.

## Responsive behavior

- Supported breakpoints/devices: modern desktop and mobile browsers from a
  320px viewport.
- Layout adaptations: one-row header on wide screens; identity row plus a
  horizontally scrollable full navigation row on narrow screens.
- Touch/hover differences: mobile destinations keep touch-sized targets and do
  not require hover or a hidden hamburger interaction.

## Interaction states

- Loading: global navigation remains available while route data loads.
- Empty: section-level empty states do not remove global navigation.
- Error: backend failures remain within page content, not the header.
- Success: use contextual confirmation outside primary navigation.
- Disabled: unavailable sections remain navigable only when a deliberate
  placeholder or real route exists; do not fake disabled links.
- Offline/slow network: Next.js links preserve client navigation and visible
  focus/active feedback.

## Content voice

- Tone: direct, factual and restrained.
- Terminology: user-facing global menu labels are Матчи, Лиги, Аналитика,
  Прогнозы and Избранное.
- Microcopy rules: short nouns for navigation; no claims of certainty beyond
  the underlying data.

## Implementation constraints

- Framework/styling system: Next.js 16 App Router, React 19, TypeScript and the
  existing global CSS architecture.
- Design-token constraints: do not save or replace the proposed exact palette
  until visual approval.
- Performance constraints: no new dependency or network request for the menu.
- Compatibility constraints: keep the existing Supabase SSR/browser auth flow.
- Test/screenshot expectations: source/contract tests, typecheck, production
  build and manual desktop/mobile review before approval.

## Open questions

- [ ] Approve or revise the preview palette before promoting exact values to
  shared design tokens / user / affects all visual surfaces.
- [ ] Decide which not-yet-built product sections receive real first pages
  after the global menu is approved / user / affects navigation destinations.
