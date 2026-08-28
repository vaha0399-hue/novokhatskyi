import Link from "next/link";

import { LeagueBadge } from "@/components/page-primitives";
import { BackendApiError, getLeagues } from "@/lib/api";

export const revalidate = 300;

export default async function HomePage() {
  let leagues = null;
  let dataError = false;
  try {
    leagues = await getLeagues();
  } catch (error) {
    dataError = error instanceof BackendApiError;
  }

  return (
    <>
      <section className="hero shell">
        <div className="hero-copy">
          <p className="eyebrow"><span /> Factual football intelligence</p>
          <h1>See the game<br /><em>before the noise.</em></h1>
          <p className="hero-description">
            Explore completed matches, true performance signals, form and home/away splits — built from our own historical database.
          </p>
          <div className="hero-actions">
            <a className="button button-primary" href="#competitions">Explore competitions <span aria-hidden="true">↓</span></a>
            <a className="button button-quiet" href="#methodology">How it works</a>
          </div>
        </div>
        <div className="hero-panel" aria-label="Analytics platform highlights">
          <div className="hero-panel-top"><span className="live-dot" /> Historical dataset</div>
          <div className="hero-stat-grid">
            <div><strong>H</strong><span>Historical only</span></div>
            <div><strong>xG</strong><span>Team-level metrics</span></div>
            <div><strong>↔</strong><span>Home / away splits</span></div>
            <div><strong>0</strong><span>Predictions</span></div>
          </div>
          <div className="hero-panel-line"><span>Metrics are calculated in backend</span><b>Verified</b></div>
        </div>
      </section>

      <section className="shell section" id="competitions">
        <div className="section-heading">
          <div><p className="eyebrow"><span /> Competition library</p><h2>Choose a competition</h2></div>
          <p>Every route is data-driven. New leagues and seasons appear here without a frontend rebuild.</p>
        </div>
        {dataError ? (
          <div className="empty-state">
            <strong>Backend data source is not reachable yet.</strong>
            <p>Start FastAPI and set the server-only <code>BACKEND_INTERNAL_URL</code> in <code>frontend/.env.local</code>.</p>
          </div>
        ) : (
          <div className="league-grid">
            {leagues?.map((league, index) => (
              <Link className="league-card" href={`/leagues/${league.id}`} key={league.id}>
                <span className="league-card-index">0{index + 1}</span>
                <LeagueBadge league={league} />
                <div><h3>{league.name}</h3><p>{league.country_name ?? "International"} · {league.competition_type ?? "competition"}</p></div>
                <span className="arrow-link" aria-hidden="true">↗</span>
              </Link>
            ))}
          </div>
        )}
      </section>

      <section className="shell methodology" id="methodology">
        <div className="methodology-copy"><p className="eyebrow"><span /> Methodology</p><h2>Numbers with provenance,<br />not manufactured certainty.</h2></div>
        <div className="methodology-points">
          <div><b>01</b><h3>Our database</h3><p>Pages read normalized historical data through FastAPI. The browser never contacts API-Football.</p></div>
          <div><b>02</b><h3>Strict cutoffs</h3><p>Fixture comparisons use only matches completed before that fixture’s kickoff.</p></div>
          <div><b>03</b><h3>Factual output</h3><p>We show performance, context and trends — never artificial win probabilities.</p></div>
        </div>
      </section>
    </>
  );
}
