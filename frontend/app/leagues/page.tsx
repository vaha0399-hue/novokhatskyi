import Link from "next/link";

import { LeagueBadge } from "@/components/page-primitives";
import { BackendApiError, getLeagues } from "@/lib/api";
import type { LeagueReference } from "@/lib/contracts";

export const revalidate = 300;

export default async function LeaguesPage() {
  let leagues: LeagueReference[] = [];
  let unavailable = false;
  try {
    leagues = await getLeagues();
  } catch (error) {
    unavailable = error instanceof BackendApiError;
  }

  return (
    <section className="shell section page-intro">
      <p className="eyebrow"><span /> Competitions</p>
      <h1>Leagues</h1>
      {unavailable ? (
        <p>The data source is temporarily unavailable.</p>
      ) : (
        <div className="league-grid">
          {leagues.map((league) => (
            <Link className="league-card" href={`/leagues/${league.id}`} key={league.id}>
              <LeagueBadge league={league} />
              <div><h3>{league.name}</h3><p>{league.country_name ?? "International"}</p></div>
              <span className="arrow-link" aria-hidden="true">↗</span>
            </Link>
          ))}
        </div>
      )}
    </section>
  );
}
