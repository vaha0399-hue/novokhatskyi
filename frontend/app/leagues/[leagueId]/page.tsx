import Link from "next/link";
import { notFound } from "next/navigation";

import { LeagueBadge } from "@/components/page-primitives";
import { BackendApiError, getLeagueSeasons } from "@/lib/api";
import { positiveId } from "@/lib/routes";

export const revalidate = 300;

export default async function LeaguePage({ params }: { params: Promise<{ leagueId: string }> }) {
  const leagueId = positiveId((await params).leagueId);
  if (!leagueId) notFound();

  let result;
  try {
    result = await getLeagueSeasons(leagueId);
  } catch (error) {
    if (error instanceof BackendApiError && error.status === 404) notFound();
    throw error;
  }

  return (
    <section className="shell section page-intro">
      <Link className="back-link" href="/">← All competitions</Link>
      <div className="league-hero">
        <LeagueBadge league={result.league} />
        <p className="eyebrow"><span /> {result.league.country_name ?? "Football analytics"}</p>
        <h1>{result.league.name}</h1>
        <p>Historical season archive. Choose a campaign to open standings, results and match-centre analytics.</p>
      </div>
      <div className="season-grid">
        {result.seasons.map((season) => (
          <Link className="season-card" key={season.id} href={`/leagues/${leagueId}/seasons/${season.id}`}>
            <span>Season</span>
            <h2>{season.label}</h2>
            <p>{season.starts_on && season.ends_on ? `${season.starts_on} → ${season.ends_on}` : "Historical dataset"}</p>
            <b>Open season <span aria-hidden="true">↗</span></b>
          </Link>
        ))}
      </div>
    </section>
  );
}
