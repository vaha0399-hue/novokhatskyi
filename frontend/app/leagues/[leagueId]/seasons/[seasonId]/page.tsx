import Link from "next/link";
import { notFound } from "next/navigation";

import { FixtureCard } from "@/components/fixture-card";
import { FormPills, LeagueBadge, TeamLink } from "@/components/page-primitives";
import { BackendApiError, getSeasonFixtures, getSeasonStandings } from "@/lib/api";
import type { FixtureSummary } from "@/lib/contracts";
import { positiveId } from "@/lib/routes";

export const revalidate = 300;

function fixtureGroups(fixtures: FixtureSummary[]): Array<[string, FixtureSummary[]]> {
  const groups = new Map<string, FixtureSummary[]>();
  for (const fixture of fixtures) {
    const key = fixture.round_label ?? "Fixtures";
    groups.set(key, [...(groups.get(key) ?? []), fixture]);
  }
  return [...groups.entries()];
}

export default async function SeasonPage({ params }: { params: Promise<{ leagueId: string; seasonId: string }> }) {
  const { leagueId: rawLeagueId, seasonId: rawSeasonId } = await params;
  const leagueId = positiveId(rawLeagueId);
  const seasonId = positiveId(rawSeasonId);
  if (!leagueId || !seasonId) notFound();

  let standings;
  let fixtures;
  try {
    [standings, fixtures] = await Promise.all([getSeasonStandings(seasonId), getSeasonFixtures(seasonId)]);
  } catch (error) {
    if (error instanceof BackendApiError && error.status === 404) notFound();
    throw error;
  }
  if (standings.season.league.id !== leagueId || fixtures.season_id !== seasonId) notFound();

  return (
    <section className="shell section season-page">
      <Link className="back-link" href={`/leagues/${leagueId}`}>← {standings.season.league.name}</Link>
      <div className="season-header">
        <div><LeagueBadge league={standings.season.league} /><p className="eyebrow"><span /> Season archive</p><h1>{standings.season.label}</h1></div>
        <div className="data-stamp"><span>Latest table snapshot</span><strong>{new Intl.DateTimeFormat("en", { dateStyle: "medium", timeZone: "UTC" }).format(new Date(standings.captured_at))}</strong></div>
      </div>

      <div className="season-layout">
        <section className="standings-panel">
          <div className="panel-title"><div><p className="eyebrow"><span /> Standings</p><h2>League table</h2></div><span>{fixtures.pagination.total} fixtures</span></div>
          {standings.groups.map((group) => (
            <div className="table-scroll" key={group.name ?? "table"}>
              {group.name && <h3>{group.name}</h3>}
              <table className="standings-table">
                <thead><tr><th>Pos</th><th>Club</th><th>PL</th><th>W</th><th>D</th><th>L</th><th>GD</th><th>Pts</th><th>Form</th></tr></thead>
                <tbody>{group.rows.map((row) => (
                  <tr key={row.team.id}>
                    <td><b>{row.rank}</b></td><td><TeamLink team={row.team} seasonId={seasonId} /></td><td>{row.played}</td><td>{row.wins}</td><td>{row.draws}</td><td>{row.losses}</td><td>{row.goals_diff > 0 ? `+${row.goals_diff}` : row.goals_diff}</td><td className="points-cell">{row.points}</td><td><FormPills form={row.form} /></td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          ))}
        </section>

        <section className="fixture-list-panel">
          <div className="panel-title"><div><p className="eyebrow"><span /> Results</p><h2>Match archive</h2></div><span>UTC</span></div>
          <div className="fixture-groups">
            {fixtureGroups(fixtures.fixtures).map(([round, items]) => (
              <div className="fixture-group" key={round}><h3>{round}</h3><div>{items.map((fixture) => <FixtureCard fixture={fixture} key={fixture.id} />)}</div></div>
            ))}
          </div>
        </section>
      </div>
    </section>
  );
}
