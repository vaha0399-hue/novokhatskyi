import Link from "next/link";

import { TeamLink } from "@/components/page-primitives";
import type { FixtureSummary } from "@/lib/contracts";

function displayDate(value: string): string {
  return new Intl.DateTimeFormat("en", { weekday: "short", day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", timeZone: "UTC" }).format(new Date(value));
}

export function FixtureCard({ fixture }: { fixture: FixtureSummary }) {
  const score = fixture.final_score;
  return (
    <article className="fixture-card">
      <div className="fixture-meta">
        <span>{fixture.round_label ?? "Fixture"}</span>
        <time dateTime={fixture.kickoff_at}>{displayDate(fixture.kickoff_at)} UTC</time>
      </div>
      <div className="fixture-teams">
        <TeamLink team={fixture.home_team} seasonId={fixture.season_id} />
        <strong className="fixture-score">{score ? `${score.home} — ${score.away}` : "vs"}</strong>
        <TeamLink team={fixture.away_team} seasonId={fixture.season_id} />
      </div>
      <Link className="fixture-open" href={`/fixtures/${fixture.id}`}>Match centre <span aria-hidden="true">↗</span></Link>
    </article>
  );
}
