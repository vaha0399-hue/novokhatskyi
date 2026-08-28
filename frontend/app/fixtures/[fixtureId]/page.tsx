import { Fragment } from "react";
import Link from "next/link";
import { notFound } from "next/navigation";

import { MetricCards, TeamLink, Value } from "@/components/page-primitives";
import { BackendApiError, getFixtureAnalytics, getFixtureStatistics } from "@/lib/api";
import type { FixtureAnalyticsSide, FixtureTeamStatistics } from "@/lib/contracts";
import { positiveId } from "@/lib/routes";

const comparisonRows: Array<{ label: string; value: (metrics: FixtureTeamStatistics) => number | null; digits?: number; suffix?: string }> = [
  { label: "Expected goals", value: (m) => m.expected_goals, digits: 2 },
  { label: "Total shots", value: (m) => m.total_shots, digits: 0 },
  { label: "Shots on goal", value: (m) => m.shots_on_goal, digits: 0 },
  { label: "Possession", value: (m) => m.possession_pct, suffix: "%" },
  { label: "Corners", value: (m) => m.corner_kicks, digits: 0 },
  { label: "Pass accuracy", value: (m) => m.pass_accuracy_pct, suffix: "%" },
  { label: "Yellow cards", value: (m) => m.yellow_cards, digits: 0 },
];

function AnalyticsSide({ title, side, split }: { title: string; side: FixtureAnalyticsSide; split: string }) {
  return <article className="fixture-analytics-side"><span>{title}</span><h3>{side.team.name}</h3><p>{split}</p><MetricCards metrics={side.venue_split} compact /><div className="side-record"><b>{side.venue_split.wins}–{side.venue_split.draws}–{side.venue_split.losses}</b><span>W / D / L</span><small>PPG <Value value={side.venue_split.points_per_game} digits={2} /></small></div></article>;
}

export const revalidate = 300;

export default async function FixturePage({ params }: { params: Promise<{ fixtureId: string }> }) {
  const fixtureId = positiveId((await params).fixtureId);
  if (!fixtureId) notFound();

  let statistics;
  try {
    statistics = await getFixtureStatistics(fixtureId);
  } catch (error) {
    if (error instanceof BackendApiError && error.status === 404) notFound();
    throw error;
  }
  let analytics = null;
  try {
    analytics = await getFixtureAnalytics(fixtureId, 10);
  } catch (error) {
    if (!(error instanceof BackendApiError && error.status === 422)) throw error;
  }
  const fixture = statistics.fixture;
  const score = fixture.final_score;

  return <section className="shell section fixture-page">
    <Link className="back-link" href="/">← Competition library</Link>
    <div className="match-hero"><p className="eyebrow"><span /> {fixture.round_label ?? "Fixture"}</p><div className="match-hero-grid"><TeamLink team={fixture.home_team} seasonId={fixture.season_id} className="match-team match-team-home" /><div className="match-score"><time dateTime={fixture.kickoff_at}>{new Intl.DateTimeFormat("en", { dateStyle: "full", timeStyle: "short", timeZone: "UTC" }).format(new Date(fixture.kickoff_at))} UTC</time><strong>{score ? `${score.home} — ${score.away}` : "vs"}</strong><span>{fixture.lifecycle_state}</span></div><TeamLink team={fixture.away_team} seasonId={fixture.season_id} className="match-team match-team-away" /></div></div>

    <section className="panel match-stat-panel"><div className="panel-title"><div><p className="eyebrow"><span /> Match facts</p><h2>Final statistics</h2></div><span>Completed fixture</span></div>{statistics.home.metrics && statistics.away.metrics ? <div className="stat-comparison"><div className="stat-team-title">{statistics.home.team.name}</div><div /><div className="stat-team-title stat-team-title-away">{statistics.away.team.name}</div>{comparisonRows.map((row) => <Fragment key={row.label}><strong><Value value={row.value(statistics.home.metrics!)} digits={row.digits ?? 1} suffix={row.suffix} /></strong><span className="stat-label">{row.label}</span><strong className="stat-away"><Value value={row.value(statistics.away.metrics!)} digits={row.digits ?? 1} suffix={row.suffix} /></strong></Fragment>)}</div> : <div className="empty-state">Final match-statistics coverage is not available for one or both teams.</div>}</section>

    <section className="fixture-analysis"><div className="panel-title"><div><p className="eyebrow"><span /> Pre-match context</p><h2>Historical comparison</h2></div><span>Strict cutoff before kickoff</span></div>{analytics ? <><p className="analysis-note">These are factual historical aggregates calculated only from fixtures before {new Intl.DateTimeFormat("en", { dateStyle: "medium", timeZone: "UTC" }).format(new Date(analytics.historical_cutoff_at))}. They are not a prediction.</p><div className="fixture-analysis-grid"><AnalyticsSide title="Home history" side={analytics.home} split="Home-only · Last 10" /><div className="versus-mark">vs</div><AnalyticsSide title="Away history" side={analytics.away} split="Away-only · Last 10" /></div></> : <div className="empty-state">There was not enough earlier completed history to calculate a pre-match comparison for this fixture.</div>}</section>
  </section>;
}
