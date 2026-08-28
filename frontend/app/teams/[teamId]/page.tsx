import Link from "next/link";
import { notFound } from "next/navigation";

import { FormPills, MetricCards, Rate, Value } from "@/components/page-primitives";
import { BackendApiError, getTeamAnalytics } from "@/lib/api";
import type { MetricSummary } from "@/lib/contracts";
import { positiveId } from "@/lib/routes";

const scopes = ["overall", "home", "away"] as const;
const windows = [5, 10, 15, 20] as const;

function QueryLink({ teamId, seasonId, scope, window, active, children }: {
  teamId: number; seasonId: number; scope: string; window: number; active: boolean; children: React.ReactNode;
}) {
  return <Link className={`filter-chip ${active ? "filter-chip-active" : ""}`} href={`/teams/${teamId}?season_id=${seasonId}&scope=${scope}&window=${window}`}>{children}</Link>;
}

function DetailMetrics({ metrics }: { metrics: MetricSummary }) {
  const totals = metrics.total_goals;
  return <div className="detail-metrics">
    <div><span>Record</span><strong>{metrics.wins}–{metrics.draws}–{metrics.losses}</strong><small>W / D / L</small></div>
    <div><span>Clean sheets</span><strong><Rate value={metrics.clean_sheets.rate} /></strong><small>{metrics.clean_sheets.count} matches</small></div>
    <div><span>BTTS</span><strong><Rate value={metrics.btts.rate} /></strong><small>{metrics.btts.count} matches</small></div>
    <div><span>Over 2.5</span><strong><Rate value={totals["2.5"]?.over.rate ?? null} /></strong><small>{totals["2.5"]?.over.count ?? 0} matches</small></div>
    <div><span>Possession</span><strong><Value value={metrics.average_possession_pct.value} suffix="%" /></strong><small>{metrics.average_possession_pct.sample_size} samples</small></div>
    <div><span>Corners</span><strong><Value value={metrics.average_corners.value} /></strong><small>per match</small></div>
  </div>;
}

export const revalidate = 300;

export default async function TeamPage({ params, searchParams }: {
  params: Promise<{ teamId: string }>;
  searchParams: Promise<{ season_id?: string; scope?: string; window?: string }>;
}) {
  const teamId = positiveId((await params).teamId);
  const query = await searchParams;
  const seasonId = positiveId(query.season_id ?? "");
  const scope = scopes.includes(query.scope as (typeof scopes)[number]) ? query.scope as (typeof scopes)[number] : "overall";
  const parsedWindow = Number(query.window ?? 10);
  const window = windows.includes(parsedWindow as (typeof windows)[number]) ? parsedWindow as (typeof windows)[number] : 10;
  if (!teamId || !seasonId) notFound();

  let analytics;
  try {
    analytics = await getTeamAnalytics(teamId, seasonId, scope, window);
  } catch (error) {
    if (error instanceof BackendApiError && error.status === 404) notFound();
    throw error;
  }

  return <section className="shell section team-page">
    <Link className="back-link" href="/">← Competition library</Link>
    <div className="team-hero"><p className="eyebrow"><span /> Team analytics</p><h1>{analytics.team.name}</h1><p>Completed-match history only · selected season ID {analytics.season_id} · data through {new Intl.DateTimeFormat("en", { dateStyle: "medium", timeZone: "UTC" }).format(new Date(analytics.as_of_kickoff))}</p></div>
    <div className="analytics-toolbar"><div><span className="toolbar-label">Context</span><div className="filter-row">{scopes.map((item) => <QueryLink key={item} teamId={teamId} seasonId={seasonId} scope={item} window={window} active={scope === item}>{item}</QueryLink>)}</div></div><div><span className="toolbar-label">Last matches</span><div className="filter-row">{windows.map((item) => <QueryLink key={item} teamId={teamId} seasonId={seasonId} scope={scope} window={item} active={window === item}>Last {item}</QueryLink>)}</div></div></div>
    <section className="analytics-overview"><div className="overview-title"><span>Sample</span><strong>{analytics.metrics.matches} / {analytics.window}</strong><p>eligible historical matches</p></div><MetricCards metrics={analytics.metrics} /><div className="streak-card"><span>Current form</span><FormPills form={null} /><strong>{analytics.metrics.streaks.unbeaten ? `${analytics.metrics.streaks.unbeaten} unbeaten` : `${analytics.metrics.streaks.losses} losses`}</strong><p>Wins {analytics.metrics.streaks.wins} · Scored {analytics.metrics.streaks.scored}</p></div></section>
    <section className="panel team-detail-panel"><div className="panel-title"><div><p className="eyebrow"><span /> Performance profile</p><h2>What the history says</h2></div><span>{scope} · Last {window}</span></div><DetailMetrics metrics={analytics.metrics} /></section>
  </section>;
}
