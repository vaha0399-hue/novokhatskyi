import Link from "next/link";
import { LocalLogo } from "@/components/local-logo";
import type { LeagueReference, MetricSummary, TeamReference } from "@/lib/contracts";

export function LeagueBadge({ league }: { league: LeagueReference }) {
  return (
    <span className="league-badge">
      <LocalLogo kind="leagues" id={league.id} name={league.name} />
      <span>{league.name}</span>
    </span>
  );
}

export function TeamLink({ team, seasonId, className }: { team: TeamReference; seasonId: number; className?: string }) {
  return (
    <Link className={`team-link ${className ?? ""}`.trim()} href={`/teams/${team.id}?season_id=${seasonId}`}>
      <LocalLogo kind="teams" id={team.id} name={team.name} />
      <span>{team.name}</span>
    </Link>
  );
}

export function Value({ value, digits = 1, suffix = "" }: { value: number | null; digits?: number; suffix?: string }) {
  return <>{value === null ? "—" : `${value.toFixed(digits)}${suffix}`}</>;
}

export function Rate({ value }: { value: number | null }) {
  return <>{value === null ? "—" : `${Math.round(value * 100)}%`}</>;
}

export function FormPills({ form }: { form: string | null }) {
  if (!form) return <span className="muted">—</span>;
  return (
    <span className="form-pills" aria-label={`Recent form ${form}`}>
      {[...form].map((result, index) => <span className={`form-pill form-${result.toLowerCase()}`} key={`${result}-${index}`}>{result}</span>)}
    </span>
  );
}

export function MetricCards({ metrics, compact = false }: { metrics: MetricSummary; compact?: boolean }) {
  const cards = [
    ["PPG", metrics.points_per_game, "", 2],
    ["Goals scored", metrics.average_goals_scored, "", 2],
    ["Goals conceded", metrics.average_goals_conceded, "", 2],
    ["xG", metrics.average_xg.value, "", 2],
    ["xGA", metrics.average_xga.value, "", 2],
    ["Shots on target", metrics.average_shots_on_goal.value, "", 2],
  ] as const;
  return (
    <div className={`metric-cards ${compact ? "metric-cards-compact" : ""}`}>
      {cards.map(([label, value, suffix, digits]) => (
        <div className="metric-card" key={label}>
          <span>{label}</span>
          <strong><Value value={value} suffix={suffix} digits={digits} /></strong>
        </div>
      ))}
    </div>
  );
}
