type MatchNavigationQuery = {
  date: string;
  timezone: string;
  leagueId?: number;
};

export function matchNavigationQuery({
  date,
  timezone,
  leagueId,
}: MatchNavigationQuery): string {
  const query = new URLSearchParams({ date, timezone });
  if (leagueId !== undefined) {
    query.set("league_id", String(leagueId));
  }
  return query.toString();
}
