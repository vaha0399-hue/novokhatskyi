export type TeamReference = {
  id: number;
  name: string;
};

export type LeagueReference = {
  id: number;
  name: string;
  country_name: string | null;
  logo_url: string | null;
  competition_type: string | null;
};

export type SeasonReference = {
  id: number;
  league: LeagueReference;
  start_year: number;
  label: string;
  starts_on: string | null;
  ends_on: string | null;
};

export type FixtureScore = {
  home: number;
  away: number;
};

export type FixtureSummary = {
  id: number;
  season_id: number;
  kickoff_at: string;
  round_label: string | null;
  lifecycle_state: string;
  home_team: TeamReference;
  away_team: TeamReference;
  final_score: FixtureScore | null;
};

export type MatchDateLeagueSummary = {
  league: LeagueReference;
  fixture_count: number;
};

export type MatchDateLeaguesResponse = {
  date: string;
  timezone: string;
  leagues: MatchDateLeagueSummary[];
};

export type LeagueMatchesResponse = {
  date: string;
  timezone: string;
  league: LeagueReference;
  fixtures: FixtureSummary[];
};

export type PaginationMetadata = {
  total: number;
  limit: number;
  offset: number;
  next_offset: number | null;
};

export type SeasonFixturesResponse = {
  season_id: number;
  fixtures: FixtureSummary[];
  pagination: PaginationMetadata;
};

export type SeasonStandingRow = {
  rank: number;
  team: TeamReference;
  points: number;
  played: number;
  wins: number;
  draws: number;
  losses: number;
  goals_for: number;
  goals_against: number;
  goals_diff: number;
  form: string | null;
  status: string | null;
  description: string | null;
};

export type StandingsGroup = {
  name: string | null;
  rows: SeasonStandingRow[];
};

export type SeasonStandingsResponse = {
  season: SeasonReference;
  captured_at: string;
  groups: StandingsGroup[];
};

export type AverageMetricSummary = {
  value: number | null;
  sample_size: number;
};

export type RateMetricSummary = {
  count: number;
  rate: number | null;
};

export type GoalTotalsRateSummary = {
  over: RateMetricSummary;
  under: RateMetricSummary;
};

export type StreakSummary = {
  wins: number;
  unbeaten: number;
  winless: number;
  losses: number;
  scored: number;
  clean_sheets: number;
  btts: number;
};

export type MetricSummary = {
  matches: number;
  wins: number;
  draws: number;
  losses: number;
  points: number;
  points_per_game: number | null;
  goals_scored: number;
  goals_conceded: number;
  average_goals_scored: number | null;
  average_goals_conceded: number | null;
  average_xg: AverageMetricSummary;
  average_xga: AverageMetricSummary;
  average_shots: AverageMetricSummary;
  average_shots_on_goal: AverageMetricSummary;
  average_possession_pct: AverageMetricSummary;
  average_corners: AverageMetricSummary;
  average_yellow_cards: AverageMetricSummary;
  average_red_cards: AverageMetricSummary;
  clean_sheets: RateMetricSummary;
  failed_to_score: RateMetricSummary;
  btts: RateMetricSummary;
  total_goals: Record<string, GoalTotalsRateSummary>;
  streaks: StreakSummary;
};

export type TeamAnalyticsResponse = {
  team: TeamReference;
  season_id: number;
  scope: "overall" | "home" | "away";
  window: 5 | 10 | 15 | 20;
  as_of_kickoff: string;
  metrics: MetricSummary;
};

export type FixtureAnalyticsSide = {
  team: TeamReference;
  overall: MetricSummary;
  venue_split: MetricSummary;
};

export type FixtureAnalyticsResponse = {
  fixture: FixtureSummary;
  window: 5 | 10 | 15 | 20;
  historical_cutoff_at: string;
  home: FixtureAnalyticsSide;
  away: FixtureAnalyticsSide;
};

export type FixtureTeamStatistics = {
  shots_on_goal: number | null;
  shots_off_goal: number | null;
  total_shots: number | null;
  blocked_shots: number | null;
  shots_inside_box: number | null;
  shots_outside_box: number | null;
  fouls: number | null;
  corner_kicks: number | null;
  offsides: number | null;
  yellow_cards: number | null;
  red_cards: number | null;
  goalkeeper_saves: number | null;
  total_passes: number | null;
  passes_accurate: number | null;
  possession_pct: number | null;
  pass_accuracy_pct: number | null;
  expected_goals: number | null;
  goals_prevented: number | null;
};

export type FixtureStatisticsResponse = {
  fixture: FixtureSummary;
  home: { team: TeamReference; metrics: FixtureTeamStatistics | null };
  away: { team: TeamReference; metrics: FixtureTeamStatistics | null };
};
