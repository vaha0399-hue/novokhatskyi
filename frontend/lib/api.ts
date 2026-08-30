import "server-only";

import type {
  FixtureAnalyticsResponse,
  FixtureStatisticsResponse,
  LeagueReference,
  SeasonFixturesResponse,
  SeasonReference,
  SeasonStandingsResponse,
  TeamAnalyticsResponse,
} from "@/lib/contracts";

const DEFAULT_REVALIDATE_SECONDS = 300;

export class BackendApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
  ) {
    super(message);
    this.name = "BackendApiError";
  }
}

export function backendBaseUrl(): string {
  const raw = process.env.BACKEND_INTERNAL_URL;
  if (!raw) {
    throw new BackendApiError("BACKEND_INTERNAL_URL is not configured", 503);
  }
  return raw.replace(/\/$/, "");
}

async function readApi<T>(path: string, revalidate = DEFAULT_REVALIDATE_SECONDS): Promise<T> {
  const response = await fetch(`${backendBaseUrl()}/web/v1${path}`, {
    next: { revalidate },
    headers: { Accept: "application/json" },
  });

  if (!response.ok) {
    throw new BackendApiError(`Backend request failed for ${path}`, response.status);
  }
  return response.json() as Promise<T>;
}

export async function getLeagues(): Promise<LeagueReference[]> {
  return (await readApi<{ leagues: LeagueReference[] }>("/leagues")).leagues;
}

export async function getLeagueSeasons(leagueId: number): Promise<{ league: LeagueReference; seasons: SeasonReference[] }> {
  return readApi(`/leagues/${leagueId}/seasons`);
}

export async function getSeasonStandings(seasonId: number): Promise<SeasonStandingsResponse> {
  return readApi(`/seasons/${seasonId}/standings`);
}

export async function getSeasonFixtures(seasonId: number): Promise<SeasonFixturesResponse> {
  return readApi(`/seasons/${seasonId}/fixtures?limit=500&offset=0`);
}

export async function getFixtureAnalytics(fixtureId: number, window = 10): Promise<FixtureAnalyticsResponse> {
  return readApi(`/fixtures/${fixtureId}/analytics?window=${window}`);
}

export async function getFixtureStatistics(fixtureId: number): Promise<FixtureStatisticsResponse> {
  return readApi(`/fixtures/${fixtureId}/statistics`);
}

export async function getTeamAnalytics(
  teamId: number,
  seasonId: number,
  scope: "overall" | "home" | "away",
  window: 5 | 10 | 15 | 20,
): Promise<TeamAnalyticsResponse> {
  return readApi(`/teams/${teamId}/analytics?season_id=${seasonId}&scope=${scope}&window=${window}`);
}
