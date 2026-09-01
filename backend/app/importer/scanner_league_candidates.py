"""Versioned API-Football scanner candidate allow-list.

This is a discovery snapshot, not an operational import allow-list. Every
entry was observed on 2026-09-01 as provider type League with a current
season, standings coverage, and fixture-statistics coverage. A candidate must
still pass a competition-format canary before it is promoted to a worker scope.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScannerLeagueCandidate:
    provider_league_id: int
    name: str
    country: str
    provider_current_season: int

    def __post_init__(self) -> None:
        if (
            self.provider_league_id <= 0
            or not self.name.strip()
            or not self.country.strip()
            or self.provider_current_season < 2000
        ):
            raise ValueError("scanner league candidate is invalid")


SCANNER_LEAGUE_CANDIDATES: tuple[ScannerLeagueCandidate, ...] = (
    ScannerLeagueCandidate(provider_league_id=1032, name="Copa de la Liga Profesional", country="Argentina", provider_current_season=2024),
    ScannerLeagueCandidate(provider_league_id=128, name="Liga Profesional Argentina", country="Argentina", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=134, name="Torneo Federal A", country="Argentina", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=421, name="Division di Honor", country="Aruba", provider_current_season=2025),
    ScannerLeagueCandidate(provider_league_id=218, name="Bundesliga", country="Austria", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=116, name="Premier League", country="Belarus", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=145, name="Challenger Pro League", country="Belgium", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=144, name="Jupiler Pro League", country="Belgium", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=344, name="Primera División", country="Bolivia", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=624, name="Carioca - 1", country="Brazil", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=475, name="Paulista - A1", country="Brazil", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=71, name="Serie A", country="Brazil", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=72, name="Serie B", country="Brazil", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=172, name="First League", country="Bulgaria", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=813, name="Elite Two", country="Cameroon", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=479, name="Canadian Premier League", country="Canada", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=265, name="Primera División", country="Chile", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=169, name="Super League", country="China", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=239, name="Primera A", country="Colombia", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=210, name="HNL", country="Croatia", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=345, name="Czech Liga", country="Czech-Republic", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=119, name="Superliga", country="Denmark", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=242, name="Liga Pro", country="Ecuador", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=233, name="Premier League", country="Egypt", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=40, name="Championship", country="England", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=41, name="League One", country="England", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=42, name="League Two", country="England", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=39, name="Premier League", country="England", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=363, name="Premier League", country="Ethiopia", provider_current_season=2025),
    ScannerLeagueCandidate(provider_league_id=244, name="Veikkausliiga", country="Finland", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=61, name="Ligue 1", country="France", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=62, name="Ligue 2", country="France", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=327, name="Erovnuli Liga", country="Georgia", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=1104, name="Liga 3", country="Georgia", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=79, name="2. Bundesliga", country="Germany", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=80, name="3. Liga", country="Germany", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=78, name="Bundesliga", country="Germany", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=82, name="Frauen Bundesliga", country="Germany", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=197, name="Super League 1", country="Greece", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=271, name="NB I", country="Hungary", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=323, name="Indian Super League", country="India", provider_current_season=2025),
    ScannerLeagueCandidate(provider_league_id=357, name="Premier Division", country="Ireland", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=383, name="Ligat Ha'al", country="Israel", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=135, name="Serie A", country="Italy", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=136, name="Serie B", country="Italy", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=98, name="J1 League", country="Japan", provider_current_season=2027),
    ScannerLeagueCandidate(provider_league_id=262, name="Liga MX", country="Mexico", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=89, name="Eerste Divisie", country="Netherlands", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=88, name="Eredivisie", country="Netherlands", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=103, name="Eliteserien", country="Norway", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=250, name="Division Profesional - Apertura", country="Paraguay", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=252, name="Division Profesional - Clausura", country="Paraguay", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=281, name="Primera División", country="Peru", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=106, name="Ekstraklasa", country="Poland", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=94, name="Primeira Liga", country="Portugal", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=95, name="Segunda Liga", country="Portugal", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=305, name="Stars League", country="Qatar", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=283, name="Liga I", country="Romania", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=236, name="First League", country="Russia", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=235, name="Premier League", country="Russia", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=307, name="Pro League", country="Saudi-Arabia", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=179, name="Premiership", country="Scotland", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=286, name="Super Liga", country="Serbia", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=288, name="Premier Soccer League", country="South-Africa", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=292, name="K League 1", country="South-Korea", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=140, name="La Liga", country="Spain", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=141, name="Segunda División", country="Spain", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=113, name="Allsvenskan", country="Sweden", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=549, name="Damallsvenskan", country="Sweden", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=114, name="Superettan", country="Sweden", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=207, name="Super League", country="Switzerland", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=204, name="1. Lig", country="Turkey", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=203, name="Süper Lig", country="Turkey", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=301, name="Pro League", country="United-Arab-Emirates", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=253, name="Major League Soccer", country="USA", provider_current_season=2026),
    ScannerLeagueCandidate(provider_league_id=254, name="NWSL Women", country="USA", provider_current_season=2026),
)
