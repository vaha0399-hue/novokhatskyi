"""Validation and timezone-safe orchestration for scanner reads."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import NULLABLE_AVERAGE_SAMPLE_COLUMNS, ScannerQuery
from .repository import ScannerFixtureRecord, ScannerRepository


class ScannerNotFoundError(LookupError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ScannerValidationError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ScannerService:
    def __init__(self, repository: ScannerRepository) -> None:
        self._repository = repository

    def scan(self, *, query: ScannerQuery) -> tuple[str, int, list[ScannerFixtureRecord]]:
        if query.window_size not in {5, 10} or not 1 <= query.min_matches <= query.window_size:
            raise ScannerValidationError("invalid_scanner_window")
        for filter_ in query.filters:
            if filter_.min_samples is not None and (
                filter_.metric not in NULLABLE_AVERAGE_SAMPLE_COLUMNS
                or not 1 <= filter_.min_samples <= query.window_size
            ):
                raise ScannerValidationError("invalid_scanner_min_samples")
        try:
            timezone = ZoneInfo(query.timezone)
        except (ValueError, ZoneInfoNotFoundError) as error:
            raise ScannerValidationError("invalid_timezone") from error
        try:
            start_at = datetime.combine(query.match_date, time.min, tzinfo=timezone).astimezone(UTC)
            end_at = datetime.combine(query.match_date + timedelta(days=1), time.min, tzinfo=timezone).astimezone(UTC)
        except OverflowError as error:
            raise ScannerValidationError("invalid_match_date") from error
        if self._repository.missing_league_ids(query.league_ids):
            raise ScannerNotFoundError("league_not_found")
        total, fixtures = self._repository.scan_scheduled_fixtures(
            query=query, start_at=start_at, end_at=end_at,
        )
        return timezone.key, total, fixtures
