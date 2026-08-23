"""Cutoff-safe historical football analytics computed from canonical data."""

from .engine import AnalyticsEngine, AnalyticsInvariantError
from .models import (
    AnalyticsScope,
    FixtureAnalytics,
    FixtureContext,
    TeamAnalyticsBundle,
    TeamMatchRecord,
    WindowAnalytics,
)
from .repository import PostgresAnalyticsRepository

__all__ = [
    "AnalyticsEngine",
    "AnalyticsInvariantError",
    "AnalyticsScope",
    "FixtureAnalytics",
    "FixtureContext",
    "PostgresAnalyticsRepository",
    "TeamAnalyticsBundle",
    "TeamMatchRecord",
    "WindowAnalytics",
]
