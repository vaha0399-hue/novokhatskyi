"""Thin Cup adapter for the shared canonical base-import transaction.

The shared transaction is intentionally not copied here: league and cup
imports must retain identical provenance, mapping, fixture and standings DML.
``write_validated_base`` is the extraction seam for that common transaction.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from psycopg import Connection

from app.importer.active_season import (
    CupSeasonScope,
    ValidatedCupBase,
    import_validated_base,
    validate_cup_base_responses,
)
from app.importer.season_bootstrap import CollectedBaseResponse


ValidatedBaseWriter = Callable[[Connection[Any], ValidatedCupBase, Sequence[CollectedBaseResponse]], Any]


class CupCanonicalSink:
    """Hand a validated cup to the single shared canonical transaction."""

    def __init__(
        self,
        conn: Connection[Any],
        *,
        write_validated_base: ValidatedBaseWriter = import_validated_base,
    ) -> None:
        self._conn = conn
        self._write_validated_base = write_validated_base

    def write_cup_base(
        self, *, validated: ValidatedCupBase, collected: Sequence[CollectedBaseResponse]
    ) -> None:
        self._write_validated_base(self._conn, validated, collected)


def import_cup_base(
    conn: Connection[Any],
    *,
    collected: Sequence[CollectedBaseResponse],
    scope: CupSeasonScope,
    write_validated_base: ValidatedBaseWriter = import_validated_base,
) -> Any:
    """Validate a Cup scope and pass it to the generic canonical core.

    The default is the same core called by ``import_active_base``.  Injection
    remains available for tests and controlled orchestration.
    """
    validated = validate_cup_base_responses(collected, scope=scope)
    return write_validated_base(conn, validated, collected)
