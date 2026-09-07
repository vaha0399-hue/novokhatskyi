from __future__ import annotations

import json

import pytest

from scripts.classify_cup_fixture_statistics import (
    SUMMARY_FILE,
    WITH_STATS_FILE,
    WITHOUT_STATS_FILE,
    classify_cups,
    write_report,
)


def _entry(
    league_id: int, name: str, entry_type: str, statistics_fixtures: bool, season: int = 2026
) -> dict:
    return {
        "league_id": league_id,
        "name": name,
        "type": entry_type,
        "season": season,
        "coverage": {"fixtures": {"statistics_fixtures": statistics_fixtures}},
    }


def test_classify_cups_filters_exact_type_deduplicates_and_sorts_reports() -> None:
    with_stats, without_stats = classify_cups(
        [
            _entry(2, "League", "League", True),
            _entry(4, "A Cup", "Cup", True),
            _entry(3, "B Cup", "Cup", True),
            _entry(3, "B Cup", "Cup", True),
        ],
        [_entry(10, "Z Cup", "Cup", False)],
    )

    assert with_stats == [{"league_id": 3, "name": "B Cup"}, {"league_id": 4, "name": "A Cup"}]
    assert without_stats == [{"league_id": 10, "name": "Z Cup"}]


def test_write_report_writes_counts_and_only_id_name(tmp_path) -> None:
    input_path = tmp_path / "catalogue.json"
    input_path.write_text(
        json.dumps([_entry(1, "Stats Cup", "Cup", True)]),
        encoding="utf-8",
    )
    output_dir = tmp_path / "cup-report"

    without_input_path = tmp_path / "without-catalogue.json"
    without_input_path.write_text(json.dumps([_entry(2, "No Stats Cup", "Cup", False)]), encoding="utf-8")

    assert write_report(input_path, without_input_path, output_dir) == {
        "TOTAL_CUP": 2,
        "WITH_FIXTURE_STATS": 1,
        "WITHOUT_FIXTURE_STATS": 1,
    }
    assert (output_dir / WITH_STATS_FILE).read_text(encoding="utf-8") == "1\tStats Cup\n"
    assert (output_dir / WITHOUT_STATS_FILE).read_text(encoding="utf-8") == "2\tNo Stats Cup\n"
    assert json.loads((output_dir / SUMMARY_FILE).read_text()) == {
        "TOTAL_CUP": 2,
        "WITH_FIXTURE_STATS": 1,
        "WITHOUT_FIXTURE_STATS": 1,
    }


def test_write_report_refuses_existing_directory(tmp_path) -> None:
    input_path = tmp_path / "catalogue.json"
    input_path.write_text(json.dumps([]), encoding="utf-8")
    without_input_path = tmp_path / "without-catalogue.json"
    without_input_path.write_text(json.dumps([]), encoding="utf-8")
    output_dir = tmp_path / "existing"
    output_dir.mkdir()

    with pytest.raises(ValueError, match="refusing to overwrite"):
        write_report(input_path, without_input_path, output_dir)


def test_classify_cups_rejects_invalid_cup_coverage() -> None:
    with pytest.raises(ValueError, match="statistics_fixtures"):
        classify_cups(
            [{"league_id": 1, "name": "Cup", "type": "Cup", "season": 2026, "coverage": {"fixtures": {}}}],
            [],
        )


def test_classify_cups_rejects_source_with_wrong_statistics_category() -> None:
    with pytest.raises(ValueError, match="unexpected"):
        classify_cups([_entry(1, "Cup", "Cup", False)], [])
