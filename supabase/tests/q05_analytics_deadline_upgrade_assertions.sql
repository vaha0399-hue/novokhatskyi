DO $$
DECLARE
    accepted_row record;
    pending_row record;
BEGIN
    SELECT analytics_window.* INTO accepted_row
      FROM ops.fixture_analytics_recalculation_windows analytics_window
      JOIN football.fixtures fixture ON fixture.id=analytics_window.fixture_id
      JOIN football.seasons season ON season.id=fixture.season_id
     WHERE season.label='Q05 deadline upgrade season' AND analytics_window.accepted_at IS NOT NULL;
    IF accepted_row.window_end IS DISTINCT FROM '2026-09-09 12:01:00+00'::timestamptz
       OR accepted_row.deadline IS DISTINCT FROM '2026-09-09 12:01:00+00'::timestamptz
       OR accepted_row.source_window_end IS NOT NULL
       OR accepted_row.accepted_input_version IS DISTINCT FROM accepted_row.latest_source_fetch_id::text
       OR accepted_row.accepted_at IS DISTINCT FROM '2026-09-09 12:01:00.000101+00'::timestamptz THEN
        RAISE EXCEPTION 'accepted analytics window changed during deadline upgrade';
    END IF;

    SELECT analytics_window.* INTO pending_row
      FROM ops.fixture_analytics_recalculation_windows analytics_window
      JOIN football.fixtures fixture ON fixture.id=analytics_window.fixture_id
      JOIN football.seasons season ON season.id=fixture.season_id
     WHERE season.label='Q05 deadline upgrade season' AND analytics_window.accepted_at IS NULL;
    IF pending_row.window_end IS DISTINCT FROM '2026-09-09 12:03:00+00'::timestamptz
       OR pending_row.deadline IS DISTINCT FROM '2026-09-09 12:02:00+00'::timestamptz
       OR pending_row.source_window_end IS DISTINCT FROM '2026-09-09 12:02:00+00'::timestamptz
       OR pending_row.accepted_input_version IS NOT NULL THEN
        RAISE EXCEPTION 'pending analytics window changed during deadline upgrade';
    END IF;

    BEGIN
        UPDATE ops.fixture_analytics_recalculation_windows
           SET deadline=deadline + interval '1 second'
         WHERE fixture_id=accepted_row.fixture_id AND window_end=accepted_row.window_end;
        RAISE EXCEPTION 'accepted analytics window guard was not restored';
    EXCEPTION
        WHEN object_not_in_prerequisite_state THEN
            IF SQLERRM NOT LIKE '%accepted analytics windows are immutable%' THEN
                RAISE;
            END IF;
    END;
END $$;
