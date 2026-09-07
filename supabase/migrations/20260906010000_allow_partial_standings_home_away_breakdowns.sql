BEGIN;

-- API-Football Cup group tables can publish aggregate ``all`` results while
-- withholding their home/away split.  Keep the provider facts as supplied:
-- the split may be partial, but can never exceed the aggregate total.
ALTER TABLE football.standings_snapshot_rows
    DROP CONSTRAINT standings_snapshot_rows_check3;

ALTER TABLE football.standings_snapshot_rows
    ADD CONSTRAINT standings_snapshot_rows_home_away_not_overstated_check
    CHECK (home_played + away_played <= played);

COMMIT;
