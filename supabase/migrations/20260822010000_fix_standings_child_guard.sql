-- Fix Stage 3B standings child validation for heterogeneous trigger records.
-- standings_snapshot_groups has no team_id column, so NEW.team_id must only be
-- referenced inside the standings_snapshot_rows branch.

CREATE OR REPLACE FUNCTION football.guard_standings_snapshot_children()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  header_txid bigint;
  snapshot_season_id bigint;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    RAISE EXCEPTION 'standings snapshot rows are immutable' USING ERRCODE = '55000';
  END IF;

  SELECT ingest_txid, season_id
  INTO header_txid, snapshot_season_id
  FROM football.standings_snapshots
  WHERE id = NEW.snapshot_id;

  IF header_txid <> txid_current() THEN
    RAISE EXCEPTION 'standings rows must be inserted in the snapshot transaction'
      USING ERRCODE = '55000';
  END IF;

  IF TG_TABLE_NAME = 'standings_snapshot_rows' THEN
    IF NOT EXISTS (
      SELECT 1
      FROM football.season_teams st
      WHERE st.season_id = snapshot_season_id
        AND st.team_id = NEW.team_id
    ) THEN
      RAISE EXCEPTION 'standings row team does not belong to snapshot season'
        USING ERRCODE = '23514';
    END IF;
  END IF;

  RETURN NEW;
END
$$;
