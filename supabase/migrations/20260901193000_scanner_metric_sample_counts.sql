BEGIN;

ALTER TABLE football.team_rolling_metrics
    ADD COLUMN xg_sample_count smallint NOT NULL DEFAULT 0 CHECK (xg_sample_count >= 0),
    ADD COLUMN xga_sample_count smallint NOT NULL DEFAULT 0 CHECK (xga_sample_count >= 0),
    ADD COLUMN shots_sample_count smallint NOT NULL DEFAULT 0 CHECK (shots_sample_count >= 0),
    ADD COLUMN shots_on_goal_sample_count smallint NOT NULL DEFAULT 0 CHECK (shots_on_goal_sample_count >= 0),
    ADD COLUMN corners_sample_count smallint NOT NULL DEFAULT 0 CHECK (corners_sample_count >= 0),
    ADD COLUMN possession_sample_count smallint NOT NULL DEFAULT 0 CHECK (possession_sample_count >= 0);

ALTER TABLE football.team_rolling_metrics
    ADD CONSTRAINT team_rolling_metrics_sample_counts_within_matches
    CHECK (
        xg_sample_count <= matches_count
        AND xga_sample_count <= matches_count
        AND shots_sample_count <= matches_count
        AND shots_on_goal_sample_count <= matches_count
        AND corners_sample_count <= matches_count
        AND possession_sample_count <= matches_count
    );

-- Existing rows predate the sample-count columns.  Derive their counts from
-- exactly the same finalized, complete-statistics history that the writer uses
-- for future upserts, without making another provider request.
WITH ranked_history AS (
    SELECT metric.team_id,metric.season_id,metric.scope,metric.window_size,
           own.expected_goals AS xg,opponent.expected_goals AS xga,
           own.total_shots,own.shots_on_goal,own.corner_kicks,own.possession_pct,
           row_number() OVER (
               PARTITION BY metric.team_id,metric.season_id,metric.scope,metric.window_size
               ORDER BY fixture.kickoff_at DESC,fixture.id DESC
           ) AS position
    FROM football.team_rolling_metrics metric
    JOIN football.fixtures fixture ON fixture.season_id=metric.season_id
    JOIN football.fixture_team_statistics own
      ON own.fixture_id=fixture.id AND own.team_id=metric.team_id
    JOIN football.fixture_team_statistics opponent
      ON opponent.fixture_id=fixture.id
     AND opponent.team_id=CASE
         WHEN fixture.home_team_id=metric.team_id THEN fixture.away_team_id
         ELSE fixture.home_team_id
     END
    WHERE metric.window_size IN (5,10)
      AND fixture.lifecycle_state='completed'
      AND fixture.result_finalized_at IS NOT NULL
      AND metric.team_id IN (fixture.home_team_id,fixture.away_team_id)
      AND (
          metric.scope='overall'
          OR (metric.scope='home' AND fixture.home_team_id=metric.team_id)
          OR (metric.scope='away' AND fixture.away_team_id=metric.team_id)
      )
), samples AS (
    SELECT team_id,season_id,scope,window_size,
           count(*) FILTER (WHERE xg IS NOT NULL)::smallint AS xg_sample_count,
           count(*) FILTER (WHERE xga IS NOT NULL)::smallint AS xga_sample_count,
           count(*) FILTER (WHERE total_shots IS NOT NULL)::smallint AS shots_sample_count,
           count(*) FILTER (WHERE shots_on_goal IS NOT NULL)::smallint AS shots_on_goal_sample_count,
           count(*) FILTER (WHERE corner_kicks IS NOT NULL)::smallint AS corners_sample_count,
           count(*) FILTER (WHERE possession_pct IS NOT NULL)::smallint AS possession_sample_count
    FROM ranked_history
    WHERE position <= window_size
    GROUP BY team_id,season_id,scope,window_size
)
UPDATE football.team_rolling_metrics metric
SET xg_sample_count=samples.xg_sample_count,
    xga_sample_count=samples.xga_sample_count,
    shots_sample_count=samples.shots_sample_count,
    shots_on_goal_sample_count=samples.shots_on_goal_sample_count,
    corners_sample_count=samples.corners_sample_count,
    possession_sample_count=samples.possession_sample_count
FROM samples
WHERE (metric.team_id,metric.season_id,metric.scope,metric.window_size)=
      (samples.team_id,samples.season_id,samples.scope,samples.window_size);

COMMIT;
