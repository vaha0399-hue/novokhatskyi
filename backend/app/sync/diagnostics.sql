-- Q07 read-only operational diagnostics for the currently migrated schema.
-- Run with psql's ON_ERROR_STOP enabled.  The reports contain no parameters,
-- request parameters, headers, payloads, or raw exception texts.

-- queue_state: due handler backlog is intentionally limited to enabled policy
-- scopes; inactive or malformed scopes remain visible separately, but do not
-- inflate the active backlog alert.
WITH item_scope AS (
    SELECT item.*, policy.enabled AS policy_enabled
      FROM ops.sync_work_items item
      LEFT JOIN ops.competition_sync_policies policy
        ON policy.provider_id=(item.scope->'_sync_policy'->>'provider_id')::smallint
       AND policy.season_id=(item.scope->'_sync_policy'->>'season_id')::bigint
), classified AS (
    SELECT CASE
        WHEN policy_enabled IS NOT TRUE THEN 'inactive_or_unrecognized_scope'
        WHEN status='pending' AND available_at <= clock_timestamp() THEN 'pending_due'
        WHEN status='pending' THEN 'pending_scheduled'
        WHEN status='running' AND lease_expires_at > clock_timestamp() THEN 'active_lease'
        WHEN status='running' THEN 'stale_lease'
        WHEN status='quarantined' THEN 'quarantined'
        ELSE status
    END AS state,
    CASE WHEN policy_enabled IS TRUE AND status='pending' AND available_at <= clock_timestamp()
         THEN extract(epoch FROM clock_timestamp()-available_at)::bigint END AS queue_age_seconds
    FROM item_scope
)
SELECT state, count(*) AS work_items, max(queue_age_seconds) AS oldest_queue_age_seconds
  FROM classified
 GROUP BY state
 ORDER BY state;

-- queue_failures: stable state/error categories only; never select last_error.
SELECT CASE
         WHEN status='quarantined' THEN 'quarantined'
         WHEN last_error='budget_pending' THEN 'budget_pending'
         WHEN last_error LIKE 'provider_http_%' THEN 'provider_http_failure'
         WHEN last_error IS NOT NULL THEN 'other_sanitized_failure'
         ELSE 'no_failure'
       END AS failure_category,
       count(*) AS work_items,
       max(attempts) AS greatest_attempts,
       max(updated_at) AS most_recent_transition_at
  FROM ops.sync_work_items
 WHERE status IN ('pending','quarantined','failed') OR last_error IS NOT NULL
 GROUP BY 1
 ORDER BY 1;

-- provider_freshness: these are observed fetch facts, not claims that an
-- unwired D-handler requested data.  A missing observation is explicitly
-- unknown rather than fabricated as provider delay or missing statistics.
WITH active_policy AS (
    SELECT provider_id, season_id
      FROM ops.competition_sync_policies
     WHERE enabled
), fetch_summary AS (
    SELECT policy.provider_id, policy.season_id,
           max(provider_fetch.response_received_at) AS last_response_received_at,
           max(provider_fetch.normalized_at) AS last_normalized_at,
           count(provider_fetch.id) FILTER (WHERE provider_fetch.outcome='success'::source.fetch_outcome) AS successful_fetches,
           count(provider_fetch.id) FILTER (WHERE provider_fetch.outcome<>'success'::source.fetch_outcome) AS failed_fetches,
           count(provider_fetch.id) FILTER (WHERE provider_fetch.outcome='success'::source.fetch_outcome AND provider_fetch.provider_results=0) AS empty_successes
      FROM active_policy policy
      LEFT JOIN source.provider_fetches provider_fetch
        ON provider_fetch.provider_id=policy.provider_id
       AND (
            provider_fetch.subject_season_id=policy.season_id
            OR EXISTS (
                SELECT 1
                  FROM source.fixture_provider_refs fixture_ref
                  JOIN football.fixtures fixture ON fixture.id=fixture_ref.fixture_id
                 WHERE fixture_ref.provider_id=policy.provider_id
                   AND fixture_ref.fixture_id=provider_fetch.subject_fixture_id
                   AND fixture.season_id=policy.season_id
            )
       )
     GROUP BY policy.provider_id, policy.season_id
)
SELECT provider_id, season_id, last_response_received_at, last_normalized_at,
       CASE WHEN last_response_received_at IS NULL THEN NULL
            ELSE extract(epoch FROM clock_timestamp()-last_response_received_at)::bigint END AS freshness_age_seconds,
       successful_fetches, failed_fetches, empty_successes,
       CASE
         WHEN last_response_received_at IS NULL THEN 'unknown_no_season_scoped_fetch'
         WHEN successful_fetches=0 THEN 'provider_failure_observed'
         WHEN empty_successes>0 AND empty_successes=successful_fetches THEN 'provider_empty_response_observed'
         WHEN last_normalized_at IS NULL THEN 'response_not_normalized'
         ELSE 'observed_data'
       END AS factual_status
  FROM fetch_summary
 ORDER BY provider_id, season_id;

-- metric_coverage: source fixture/provider refs and policies pin every count
-- to one provider/season.  fixture_pairs is one row per fixture before metrics
-- are expanded, preventing joins from multiplying fixture or team-pair counts.
WITH active_scope AS (
    SELECT ref.provider_id, fixture.season_id, fixture.id AS fixture_id,
           fixture.home_team_id, fixture.away_team_id,
           coverage.coverage_state::text AS coverage_state,
           facts.statistics_rows, facts.participant_statistics_rows
      FROM source.fixture_provider_refs ref
      JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
      JOIN ops.competition_sync_policies policy
        ON policy.provider_id=ref.provider_id AND policy.season_id=fixture.season_id AND policy.enabled
      LEFT JOIN football.fixture_statistics_coverage coverage ON coverage.fixture_id=fixture.id
      LEFT JOIN LATERAL (
          SELECT count(*)::integer AS statistics_rows,
                 count(*) FILTER (WHERE statistics.team_id IN (fixture.home_team_id,fixture.away_team_id))::integer
                   AS participant_statistics_rows
            FROM football.fixture_team_statistics statistics
           WHERE statistics.fixture_id=fixture.id
      ) facts ON true
), metric_per_fixture AS (
    SELECT scope.provider_id, scope.season_id, scope.fixture_id,
           scope.statistics_rows, scope.participant_statistics_rows, scope.coverage_state,
           metric_name.name AS metric,
           count(statistics.team_id) FILTER (WHERE metric.value IS NOT NULL)::integer AS observed_team_pairs,
           count(statistics.team_id) FILTER (WHERE metric.value IS NULL)::integer AS null_team_pairs
      FROM active_scope scope
      CROSS JOIN (VALUES
          ('total_shots'), ('shots_on_goal'), ('corner_kicks'), ('yellow_cards'), ('expected_goals')
      ) AS selected(metric_name)
      CROSS JOIN LATERAL (
          SELECT selected.metric_name AS name
      ) metric_name
      LEFT JOIN football.fixture_team_statistics statistics
        ON statistics.fixture_id=scope.fixture_id
       AND statistics.team_id IN (scope.home_team_id,scope.away_team_id)
      CROSS JOIN LATERAL (VALUES (
          CASE metric_name.name
              WHEN 'total_shots' THEN statistics.total_shots::numeric
              WHEN 'shots_on_goal' THEN statistics.shots_on_goal::numeric
              WHEN 'corner_kicks' THEN statistics.corner_kicks::numeric
              WHEN 'yellow_cards' THEN statistics.yellow_cards::numeric
              WHEN 'expected_goals' THEN statistics.expected_goals
          END
      )) AS metric(value)
     GROUP BY scope.provider_id, scope.season_id, scope.fixture_id,
              scope.statistics_rows, scope.participant_statistics_rows, scope.coverage_state, metric_name.name
)
SELECT provider_id, season_id, metric,
       count(*) AS fixtures,
       count(*) * 2 AS expected_team_pairs,
       sum(observed_team_pairs) AS observed_metric_team_pairs,
       sum(null_team_pairs) AS null_metric_team_pairs,
       count(*) FILTER (WHERE statistics_rows=0) AS fixtures_without_statistics,
       count(*) FILTER (WHERE statistics_rows=1) AS fixtures_with_one_team_statistics,
       count(*) FILTER (WHERE statistics_rows=2 AND participant_statistics_rows=2
                         AND observed_team_pairs<2) AS complete_team_pair_missing_selected_metric,
       count(*) FILTER (WHERE coverage_state='empty') AS coverage_empty,
       count(*) FILTER (WHERE coverage_state='partial') AS coverage_partial,
       count(*) FILTER (WHERE coverage_state='unknown' OR coverage_state IS NULL) AS coverage_unknown_or_absent
  FROM metric_per_fixture
 GROUP BY provider_id, season_id, metric
 ORDER BY provider_id, season_id, metric;

-- api_budget: report the durable accounting and reason-relevant caps exactly as
-- stored.  It intentionally does not call reserve/observe or repair Q04 data.
SELECT config.daily_limit, state.daily_window, state.daily_used,
       config.protected_reserve, config.minute_limit, state.minute_window, state.minute_used,
       config.operations_limit, state.operations_used,
       config.history_limit, state.history_used,
       config.legacy_manual_limit, state.legacy_manual_used,
       state.provider_daily_remaining, state.provider_daily_exhausted,
       state.provider_minute_remaining, state.cooldown_until,
       CASE
         WHEN state.singleton IS NULL THEN 'state_uninitialized'
         WHEN state.provider_daily_exhausted OR state.provider_daily_remaining=0 THEN 'provider_daily_exhausted'
         WHEN state.provider_minute_remaining=0 THEN 'provider_minute_exhausted'
         WHEN state.cooldown_until > clock_timestamp() THEN 'cooldown_active'
         WHEN state.daily_used >= config.daily_limit-config.protected_reserve THEN 'daily_limit_reached'
         WHEN state.minute_used >= config.minute_limit THEN 'minute_limit_reached'
         ELSE 'available_by_local_accounting'
       END AS accounting_status
  FROM ops.api_football_budget_config config
  LEFT JOIN ops.api_football_budget_state state ON state.singleton=true
 WHERE config.singleton=true;
