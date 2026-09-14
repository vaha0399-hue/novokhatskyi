-- Q07 read-only operational diagnostics for the currently migrated schema.
-- Run with psql's ON_ERROR_STOP enabled.  The reports contain no parameters,
-- request parameters, headers, payloads, or raw exception texts.

-- queue_state: due handler backlog is intentionally limited to enabled policy
-- scopes; inactive or malformed scopes remain visible separately, but do not
-- inflate the active backlog alert.
WITH scope_values AS (
    SELECT item.*,
           CASE
             WHEN jsonb_typeof(item.scope->'_sync_policy')='object'
              AND jsonb_typeof(item.scope->'_sync_policy'->'provider_id')='number'
              AND item.scope->'_sync_policy'->>'provider_id' ~ '^[1-9][0-9]{0,4}$'
              AND (length(item.scope->'_sync_policy'->>'provider_id') < 5
                   OR item.scope->'_sync_policy'->>'provider_id' <= '32767')
             THEN (item.scope->'_sync_policy'->>'provider_id')::smallint
           END AS provider_id,
           CASE
             WHEN jsonb_typeof(item.scope->'_sync_policy')='object'
              AND jsonb_typeof(item.scope->'_sync_policy'->'season_id')='number'
              AND item.scope->'_sync_policy'->>'season_id' ~ '^[1-9][0-9]{0,18}$'
              AND (length(item.scope->'_sync_policy'->>'season_id') < 19
                   OR item.scope->'_sync_policy'->>'season_id' <= '9223372036854775807')
             THEN (item.scope->'_sync_policy'->>'season_id')::bigint
           END AS season_id
      FROM ops.sync_work_items item
), item_scope AS (
    SELECT item.*, policy.enabled AS policy_enabled,
           item.provider_id IS NOT NULL AND item.season_id IS NOT NULL AS scope_valid
      FROM scope_values item
      LEFT JOIN ops.competition_sync_policies policy
        ON policy.provider_id=item.provider_id
       AND policy.season_id=item.season_id
), classified AS (
    SELECT CASE
        WHEN job_type='legacy' THEN 'legacy_compatibility_scope'
        WHEN NOT scope_valid THEN 'invalid_scope'
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

-- overdue_football_lifecycle: football states are distinct from queue leases.
-- This is a read-only D04 diagnostic for known nonterminal kickoffs, not a
-- request to the provider and not a status-repair action.
SELECT ref.provider_id, fixture.season_id, fixture.lifecycle_state::text AS lifecycle_state,
       count(*) AS overdue_fixtures, min(fixture.kickoff_at) AS oldest_kickoff_at,
       max(extract(epoch FROM clock_timestamp()-fixture.kickoff_at)::bigint) AS greatest_overdue_seconds
  FROM source.fixture_provider_refs ref
  JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
  JOIN ops.competition_sync_policies policy
    ON policy.provider_id=ref.provider_id AND policy.season_id=fixture.season_id AND policy.enabled
 WHERE fixture.lifecycle_state IN ('scheduled','in_progress')
   AND fixture.kickoff_at <= clock_timestamp()-interval '15 minutes'
 GROUP BY ref.provider_id, fixture.season_id, fixture.lifecycle_state
 ORDER BY ref.provider_id, fixture.season_id, fixture.lifecycle_state;

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
), scoped_fetches AS (
    SELECT policy.provider_id, policy.season_id, provider_fetch.id,
           provider_fetch.endpoint, provider_fetch.request_started_at,
           provider_fetch.response_received_at, provider_fetch.normalized_at,
           provider_fetch.outcome, provider_fetch.provider_results
      FROM active_policy policy
      JOIN source.provider_fetches provider_fetch ON provider_fetch.provider_id=policy.provider_id
       AND (provider_fetch.subject_season_id=policy.season_id
            OR EXISTS (
                SELECT 1
                  FROM source.fixture_provider_refs fixture_ref
                  JOIN football.fixtures fixture ON fixture.id=fixture_ref.fixture_id
                 WHERE fixture_ref.provider_id=policy.provider_id
                   AND fixture_ref.fixture_id=provider_fetch.subject_fixture_id
                   AND fixture.season_id=policy.season_id
            )
       )
), ranked_fetches AS (
    SELECT provider_fetch.*, row_number() OVER (
               PARTITION BY provider_id, season_id, endpoint
               ORDER BY coalesce(response_received_at, request_started_at) DESC, id DESC
           ) AS position
      FROM scoped_fetches provider_fetch
), lifetime_counts AS (
    SELECT provider_id, season_id, endpoint,
           count(*) FILTER (WHERE outcome='success'::source.fetch_outcome) AS lifetime_successes,
           count(*) FILTER (WHERE outcome<>'success'::source.fetch_outcome) AS lifetime_failures
      FROM scoped_fetches
     GROUP BY provider_id, season_id, endpoint
)
SELECT policy.provider_id, policy.season_id, latest.endpoint,
       latest.response_received_at AS last_response_received_at,
       latest.normalized_at AS last_normalized_at,
       CASE WHEN latest.response_received_at IS NULL THEN NULL
            ELSE extract(epoch FROM clock_timestamp()-latest.response_received_at)::bigint END AS freshness_age_seconds,
       coalesce(counts.lifetime_successes,0) AS lifetime_successes,
       coalesce(counts.lifetime_failures,0) AS lifetime_failures,
       latest.outcome::text AS latest_outcome,
       latest.provider_results AS latest_provider_results,
       CASE
         WHEN latest.id IS NULL THEN 'unknown_no_season_scoped_fetch'
         WHEN latest.outcome<>'success'::source.fetch_outcome THEN 'provider_failure_observed'
         WHEN latest.provider_results=0 THEN 'provider_empty_response_observed'
         WHEN latest.normalized_at IS NULL THEN 'response_not_normalized'
         ELSE 'observed_data'
       END AS factual_status
  FROM active_policy policy
  LEFT JOIN ranked_fetches latest
    ON latest.provider_id=policy.provider_id AND latest.season_id=policy.season_id AND latest.position=1
  LEFT JOIN lifetime_counts counts
    ON counts.provider_id=latest.provider_id AND counts.season_id=latest.season_id AND counts.endpoint=latest.endpoint
 ORDER BY policy.provider_id, policy.season_id, latest.endpoint NULLS FIRST;

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
     WHERE fixture.lifecycle_state='completed'
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
