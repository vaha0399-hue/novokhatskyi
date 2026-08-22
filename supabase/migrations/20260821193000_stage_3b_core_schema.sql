-- Football Analytics Stage 3B: pre-match, non-live canonical storage.
-- This migration intentionally creates no provider credentials, HTTP headers,
-- live-event storage, or live score/state columns.

CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS football;
CREATE SCHEMA IF NOT EXISTS ml;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TYPE source.fetch_outcome AS ENUM ('success', 'provider_error', 'http_error', 'transport_error');
CREATE TYPE source.fetch_purpose AS ENUM ('bootstrap', 'scheduled_refresh', 'prematch', 'postmatch_reconciliation', 'research');
CREATE TYPE source.raw_retention_class AS ENUM ('standard', 'anomaly', 'prediction_input', 'contract_sample');
CREATE TYPE football.fixture_lifecycle_state AS ENUM ('scheduled', 'postponed', 'cancelled', 'abandoned', 'completed');
CREATE TYPE football.availability_basis AS ENUM ('observed', 'reconstructed_conservative');
CREATE TYPE football.snapshot_coverage_state AS ENUM ('complete', 'partial', 'empty', 'unknown');
CREATE TYPE football.lineup_role AS ENUM ('starter', 'substitute');
CREATE TYPE ml.model_status AS ENUM ('draft', 'active', 'retired');
CREATE TYPE ops.reconciliation_state AS ENUM ('waiting', 'pending', 'completed', 'exhausted');

CREATE TABLE source.providers (
    id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code text NOT NULL UNIQUE CHECK (code ~ '^[a-z0-9][a-z0-9_-]*$'),
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE football.leagues (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    country_name text,
    logo_url text,
    flag_url text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz
);

CREATE TABLE football.teams (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    code text,
    country_name text,
    founded_year smallint CHECK (founded_year IS NULL OR founded_year BETWEEN 1800 AND 3000),
    is_national boolean,
    logo_url text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz
);

CREATE TABLE football.venues (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    address text,
    city text,
    capacity integer CHECK (capacity IS NULL OR capacity >= 0),
    surface text,
    image_url text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz
);

CREATE TABLE football.players (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    display_name text NOT NULL,
    photo_url text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz
);

CREATE TABLE football.coaches (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    display_name text NOT NULL,
    photo_url text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz
);

CREATE TABLE football.seasons (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    league_id bigint NOT NULL REFERENCES football.leagues(id) ON DELETE RESTRICT,
    start_year smallint NOT NULL CHECK (start_year BETWEEN 1800 AND 3000),
    label text NOT NULL,
    starts_on date,
    ends_on date,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (league_id, start_year),
    CHECK (ends_on IS NULL OR starts_on IS NULL OR ends_on >= starts_on)
);

CREATE TABLE source.league_provider_refs (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    external_id text NOT NULL CHECK (external_id <> ''),
    league_id bigint NOT NULL REFERENCES football.leagues(id) ON DELETE RESTRICT,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, external_id),
    UNIQUE (provider_id, league_id),
    CHECK (last_seen_at >= first_seen_at)
);
CREATE TABLE source.team_provider_refs (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    external_id text NOT NULL CHECK (external_id <> ''),
    team_id bigint NOT NULL REFERENCES football.teams(id) ON DELETE RESTRICT,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, external_id), UNIQUE (provider_id, team_id),
    CHECK (last_seen_at >= first_seen_at)
);
CREATE TABLE source.venue_provider_refs (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    external_id text NOT NULL CHECK (external_id <> ''),
    venue_id bigint NOT NULL REFERENCES football.venues(id) ON DELETE RESTRICT,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(), last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, external_id), UNIQUE (provider_id, venue_id), CHECK (last_seen_at >= first_seen_at)
);
CREATE TABLE source.player_provider_refs (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    external_id text NOT NULL CHECK (external_id <> ''),
    player_id bigint NOT NULL REFERENCES football.players(id) ON DELETE RESTRICT,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(), last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, external_id), UNIQUE (provider_id, player_id), CHECK (last_seen_at >= first_seen_at)
);
CREATE TABLE source.coach_provider_refs (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    external_id text NOT NULL CHECK (external_id <> ''),
    coach_id bigint NOT NULL REFERENCES football.coaches(id) ON DELETE RESTRICT,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(), last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, external_id), UNIQUE (provider_id, coach_id), CHECK (last_seen_at >= first_seen_at)
);
CREATE TABLE source.season_provider_refs (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    league_external_id text NOT NULL CHECK (league_external_id <> ''),
    external_season integer NOT NULL CHECK (external_season BETWEEN 1800 AND 3000),
    season_id bigint NOT NULL REFERENCES football.seasons(id) ON DELETE RESTRICT,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(), last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, league_external_id, external_season), UNIQUE (provider_id, season_id),
    FOREIGN KEY (provider_id, league_external_id) REFERENCES source.league_provider_refs(provider_id, external_id) ON DELETE RESTRICT,
    CHECK (last_seen_at >= first_seen_at)
);

CREATE TABLE source.provider_fetches (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    endpoint text NOT NULL CHECK (endpoint <> ''),
    request_params jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(request_params) = 'object'),
    request_params_sha256 bytea CHECK (request_params_sha256 IS NULL OR octet_length(request_params_sha256) = 32),
    purpose source.fetch_purpose NOT NULL,
    request_started_at timestamptz NOT NULL,
    response_received_at timestamptz,
    http_status smallint CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    outcome source.fetch_outcome NOT NULL,
    provider_results integer CHECK (provider_results IS NULL OR provider_results >= 0),
    paging_current integer CHECK (paging_current IS NULL OR paging_current >= 1),
    paging_total integer CHECK (paging_total IS NULL OR paging_total >= 1),
    content_sha256 bytea CHECK (content_sha256 IS NULL OR octet_length(content_sha256) = 32),
    normalized_at timestamptz,
    sanitized_error_class text,
    sanitized_error_text text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (response_received_at IS NULL OR response_received_at >= request_started_at),
    CHECK (paging_total IS NULL OR paging_current IS NULL OR paging_current <= paging_total)
);

CREATE TABLE source.provider_raw_payloads (
    fetch_id bigint PRIMARY KEY REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    inline_body bytea,
    object_key text,
    content_type text NOT NULL DEFAULT 'application/json',
    content_encoding text,
    byte_count bigint NOT NULL CHECK (byte_count >= 0),
    retention_class source.raw_retention_class NOT NULL,
    expires_at timestamptz,
    purged_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((purged_at IS NULL AND (inline_body IS NOT NULL)::integer + (object_key IS NOT NULL)::integer = 1)
        OR (purged_at IS NOT NULL AND inline_body IS NULL AND object_key IS NULL)),
    CHECK (purged_at IS NULL OR purged_at >= created_at),
    CHECK (retention_class = 'contract_sample' OR expires_at IS NOT NULL),
    CHECK (expires_at IS NULL OR expires_at > created_at)
);
COMMENT ON COLUMN source.provider_raw_payloads.expires_at IS 'Required purge deadline except for explicitly curated contract samples. Suggested policy: standard 30 days, anomaly 90 days, prediction_input through the season audit window; purge removes body/object but retains fetch metadata and hashes.';

CREATE TABLE football.season_teams (
    season_id bigint NOT NULL REFERENCES football.seasons(id) ON DELETE RESTRICT,
    team_id bigint NOT NULL REFERENCES football.teams(id) ON DELETE RESTRICT,
    default_venue_id bigint REFERENCES football.venues(id) ON DELETE RESTRICT,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_source_fetch_id bigint REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    PRIMARY KEY (season_id, team_id), CHECK (last_seen_at >= first_seen_at)
);

CREATE TABLE football.fixtures (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    season_id bigint NOT NULL REFERENCES football.seasons(id) ON DELETE RESTRICT,
    home_team_id bigint NOT NULL REFERENCES football.teams(id) ON DELETE RESTRICT,
    away_team_id bigint NOT NULL REFERENCES football.teams(id) ON DELETE RESTRICT,
    venue_id bigint REFERENCES football.venues(id) ON DELETE RESTRICT,
    round_label text,
    kickoff_at timestamptz NOT NULL,
    source_timezone text,
    referee_name text,
    lifecycle_state football.fixture_lifecycle_state NOT NULL DEFAULT 'scheduled',
    home_goals smallint, away_goals smallint,
    home_halftime_goals smallint, away_halftime_goals smallint,
    home_fulltime_goals smallint, away_fulltime_goals smallint,
    home_extratime_goals smallint, away_extratime_goals smallint,
    home_penalty_goals smallint, away_penalty_goals smallint,
    terminal_status_observed_at timestamptz,
    result_available_at timestamptz,
    availability_basis football.availability_basis NOT NULL DEFAULT 'observed',
    result_finalized_at timestamptz,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_source_fetch_id bigint REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (season_id, home_team_id) REFERENCES football.season_teams(season_id, team_id) ON DELETE RESTRICT,
    FOREIGN KEY (season_id, away_team_id) REFERENCES football.season_teams(season_id, team_id) ON DELETE RESTRICT,
    CHECK (home_team_id <> away_team_id),
    CHECK (last_seen_at >= first_seen_at),
    CHECK (home_goals IS NULL OR home_goals >= 0), CHECK (away_goals IS NULL OR away_goals >= 0),
    CHECK (home_halftime_goals IS NULL OR home_halftime_goals >= 0), CHECK (away_halftime_goals IS NULL OR away_halftime_goals >= 0),
    CHECK (home_fulltime_goals IS NULL OR home_fulltime_goals >= 0), CHECK (away_fulltime_goals IS NULL OR away_fulltime_goals >= 0),
    CHECK (home_extratime_goals IS NULL OR home_extratime_goals >= 0), CHECK (away_extratime_goals IS NULL OR away_extratime_goals >= 0),
    CHECK (home_penalty_goals IS NULL OR home_penalty_goals >= 0), CHECK (away_penalty_goals IS NULL OR away_penalty_goals >= 0),
    CHECK ((lifecycle_state IN ('completed', 'cancelled', 'abandoned')) = (terminal_status_observed_at IS NOT NULL)),
    CHECK (lifecycle_state <> 'completed' OR (home_goals IS NOT NULL AND away_goals IS NOT NULL AND result_available_at IS NOT NULL)),
    CHECK (lifecycle_state <> 'completed' OR (terminal_status_observed_at >= kickoff_at AND result_available_at >= kickoff_at AND result_available_at >= terminal_status_observed_at)),
    CHECK (lifecycle_state = 'completed' OR (home_goals IS NULL AND away_goals IS NULL AND home_halftime_goals IS NULL AND away_halftime_goals IS NULL AND home_fulltime_goals IS NULL AND away_fulltime_goals IS NULL AND home_extratime_goals IS NULL AND away_extratime_goals IS NULL AND home_penalty_goals IS NULL AND away_penalty_goals IS NULL AND result_available_at IS NULL)),
    CHECK (result_finalized_at IS NULL OR (lifecycle_state = 'completed' AND result_finalized_at >= result_available_at))
);

CREATE TABLE source.fixture_provider_refs (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    external_id text NOT NULL CHECK (external_id <> ''),
    fixture_id bigint NOT NULL REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(), last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, external_id), UNIQUE (provider_id, fixture_id), CHECK (last_seen_at >= first_seen_at)
);

-- Typed request-subject bindings are relational provenance. Several API-Football
-- responses omit the requested fixture/season from their body, so JSON params alone
-- are not sufficient evidence for normalized rows.
ALTER TABLE source.provider_fetches
    ADD COLUMN subject_fixture_id bigint,
    ADD COLUMN subject_season_id bigint,
    ADD COLUMN subject_team_id bigint,
    ADD CONSTRAINT provider_fetches_subject_fixture_fk FOREIGN KEY (subject_fixture_id) REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    ADD CONSTRAINT provider_fetches_subject_season_fk FOREIGN KEY (subject_season_id) REFERENCES football.seasons(id) ON DELETE RESTRICT,
    ADD CONSTRAINT provider_fetches_subject_team_fk FOREIGN KEY (subject_team_id) REFERENCES football.teams(id) ON DELETE RESTRICT;
CREATE INDEX provider_fetches_fixture_subject_idx ON source.provider_fetches (subject_fixture_id, response_received_at DESC) WHERE subject_fixture_id IS NOT NULL;
CREATE INDEX provider_fetches_season_subject_idx ON source.provider_fetches (subject_season_id, response_received_at DESC) WHERE subject_season_id IS NOT NULL;
CREATE INDEX provider_fetches_team_subject_idx ON source.provider_fetches (subject_team_id, response_received_at DESC) WHERE subject_team_id IS NOT NULL;

CREATE TABLE football.fixture_team_statistics (
    fixture_id bigint NOT NULL REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    team_id bigint NOT NULL REFERENCES football.teams(id) ON DELETE RESTRICT,
    shots_on_goal integer, shots_off_goal integer, total_shots integer, blocked_shots integer,
    shots_inside_box integer, shots_outside_box integer, fouls integer, corner_kicks integer,
    offsides integer, yellow_cards integer, red_cards integer, goalkeeper_saves integer,
    total_passes integer, passes_accurate integer,
    possession_pct numeric(5,2), pass_accuracy_pct numeric(5,2), expected_goals numeric(8,3),
    goals_prevented numeric(8,3),
    extra_metrics jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(extra_metrics) = 'object'),
    mapping_version text NOT NULL,
    observed_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    availability_basis football.availability_basis NOT NULL,
    last_source_fetch_id bigint REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    finalized_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (fixture_id, team_id),
    CHECK (available_at >= observed_at),
    CHECK (possession_pct IS NULL OR possession_pct BETWEEN 0 AND 100),
    CHECK (pass_accuracy_pct IS NULL OR pass_accuracy_pct BETWEEN 0 AND 100),
    CHECK (shots_on_goal IS NULL OR shots_on_goal >= 0), CHECK (shots_off_goal IS NULL OR shots_off_goal >= 0),
    CHECK (total_shots IS NULL OR total_shots >= 0), CHECK (blocked_shots IS NULL OR blocked_shots >= 0),
    CHECK (shots_inside_box IS NULL OR shots_inside_box >= 0), CHECK (shots_outside_box IS NULL OR shots_outside_box >= 0),
    CHECK (fouls IS NULL OR fouls >= 0), CHECK (corner_kicks IS NULL OR corner_kicks >= 0),
    CHECK (offsides IS NULL OR offsides >= 0), CHECK (yellow_cards IS NULL OR yellow_cards >= 0),
    CHECK (red_cards IS NULL OR red_cards >= 0), CHECK (goalkeeper_saves IS NULL OR goalkeeper_saves >= 0),
    CHECK (total_passes IS NULL OR total_passes >= 0), CHECK (passes_accurate IS NULL OR passes_accurate >= 0),
    CHECK (expected_goals IS NULL OR expected_goals >= 0),
    CHECK (passes_accurate IS NULL OR total_passes IS NULL OR passes_accurate <= total_passes)
);

CREATE TABLE football.fixture_availability_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fixture_id bigint NOT NULL REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    captured_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    availability_basis football.availability_basis NOT NULL DEFAULT 'observed',
    source_fetch_id bigint NOT NULL REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    coverage_state football.snapshot_coverage_state NOT NULL,
    record_count integer NOT NULL CHECK (record_count >= 0),
    ingest_txid bigint NOT NULL DEFAULT txid_current(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (fixture_id, source_fetch_id),
    CHECK (availability_basis = 'observed'),
    CHECK (available_at = captured_at)
);
COMMENT ON TABLE football.fixture_availability_snapshots IS 'Append-only actual pre-kickoff observations. Retrospectively fetched historical availability must not be inserted here as ML-safe input.';

CREATE TABLE football.fixture_player_availability (
    snapshot_id bigint NOT NULL REFERENCES football.fixture_availability_snapshots(id) ON DELETE RESTRICT,
    team_id bigint NOT NULL REFERENCES football.teams(id) ON DELETE RESTRICT,
    player_id bigint NOT NULL REFERENCES football.players(id) ON DELETE RESTRICT,
    availability_kind text NOT NULL CHECK (availability_kind <> ''),
    provider_type text,
    reason text,
    PRIMARY KEY (snapshot_id, team_id, player_id)
);

CREATE TABLE football.fixture_lineup_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fixture_id bigint NOT NULL REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    captured_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    availability_basis football.availability_basis NOT NULL DEFAULT 'observed',
    source_fetch_id bigint NOT NULL REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    coverage_state football.snapshot_coverage_state NOT NULL,
    team_count smallint NOT NULL CHECK (team_count BETWEEN 0 AND 2),
    ingest_txid bigint NOT NULL DEFAULT txid_current(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (fixture_id, source_fetch_id),
    CHECK (availability_basis = 'observed'),
    CHECK (available_at = captured_at)
);
COMMENT ON TABLE football.fixture_lineup_snapshots IS 'Append-only actual pre-kickoff observations. Retrospectively fetched historical lineups must not be inserted here as ML-safe input.';

CREATE TABLE football.fixture_lineups (
    snapshot_id bigint NOT NULL REFERENCES football.fixture_lineup_snapshots(id) ON DELETE RESTRICT,
    team_id bigint NOT NULL REFERENCES football.teams(id) ON DELETE RESTRICT,
    coach_id bigint REFERENCES football.coaches(id) ON DELETE RESTRICT,
    formation text,
    player_count smallint NOT NULL DEFAULT 0 CHECK (player_count >= 0),
    PRIMARY KEY (snapshot_id, team_id)
);
CREATE TABLE football.fixture_lineup_players (
    snapshot_id bigint NOT NULL,
    team_id bigint NOT NULL,
    player_id bigint NOT NULL REFERENCES football.players(id) ON DELETE RESTRICT,
    lineup_role football.lineup_role NOT NULL,
    position text,
    shirt_number smallint CHECK (shirt_number IS NULL OR shirt_number BETWEEN 0 AND 199),
    grid text,
    provider_order smallint CHECK (provider_order IS NULL OR provider_order >= 0),
    PRIMARY KEY (snapshot_id, team_id, player_id),
    FOREIGN KEY (snapshot_id, team_id) REFERENCES football.fixture_lineups(snapshot_id, team_id) ON DELETE RESTRICT
);

CREATE TABLE football.standings_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    season_id bigint NOT NULL REFERENCES football.seasons(id) ON DELETE RESTRICT,
    captured_at timestamptz NOT NULL,
    source_fetch_id bigint NOT NULL REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    group_count smallint NOT NULL CHECK (group_count >= 0),
    ingest_txid bigint NOT NULL DEFAULT txid_current(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (season_id, source_fetch_id)
);
CREATE TABLE football.standings_snapshot_groups (
    snapshot_id bigint NOT NULL REFERENCES football.standings_snapshots(id) ON DELETE RESTRICT,
    group_index smallint NOT NULL CHECK (group_index >= 0),
    group_name text,
    row_count smallint NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    PRIMARY KEY (snapshot_id, group_index)
);
CREATE TABLE football.standings_snapshot_rows (
    snapshot_id bigint NOT NULL,
    group_index smallint NOT NULL,
    team_id bigint NOT NULL REFERENCES football.teams(id) ON DELETE RESTRICT,
    rank smallint NOT NULL CHECK (rank > 0),
    points smallint NOT NULL CHECK (points >= 0),
    goals_diff smallint NOT NULL,
    form text,
    status text,
    description text,
    played integer NOT NULL CHECK (played >= 0), wins integer NOT NULL CHECK (wins >= 0), draws integer NOT NULL CHECK (draws >= 0), losses integer NOT NULL CHECK (losses >= 0),
    goals_for integer NOT NULL CHECK (goals_for >= 0), goals_against integer NOT NULL CHECK (goals_against >= 0),
    home_played integer NOT NULL CHECK (home_played >= 0), home_wins integer NOT NULL CHECK (home_wins >= 0), home_draws integer NOT NULL CHECK (home_draws >= 0), home_losses integer NOT NULL CHECK (home_losses >= 0), home_goals_for integer NOT NULL CHECK (home_goals_for >= 0), home_goals_against integer NOT NULL CHECK (home_goals_against >= 0),
    away_played integer NOT NULL CHECK (away_played >= 0), away_wins integer NOT NULL CHECK (away_wins >= 0), away_draws integer NOT NULL CHECK (away_draws >= 0), away_losses integer NOT NULL CHECK (away_losses >= 0), away_goals_for integer NOT NULL CHECK (away_goals_for >= 0), away_goals_against integer NOT NULL CHECK (away_goals_against >= 0),
    provider_updated_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (snapshot_id, group_index, team_id),
    FOREIGN KEY (snapshot_id, group_index) REFERENCES football.standings_snapshot_groups(snapshot_id, group_index) ON DELETE RESTRICT,
    CHECK (wins + draws + losses = played),
    CHECK (home_wins + home_draws + home_losses = home_played),
    CHECK (away_wins + away_draws + away_losses = away_played),
    CHECK (home_played + away_played = played)
);
CREATE UNIQUE INDEX standings_snapshot_rows_rank_uniq ON football.standings_snapshot_rows (snapshot_id, group_index, rank);

CREATE TABLE ml.model_versions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_name text NOT NULL,
    model_version text NOT NULL,
    feature_schema_version text NOT NULL,
    training_data_cutoff_at timestamptz,
    artifact_uri text,
    artifact_sha256 bytea CHECK (artifact_sha256 IS NULL OR octet_length(artifact_sha256) = 32),
    status ml.model_status NOT NULL DEFAULT 'draft',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz,
    UNIQUE (model_name, model_version),
    CHECK (retired_at IS NULL OR retired_at >= created_at)
);

CREATE TABLE ml.predictions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fixture_id bigint NOT NULL REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    model_version_id bigint NOT NULL REFERENCES ml.model_versions(id) ON DELETE RESTRICT,
    supersedes_prediction_id bigint REFERENCES ml.predictions(id) ON DELETE RESTRICT,
    calculated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    feature_cutoff_at timestamptz NOT NULL,
    target_kickoff_at timestamptz NOT NULL,
    home_probability numeric(9,8) NOT NULL,
    draw_probability numeric(9,8) NOT NULL,
    away_probability numeric(9,8) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (fixture_id, model_version_id, calculated_at),
    CHECK (home_probability BETWEEN 0 AND 1), CHECK (draw_probability BETWEEN 0 AND 1), CHECK (away_probability BETWEEN 0 AND 1),
    CHECK (abs((home_probability + draw_probability + away_probability) - 1.00000000) <= 0.00000100),
    CHECK (feature_cutoff_at <= calculated_at),
    CHECK (calculated_at < target_kickoff_at)
);
CREATE TABLE ml.prediction_feature_snapshots (
    prediction_id bigint PRIMARY KEY REFERENCES ml.predictions(id) ON DELETE RESTRICT,
    feature_schema_version text NOT NULL,
    features jsonb NOT NULL CHECK (jsonb_typeof(features) = 'object'),
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(provenance) = 'object'),
    max_source_available_at timestamptz NOT NULL,
    max_source_fixture_kickoff_at timestamptz,
    standings_snapshot_id bigint REFERENCES football.standings_snapshots(id) ON DELETE RESTRICT,
    availability_snapshot_id bigint REFERENCES football.fixture_availability_snapshots(id) ON DELETE RESTRICT,
    lineup_snapshot_id bigint REFERENCES football.fixture_lineup_snapshots(id) ON DELETE RESTRICT,
    input_sha256 bytea NOT NULL CHECK (octet_length(input_sha256) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE ml.prediction_fixture_inputs (
    prediction_id bigint NOT NULL REFERENCES ml.predictions(id) ON DELETE RESTRICT,
    source_fixture_id bigint NOT NULL REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    source_terminal_observed_at timestamptz NOT NULL,
    source_statistics_available_at timestamptz,
    fact_sha256 bytea NOT NULL CHECK (octet_length(fact_sha256) = 32),
    PRIMARY KEY (prediction_id, source_fixture_id)
);

CREATE TABLE ops.fixture_reconciliation_state (
    fixture_id bigint PRIMARY KEY REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    eligible_at timestamptz NOT NULL,
    next_attempt_at timestamptz NOT NULL,
    last_attempt_at timestamptz,
    attempt_count smallint NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 8),
    max_attempts smallint NOT NULL DEFAULT 4 CHECK (max_attempts BETWEEN 1 AND 8),
    state ops.reconciliation_state NOT NULL DEFAULT 'waiting',
    last_source_fetch_id bigint REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    terminal_observed_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (eligible_at <= next_attempt_at),
    CHECK (last_attempt_at IS NULL OR last_attempt_at >= eligible_at),
    CHECK (attempt_count <= max_attempts),
    CHECK ((state = 'completed') = (completed_at IS NOT NULL)),
    CHECK (state <> 'exhausted' OR attempt_count = max_attempts)
);
CREATE INDEX fixture_reconciliation_due_idx ON ops.fixture_reconciliation_state (next_attempt_at) WHERE state IN ('waiting', 'pending');

CREATE INDEX provider_fetches_audit_idx ON source.provider_fetches (provider_id, response_received_at DESC);
CREATE INDEX fixture_provider_refs_fixture_idx ON source.fixture_provider_refs (fixture_id);
CREATE INDEX fixtures_season_kickoff_idx ON football.fixtures (season_id, kickoff_at);
CREATE INDEX fixtures_home_kickoff_idx ON football.fixtures (home_team_id, kickoff_at DESC);
CREATE INDEX fixtures_away_kickoff_idx ON football.fixtures (away_team_id, kickoff_at DESC);
CREATE INDEX fixtures_scheduled_kickoff_idx ON football.fixtures (kickoff_at) WHERE lifecycle_state = 'scheduled';
CREATE INDEX fixtures_completed_kickoff_idx ON football.fixtures (season_id, kickoff_at DESC) WHERE lifecycle_state = 'completed';
CREATE INDEX fixture_statistics_team_idx ON football.fixture_team_statistics (team_id, fixture_id);
CREATE INDEX availability_snapshots_fixture_idx ON football.fixture_availability_snapshots (fixture_id, available_at DESC);
CREATE INDEX lineup_snapshots_fixture_idx ON football.fixture_lineup_snapshots (fixture_id, available_at DESC);
CREATE INDEX standings_snapshots_season_capture_idx ON football.standings_snapshots (season_id, captured_at DESC);
CREATE INDEX standings_rows_team_idx ON football.standings_snapshot_rows (team_id, snapshot_id);
CREATE INDEX predictions_fixture_idx ON ml.predictions (fixture_id, calculated_at DESC);
CREATE INDEX predictions_model_idx ON ml.predictions (model_version_id, calculated_at DESC);

-- Timestamp/default updates are deliberately explicit: no live-state update path exists.
CREATE OR REPLACE FUNCTION football.touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := clock_timestamp(); RETURN NEW; END $$;
CREATE TRIGGER leagues_touch_updated_at BEFORE UPDATE ON football.leagues FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();
CREATE TRIGGER teams_touch_updated_at BEFORE UPDATE ON football.teams FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();
CREATE TRIGGER venues_touch_updated_at BEFORE UPDATE ON football.venues FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();
CREATE TRIGGER players_touch_updated_at BEFORE UPDATE ON football.players FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();
CREATE TRIGGER coaches_touch_updated_at BEFORE UPDATE ON football.coaches FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();
CREATE TRIGGER seasons_touch_updated_at BEFORE UPDATE ON football.seasons FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();
CREATE TRIGGER fixtures_touch_updated_at BEFORE UPDATE ON football.fixtures FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();
CREATE TRIGGER fixture_statistics_touch_updated_at BEFORE UPDATE ON football.fixture_team_statistics FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();
CREATE TRIGGER reconciliation_touch_updated_at BEFORE UPDATE ON ops.fixture_reconciliation_state FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();

CREATE OR REPLACE FUNCTION football.assert_fixture_participant(p_fixture_id bigint, p_team_id bigint) RETURNS void LANGUAGE plpgsql STABLE AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM football.fixtures f WHERE f.id = p_fixture_id AND p_team_id IN (f.home_team_id, f.away_team_id)) THEN
    RAISE EXCEPTION 'team % is not a participant in fixture %', p_team_id, p_fixture_id USING ERRCODE = '23514';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION football.guard_fixture_statistics() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE f football.fixtures%ROWTYPE; fetch_row source.provider_fetches%ROWTYPE;
BEGIN
  IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'fixture statistics cannot be deleted' USING ERRCODE = '55000'; END IF;
  PERFORM football.assert_fixture_participant(NEW.fixture_id, NEW.team_id);
  SELECT * INTO f FROM football.fixtures WHERE id = NEW.fixture_id;
  IF f.lifecycle_state <> 'completed' THEN RAISE EXCEPTION 'fixture statistics require a completed fixture' USING ERRCODE = '23514'; END IF;
  IF NEW.observed_at < f.kickoff_at OR NEW.available_at < f.kickoff_at THEN RAISE EXCEPTION 'fixture statistics cannot be available before kickoff' USING ERRCODE = '23514'; END IF;
  IF NEW.availability_basis = 'observed' THEN
    IF NEW.last_source_fetch_id IS NULL THEN RAISE EXCEPTION 'observed fixture statistics require source fetch provenance' USING ERRCODE = '23514'; END IF;
    SELECT * INTO fetch_row FROM source.provider_fetches WHERE id = NEW.last_source_fetch_id;
    IF fetch_row.outcome IS DISTINCT FROM 'success'::source.fetch_outcome
       OR fetch_row.endpoint IS DISTINCT FROM '/fixtures/statistics'
       OR fetch_row.subject_fixture_id IS DISTINCT FROM NEW.fixture_id
       OR fetch_row.response_received_at IS DISTINCT FROM NEW.available_at THEN
      RAISE EXCEPTION 'observed fixture statistics must match a successful statistics fetch' USING ERRCODE = '23514';
    END IF;
  ELSIF NEW.available_at < f.kickoff_at + interval '3 hours' THEN
    RAISE EXCEPTION 'conservative fixture statistics availability must use a post-match safety interval' USING ERRCODE = '23514';
  END IF;
  IF NEW.finalized_at IS NOT NULL AND NEW.finalized_at < NEW.observed_at THEN RAISE EXCEPTION 'statistics finalized_at precedes observed_at' USING ERRCODE = '23514'; END IF;
  IF TG_OP = 'UPDATE' AND OLD.finalized_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'finalized fixture statistics are immutable' USING ERRCODE = '55000'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER fixture_statistics_guard BEFORE INSERT OR UPDATE OR DELETE ON football.fixture_team_statistics FOR EACH ROW EXECUTE FUNCTION football.guard_fixture_statistics();

CREATE OR REPLACE FUNCTION football.guard_final_fixture() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE fetch_row source.provider_fetches%ROWTYPE;
BEGIN
  IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'fixtures cannot be deleted' USING ERRCODE = '55000'; END IF;
  IF TG_OP = 'UPDATE' AND OLD.result_finalized_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'finalized fixture result is immutable' USING ERRCODE = '55000';
  END IF;
  IF TG_OP = 'UPDATE' AND NEW.kickoff_at IS DISTINCT FROM OLD.kickoff_at AND
     (EXISTS (SELECT 1 FROM football.fixture_availability_snapshots s WHERE s.fixture_id = OLD.id AND s.available_at >= NEW.kickoff_at)
      OR EXISTS (SELECT 1 FROM football.fixture_lineup_snapshots s WHERE s.fixture_id = OLD.id AND s.available_at >= NEW.kickoff_at)) THEN
    RAISE EXCEPTION 'fixture kickoff cannot invalidate an existing pre-match snapshot' USING ERRCODE = '55000';
  END IF;
  IF NEW.lifecycle_state IN ('completed', 'cancelled', 'abandoned') AND NEW.availability_basis = 'observed' THEN
    IF NEW.last_source_fetch_id IS NULL THEN
      RAISE EXCEPTION 'observed terminal fixture state requires source fetch provenance' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO fetch_row FROM source.provider_fetches WHERE id = NEW.last_source_fetch_id;
    IF fetch_row.outcome IS DISTINCT FROM 'success'::source.fetch_outcome OR fetch_row.response_received_at IS NULL
       OR NOT (fetch_row.subject_fixture_id IS NOT DISTINCT FROM NEW.id OR fetch_row.subject_season_id IS NOT DISTINCT FROM NEW.season_id)
       OR NEW.terminal_status_observed_at IS DISTINCT FROM fetch_row.response_received_at
       OR (NEW.lifecycle_state = 'completed' AND NEW.result_available_at IS DISTINCT FROM fetch_row.response_received_at) THEN
      RAISE EXCEPTION 'observed terminal fixture timestamps must match a successful source response' USING ERRCODE = '23514';
    END IF;
  ELSIF NEW.lifecycle_state = 'completed' AND NEW.availability_basis = 'reconstructed_conservative'
        AND NEW.result_available_at < NEW.kickoff_at + interval '3 hours' THEN
    RAISE EXCEPTION 'conservative historical result availability must use a post-match safety interval' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER fixtures_finalization_guard BEFORE INSERT OR UPDATE OR DELETE ON football.fixtures FOR EACH ROW EXECUTE FUNCTION football.guard_final_fixture();

CREATE OR REPLACE FUNCTION football.guard_prematch_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE kickoff timestamptz; fixture_season_id bigint; fetch_row source.provider_fetches%ROWTYPE; expected_endpoint text;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN RAISE EXCEPTION 'pre-match snapshots are immutable' USING ERRCODE = '55000'; END IF;
  SELECT kickoff_at, season_id INTO kickoff, fixture_season_id FROM football.fixtures WHERE id = NEW.fixture_id FOR SHARE;
  SELECT * INTO fetch_row FROM source.provider_fetches WHERE id = NEW.source_fetch_id;
  expected_endpoint := CASE TG_TABLE_NAME WHEN 'fixture_availability_snapshots' THEN '/injuries' ELSE '/fixtures/lineups' END;
  IF kickoff IS NULL OR NEW.captured_at >= kickoff OR NEW.available_at >= kickoff OR clock_timestamp() >= kickoff THEN
    RAISE EXCEPTION 'pre-match snapshot must be captured and committed before kickoff' USING ERRCODE = '23514';
  END IF;
  IF fetch_row.response_received_at IS NULL OR NEW.captured_at <> fetch_row.response_received_at OR NEW.available_at <> fetch_row.response_received_at THEN
    RAISE EXCEPTION 'pre-match snapshot timestamps must equal provider response_received_at' USING ERRCODE = '23514';
  END IF;
  IF fetch_row.outcome IS DISTINCT FROM 'success'::source.fetch_outcome
     OR fetch_row.purpose IS DISTINCT FROM 'prematch'::source.fetch_purpose
     OR fetch_row.endpoint IS DISTINCT FROM expected_endpoint THEN
    RAISE EXCEPTION 'pre-match snapshot requires a successful purpose-specific provider fetch' USING ERRCODE = '23514';
  END IF;
  IF (TG_TABLE_NAME = 'fixture_lineup_snapshots' AND fetch_row.subject_fixture_id IS DISTINCT FROM NEW.fixture_id)
     OR (TG_TABLE_NAME = 'fixture_availability_snapshots'
         AND fetch_row.subject_fixture_id IS DISTINCT FROM NEW.fixture_id
         AND fetch_row.subject_season_id IS DISTINCT FROM fixture_season_id) THEN
    RAISE EXCEPTION 'pre-match snapshot source fetch is bound to a different fixture or season' USING ERRCODE = '23514';
  END IF;
  NEW.ingest_txid := txid_current();
  RETURN NEW;
END $$;
CREATE TRIGGER availability_snapshot_guard BEFORE INSERT OR UPDATE OR DELETE ON football.fixture_availability_snapshots FOR EACH ROW EXECUTE FUNCTION football.guard_prematch_snapshot();
CREATE TRIGGER lineup_snapshot_guard BEFORE INSERT OR UPDATE OR DELETE ON football.fixture_lineup_snapshots FOR EACH ROW EXECUTE FUNCTION football.guard_prematch_snapshot();

CREATE OR REPLACE FUNCTION football.guard_availability_participant() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE fixture_key bigint; kickoff timestamptz; header_txid bigint;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN RAISE EXCEPTION 'pre-match snapshot rows are immutable' USING ERRCODE = '55000'; END IF;
  SELECT fixture_id, ingest_txid INTO fixture_key, header_txid FROM football.fixture_availability_snapshots WHERE id = NEW.snapshot_id;
  IF header_txid <> txid_current() THEN RAISE EXCEPTION 'snapshot rows must be inserted in the snapshot transaction' USING ERRCODE = '55000'; END IF;
  SELECT kickoff_at INTO kickoff FROM football.fixtures WHERE id = fixture_key;
  IF clock_timestamp() >= kickoff THEN RAISE EXCEPTION 'pre-match snapshot rows cannot be added at or after kickoff' USING ERRCODE = '23514'; END IF;
  PERFORM football.assert_fixture_participant(fixture_key, NEW.team_id);
  RETURN NEW;
END $$;
CREATE TRIGGER availability_participant_guard BEFORE INSERT OR UPDATE OR DELETE ON football.fixture_player_availability FOR EACH ROW EXECUTE FUNCTION football.guard_availability_participant();

CREATE OR REPLACE FUNCTION football.guard_lineup_participant() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE fixture_key bigint; kickoff timestamptz; header_txid bigint;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN RAISE EXCEPTION 'pre-match lineup rows are immutable' USING ERRCODE = '55000'; END IF;
  SELECT fixture_id, ingest_txid INTO fixture_key, header_txid FROM football.fixture_lineup_snapshots WHERE id = NEW.snapshot_id;
  IF header_txid <> txid_current() THEN RAISE EXCEPTION 'lineup rows must be inserted in the snapshot transaction' USING ERRCODE = '55000'; END IF;
  SELECT kickoff_at INTO kickoff FROM football.fixtures WHERE id = fixture_key;
  IF clock_timestamp() >= kickoff THEN RAISE EXCEPTION 'pre-match lineup rows cannot be added at or after kickoff' USING ERRCODE = '23514'; END IF;
  PERFORM football.assert_fixture_participant(fixture_key, NEW.team_id);
  RETURN NEW;
END $$;
CREATE TRIGGER lineups_participant_guard BEFORE INSERT OR UPDATE OR DELETE ON football.fixture_lineups FOR EACH ROW EXECUTE FUNCTION football.guard_lineup_participant();
CREATE TRIGGER lineup_players_guard BEFORE INSERT OR UPDATE OR DELETE ON football.fixture_lineup_players FOR EACH ROW EXECUTE FUNCTION football.guard_lineup_participant();

CREATE OR REPLACE FUNCTION football.guard_standings_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE fetch_row source.provider_fetches%ROWTYPE;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN RAISE EXCEPTION 'standings snapshots are immutable' USING ERRCODE = '55000'; END IF;
  SELECT * INTO fetch_row FROM source.provider_fetches WHERE id = NEW.source_fetch_id;
  IF fetch_row.response_received_at IS NULL OR NEW.captured_at <> fetch_row.response_received_at THEN RAISE EXCEPTION 'standings snapshot captured_at must equal provider response_received_at' USING ERRCODE = '23514'; END IF;
  IF fetch_row.outcome IS DISTINCT FROM 'success'::source.fetch_outcome OR fetch_row.endpoint IS DISTINCT FROM '/standings'
     OR fetch_row.subject_season_id IS DISTINCT FROM NEW.season_id
     OR fetch_row.purpose NOT IN ('bootstrap', 'scheduled_refresh', 'prematch', 'research') THEN
    RAISE EXCEPTION 'standings snapshot requires a successful standings fetch' USING ERRCODE = '23514';
  END IF;
  NEW.ingest_txid := txid_current();
  RETURN NEW;
END $$;
CREATE TRIGGER standings_snapshots_guard BEFORE INSERT OR UPDATE OR DELETE ON football.standings_snapshots FOR EACH ROW EXECUTE FUNCTION football.guard_standings_snapshot();
CREATE OR REPLACE FUNCTION football.guard_standings_snapshot_children() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE header_txid bigint; snapshot_season_id bigint;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN RAISE EXCEPTION 'standings snapshot rows are immutable' USING ERRCODE = '55000'; END IF;
  SELECT ingest_txid, season_id INTO header_txid, snapshot_season_id FROM football.standings_snapshots WHERE id = NEW.snapshot_id;
  IF header_txid <> txid_current() THEN RAISE EXCEPTION 'standings rows must be inserted in the snapshot transaction' USING ERRCODE = '55000'; END IF;
  IF TG_TABLE_NAME = 'standings_snapshot_rows' AND NOT EXISTS (
    SELECT 1 FROM football.season_teams st WHERE st.season_id = snapshot_season_id AND st.team_id = NEW.team_id
  ) THEN
    RAISE EXCEPTION 'standings row team does not belong to snapshot season' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER standings_groups_guard BEFORE INSERT OR UPDATE OR DELETE ON football.standings_snapshot_groups FOR EACH ROW EXECUTE FUNCTION football.guard_standings_snapshot_children();
CREATE TRIGGER standings_rows_guard BEFORE INSERT OR UPDATE OR DELETE ON football.standings_snapshot_rows FOR EACH ROW EXECUTE FUNCTION football.guard_standings_snapshot_children();

CREATE OR REPLACE FUNCTION football.assert_availability_snapshot_commit_valid() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE kickoff timestamptz;
BEGIN
  SELECT kickoff_at INTO kickoff FROM football.fixtures WHERE id = NEW.fixture_id;
  IF clock_timestamp() >= kickoff THEN RAISE EXCEPTION 'availability snapshot committed at or after kickoff' USING ERRCODE = '23514'; END IF;
  IF (SELECT count(*) FROM football.fixture_player_availability WHERE snapshot_id = NEW.id) <> NEW.record_count THEN RAISE EXCEPTION 'availability snapshot record_count does not match rows' USING ERRCODE = '23514'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER availability_snapshot_commit_guard AFTER INSERT ON football.fixture_availability_snapshots DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION football.assert_availability_snapshot_commit_valid();

CREATE OR REPLACE FUNCTION football.assert_lineup_snapshot_commit_valid() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE kickoff timestamptz;
BEGIN
  SELECT kickoff_at INTO kickoff FROM football.fixtures WHERE id = NEW.fixture_id;
  IF clock_timestamp() >= kickoff THEN RAISE EXCEPTION 'lineup snapshot committed at or after kickoff' USING ERRCODE = '23514'; END IF;
  IF (SELECT count(*) FROM football.fixture_lineups WHERE snapshot_id = NEW.id) <> NEW.team_count THEN RAISE EXCEPTION 'lineup snapshot team_count does not match rows' USING ERRCODE = '23514'; END IF;
  IF EXISTS (SELECT 1 FROM football.fixture_lineups l WHERE l.snapshot_id = NEW.id AND l.player_count <> (SELECT count(*) FROM football.fixture_lineup_players p WHERE p.snapshot_id = l.snapshot_id AND p.team_id = l.team_id)) THEN RAISE EXCEPTION 'lineup player_count does not match rows' USING ERRCODE = '23514'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER lineup_snapshot_commit_guard AFTER INSERT ON football.fixture_lineup_snapshots DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION football.assert_lineup_snapshot_commit_valid();

CREATE OR REPLACE FUNCTION football.assert_standings_snapshot_commit_valid() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (SELECT count(*) FROM football.standings_snapshot_groups WHERE snapshot_id = NEW.id) <> NEW.group_count THEN RAISE EXCEPTION 'standings snapshot group_count does not match rows' USING ERRCODE = '23514'; END IF;
  IF EXISTS (SELECT 1 FROM football.standings_snapshot_groups g WHERE g.snapshot_id = NEW.id AND g.row_count <> (SELECT count(*) FROM football.standings_snapshot_rows r WHERE r.snapshot_id = g.snapshot_id AND r.group_index = g.group_index)) THEN RAISE EXCEPTION 'standings group row_count does not match rows' USING ERRCODE = '23514'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER standings_snapshot_commit_guard AFTER INSERT ON football.standings_snapshots DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION football.assert_standings_snapshot_commit_valid();

CREATE OR REPLACE FUNCTION ml.guard_prediction_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE kickoff timestamptz; training_cutoff timestamptz; superseded ml.predictions%ROWTYPE;
BEGIN
  SELECT kickoff_at INTO kickoff FROM football.fixtures WHERE id = NEW.fixture_id FOR SHARE;
  SELECT training_data_cutoff_at INTO training_cutoff FROM ml.model_versions WHERE id = NEW.model_version_id;
  IF kickoff IS NULL OR clock_timestamp() >= kickoff THEN RAISE EXCEPTION 'prediction creation is forbidden at or after kickoff' USING ERRCODE = '23514'; END IF;
  IF NEW.target_kickoff_at <> kickoff THEN RAISE EXCEPTION 'prediction target_kickoff_at must match fixture kickoff_at' USING ERRCODE = '23514'; END IF;
  IF NEW.calculated_at > clock_timestamp() THEN RAISE EXCEPTION 'prediction calculated_at cannot be future-dated' USING ERRCODE = '23514'; END IF;
  IF NEW.feature_cutoff_at >= kickoff THEN RAISE EXCEPTION 'prediction feature cutoff must precede kickoff' USING ERRCODE = '23514'; END IF;
  IF training_cutoff IS NOT NULL AND training_cutoff > NEW.feature_cutoff_at THEN RAISE EXCEPTION 'model training cutoff exceeds prediction feature cutoff' USING ERRCODE = '23514'; END IF;
  IF NEW.supersedes_prediction_id IS NOT NULL THEN
    SELECT * INTO superseded FROM ml.predictions WHERE id = NEW.supersedes_prediction_id;
    IF superseded.fixture_id <> NEW.fixture_id OR superseded.model_version_id <> NEW.model_version_id OR superseded.calculated_at >= NEW.calculated_at THEN
      RAISE EXCEPTION 'superseded prediction must be an earlier prediction for the same fixture and model' USING ERRCODE = '23514';
    END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION ml.guard_prediction_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'predictions are immutable' USING ERRCODE = '55000'; END $$;
CREATE TRIGGER predictions_insert_guard BEFORE INSERT ON ml.predictions FOR EACH ROW EXECUTE FUNCTION ml.guard_prediction_insert();
CREATE TRIGGER predictions_immutable_guard BEFORE UPDATE OR DELETE ON ml.predictions FOR EACH ROW EXECUTE FUNCTION ml.guard_prediction_immutable();

CREATE OR REPLACE FUNCTION ml.guard_feature_snapshot_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  p ml.predictions%ROWTYPE;
  expected_schema text;
  snapshot_fixture bigint;
  snapshot_season bigint;
  snapshot_available_at timestamptz;
  known_max_available_at timestamptz := '-infinity'::timestamptz;
  actual_max_input_kickoff_at timestamptz;
BEGIN
  SELECT * INTO p FROM ml.predictions WHERE id = NEW.prediction_id;
  IF p.id IS NULL THEN RAISE EXCEPTION 'feature snapshot requires prediction' USING ERRCODE = '23503'; END IF;
  SELECT feature_schema_version INTO expected_schema FROM ml.model_versions WHERE id = p.model_version_id;
  IF NEW.feature_schema_version <> expected_schema THEN RAISE EXCEPTION 'feature snapshot schema must match model version' USING ERRCODE = '23514'; END IF;
  IF NEW.max_source_available_at > p.feature_cutoff_at THEN RAISE EXCEPTION 'feature snapshot exceeds prediction cutoff' USING ERRCODE = '23514'; END IF;
  IF NEW.max_source_fixture_kickoff_at IS NOT NULL AND NEW.max_source_fixture_kickoff_at >= p.target_kickoff_at THEN RAISE EXCEPTION 'feature snapshot contains target/future fixture input' USING ERRCODE = '23514'; END IF;
  IF NEW.availability_snapshot_id IS NOT NULL THEN
    SELECT s.fixture_id, s.available_at INTO snapshot_fixture, snapshot_available_at FROM football.fixture_availability_snapshots s WHERE s.id = NEW.availability_snapshot_id;
    IF snapshot_fixture <> p.fixture_id OR snapshot_available_at > p.feature_cutoff_at THEN RAISE EXCEPTION 'availability snapshot is not valid for prediction cutoff' USING ERRCODE = '23514'; END IF;
    known_max_available_at := greatest(known_max_available_at, snapshot_available_at);
  END IF;
  IF NEW.lineup_snapshot_id IS NOT NULL THEN
    SELECT s.fixture_id, s.available_at INTO snapshot_fixture, snapshot_available_at FROM football.fixture_lineup_snapshots s WHERE s.id = NEW.lineup_snapshot_id;
    IF snapshot_fixture <> p.fixture_id OR snapshot_available_at > p.feature_cutoff_at THEN RAISE EXCEPTION 'lineup snapshot is not valid for prediction cutoff' USING ERRCODE = '23514'; END IF;
    known_max_available_at := greatest(known_max_available_at, snapshot_available_at);
  END IF;
  IF NEW.standings_snapshot_id IS NOT NULL THEN
    SELECT s.season_id, s.captured_at INTO snapshot_season, snapshot_available_at FROM football.standings_snapshots s WHERE s.id = NEW.standings_snapshot_id;
    IF snapshot_season <> (SELECT season_id FROM football.fixtures WHERE id = p.fixture_id) OR snapshot_available_at > p.feature_cutoff_at THEN RAISE EXCEPTION 'standings snapshot is not valid for prediction cutoff' USING ERRCODE = '23514'; END IF;
    known_max_available_at := greatest(known_max_available_at, snapshot_available_at);
  END IF;
  SELECT max(greatest(f.result_available_at, i.source_terminal_observed_at, i.source_statistics_available_at)), max(f.kickoff_at)
  INTO snapshot_available_at, actual_max_input_kickoff_at
  FROM ml.prediction_fixture_inputs i JOIN football.fixtures f ON f.id = i.source_fixture_id
  WHERE i.prediction_id = p.id;
  IF snapshot_available_at IS NOT NULL THEN known_max_available_at := greatest(known_max_available_at, snapshot_available_at); END IF;
  IF known_max_available_at <> '-infinity'::timestamptz AND NEW.max_source_available_at < known_max_available_at THEN
    RAISE EXCEPTION 'feature snapshot max_source_available_at understates known source availability' USING ERRCODE = '23514';
  END IF;
  IF actual_max_input_kickoff_at IS NOT NULL AND NEW.max_source_fixture_kickoff_at IS DISTINCT FROM actual_max_input_kickoff_at THEN
    RAISE EXCEPTION 'feature snapshot max_source_fixture_kickoff_at must match fixture input lineage' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER feature_snapshots_insert_guard BEFORE INSERT ON ml.prediction_feature_snapshots FOR EACH ROW EXECUTE FUNCTION ml.guard_feature_snapshot_insert();
CREATE TRIGGER feature_snapshots_immutable_guard BEFORE UPDATE OR DELETE ON ml.prediction_feature_snapshots FOR EACH ROW EXECUTE FUNCTION ml.guard_prediction_immutable();

CREATE OR REPLACE FUNCTION ml.guard_prediction_fixture_input() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE p ml.predictions%ROWTYPE; f football.fixtures%ROWTYPE; actual_statistics_available_at timestamptz;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN RAISE EXCEPTION 'prediction inputs are immutable' USING ERRCODE = '55000'; END IF;
  SELECT * INTO p FROM ml.predictions WHERE id = NEW.prediction_id;
  SELECT * INTO f FROM football.fixtures WHERE id = NEW.source_fixture_id;
  SELECT max(s.available_at) INTO actual_statistics_available_at
  FROM football.fixture_team_statistics s WHERE s.fixture_id = NEW.source_fixture_id;
  IF clock_timestamp() >= p.target_kickoff_at
     OR EXISTS (SELECT 1 FROM ml.prediction_feature_snapshots s WHERE s.prediction_id = p.id)
     OR NEW.source_fixture_id = p.fixture_id
     OR f.lifecycle_state <> 'completed'
     OR f.terminal_status_observed_at IS NULL
     OR f.result_available_at IS NULL
     OR f.kickoff_at >= p.feature_cutoff_at
     OR NEW.source_terminal_observed_at IS DISTINCT FROM f.terminal_status_observed_at
     OR (NEW.source_statistics_available_at IS NOT NULL AND NEW.source_statistics_available_at IS DISTINCT FROM actual_statistics_available_at)
     OR f.terminal_status_observed_at > p.feature_cutoff_at
     OR f.result_available_at > p.feature_cutoff_at
     OR (NEW.source_statistics_available_at IS NOT NULL AND actual_statistics_available_at > p.feature_cutoff_at) THEN
    RAISE EXCEPTION 'prediction input violates temporal lineage' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER prediction_inputs_guard BEFORE INSERT OR UPDATE OR DELETE ON ml.prediction_fixture_inputs FOR EACH ROW EXECUTE FUNCTION ml.guard_prediction_fixture_input();

-- Deferred checks use clock_timestamp(), not transaction_timestamp(), so a transaction
-- opened pre-kickoff cannot successfully commit after kickoff.
CREATE OR REPLACE FUNCTION ml.assert_prediction_commit_valid() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE p ml.predictions%ROWTYPE;
BEGIN
  SELECT * INTO p FROM ml.predictions WHERE id = NEW.id;
  IF clock_timestamp() >= p.target_kickoff_at THEN RAISE EXCEPTION 'prediction transaction committed at or after kickoff' USING ERRCODE = '23514'; END IF;
  IF NOT EXISTS (SELECT 1 FROM ml.prediction_feature_snapshots s WHERE s.prediction_id = p.id) THEN RAISE EXCEPTION 'prediction requires an immutable feature snapshot in the same transaction' USING ERRCODE = '23514'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER predictions_commit_guard AFTER INSERT ON ml.predictions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ml.assert_prediction_commit_valid();

CREATE OR REPLACE FUNCTION ml.assert_feature_snapshot_commit_valid() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE p ml.predictions%ROWTYPE;
BEGIN
  SELECT * INTO p FROM ml.predictions WHERE id = NEW.prediction_id;
  IF clock_timestamp() >= p.target_kickoff_at THEN RAISE EXCEPTION 'feature snapshot transaction committed at or after kickoff' USING ERRCODE = '23514'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER feature_snapshots_commit_guard AFTER INSERT ON ml.prediction_feature_snapshots DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ml.assert_feature_snapshot_commit_valid();

CREATE OR REPLACE FUNCTION source.jsonb_contains_forbidden_metadata_key(candidate jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE STRICT
SET search_path = pg_catalog, source AS $$
DECLARE key_name text; child jsonb; normalized_key text;
BEGIN
  IF jsonb_typeof(candidate) = 'object' THEN
    FOR key_name, child IN SELECT key, value FROM jsonb_each(candidate) LOOP
      normalized_key := lower(regexp_replace(key_name, '[^a-z0-9]+', '', 'g'));
      IF normalized_key = 'key'
         OR normalized_key ~ '(authorization|apikey|apifootballkey|xapisportskey|header|cookie|token|password|secret|credential)'
         OR source.jsonb_contains_forbidden_metadata_key(child) THEN
        RETURN true;
      END IF;
    END LOOP;
  ELSIF jsonb_typeof(candidate) = 'array' THEN
    FOR child IN SELECT value FROM jsonb_array_elements(candidate) LOOP
      IF source.jsonb_contains_forbidden_metadata_key(child) THEN RETURN true; END IF;
    END LOOP;
  END IF;
  RETURN false;
END $$;

CREATE OR REPLACE FUNCTION source.guard_safe_request_params() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF source.jsonb_contains_forbidden_metadata_key(NEW.request_params) THEN
    RAISE EXCEPTION 'request_params must not contain credentials or headers' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER provider_fetches_safe_params_guard BEFORE INSERT OR UPDATE ON source.provider_fetches FOR EACH ROW EXECUTE FUNCTION source.guard_safe_request_params();

CREATE OR REPLACE FUNCTION source.guard_raw_payload_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'raw payload audit rows cannot be deleted; purge body bytes instead' USING ERRCODE = '55000';
  END IF;
  IF OLD.purged_at IS NOT NULL THEN
    RAISE EXCEPTION 'purged raw payload metadata is immutable' USING ERRCODE = '55000';
  END IF;
  IF NEW.fetch_id IS DISTINCT FROM OLD.fetch_id OR NEW.content_type IS DISTINCT FROM OLD.content_type
     OR NEW.content_encoding IS DISTINCT FROM OLD.content_encoding OR NEW.byte_count IS DISTINCT FROM OLD.byte_count
     OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'raw payload identity and content metadata are immutable' USING ERRCODE = '55000';
  END IF;
  IF NEW.purged_at IS NULL AND
     (NEW.inline_body IS DISTINCT FROM OLD.inline_body OR NEW.object_key IS DISTINCT FROM OLD.object_key) THEN
    RAISE EXCEPTION 'raw payload bytes/object cannot be replaced' USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER provider_raw_payloads_retention_guard
BEFORE UPDATE OR DELETE ON source.provider_raw_payloads
FOR EACH ROW EXECUTE FUNCTION source.guard_raw_payload_retention();

CREATE OR REPLACE FUNCTION source.guard_season_provider_ref() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE referenced_league_id bigint; season_league_id bigint;
BEGIN
  SELECT league_id INTO referenced_league_id
  FROM source.league_provider_refs
  WHERE provider_id = NEW.provider_id AND external_id = NEW.league_external_id;
  SELECT league_id INTO season_league_id FROM football.seasons WHERE id = NEW.season_id;
  IF referenced_league_id IS DISTINCT FROM season_league_id THEN
    RAISE EXCEPTION 'provider season reference must map to the same internal league' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER season_provider_refs_league_guard
BEFORE INSERT OR UPDATE ON source.season_provider_refs
FOR EACH ROW EXECUTE FUNCTION source.guard_season_provider_ref();

CREATE OR REPLACE FUNCTION source.guard_provider_fetch_subject() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE fixture_season_id bigint;
BEGIN
  IF TG_OP = 'UPDATE' AND (
    NEW.provider_id IS DISTINCT FROM OLD.provider_id OR NEW.endpoint IS DISTINCT FROM OLD.endpoint
    OR NEW.request_params IS DISTINCT FROM OLD.request_params OR NEW.request_params_sha256 IS DISTINCT FROM OLD.request_params_sha256
    OR NEW.purpose IS DISTINCT FROM OLD.purpose OR NEW.request_started_at IS DISTINCT FROM OLD.request_started_at
    OR NEW.subject_fixture_id IS DISTINCT FROM OLD.subject_fixture_id
    OR NEW.subject_season_id IS DISTINCT FROM OLD.subject_season_id
    OR NEW.subject_team_id IS DISTINCT FROM OLD.subject_team_id
  ) THEN
    RAISE EXCEPTION 'provider fetch request identity and typed subject are immutable' USING ERRCODE = '55000';
  END IF;
  IF NEW.subject_fixture_id IS NOT NULL THEN
    IF NOT EXISTS (SELECT 1 FROM source.fixture_provider_refs r WHERE r.provider_id = NEW.provider_id AND r.fixture_id = NEW.subject_fixture_id) THEN
      RAISE EXCEPTION 'fixture fetch subject must have a provider-consistent external reference' USING ERRCODE = '23514';
    END IF;
    SELECT season_id INTO fixture_season_id FROM football.fixtures WHERE id = NEW.subject_fixture_id;
    IF NEW.subject_season_id IS NOT NULL AND NEW.subject_season_id <> fixture_season_id THEN
      RAISE EXCEPTION 'fixture and season fetch subjects are inconsistent' USING ERRCODE = '23514';
    END IF;
  END IF;
  IF NEW.subject_season_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM source.season_provider_refs r WHERE r.provider_id = NEW.provider_id AND r.season_id = NEW.subject_season_id
  ) THEN
    RAISE EXCEPTION 'season fetch subject must have a provider-consistent external reference' USING ERRCODE = '23514';
  END IF;
  IF NEW.subject_team_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM source.team_provider_refs r WHERE r.provider_id = NEW.provider_id AND r.team_id = NEW.subject_team_id
  ) THEN
    RAISE EXCEPTION 'team fetch subject must have a provider-consistent external reference' USING ERRCODE = '23514';
  END IF;
  IF NEW.endpoint IN ('/fixtures/statistics', '/fixtures/lineups') AND NEW.subject_fixture_id IS NULL THEN
    RAISE EXCEPTION 'fixture-specific endpoint requires subject_fixture_id' USING ERRCODE = '23514';
  ELSIF NEW.endpoint = '/standings' AND NEW.subject_season_id IS NULL THEN
    RAISE EXCEPTION 'standings endpoint requires subject_season_id' USING ERRCODE = '23514';
  ELSIF NEW.endpoint = '/injuries' AND NEW.subject_fixture_id IS NULL AND NEW.subject_season_id IS NULL THEN
    RAISE EXCEPTION 'injuries endpoint requires fixture or season subject' USING ERRCODE = '23514';
  ELSIF NEW.endpoint = '/teams/statistics' AND (NEW.subject_team_id IS NULL OR NEW.subject_season_id IS NULL) THEN
    RAISE EXCEPTION 'team statistics endpoint requires team and season subjects' USING ERRCODE = '23514';
  ELSIF NEW.purpose = 'postmatch_reconciliation' AND (NEW.endpoint <> '/fixtures' OR NEW.subject_fixture_id IS NULL) THEN
    RAISE EXCEPTION 'post-match reconciliation requires a fixture-bound /fixtures fetch' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER provider_fetches_subject_guard
BEFORE INSERT OR UPDATE ON source.provider_fetches
FOR EACH ROW EXECUTE FUNCTION source.guard_provider_fetch_subject();

CREATE OR REPLACE FUNCTION ops.guard_fixture_reconciliation_state() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  fixture_kickoff timestamptz;
  fetch_purpose source.fetch_purpose;
  fetch_endpoint text;
  fetch_fixture_id bigint;
BEGIN
  IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'reconciliation state cannot be deleted' USING ERRCODE = '55000'; END IF;
  SELECT kickoff_at INTO fixture_kickoff FROM football.fixtures WHERE id = NEW.fixture_id;
  IF NEW.eligible_at < fixture_kickoff + interval '3 hours' THEN
    RAISE EXCEPTION 'post-match reconciliation cannot be eligible before kickoff plus three hours' USING ERRCODE = '23514';
  END IF;
  IF TG_OP = 'INSERT' THEN
    IF NEW.state <> 'waiting' OR NEW.attempt_count <> 0 OR NEW.last_attempt_at IS NOT NULL
       OR NEW.last_source_fetch_id IS NOT NULL OR NEW.terminal_observed_at IS NOT NULL OR NEW.completed_at IS NOT NULL THEN
      RAISE EXCEPTION 'new reconciliation state must start waiting with no attempts' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
  END IF;
  IF OLD.state IN ('completed', 'exhausted') AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'terminal reconciliation state is immutable' USING ERRCODE = '55000';
  END IF;
  IF NEW.fixture_id IS DISTINCT FROM OLD.fixture_id OR NEW.eligible_at IS DISTINCT FROM OLD.eligible_at
     OR NEW.max_attempts IS DISTINCT FROM OLD.max_attempts OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'reconciliation identity, eligibility and attempt budget are immutable' USING ERRCODE = '55000';
  END IF;
  IF NEW.attempt_count <> OLD.attempt_count + 1 OR NEW.last_source_fetch_id IS NULL
     OR NEW.last_source_fetch_id IS NOT DISTINCT FROM OLD.last_source_fetch_id OR NEW.last_attempt_at IS NULL THEN
    RAISE EXCEPTION 'each reconciliation update must record exactly one new fetch attempt' USING ERRCODE = '23514';
  END IF;
  SELECT purpose, endpoint, subject_fixture_id INTO fetch_purpose, fetch_endpoint, fetch_fixture_id
  FROM source.provider_fetches WHERE id = NEW.last_source_fetch_id;
  IF fetch_purpose IS DISTINCT FROM 'postmatch_reconciliation'::source.fetch_purpose
     OR fetch_endpoint IS DISTINCT FROM '/fixtures' OR fetch_fixture_id IS DISTINCT FROM NEW.fixture_id THEN
    RAISE EXCEPTION 'reconciliation attempt requires a postmatch_reconciliation fetch' USING ERRCODE = '23514';
  END IF;
  IF NEW.state = 'completed' THEN
    IF NEW.terminal_observed_at IS NULL OR NEW.completed_at IS NULL
       OR NOT EXISTS (SELECT 1 FROM football.fixtures f WHERE f.id = NEW.fixture_id AND f.lifecycle_state = 'completed' AND f.result_finalized_at IS NOT NULL) THEN
      RAISE EXCEPTION 'completed reconciliation requires an atomically finalized fixture result' USING ERRCODE = '23514';
    END IF;
  ELSIF NEW.terminal_observed_at IS NOT NULL OR NEW.completed_at IS NOT NULL THEN
    RAISE EXCEPTION 'non-completed reconciliation cannot claim terminal completion' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER fixture_reconciliation_state_guard
BEFORE INSERT OR UPDATE OR DELETE ON ops.fixture_reconciliation_state
FOR EACH ROW EXECUTE FUNCTION ops.guard_fixture_reconciliation_state();

CREATE OR REPLACE FUNCTION ops.record_fixture_reconciliation_attempt(
  p_fixture_id bigint,
  p_source_fetch_id bigint,
  p_next_attempt_at timestamptz
) RETURNS ops.fixture_reconciliation_state
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, source, football, ops AS $$
DECLARE
  current_state ops.fixture_reconciliation_state%ROWTYPE;
  fetch_row source.provider_fetches%ROWTYPE;
  attempt_at timestamptz;
BEGIN
  SELECT * INTO current_state FROM ops.fixture_reconciliation_state WHERE fixture_id = p_fixture_id FOR UPDATE;
  IF NOT FOUND OR current_state.state IN ('completed', 'exhausted') THEN
    RAISE EXCEPTION 'fixture has no mutable reconciliation state' USING ERRCODE = '55000';
  END IF;
  SELECT * INTO fetch_row FROM source.provider_fetches WHERE id = p_source_fetch_id;
  attempt_at := coalesce(fetch_row.response_received_at, fetch_row.request_started_at);
  IF fetch_row.purpose IS DISTINCT FROM 'postmatch_reconciliation'::source.fetch_purpose
     OR fetch_row.endpoint IS DISTINCT FROM '/fixtures'
     OR fetch_row.subject_fixture_id IS DISTINCT FROM p_fixture_id
     OR attempt_at < current_state.eligible_at THEN
    RAISE EXCEPTION 'fetch is not an eligible post-match reconciliation attempt' USING ERRCODE = '23514';
  END IF;
  IF current_state.attempt_count + 1 >= current_state.max_attempts THEN
    UPDATE ops.fixture_reconciliation_state
    SET attempt_count = attempt_count + 1, last_attempt_at = attempt_at,
        last_source_fetch_id = p_source_fetch_id, state = 'exhausted'
    WHERE fixture_id = p_fixture_id RETURNING * INTO current_state;
  ELSE
    IF p_next_attempt_at IS NULL OR p_next_attempt_at <= attempt_at THEN
      RAISE EXCEPTION 'next reconciliation attempt must be scheduled after this attempt' USING ERRCODE = '23514';
    END IF;
    UPDATE ops.fixture_reconciliation_state
    SET attempt_count = attempt_count + 1, last_attempt_at = attempt_at,
        last_source_fetch_id = p_source_fetch_id, next_attempt_at = p_next_attempt_at, state = 'waiting'
    WHERE fixture_id = p_fixture_id RETURNING * INTO current_state;
  END IF;
  RETURN current_state;
END $$;

CREATE OR REPLACE FUNCTION ops.finalize_fixture_result(
  p_fixture_id bigint,
  p_source_fetch_id bigint,
  p_home_goals smallint,
  p_away_goals smallint,
  p_home_halftime_goals smallint DEFAULT NULL,
  p_away_halftime_goals smallint DEFAULT NULL,
  p_home_fulltime_goals smallint DEFAULT NULL,
  p_away_fulltime_goals smallint DEFAULT NULL,
  p_home_extratime_goals smallint DEFAULT NULL,
  p_away_extratime_goals smallint DEFAULT NULL,
  p_home_penalty_goals smallint DEFAULT NULL,
  p_away_penalty_goals smallint DEFAULT NULL
) RETURNS ops.fixture_reconciliation_state
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, source, football, ops AS $$
DECLARE
  current_state ops.fixture_reconciliation_state%ROWTYPE;
  fetch_row source.provider_fetches%ROWTYPE;
  fixture_row football.fixtures%ROWTYPE;
BEGIN
  SELECT * INTO current_state FROM ops.fixture_reconciliation_state WHERE fixture_id = p_fixture_id FOR UPDATE;
  IF NOT FOUND OR current_state.state IN ('completed', 'exhausted') OR current_state.attempt_count >= current_state.max_attempts THEN
    RAISE EXCEPTION 'fixture has no available final reconciliation attempt' USING ERRCODE = '55000';
  END IF;
  SELECT * INTO fixture_row FROM football.fixtures WHERE id = p_fixture_id FOR UPDATE;
  SELECT * INTO fetch_row FROM source.provider_fetches WHERE id = p_source_fetch_id;
  IF fetch_row.purpose IS DISTINCT FROM 'postmatch_reconciliation'::source.fetch_purpose
     OR fetch_row.outcome IS DISTINCT FROM 'success'::source.fetch_outcome
     OR fetch_row.endpoint IS DISTINCT FROM '/fixtures'
     OR fetch_row.subject_fixture_id IS DISTINCT FROM p_fixture_id
     OR fetch_row.response_received_at IS NULL
     OR fetch_row.response_received_at < current_state.eligible_at
     OR fetch_row.request_started_at < current_state.eligible_at THEN
    RAISE EXCEPTION 'terminal result requires a successful eligible reconciliation fetch' USING ERRCODE = '23514';
  END IF;
  UPDATE football.fixtures
  SET lifecycle_state = 'completed', home_goals = p_home_goals, away_goals = p_away_goals,
      home_halftime_goals = p_home_halftime_goals, away_halftime_goals = p_away_halftime_goals,
      home_fulltime_goals = p_home_fulltime_goals, away_fulltime_goals = p_away_fulltime_goals,
      home_extratime_goals = p_home_extratime_goals, away_extratime_goals = p_away_extratime_goals,
      home_penalty_goals = p_home_penalty_goals, away_penalty_goals = p_away_penalty_goals,
      terminal_status_observed_at = fetch_row.response_received_at,
      result_available_at = fetch_row.response_received_at,
      availability_basis = 'observed', result_finalized_at = fetch_row.response_received_at,
      last_seen_at = greatest(last_seen_at, fetch_row.response_received_at),
      last_source_fetch_id = p_source_fetch_id
  WHERE id = p_fixture_id;
  UPDATE ops.fixture_reconciliation_state
  SET attempt_count = attempt_count + 1, last_attempt_at = fetch_row.response_received_at,
      last_source_fetch_id = p_source_fetch_id, terminal_observed_at = fetch_row.response_received_at,
      completed_at = fetch_row.response_received_at, state = 'completed'
  WHERE fixture_id = p_fixture_id RETURNING * INTO current_state;
  RETURN current_state;
END $$;

-- Base tables are private. RLS has no policies: anon/authenticated receive no rows or DML.
ALTER TABLE source.providers ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.league_provider_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.team_provider_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.venue_provider_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.player_provider_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.coach_provider_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.season_provider_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.fixture_provider_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.provider_fetches ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.provider_raw_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.leagues ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.seasons ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.teams ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.venues ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.players ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.coaches ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.season_teams ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.fixtures ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.fixture_team_statistics ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.fixture_availability_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.fixture_player_availability ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.fixture_lineup_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.fixture_lineups ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.fixture_lineup_players ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.standings_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.standings_snapshot_groups ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.standings_snapshot_rows ENABLE ROW LEVEL SECURITY;
ALTER TABLE ml.model_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE ml.predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE ml.prediction_feature_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE ml.prediction_fixture_inputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.fixture_reconciliation_state ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON SCHEMA source, football, ml, ops FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA source, football, ml, ops FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA source, football, ml, ops FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA source, football, ml, ops FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA source REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA football REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA ml REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA source REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA football REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA ml REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
DO $$
DECLARE role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format('REVOKE ALL ON SCHEMA source, football, ml, ops FROM %I', role_name);
      EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA source, football, ml, ops FROM %I', role_name);
      EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA source, football, ml, ops FROM %I', role_name);
      EXECUTE format('REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA source, football, ml, ops FROM %I', role_name);
    END IF;
  END LOOP;
END $$;
