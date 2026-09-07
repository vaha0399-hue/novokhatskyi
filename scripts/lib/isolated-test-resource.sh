#!/usr/bin/env bash

# Shared fail-closed guard for disposable database and Redis test runs.
# The caller creates a private runtime directory first, then calls
# fa_initialize_test_resource before the first client connection.

_fa_fail() {
  printf '%s\n' "$1" >&2
  return 1
}

fa_assert_clean_libpq_environment() {
  local variable
  for variable in PGHOST PGHOSTADDR PGPORT PGDATABASE PGUSER PGPASSWORD PGPASSFILE PGSERVICE PGSERVICEFILE PGOPTIONS; do
    if [[ -n "${!variable-}" ]]; then
      _fa_fail "unsafe inherited libpq setting: $variable"
      return 1
    fi
  done
  return 0
}

fa_initialize_test_resource() {
  if (( $# < 5 )); then
    _fa_fail "isolated test resource requires runtime, PostgreSQL socket, port, Redis socket, and database names"
    return 1
  fi

  local runtime_dir="$1" postgres_socket_dir="$2" postgres_port="$3" redis_socket="$4"
  shift 4

  case "$runtime_dir" in
    /tmp/football-analytics-*) ;;
    *) _fa_fail "unsafe test runtime directory: $runtime_dir"; return 1 ;;
  esac
  if ! fa_assert_clean_libpq_environment; then return 1; fi
  if [[ ! -d "$runtime_dir" || ! -d "$postgres_socket_dir" ]]; then
    _fa_fail "test runtime and PostgreSQL socket directory must exist"
    return 1
  fi
  if [[ "$postgres_socket_dir" != "$runtime_dir"/* || "$redis_socket" != "$runtime_dir"/* ]]; then
    _fa_fail "test sockets must be inside the test runtime"
    return 1
  fi
  if [[ ! "$postgres_port" =~ ^[0-9]{4,5}$ ]] || (( postgres_port < 1024 || postgres_port > 65535 )); then
    _fa_fail "unsafe PostgreSQL test port: $postgres_port"
    return 1
  fi

  local database database_json=""
  for database in "$@"; do
    if [[ ! "$database" =~ ^fa_[a-z0-9_]+$ ]]; then
      _fa_fail "unsafe test database name: $database"
      return 1
    fi
    database_json+="\"$database\","
  done
  database_json="${database_json%,}"

  local token manifest manifest_digest manifest_digest_line manifest_digest_file
  if ! token="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"; then return 1; fi
  if [[ ! "$token" =~ ^[a-f0-9]{32}$ ]]; then
    _fa_fail "unable to create isolated test resource token"
    return 1
  fi
  manifest="$runtime_dir/test-resource.json"
  if ! printf '%s\n' "$token" >"$runtime_dir/.fa-test-resource"; then return 1; fi
  if ! chmod 600 "$runtime_dir/.fa-test-resource"; then return 1; fi
  if ! printf '{"runtime_dir":"%s","token":"%s","postgres_socket":"%s","postgres_port":%s,"database_names":[%s],"redis_socket":"%s"}\n' \
    "$runtime_dir" "$token" "$postgres_socket_dir" "$postgres_port" "$database_json" "$redis_socket" \
    >"$manifest"; then
    return 1
  fi
  if ! chmod 600 "$manifest"; then return 1; fi
  if ! manifest_digest_line="$(sha256sum -- "$manifest")"; then
    _fa_fail "unable to calculate isolated test resource manifest digest"
    return 1
  fi
  manifest_digest="${manifest_digest_line%% *}"
  if [[ ! "$manifest_digest" =~ ^[a-f0-9]{64}$ ]]; then
    _fa_fail "isolated test resource manifest digest is invalid"
    return 1
  fi
  manifest_digest_file="$runtime_dir/.fa-test-resource-manifest.sha256"
  if ! printf '%s\n' "$manifest_digest" >"$manifest_digest_file"; then return 1; fi
  if ! chmod 600 "$manifest_digest_file"; then return 1; fi

  FA_TEST_RESOURCE_TOKEN="$token"
  FA_TEST_RESOURCE_MANIFEST="$manifest"
  FA_TEST_RESOURCE_RUNTIME="$runtime_dir"
  FA_TEST_RESOURCE_PG_SOCKET_DIR="$postgres_socket_dir"
  FA_TEST_RESOURCE_PG_PORT="$postgres_port"
  FA_TEST_RESOURCE_REDIS_SOCKET="$redis_socket"
  FA_TEST_RESOURCE_DATABASES=" $* "
  export FA_TEST_RESOURCE_TOKEN FA_TEST_RESOURCE_MANIFEST FA_TEST_RESOURCE_RUNTIME
  export FA_TEST_RESOURCE_PG_SOCKET_DIR FA_TEST_RESOURCE_PG_PORT
  export FA_TEST_RESOURCE_REDIS_SOCKET FA_TEST_RESOURCE_DATABASES
  return 0
}

fa_assert_test_resource() {
  local runtime_dir="${FA_TEST_RESOURCE_RUNTIME:-}" marker="${FA_TEST_RESOURCE_TOKEN:-}"
  local manifest_digest manifest_digest_line actual_digest
  local manifest_digest_file
  if [[ -z "$runtime_dir" || -z "$marker" ]]; then
    _fa_fail "isolated test resource is not initialized"
    return 1
  fi
  case "$runtime_dir" in
    /tmp/football-analytics-*) ;;
    *) _fa_fail "unsafe test runtime directory: $runtime_dir"; return 1 ;;
  esac
  if [[ ! -f "$runtime_dir/.fa-test-resource" || "$(<"$runtime_dir/.fa-test-resource")" != "$marker" ]]; then
    _fa_fail "isolated test resource ownership marker is missing or invalid"
    return 1
  fi
  if [[ ! -f "${FA_TEST_RESOURCE_MANIFEST:-}" || "${FA_TEST_RESOURCE_MANIFEST:-}" != "$runtime_dir/test-resource.json" ]]; then
    _fa_fail "isolated test resource manifest is missing or invalid"
    return 1
  fi
  manifest_digest_file="$runtime_dir/.fa-test-resource-manifest.sha256"
  if [[ ! -f "$manifest_digest_file" ]]; then
    _fa_fail "isolated test resource manifest digest is missing or invalid"
    return 1
  fi
  manifest_digest="$(<"$manifest_digest_file")"
  if [[ ! "$manifest_digest" =~ ^[a-f0-9]{64}$ ]]; then
    _fa_fail "isolated test resource manifest digest is missing or invalid"
    return 1
  fi
  if ! manifest_digest_line="$(sha256sum -- "$FA_TEST_RESOURCE_MANIFEST")"; then
    _fa_fail "unable to calculate isolated test resource manifest digest"
    return 1
  fi
  actual_digest="${manifest_digest_line%% *}"
  if [[ ! "$actual_digest" =~ ^[a-f0-9]{64}$ || "$actual_digest" != "$manifest_digest" ]]; then
    _fa_fail "isolated test resource manifest digest does not match"
    return 1
  fi
  if [[ "${FA_TEST_RESOURCE_PG_SOCKET_DIR:-}" != "$runtime_dir"/* || "${FA_TEST_RESOURCE_REDIS_SOCKET:-}" != "$runtime_dir"/* ]]; then
    _fa_fail "isolated test resource sockets are outside the runtime"
    return 1
  fi
  return 0
}

fa_assert_cleanup_resource() {
  if ! fa_assert_test_resource; then return 1; fi
  return 0
}

fa_assert_pg_database() {
  local database="${1:-}"
  if ! fa_assert_test_resource; then return 1; fi
  if [[ " ${FA_TEST_RESOURCE_DATABASES:-} " != *" $database "* ]]; then
    _fa_fail "database is not owned by this isolated test run: $database"
    return 1
  fi
  return 0
}

fa_assert_pg_control_connection() {
  if ! fa_assert_test_resource; then return 1; fi
  if ! fa_assert_clean_libpq_environment; then return 1; fi
  return 0
}

fa_pg_url() {
  local database="${1:-}"
  if ! fa_assert_pg_database "$database"; then return 1; fi
  printf 'postgresql://postgres@/%s?host=%s&port=%s' \
    "$database" "$FA_TEST_RESOURCE_PG_SOCKET_DIR" "$FA_TEST_RESOURCE_PG_PORT"
  return 0
}

fa_assert_redis_url() {
  local url="${1:-}"
  if ! fa_assert_test_resource; then return 1; fi
  if [[ "$url" != "unix://${FA_TEST_RESOURCE_REDIS_SOCKET}" ]]; then
    _fa_fail "Redis URL is not owned by this isolated test run"
    return 1
  fi
  return 0
}

fa_pg_client() {
  if ! fa_assert_pg_control_connection; then return 1; fi
  env -u PGHOST -u PGHOSTADDR -u PGPORT -u PGDATABASE -u PGUSER -u PGPASSWORD \
    -u PGPASSFILE -u PGSERVICE -u PGSERVICEFILE -u PGOPTIONS "$@"
}

fa_redis_client() {
  if ! fa_assert_test_resource; then return 1; fi
  env -u REDIS_URL -u REDISCLI_AUTH "$@"
}
