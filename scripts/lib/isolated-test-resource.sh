#!/usr/bin/env bash

# Shared fail-closed guard for disposable database and Redis test runs.
# The caller creates a private runtime directory first, then calls
# fa_initialize_test_resource before the first client connection.

fa_initialize_test_resource() {
  local runtime_dir="$1"
  local postgres_socket_dir="$2"
  local postgres_port="$3"
  local redis_socket="$4"
  shift 4

  case "$runtime_dir" in
    /tmp/football-analytics-isolated-*) ;;
    *) printf 'unsafe test runtime directory: %s\n' "$runtime_dir" >&2; return 1 ;;
  esac
  [[ -d "$runtime_dir" && -d "$postgres_socket_dir" ]]
  [[ "$postgres_socket_dir" == "$runtime_dir"/* ]]
  [[ "$redis_socket" == "$runtime_dir"/* ]]
  [[ "$postgres_port" =~ ^[0-9]{4,5}$ ]] && (( postgres_port >= 1024 && postgres_port <= 65535 ))
  [[ "$#" -gt 0 ]]

  FA_TEST_RESOURCE_TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  FA_TEST_RESOURCE_MANIFEST="$runtime_dir/test-resource.json"
  export FA_TEST_RESOURCE_TOKEN FA_TEST_RESOURCE_MANIFEST
  printf '%s\n' "$FA_TEST_RESOURCE_TOKEN" >"$runtime_dir/.fa-test-resource"
  chmod 600 "$runtime_dir/.fa-test-resource"

  local database_json="" database
  for database in "$@"; do
    [[ "$database" =~ ^fa_iso_[a-z0-9_]+$ ]] || {
      printf 'unsafe test database name: %s\n' "$database" >&2
      return 1
    }
    database_json+="\"$database\","
  done
  database_json="${database_json%,}"
  printf '{"runtime_dir":"%s","token":"%s","postgres_socket":"%s","postgres_port":%s,"database_names":[%s],"redis_socket":"%s"}\n' \
    "$runtime_dir" "$FA_TEST_RESOURCE_TOKEN" "$postgres_socket_dir" "$postgres_port" "$database_json" "$redis_socket" \
    >"$FA_TEST_RESOURCE_MANIFEST"
  chmod 600 "$FA_TEST_RESOURCE_MANIFEST"

  FA_TEST_RESOURCE_RUNTIME="$runtime_dir"
  FA_TEST_RESOURCE_PG_SOCKET_DIR="$postgres_socket_dir"
  FA_TEST_RESOURCE_PG_PORT="$postgres_port"
  FA_TEST_RESOURCE_REDIS_SOCKET="$redis_socket"
  FA_TEST_RESOURCE_DATABASES=" $* "
  export FA_TEST_RESOURCE_RUNTIME FA_TEST_RESOURCE_PG_SOCKET_DIR FA_TEST_RESOURCE_PG_PORT
  export FA_TEST_RESOURCE_REDIS_SOCKET FA_TEST_RESOURCE_DATABASES
}

fa_assert_test_resource() {
  [[ -n "${FA_TEST_RESOURCE_RUNTIME:-}" ]]
  [[ -f "${FA_TEST_RESOURCE_RUNTIME}/.fa-test-resource" ]]
  [[ "$(<"${FA_TEST_RESOURCE_RUNTIME}/.fa-test-resource")" == "${FA_TEST_RESOURCE_TOKEN:-}" ]]
  [[ -f "${FA_TEST_RESOURCE_MANIFEST:-}" ]]
  [[ "${FA_TEST_RESOURCE_PG_SOCKET_DIR}" == "${FA_TEST_RESOURCE_RUNTIME}"/* ]]
  [[ "${FA_TEST_RESOURCE_REDIS_SOCKET}" == "${FA_TEST_RESOURCE_RUNTIME}"/* ]]
}

fa_assert_pg_database() {
  local database="$1"
  fa_assert_test_resource
  [[ " ${FA_TEST_RESOURCE_DATABASES} " == *" $database "* ]]
}

fa_assert_pg_control_connection() {
  fa_assert_test_resource
}

fa_pg_url() {
  local database="$1"
  fa_assert_pg_database "$database"
  printf 'postgresql://postgres@/%s?host=%s&port=%s' \
    "$database" "$FA_TEST_RESOURCE_PG_SOCKET_DIR" "$FA_TEST_RESOURCE_PG_PORT"
}

fa_assert_redis_url() {
  local url="$1"
  fa_assert_test_resource
  [[ "$url" == "unix://${FA_TEST_RESOURCE_REDIS_SOCKET}" ]]
}
