#!/usr/bin/env bash
set -euo pipefail

# Airflow webserver entrypoint
# - Cleans up stale PID file before starting
# - Refuses to start a second webserver if a PID is already alive

AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"

# Prefer Airflow’s own PID setting if provided, otherwise default
PID_FILE="${AIRFLOW__WEBSERVER__WEB_SERVER_PID:-${AIRFLOW_HOME}/airflow-webserver.pid}"

echo "Using AIRFLOW_HOME=${AIRFLOW_HOME}"
echo "Webserver PID file: ${PID_FILE}"

if [ -f "$PID_FILE" ]; then
  echo "Found existing PID file at: $PID_FILE"

  # Optional paranoia check: is that PID actually alive?
  PID="$(cat "$PID_FILE" || true)"
  if [ -n "${PID:-}" ] && kill -0 "$PID" 2>/dev/null; then
    echo "PID $PID appears to be alive; refusing to start a second webserver."
    exit 1
  fi

  echo "PID file is stale; removing."
  rm -f "$PID_FILE"
fi

# start the webserver
exec airflow webserver
