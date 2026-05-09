#!/bin/bash
SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SERVICE_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
PID_FILE="$SERVICE_DIR/logs/sql_service.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "PID file not found"
    exit 1
fi

PID=$(cat "$PID_FILE")

if ! ps -p "$PID" >/dev/null 2>&1; then
    echo "Process $PID not found"
    rm -f "$PID_FILE"
    exit 1
fi

echo "Stopping Schemist ai_service_2 (PID: $PID)..."
kill "$PID"

WAIT=0
while ps -p "$PID" >/dev/null 2>&1 && [ $WAIT -lt 10 ]; do
    sleep 1
    WAIT=$((WAIT + 1))
done

if ps -p "$PID" >/dev/null 2>&1; then
    kill -9 "$PID"
    sleep 1
fi

echo "Service stopped"
rm -f "$PID_FILE"
