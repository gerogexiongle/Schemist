#!/bin/bash

# Schemist · ai_service_2 — start script (paths relative to this file)

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SERVICE_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"

if [ ! -d "$SERVICE_DIR" ]; then
    echo "Error: service dir $SERVICE_DIR does not exist"
    exit 1
fi

# Temporary SQL/CSV：与 Python SCHEMIST_WORKSPACE_DIR 对齐；未设则落在 ai_service_2 下
WORKSPACE_DIR="${SCHEMIST_WORKSPACE_DIR:-${SQL_AI_WORKSPACE_DIR:-$SERVICE_DIR}}"
mkdir -p "$WORKSPACE_DIR/temp_sql_v2/csv"

LOG_DIR="$SERVICE_DIR/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/sql_service_$TIMESTAMP.log"
PID_FILE="$LOG_DIR/sql_service.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" >/dev/null 2>&1; then
        echo "Service already running (PID: $PID)"
        echo "Run $SERVICE_DIR/stop_service.sh first"
        exit 1
    else
        echo "Stale PID file found, will overwrite"
    fi
fi

echo "Starting Schemist ai_service_2 (DeepAgent + Skills)..."
echo "Log: $LOG_FILE"

# Conda base（非交互 shell 需先 source conda.sh）
if [ -f "${CONDA_PREFIX:-}/etc/profile.d/conda.sh" ]; then
    source "${CONDA_PREFIX}/etc/profile.d/conda.sh"
elif [ -f "/minibdp/apps/anaconda3/etc/profile.d/conda.sh" ]; then
    source "/minibdp/apps/anaconda3/etc/profile.d/conda.sh"
fi
if command -v conda >/dev/null 2>&1; then
    conda activate base
fi

cd "$SERVICE_DIR" || exit 1

if [ -n "${CONDA_PREFIX}" ] && [ -x "${CONDA_PREFIX}/bin/python" ]; then
    PY="${CONDA_PREFIX}/bin/python"
else
    PY="python"
fi
nohup "$PY" -u app.py >"$LOG_FILE" 2>&1 &

PID=$!
echo "$PID" >"$PID_FILE"

echo "Service started! (PID: $PID)"
echo "View logs: tail -f $LOG_FILE"
echo "Stop: $SERVICE_DIR/stop_service.sh"
