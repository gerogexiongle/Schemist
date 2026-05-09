#!/bin/bash
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SERVICE_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"

cd "$SERVICE_DIR" || exit 1
chmod +x start_service.sh stop_service.sh 2>/dev/null

get_latest_log() {
    ls -t "$SERVICE_DIR/logs/"sql_service_*.log 2>/dev/null | head -n 1
}

show_menu() {
    clear
    echo -e "${YELLOW}==========================================${NC}"
    echo -e "${YELLOW}  Schemist · ai_service_2${NC}"
    echo -e "${YELLOW}  DeepAgent + Skills | Spark + Trino${NC}"
    echo -e "${YELLOW}  SERVICE_DIR=${SERVICE_DIR}${NC}"
    echo -e "${YELLOW}==========================================${NC}"
    echo -e "${GREEN}1.${NC} Start service"
    echo -e "${RED}2.${NC} Stop service"
    echo -e "${BLUE}3.${NC} Service status"
    echo -e "${YELLOW}4.${NC} View live logs"
    echo -e "${YELLOW}5.${NC} Exit"
    echo
    echo -e "Select [1-5]: \c"
}

while true; do
    show_menu
    read -r opt
    case $opt in
    1) "$SERVICE_DIR/start_service.sh"; echo -e "${YELLOW}Press any key${NC}"; read -n 1;;
    2) "$SERVICE_DIR/stop_service.sh"; echo -e "${YELLOW}Press any key${NC}"; read -n 1;;
    3)
        PID_FILE="$SERVICE_DIR/logs/sql_service.pid"
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if ps -p "$PID" >/dev/null 2>&1; then
                echo -e "${GREEN}Running (PID: $PID)${NC}"
            else
                echo -e "${RED}Not running (stale PID)${NC}"
            fi
        else
            echo -e "${RED}Not running${NC}"
        fi
        echo -e "${YELLOW}Press any key${NC}"; read -n 1
        ;;
    4)
        LOG=$(get_latest_log)
        if [ -n "$LOG" ]; then
            echo -e "${BLUE}Viewing: $LOG${NC}"
            echo -e "${YELLOW}(Ctrl+C to exit)${NC}"
            tail -f "$LOG"
        else
            echo -e "${RED}No log files found${NC}"
            sleep 2
        fi
        ;;
    5) echo -e "${GREEN}Goodbye!${NC}"; exit 0;;
    *) echo -e "${RED}Invalid option${NC}"; sleep 1;;
    esac
done
