#!/usr/bin/env bash
# Start / stop the CloudStream control plane (dev mode).
set -e
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG=/tmp/cloudstream-server.log
PIDFILE=/tmp/cloudstream-server.pid

case "${1:-start}" in
  start)
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "already running (pid $(cat "$PIDFILE"))"
      exit 0
    fi
    nohup "$DIR/server/.venv/bin/uvicorn" app.main:app \
      --app-dir "$DIR/server" --port "${PORT:-8000}" > "$LOG" 2>&1 &
    echo $! > "$PIDFILE"
    sleep 1
    echo "control plane on http://127.0.0.1:${PORT:-8000} (log: $LOG)"
    ;;
  stop)
    if [ -f "$PIDFILE" ]; then kill "$(cat "$PIDFILE")" && rm -f "$PIDFILE"; fi
    echo "stopped"
    ;;
  *)
    echo "usage: $0 [start|stop]" >&2; exit 1 ;;
esac