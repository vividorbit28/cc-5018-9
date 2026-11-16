#!/usr/bin/env bash
# Stop every process started by start_all.sh (source supervisors + children,
# relay, workers, encoders). PIDs are killed as whole process groups, then any
# stale instance matching this project is cleaned up by pattern.
set -uo pipefail
cd "$(dirname "$0")"

kill_tree() {
  local pid="$1"
  kill -TERM "$pid" 2>/dev/null || true
  # Also terminate direct children (supervised source, spawned workers...).
  pkill -TERM -P "$pid" 2>/dev/null || true
}

for f in run/*.pid; do
  [ -e "$f" ] || continue
  pid="$(cat "$f")"
  if kill -0 "$pid" 2>/dev/null; then
    kill_tree "$pid"
    echo "stopped $f ($pid)"
  fi
done
sleep 1
# Pattern-based sweep for anything left behind.
pkill -f "relay_live/source.py" 2>/dev/null || true
pkill -f "relay_live/relay.py" 2>/dev/null || true
pkill -f "relay_live/worker.py" 2>/dev/null || true
pkill -f "relay_live/bin/ffmpeg" 2>/dev/null || true
# Wait until the relay control port is actually free before returning.
for _ in $(seq 1 40); do
  if ! python3 - <<'PY'
import socket, sys
s = socket.socket()
try:
    s.connect(("127.0.0.1", 8080))
    sys.exit(0)
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
  then
    break
  fi
  sleep 0.25
done
rm -f run/*.pid
echo "all stopped"
