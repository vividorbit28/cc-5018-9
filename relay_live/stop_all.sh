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
# Pattern-based sweep for anything left behind, including a copy of the demo
# started from another checkout path (benchmarks run HEAD in a worktree, so a
# workspace-only pattern would leave that relay holding the port).
pkill -f "source.py --host 127.0.0.1 --port" 2>/dev/null || true
pkill -f "relay.py --port" 2>/dev/null || true
pkill -f "worker.py --relay" 2>/dev/null || true
pkill -f "bin/ffmpeg" 2>/dev/null || true
# Anything still alive after a TERM gets one KILL, so a restart never inherits
# a half-dead listener that makes the next run's numbers meaningless.
sleep 1
for pat in "source.py --host 127.0.0.1 --port" "relay.py --port" \
           "worker.py --relay" "bin/ffmpeg"; do
  pkill -KILL -f "$pat" 2>/dev/null || true
done
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
if python3 - <<'PY'
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
  echo "warning: 8080 still in use after stop_all.sh" >&2
  exit 1
fi
echo "all stopped"
