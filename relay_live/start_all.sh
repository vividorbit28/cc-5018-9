#!/usr/bin/env bash
# Bring up the full single-machine, multi-process demo:
#   - one supervised source process per region (ffmpeg encoder inside)
#   - one relay kernel process hosting every region + the control web page
# Everything logs under logs/, pids under run/.
set -euo pipefail
cd "$(dirname "$0")"

RELAY_PORT="${RELAY_PORT:-8080}"
RETAIN_SECONDS="${RETAIN_SECONDS:-10}"
mkdir -p logs run

port_busy() {
  python3 -I - "$1" <<'PY'
import socket, sys
s = socket.socket(); s.settimeout(0.4)
try:
    s.connect(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(1)
finally:
    s.close()
sys.exit(0)
PY
}

# Refuse to start on top of an existing listener. Otherwise the readiness
# probe below would succeed against a *different* (possibly stale or
# unoptimized) relay, and every before/after measurement would be garbage.
if port_busy "$RELAY_PORT"; then
  echo "port ${RELAY_PORT} already in use; run ./stop_all.sh first" >&2
  exit 1
fi

while read -r region port cport label; do
  [ -z "$region" ] && continue
  nohup python3 -I source.py --host 127.0.0.1 --port "$port" \
      --label "$label" --supervise \
      > "logs/source_${region}.log" 2>&1 &
  echo "$!" > "run/source_${region}.pid"
done < <(python3 -I - <<'PY'
import json
for name, s in json.load(open("config.json"))["regions"].items():
    print(name, s["port"], s["control_port"], s["label"])
PY
)

# Wait until every source control endpoint answers before starting the relay.
while read -r region port cport label; do
  [ -z "$cport" ] && continue
  for _ in $(seq 1 40); do
    if curl -s -o /dev/null "http://127.0.0.1:${cport}/stats"; then
      break
    fi
    sleep 0.25
  done
done < <(python3 -I - <<'PY'
import json
for name, s in json.load(open("config.json"))["regions"].items():
    print(name, s["port"], s["control_port"], s["label"])
PY
)

nohup python3 -I relay.py --port "$RELAY_PORT" --retain "$RETAIN_SECONDS" \
    > logs/relay.log 2>&1 &
echo "$!" > run/relay.pid

# Wait for OUR relay pid to answer. Checking the pid, not just the port,
# catches the case where another process owns the port.
RELAY_PID="$(cat run/relay.pid)"
ready=0
for _ in $(seq 1 40); do
  if kill -0 "$RELAY_PID" 2>/dev/null \
     && curl -s -o /dev/null "http://127.0.0.1:${RELAY_PORT}/api/config"; then
    ready=1
    break
  fi
  sleep 0.5
done
if [ "$ready" != 1 ]; then
  echo "relay (pid $RELAY_PID) failed to start; see logs/relay.log" >&2
  tail -5 logs/relay.log >&2 || true
  exit 1
fi

echo "source pids : $(cat run/source_*.pid | tr '\n' ' ')"
echo "relay pid   : $(cat run/relay.pid)"
echo "control page: http://127.0.0.1:${RELAY_PORT}/"
