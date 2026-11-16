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

# Wait for the control port instead of racing a fixed sleep.
for _ in $(seq 1 40); do
  if curl -s -o /dev/null "http://127.0.0.1:${RELAY_PORT}/api/config"; then
    ready=1
    break
  fi
  sleep 0.5
done
[ "${ready:-0}" = 1 ] || { echo "relay failed to start; see logs/relay.log" >&2; exit 1; }

echo "source pids : $(cat run/source_*.pid | tr '\n' ' ')"
echo "relay pid   : $(cat run/relay.pid)"
echo "control page: http://127.0.0.1:${RELAY_PORT}/"
