#!/usr/bin/env bash
# End-to-end smoke test with two real uvicorn processes.
#
# Covers:
#   1. lost receipt (armed on submit) -> verified by command id, exactly one
#      execution at the gateway;
#   2. per-device FIFO: follower not delivered while the head is unresolved,
#      other devices unaffected;
#   3. duplicate submit -> original record, no new execution;
#   4. live "drop receipt" of an in-flight command;
#   5. gateway outage -> command stays pending, never lost;
#   6. restart of both services -> unfinished work resumes, nothing re-executed;
#   7. gateway killed mid-execution -> id frozen as unknown, web keeps it
#      unresolved and blocks that device; the id is never executed;
#   8. one device with a deep backlog does not starve later devices, submits
#      stay fast, and every backlog command runs exactly once.
#
# Expiry rules (unexecuted vs. still-verifying) are covered by pytest at
# second granularity; the web form only accepts whole minutes.
#
# Requirements: curl, python3 with the service dependencies installed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PYTHON:-python3}"
TMP="$(mktemp -d)"
GW_PORT="${GW_PORT:-18001}"
WEB_PORT="${WEB_PORT:-18000}"
DELAY="${DELAY:-1.2}"
GW_PID=""
WEB_PID=""

cleanup() {
  [ -n "$WEB_PID" ] && kill "$WEB_PID" 2>/dev/null || true
  [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

start_gateway() {
  ( cd "$ROOT" && GATEWAY_DB="$TMP/gateway.db" GATEWAY_EXECUTION_DELAY="$DELAY" \
      "$PY" -m uvicorn gateway.app.main:app --host 127.0.0.1 --port "$GW_PORT" \
      --log-level warning ) &
  GW_PID=$!
}
start_web() {
  ( cd "$ROOT" && OPS_DB="$TMP/ops.db" GATEWAY_URL="http://127.0.0.1:$GW_PORT" \
      "$PY" -m uvicorn web.app.main:app --host 127.0.0.1 --port "$WEB_PORT" \
      --log-level warning ) &
  WEB_PID=$!
}
stop_web() { kill "$WEB_PID" 2>/dev/null || true; wait "$WEB_PID" 2>/dev/null || true; WEB_PID=""; }
stop_gateway() { kill "$GW_PID" 2>/dev/null || true; wait "$GW_PID" 2>/dev/null || true; GW_PID=""; }
hard_kill_gateway() { kill -9 "$GW_PID" 2>/dev/null || true; wait "$GW_PID" 2>/dev/null || true; GW_PID=""; }

wait_http() {
  for _ in $(seq 1 150); do
    if curl -fsS "$1" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "service at $1 did not start" >&2
  return 1
}

field() { "$PY" -c "import json,sys; print(json.load(sys.stdin).get('$1'))"; }
web_state() { curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/$1" | field state; }
web_detail() { curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/$1" | field detail; }
wait_state() { # command expected [tries]
  for _ in $(seq 1 "${3:-200}"); do
    [ "$(web_state "$1")" = "$2" ] && return 0
    sleep 0.1
  done
  echo "timed out waiting for $1=$2 (got $(web_state "$1"))" >&2
  return 1
}
wait_detail() { # command substring [tries]
  for _ in $(seq 1 "${3:-200}"); do
    case "$(web_detail "$1")" in *"$2"*) return 0;; esac
    sleep 0.1
  done
  echo "timed out waiting for '$2' in $1 detail (got $(web_detail "$1"))" >&2
  return 1
}
submit() { # device command minutes [drop]
  curl -sS -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$WEB_PORT/commands" \
    --data-urlencode "device_id=$1" --data-urlencode "command_id=$2" \
    --data-urlencode "deadline_minutes=$3" ${4:+--data-urlencode "drop_receipt=$4"}
}
gw_get() { curl -sS "http://127.0.0.1:$GW_PORT/api/commands/$1"; }
gw_code() { curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:$GW_PORT/api/commands/$1"; }

start_gateway
start_web
wait_http "http://127.0.0.1:$WEB_PORT/healthz"
wait_http "http://127.0.0.1:$GW_PORT/healthz"
echo "== services up =="

echo "-- [1/2] lost receipt + FIFO"
submit device-A ok-1 5 true >/dev/null
submit device-A ok-2 5 >/dev/null
submit device-B ok-3 5 >/dev/null

sleep 0.3
echo -n "ok-2 at gateway while ok-1 unresolved: http="
gw_code ok-2
wait_state ok-1 executed
echo "ok-1: $(curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/ok-1")"
echo "ok-1 at gateway: $(gw_get ok-1)"

echo "-- [3] duplicate submit of ok-1"
submit device-A ok-1 5 >/dev/null
sleep 0.3
echo "ok-1 after duplicate: $(curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/ok-1")"

echo "-- [4] live drop-receipt of an in-flight command"
submit device-F live-1 5 >/dev/null
wait_state live-1 inflight 30
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  "http://127.0.0.1:$WEB_PORT/commands/live-1/drop-receipt")
echo "drop-receipt endpoint http=$code"
wait_state live-1 executed
echo "live-1: $(curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/live-1")"

wait_state ok-2 executed
wait_state ok-3 executed

echo "-- [8] one backlogged device does not starve later devices"
for i in $(seq 1 12); do submit device-Z "bk-$i" 10 >/dev/null; done
submit device-Y1 late-1 10 >/dev/null
submit device-Y2 late-2 10 >/dev/null
submit device-Y3 late-3 10 >/dev/null
# the web must keep accepting commands quickly while the backlog is deep
t=$(curl -sS -o /dev/null -w '%{time_total}' -X POST "http://127.0.0.1:$WEB_PORT/commands" \
  --data-urlencode "device_id=device-Y4" --data-urlencode "command_id=late-4" \
  --data-urlencode "deadline_minutes=10")
echo "submit latency under backlog: ${t}s"
wait_state late-1 executed 150
wait_state late-2 executed 150
wait_state late-3 executed 150
wait_state late-4 executed 150
z_pending=$(curl -sS "http://127.0.0.1:$WEB_PORT/api/commands?limit=500" | "$PY" -c \
  "import json,sys; rows=json.load(sys.stdin); print(sum(1 for r in rows if r['device_id']=='device-Z' and r['state']=='pending'))")
echo "device-Z still pending after later devices finished: $z_pending"
[ "$z_pending" -gt 0 ]
for i in $(seq 1 12); do wait_state "bk-$i" executed 400; done
echo "backlog drained; late devices were not starved"

echo "-- [7] gateway killed mid-execution"
submit device-K kill-1 5 >/dev/null
submit device-K kill-2 5 >/dev/null
for _ in $(seq 1 100); do
  [ "$(gw_code kill-1)" = "200" ] && break
  sleep 0.05
done
echo "kill-1 accepted by gateway: $(gw_get kill-1)"
hard_kill_gateway   # SIGKILL mid-execution: row stays 'running' in SQLite
wait_detail kill-1 "不明" 100
echo "kill-1 after gateway death: state=$(web_state kill-1) detail=$(web_detail kill-1)"
echo "kill-2 while kill-1 unresolved: state=$(web_state kill-2)"

start_gateway
wait_http "http://127.0.0.1:$GW_PORT/healthz"
wait_detail kill-1 "重启" 150
echo "kill-1 after gateway restart: state=$(web_state kill-1) detail=$(web_detail kill-1)"
echo "kill-1 at gateway after restart: $(gw_get kill-1)"
submit device-B kill-3 5 >/dev/null
wait_state kill-3 executed
echo "kill-3 (other device) ran while kill-1 is unresolved: $(gw_get kill-3)"
echo "kill-2 still blocked: state=$(web_state kill-2) gateway_http=$(gw_code kill-2)"

echo "-- [5] gateway down: submit stays pending, not lost"
stop_gateway
submit device-D resume-1 5 >/dev/null
sleep 1.5
echo "resume-1 while gateway down: $(web_state resume-1)"

echo "-- [6] restart both services"
stop_web
start_gateway
start_web
wait_http "http://127.0.0.1:$WEB_PORT/healthz"
wait_state resume-1 executed
echo "resume-1 after restart: $(curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/resume-1")"

sleep 0.5
echo "-- final states"
for c in ok-1 ok-2 ok-3 live-1 resume-1 kill-1 kill-2 kill-3 bk-1 bk-12 late-1 late-4; do
  printf '%s=%s ' "$c" "$(web_state "$c")"
done
echo
echo "-- gateway execution records"
curl -sS "http://127.0.0.1:$GW_PORT/api/executions"; echo

"$PY" - "$GW_PORT" <<'PYCHECK'
import json, sys, urllib.request
port = sys.argv[1]
rows = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/executions"))
by_id = {r["command_id"]: r for r in rows}

must_execute_once = (
    ["ok-1", "ok-2", "ok-3", "live-1", "resume-1", "kill-3"]
    + [f"bk-{i}" for i in range(1, 13)]
    + [f"late-{i}" for i in range(1, 5)]
)
for cid in must_execute_once:
    assert cid in by_id, f"{cid} missing at gateway"
    assert by_id[cid]["state"] == "executed", by_id[cid]
    assert by_id[cid]["executions"] == 1, by_id[cid]

assert by_id.get("kill-1", {}).get("state") == "unknown", by_id.get("kill-1")
assert by_id.get("kill-1", {}).get("executions", 0) == 0, by_id.get("kill-1")
assert "kill-2" not in by_id, "kill-2 must never reach the gateway while blocked"
print("executed exactly once:", ", ".join(must_execute_once))
print("kill-1 frozen as unknown, kill-2 never delivered")
PYCHECK

echo "== PASS =="
