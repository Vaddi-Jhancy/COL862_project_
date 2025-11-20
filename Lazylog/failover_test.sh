#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="logs"
PORTS=(50051 50052 50053)

# How long to wait for a new leader (seconds)
POST_KILL_TIMEOUT=50
POLL_INTERVAL=1

echo "====================================================="
echo "[FAILOVER TEST] Starting"
echo "Log directory: $LOG_DIR"
echo "Ports: ${PORTS[*]}"
echo "====================================================="

# helper: get last 'elected leader' entry info for each replica
# outputs lines: "<replica_idx>:<line_no>:<node_name>"
get_last_elected_entries() {
    for i in "${!PORTS[@]}"; do
        logfile="$LOG_DIR/replica_${i}.log"
        if [[ ! -f "$logfile" ]]; then
            # no log yet
            continue
        fi
        # find last matching line (with line number)
        last=$(grep -n "\[ELECTION\] elected leader" "$logfile" 2>/dev/null | tail -n 1 || true)
        if [[ -n "$last" ]]; then
            lineno=$(echo "$last" | cut -d: -f1)
            text=$(echo "$last" | cut -d: -f2-)
            # extract node-XXXXX (best effort)
            node=$(echo "$text" | grep -o -E "node-[0-9]+" || true)
            echo "${i}:${lineno}:${node}"
        fi
    done
}

# pick replica index that has the largest line number (most recent 'elected leader')
detect_current_leader() {
    best_idx=""
    best_ln=-1
    best_node=""
    while read -r entry; do
        # entry -> i:lineno:node
        IFS=':' read -r idx ln node <<< "$entry"
        # make sure numeric
        if [[ "$ln" =~ ^[0-9]+$ ]]; then
            if (( ln > best_ln )); then
                best_ln=$ln
                best_idx=$idx
                best_node=$node
            fi
        fi
    done < <(get_last_elected_entries)

    if [[ -z "$best_idx" ]]; then
        return 1
    fi
    echo "$best_idx" "$best_ln" "$best_node"
    return 0
}

echo "====================================================="
echo "[STEP] Waiting for '[ELECTION] elected leader' to appear in logs..."
echo "====================================================="

# Wait up to a few seconds for an initial election line if not present (short loop)
startup_wait=8
t=0
while ! detect_current_leader >/dev/null 2>&1 && (( t < startup_wait )); do
    sleep 1
    ((t++))
done

if ! detect_current_leader >/dev/null 2>&1; then
    echo "❌ ERROR: Could not find any '[ELECTION] elected leader' line in logs."
    echo "Check logs in $LOG_DIR/ for election progress."
    exit 1
fi

read leader_idx leader_ln leader_node < <(detect_current_leader)
leader_port=${PORTS[$leader_idx]}

echo ""
echo "====================================================="
echo "🔥 CURRENT LEADER: Replica $leader_idx (port $leader_port)"
echo "   detected node: ${leader_node:-<unknown>}   (log line ${leader_ln})"
echo "====================================================="

# find leader process PID robustly (match --port=PORT or --id=IDX)
pid=""
# prefer matching --port
pid=$(pgrep -f "sequencer.*--port=${leader_port}" || true)
if [[ -z "$pid" ]]; then
    pid=$(pgrep -f "sequencer.*--id=${leader_idx}" || true)
fi

if [[ -z "$pid" ]]; then
    echo "⚠️  Could not find process for leader using pgrep - trying ps+grep fallback..."
    pid=$(ps aux | grep "./sequencer" | grep -- "--port=${leader_port}" | awk '{print $2}' | head -n1 || true)
fi

if [[ -z "$pid" ]]; then
    echo "❌ ERROR: Leader process not found for replica $leader_idx (port $leader_port)."
    echo "Check 'ps aux | grep sequencer' and logs."
    exit 1
fi

echo "[KILL] Killing leader process $pid (replica $leader_idx / port $leader_port)..."
kill -9 "$pid" || true
echo "Leader process $pid killed."

# Wait small time for ZooKeeper session to be noticed / ephemeral znode to disappear
echo "[WAIT] Sleeping 2s to let ZooKeeper detect session loss..."
sleep 2

echo "====================================================="
echo "[STEP] Detecting new leader (will wait up to ${POST_KILL_TIMEOUT}s)..."
echo "====================================================="

start_ts=$(date +%s)
new_leader_idx=""
new_leader_ln=-1
new_leader_node=""

while true; do
    # get the most recent 'elected leader' across replicas
    if detect_current_leader >/dev/null 2>&1; then
        read cur_idx cur_ln cur_node < <(detect_current_leader)
        # if current leader index differs from killed leader => success
        if [[ "$cur_idx" != "$leader_idx" ]]; then
            new_leader_idx=$cur_idx
            new_leader_ln=$cur_ln
            new_leader_node=$cur_node
            break
        else
            # it's still the old leader line being the most recent — wait
            :
        fi
    fi

    # timeout check
    now_ts=$(date +%s)
    elapsed=$(( now_ts - start_ts ))
    if (( elapsed >= POST_KILL_TIMEOUT )); then
        break
    fi

    echo "[DEBUG] Poll attempt $((elapsed / POLL_INTERVAL + 1)) (post-failover)"
    sleep $POLL_INTERVAL
done

echo "====================================================="
if [[ -n "$new_leader_idx" ]]; then
    echo "🎉 NEW LEADER ELECTED: Replica $new_leader_idx (port ${PORTS[$new_leader_idx]})"
    echo "    node=${new_leader_node:-<unknown>}  (log line ${new_leader_ln})"
else
    echo "❌ ERROR: No new leader elected (within ${POST_KILL_TIMEOUT}s)."
    echo "Hints to debug:"
    echo "  - Check ZooKeeper election znodes: /lazylog/election (use zkCli.sh)"
    echo "  - Check processes: ps aux | grep sequencer"
    echo "  - Check logs for 'session'/'connected'/'Created election node' lines"
    echo "  - Maybe the killed process did not actually die or ephemeral znode still present"
fi
echo "====================================================="
