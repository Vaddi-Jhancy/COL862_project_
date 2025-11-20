#!/bin/bash

############################################
# Navigate to build directory
############################################
cd "$(dirname "$0")/build"

LOG_DIR="../logs"
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR/*"

############################################
# 🔥 CLEANUP
############################################
echo "[CLEAN] Killing old sequencer processes..."
pkill -f "./sequencer" 2>/dev/null
sleep 1

echo "[CLEAN] Clearing old logs..."
rm -f "$LOG_DIR/*"

PORTS=(50051 50052 50053)

############################################
# 1) START ALL REPLICAS AS FOLLOWERS
############################################
echo "[START] Starting 3 replicas (all as FOLLOWERS)..."

PIDS=()
i=0
for PORT in "${PORTS[@]}"; do
    echo "[START] Replica $i on port $PORT"
    stdbuf -i0 -o0 -e0 ./sequencer \
        --id=$i \
        --role=follower \
        --port=$PORT \
        > "$LOG_DIR/replica_$i.log" 2>&1 &

    PIDS+=($!)
    ((i++))
done

sleep 2   # Allow time for ZooKeeper to assign sequential IDs

############################################
# 2) DETECT LEADER BASED ON SMALLEST SEQ NODE
############################################
echo "[CHECK] Detecting elected leader..."

LEADER_PORT=""
leader_index=-1
min_seq=999999999

for i in "${!PORTS[@]}"; do
    PORT=${PORTS[$i]}
    LOGFILE="$LOG_DIR/replica_$i.log"

    # Read "My election node" line
    line=$(grep "My election node" "$LOGFILE" | tail -n1)
    if [[ -z "$line" ]]; then
        echo "[WARN] Replica $i has no election node yet."
        continue
    fi

    # Extract node-000000000X
    node_name=$(echo "$line" | sed -E 's#.*(node-[0-9]+).*#\1#')
    seq=$(echo "$node_name" | sed -E 's/node-0*//')

    echo "[DEBUG] Replica $i -> $node_name -> seq=$seq"

    # Find smallest sequence number
    if (( seq < min_seq )); then
        min_seq=$seq
        leader_index=$i
        LEADER_PORT=$PORT
    fi
done

if [[ -z "$LEADER_PORT" ]]; then
    echo "❌ ERROR: No leader could be detected!"
    echo "Check logs in $LOG_DIR/"
    exit 1
fi

echo "✅ Leader elected: replica $leader_index (port $LEADER_PORT) with sequence $min_seq"

############################################
# 3) SEND CLIENT APPENDS TO LEADER
############################################
RECORDS=("Hello" "World" "Test1" "Test2" "Test3" "Test4")

i=0
for rec in "${RECORDS[@]}"; do
    echo "[CLIENT] Sending: '$rec' to leader $LEADER_PORT"
    stdbuf -i0 -o0 -e0 ./append_client \
        --id=$((1000 + i)) \
        --record="$rec" \
        --server_addr=127.0.0.1:$LEADER_PORT \
        > "$LOG_DIR/client_$i.log" 2>&1

    ((i++))
    sleep 0.2
done

############################################
# 4) SUMMARY
############################################
echo ""
echo "[DONE] Logs stored in $LOG_DIR:"
ls -l "$LOG_DIR"

echo ""
echo "Replica PIDs: ${PIDS[@]}"
echo "Elected Leader: $LEADER_PORT"
echo ""
echo "To view logs:"
echo "  cat $LOG_DIR/replica_0.log"
echo "  cat $LOG_DIR/replica_1.log"
echo "  cat $LOG_DIR/replica_2.log"
echo "  cat $LOG_DIR/client_0.log"
