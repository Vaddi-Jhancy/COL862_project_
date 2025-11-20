#!/bin/bash

############################################################
# 0) Navigate to build directory
############################################################
cd "$(dirname "$0")/build" || exit 1

LOG_DIR="../logs"
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*

############################################################
# 1) CLEANUP OLD PROCESSES & LOGS
############################################################
echo "====================================================="
echo "[CLEAN] Killing old sequencer processes..."
pkill -f "./sequencer" 2>/dev/null
sleep 1

echo "[CLEAN] Clearing old logs..."
rm -f "$LOG_DIR"/*

PORTS=(50051 50052 50053)

echo "====================================================="
echo "[STEP 1] Starting replicas (ALL as FOLLOWERS)..."
echo "====================================================="

############################################################
# 2) START ALL REPLICAS AS FOLLOWERS
############################################################
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

############################################################
# 3) WAIT FOR ZOOKEEPER ELECTION
############################################################
echo ""
echo "[WAIT] Allowing time for ZooKeeper leader election..."
sleep 5

############################################################
# 4) DETECT ELECTED LEADER BASED ON SMALLEST SEQ NODE
############################################################
echo "====================================================="
echo "[STEP 2] Detecting elected leader from logs..."
echo "====================================================="

LEADER_PORT=""
leader_index=-1
min_seq=999999999

for i in "${!PORTS[@]}"; do
    LOGFILE="$LOG_DIR/replica_$i.log"
    PORT=${PORTS[$i]}

    # Extract election node name
    line=$(grep "My election node:" "$LOGFILE" | tail -n1)
    if [[ -z "$line" ]]; then
        echo "[WARN] Replica $i produced no election node line!"
        continue
    fi

    # Extract node-XXXXXXXXXX
    node_name=$(echo "$line" | sed -E 's#.*(node-[0-9]+).*#\1#')
    seq=$(echo "$node_name" | sed -E 's/node-0*//')

    echo "[DEBUG] Replica $i -> $node_name -> seq=$seq"

    if (( seq < min_seq )); then
        min_seq=$seq
        leader_index=$i
        LEADER_PORT=$PORT
    fi
done

if [[ -z "$LEADER_PORT" ]]; then
    echo "❌ ERROR: Could not determine leader."
    echo "Check logs in $LOG_DIR"
    exit 1
fi

echo ""
echo "====================================================="
echo "✅ Leader elected: Replica $leader_index (port $LEADER_PORT)"
echo "====================================================="
echo ""

############################################################
# 5) SEND CLIENT APPENDS TO THE LEADER
############################################################
echo "[STEP 3] Sending client appends to elected leader ($LEADER_PORT)..."

RECORDS=("Hello" "World" "Test1" "Test2" "Test3")

i=0
for rec in "${RECORDS[@]}"; do
    echo "[CLIENT] Sending '$rec' to leader $LEADER_PORT"
    stdbuf -i0 -o0 -e0 ./append_client \
        --id=$((2000 + i)) \
        --record="$rec" \
        --server_addr=127.0.0.1:$LEADER_PORT \
        > "$LOG_DIR/client_$i.log" 2>&1
    ((i++))
    sleep 0.2
done

############################################################
# 6) SUMMARY OUTPUT
############################################################
echo ""
echo "====================================================="
echo "[DONE] Execution complete."
echo "-----------------------------------------------------"
echo "Replica PIDs: ${PIDS[@]}"
echo "Leader was: $LEADER_PORT"
echo ""
echo "Logs stored in $LOG_DIR"
echo "-----------------------------------------------------"
echo "Useful files:"
echo "  $LOG_DIR/replica_0.log"
echo "  $LOG_DIR/replica_1.log"
echo "  $LOG_DIR/replica_2.log"
echo "  $LOG_DIR/client_0.log"
echo "====================================================="
