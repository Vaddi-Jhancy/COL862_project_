*** LazyLog Replica System

A minimal distributed sequencer/log replication system inspired by LazyLog and Erwin, providing:

A single active leader at any time

Followers that maintain the same replicated log

ZooKeeper-based leader election & failover

A clean append-only log interface

This README explains the architecture, code flow, file structure, and execution steps.

*** 1. System Architecture (High-Level)
Core Components

Leader

Accepts client appends

Assigns sequence numbers

(Optional) replicates to followers

“Unsealed view” → accepts writes

Followers

Reject client appends

Maintain sealed view

Become leader if promoted by ZooKeeper

ZooKeeper (ZK)

Manages membership via ephemeral nodes

Elects leader via ephemeral-sequential nodes

Detects process failures automatically

Leader Election Logic

Each replica creates an ephemeral sequential node under /lazylog/election/

ZK assigns names like node-0000000042

The replica with the smallest numeric suffix becomes leader

If a leader dies, ZK deletes its node → next smallest becomes leader

*** 2. Code Flow Overview
1. Startup (sequencer_server.cc)

Connect to ZooKeeper

Register replica (/lazylog/replicas/replica-PORT)

Create ephemeral-sequential znode (/lazylog/election/node-XXXX)

Start background election loop

Start gRPC server for Append RPC

2. Election Loop (zk_helpers.cc)

Reads children under /lazylog/election

Determines smallest sequential znode

Calls:

become_leader() → unseal view

become_follower() → seal view

3. Handling Client Appends (sequencer_service_impl.cc)

Only leader accepts Append(record)

Followers reject requests

Leader returns sequence number

4. Failover

Leader dies → ZK detects session loss

ZK removes its ephemeral nodes

Next smallest sequential znode becomes new leader

*** 3. Project File Structure
LazyLog-Replica-System/
│
├── build/
│   ├── sequencer
│   └── append_client
│
├── src/
│   ├── sequencer_server.cc
│   ├── sequencer.cc
│   ├── zk_helpers.cc
│   ├── sequencer_service_impl.cc
│   └── utils.cc
│
├── proto/
│   ├── sequencer.proto
│   └── sequencer_internal.proto
│
├── logs/
│
├── run_complete.sh
└── failover_test.sh

*** 4. File Responsibilities
sequencer_server.cc

Starts replica

Connects & registers with ZK

Creates election znodes

Starts gRPC server

Launches election loop thread

sequencer.cc

Core sequencing logic

append() / sequence numbers

become_leader() / become_follower()

seal_view(), unseal_view()

zk_helpers.cc

Zookeeper connection helpers

Create ephemeral znodes

Create sequential znodes

List children

Parse sequence suffix

sequencer_service_impl.cc

Implements gRPC Append API

Called by external clients

Calls Sequencer::append()

run_complete.sh

Starts replicas

Detects leader

Sends client appends

failover_test.sh

Detects current leader through logs

Kills it

Waits for ZK to elect next leader

*** 5. Building the System
cmake -S . -B build
cmake --build build -j

*** 6. Running the System
Step 1 — Start everything + client appends
chmod +x run_complete.sh
./run_complete.sh


You will see:

Leader elected

Appends sent to leader

Logs created in logs/

Step 2 — Test Failover
chmod +x failover_test.sh
./failover_test.sh


You will see:

Current leader detected

Leader process killed

New leader elected by ZK
